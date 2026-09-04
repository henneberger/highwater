use crate::*;

pub(crate) fn maintain_event_time(app: &AppState) -> Result<()> {
    app.commit(|transaction| {
        let configs: Vec<StreamConfig> = transaction
            .scan::<Value>("stream/")?
            .into_iter()
            .filter(|(key, _)| key.ends_with("/config"))
            .map(|(_, config)| serde_json::from_value(config))
            .collect::<std::result::Result<_, _>>()?;
        let timestamp = now();
        for config in configs {
            let mut state = transaction
                .get::<StreamState>(&stream_state_key(&config.name))?
                .ok_or_else(|| anyhow!("stream state missing: {}", config.name))?;
            let mut partitions = load_stream_partitions(transaction, &config)?;
            refresh_stream(transaction, &config, &mut state, &mut partitions, timestamp)?;
        }
        refresh_declarative_operators(transaction, None)?;
        drain_operator_edges(transaction, app)
    })
}

pub(crate) fn promote_process_outputs(app: &AppState, selected_sink: Option<&str>) -> Result<()> {
    app.store.sync_all_remote()?;
    let pending = app
        .store
        .scan::<PendingProcessOutput>(pending_process_output_prefix())?
        .into_iter()
        .filter(|(_, pending)| selected_sink.is_none_or(|sink| pending.message.sink == sink))
        .collect::<Vec<_>>();
    if pending.is_empty() {
        return Ok(());
    }
    app.commit_output(0, |transaction| {
        for (_, pending) in &pending {
            let key = outbox_key(&pending.message.sink, &pending.message.message_id);
            if let Some(existing) = transaction.get::<OutboxMessage>(&key)? {
                if existing.message_id != pending.message.message_id
                    || existing.payload != pending.message.payload
                {
                    bail!("process output promotion identity collision");
                }
            } else {
                transaction.put(key, &pending.message)?;
            }
        }
        Ok(())
    })?;

    // The source-shard marker is intentionally immutable. Deleting it would
    // require a published checkpoint vector that covers both this marker and
    // the control-shard outbox insertion; otherwise a cross-shard snapshot
    // could observe neither side of the handoff.
    Ok(())
}

pub(crate) fn recover_process_tasks(app: &AppState, recover_orphans: bool) -> Result<()> {
    let timestamp = now();
    let mut expired_by_shard = BTreeMap::<usize, Vec<(String, ProcessBatchLease)>>::new();
    for (lease_key, lease) in app
        .store
        .scan::<ProcessBatchLease>(process_batch_lease_prefix())?
    {
        let owner_epoch = app
            .store
            .get::<ProcessPartitionOwner>(&process_partition_owner_key(lease.shard as usize))?
            .map(|owner| owner.epoch);
        if lease.lease_expires <= timestamp
            || recover_orphans && owner_epoch != Some(lease.owner_epoch)
        {
            expired_by_shard
                .entry(lease.shard as usize)
                .or_default()
                .push((lease_key, lease));
        }
    }
    for (shard, leases) in expired_by_shard {
        app.commit_shard(shard, |transaction| {
            let owner = transaction
                .get::<ProcessPartitionOwner>(&process_partition_owner_key(shard))?
                .ok_or_else(|| anyhow!("process partition {shard} is unassigned"))?;
            if owner.owner != app.runtime_id
                || owner.status != "ACTIVE"
                || owner.lease_expires <= now()
            {
                return Ok(());
            }
            for (lease_key, _) in leases {
                let Some(lease) = transaction.get::<ProcessBatchLease>(&lease_key)? else {
                    continue;
                };
                let orphaned = owner.epoch != lease.owner_epoch;
                if lease.lease_expires > now() && !(recover_orphans && orphaned) {
                    continue;
                }
                let state_key = process_shard_state_key(&lease.process_id, shard);
                let mut shard_state = transaction
                    .get::<ProcessShardState>(&state_key)?
                    .unwrap_or_default();
                let process = transaction
                    .get::<DurableProcess>(&process_key(&lease.process_id))?
                    .ok_or_else(|| anyhow!("process not found: {}", lease.process_id))?;
                let failure = if orphaned {
                    "activation lost when its partition owner was fenced"
                } else {
                    "activation lease expired before completion"
                };
                for ready_execution in lease.executions {
                    retry_or_quarantine_sharded_execution(
                        transaction,
                        &process,
                        shard,
                        ready_execution,
                        failure.to_owned(),
                        &mut shard_state,
                    )?;
                }
                let data_shards = app.shard_locks.len().saturating_sub(1).max(1);
                dispatch_sharded_process(
                    transaction,
                    &process,
                    shard,
                    data_shards,
                    &mut shard_state,
                )?;
                transaction.put(&state_key, &shard_state)?;
                transaction.delete(lease_key);
            }
            Ok(())
        })?;
    }
    Ok(())
}

pub(crate) async fn event_time_maintenance_loop(app: AppState) {
    let mut interval = tokio::time::interval(std::time::Duration::from_millis(250));
    let mut ticks = 0_u32;
    loop {
        interval.tick().await;
        ticks = ticks.wrapping_add(1);
        if app.control_plane {
            if let Err(error) = maintain_event_time(&app) {
                eprintln!("event-time maintenance failed: {error:#}");
            }
            if let Err(error) = renew_key_groups(&app) {
                eprintln!("key-group lease renewal failed: {error:#}");
            }
        }
        if let Err(error) = initialize_process_partitions(&app) {
            eprintln!("process partition acquisition failed: {error:#}");
        }
        if let Err(error) = renew_process_partitions(&app) {
            eprintln!("process partition renewal failed: {error:#}");
        }
        if let Err(error) = promote_process_outputs(&app, None) {
            eprintln!("process output promotion failed: {error:#}");
        }
        let has_remote_owner = app
            .store
            .scan::<KeyGroupLease>("key-group/")
            .is_ok_and(|leases| {
                leases
                    .into_iter()
                    .any(|(_, lease)| lease.lease_expires > now() && lease.owner != app.node_id)
            });
        if app.control_plane
            && !has_remote_owner
            && let Err(error) = app.store.checkpoint_if_needed(512)
        {
            eprintln!("checkpoint maintenance failed: {error:#}");
        }
        if ticks.is_multiple_of(20)
            && let Err(error) = recover_process_tasks(&app, false)
        {
            eprintln!("process task recovery failed: {error:#}");
        }
    }
}

pub(crate) async fn create_checkpoint(
    State(app): State<AppState>,
) -> Result<impl IntoResponse, ApiError> {
    Ok((StatusCode::CREATED, Json(app.store.checkpoint()?)))
}

pub(crate) async fn create_checkpoint_barrier(
    State(app): State<AppState>,
) -> Result<(StatusCode, Json<CheckpointBarrier>), ApiError> {
    let _guard = app
        .mutation_lock
        .lock()
        .map_err(|_| anyhow!("mutation lock poisoned"))?;
    // Every node can rebuild every shard from the authoritative object journal.
    // prepare_checkpoint synchronizes those tails and snapshots the resulting
    // vector cut, so owner-local state handles and remote acknowledgements are
    // neither required nor part of the restore protocol.
    let manifest = app.store.prepare_checkpoint()?;
    app.store.publish_checkpoint(&manifest)?;
    let barrier = CheckpointBarrier {
        checkpoint_id: manifest.checkpoint_id.clone(),
        sequence: manifest.sequence,
        status: "COMPLETE".to_owned(),
        expected_nodes: Vec::new(),
        expected_key_group_epochs: BTreeMap::new(),
        acknowledgements: BTreeMap::new(),
        manifest,
        created_at: now(),
    };
    let mutation = Mutation {
        op: "put".to_owned(),
        key: checkpoint_barrier_key(&barrier.checkpoint_id),
        end_key: None,
        value: Some(serde_json::to_value(&barrier)?),
        encoded_value: None,
    };
    let mut committed = false;
    for _ in 0..8 {
        app.store.sync_remote_shard(0)?;
        match app.store.commit(vec![mutation.clone()]) {
            Ok(()) => {
                committed = true;
                break;
            }
            Err(error) if error.to_string().contains("conditional append was fenced") => {}
            Err(error) => return Err(ApiError(error)),
        }
    }
    if !committed {
        return Err(ApiError(anyhow!(
            "checkpoint {} was published, but its compatibility record remained fenced after retries",
            barrier.checkpoint_id
        )));
    }
    Ok((StatusCode::CREATED, Json(barrier)))
}

pub(crate) async fn get_checkpoint_barrier(
    State(app): State<AppState>,
    Path(checkpoint_id): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    Ok(Json(
        app.store
            .get::<CheckpointBarrier>(&checkpoint_barrier_key(&checkpoint_id))?
            .ok_or_else(|| anyhow!("checkpoint barrier not found: {checkpoint_id}"))?,
    ))
}

pub(crate) async fn pending_checkpoint_barriers(
    State(app): State<AppState>,
    Path(node_id): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let barriers = app
        .store
        .scan::<CheckpointBarrier>("checkpoint-barrier/")?
        .into_iter()
        .map(|(_, barrier)| barrier)
        .filter(|barrier| {
            barrier.status == "ALIGNING"
                && barrier.expected_nodes.contains(&node_id)
                && !barrier.acknowledgements.contains_key(&node_id)
        })
        .collect::<Vec<_>>();
    Ok(Json(barriers))
}

pub(crate) async fn acknowledge_checkpoint_barrier(
    State(app): State<AppState>,
    Path((checkpoint_id, node_id)): Path<(String, String)>,
    Json(_request): Json<AcknowledgeCheckpointRequest>,
) -> Result<Json<CheckpointBarrier>, ApiError> {
    let _guard = app
        .mutation_lock
        .lock()
        .map_err(|_| anyhow!("mutation lock poisoned"))?;
    let key = checkpoint_barrier_key(&checkpoint_id);
    let _barrier = app
        .store
        .get::<CheckpointBarrier>(&key)?
        .ok_or_else(|| anyhow!("checkpoint barrier not found: {checkpoint_id}"))?;
    Err(ApiError(anyhow!(
        "checkpoint {checkpoint_id} uses journal-vector publication; remote acknowledgement from {node_id} is not accepted"
    )))
}

pub(crate) async fn get_checkpoint_manifest(
    State(app): State<AppState>,
) -> Result<impl IntoResponse, ApiError> {
    let manifest = DurableStore::read_manifest(&app.store.manifest_path)?
        .ok_or_else(|| anyhow!("no checkpoint has been created"))?;
    Ok(Json(manifest))
}

pub(crate) async fn list_key_groups(
    State(app): State<AppState>,
) -> Result<impl IntoResponse, ApiError> {
    let mut leases: Vec<KeyGroupLease> = app
        .store
        .scan::<KeyGroupLease>("key-group/")?
        .into_iter()
        .map(|(_, lease)| lease)
        .collect();
    leases.sort_by_key(|lease| lease.key_group);
    Ok(Json(leases))
}

pub(crate) async fn list_process_partitions(
    State(app): State<AppState>,
) -> Result<impl IntoResponse, ApiError> {
    app.store.sync_all_remote()?;
    let mut owners = app
        .store
        .scan::<ProcessPartitionOwner>("process-partition-owner/")?
        .into_iter()
        .map(|(_, owner)| owner)
        .collect::<Vec<_>>();
    owners.sort_by_key(|owner| owner.partition_id);
    Ok(Json(owners))
}

pub(crate) async fn transfer_process_partition(
    State(app): State<AppState>,
    Path(partition): Path<u32>,
    Json(request): Json<TransferProcessPartitionRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let shard = partition as usize;
    if shard == 0 || shard >= app.shard_locks.len() {
        return Err(ApiError(anyhow!(
            "process partition {partition} does not exist"
        )));
    }
    if request.target_node.trim().is_empty() || request.target_endpoint.trim().is_empty() {
        return Err(ApiError(anyhow!(
            "target_node and target_endpoint must not be empty"
        )));
    }
    app.commit_shard(shard, |transaction| {
        let key = process_partition_owner_key(shard);
        let mut owner = transaction
            .get::<ProcessPartitionOwner>(&key)?
            .ok_or_else(|| anyhow!("process partition {partition} is unassigned"))?;
        if owner.owner != app.runtime_id
            || owner.epoch != request.expected_epoch
            || owner.status != "ACTIVE"
        {
            bail!("process partition {partition} transfer was fenced");
        }
        owner.status = "DRAINING".to_owned();
        owner.lease_expires = now() + app.lease_seconds;
        transaction.put(key, &owner)
    })?;
    app.commit_shard(shard, |transaction| {
        for (lease_key, lease) in
            transaction.scan::<ProcessBatchLease>(process_batch_lease_prefix())?
        {
            if lease.shard != partition {
                continue;
            }
            let state_key = process_shard_state_key(&lease.process_id, shard);
            let mut shard_state = transaction
                .get::<ProcessShardState>(&state_key)?
                .unwrap_or_default();
            for mut execution in lease.executions {
                if execution.execution.attempt > 0 {
                    if execution.execution.isolated_retry {
                        shard_state.retry_running = shard_state.retry_running.saturating_sub(1);
                    } else {
                        shard_state.running = shard_state.running.saturating_sub(1);
                        execution.execution.isolated_retry = true;
                    }
                    shard_state.retry_pending += 1;
                }
                transaction.put(
                    process_ready_key(
                        shard,
                        &execution.execution.process_id,
                        execution.execution.sequence,
                    ),
                    &execution,
                )?;
            }
            transaction.put(&state_key, &shard_state)?;
            transaction.delete(lease_key);
        }
        Ok(())
    })?;
    let checkpoint = app.store.checkpoint()?;
    let mut transferred = None;
    app.commit_shard(shard, |transaction| {
        let key = process_partition_owner_key(shard);
        let mut owner = transaction
            .get::<ProcessPartitionOwner>(&key)?
            .ok_or_else(|| anyhow!("process partition {partition} is unassigned"))?;
        if owner.owner != app.runtime_id
            || owner.epoch != request.expected_epoch
            || owner.status != "DRAINING"
        {
            bail!("process partition {partition} changed while draining");
        }
        owner.owner = format!("{}:pending", request.target_node);
        owner.node_id.clone_from(&request.target_node);
        owner.endpoint.clone_from(&request.target_endpoint);
        owner.epoch = owner.epoch.saturating_add(1);
        owner.lease_expires = now() + app.lease_seconds;
        owner.status = "RESTORING".to_owned();
        owner.checkpoint_id = Some(checkpoint.checkpoint_id.clone());
        transaction.put(key, &owner)?;
        transferred = Some(owner);
        Ok(())
    })?;
    Ok(Json(transferred.expect("partition transfer committed")))
}

pub(crate) async fn assign_key_group(
    State(app): State<AppState>,
    Path(key_group): Path<u32>,
    Json(request): Json<AssignKeyGroupRequest>,
) -> Result<impl IntoResponse, ApiError> {
    if key_group >= app.key_group_count {
        return Err(ApiError(anyhow!(
            "key group {key_group} is outside the cluster"
        )));
    }
    if request.owner.trim().is_empty() {
        return Err(ApiError(anyhow!("owner must not be empty")));
    }
    let mut assigned = None;
    app.commit(|transaction| {
        let mut lease = transaction
            .get::<KeyGroupLease>(&key_group_key(key_group))?
            .ok_or_else(|| anyhow!("key group {key_group} is unassigned"))?;
        if lease.epoch != request.expected_epoch {
            bail!(
                "key group {key_group} epoch changed from {} to {}",
                request.expected_epoch,
                lease.epoch,
            );
        }
        lease.owner.clone_from(&request.owner);
        lease.epoch += 1;
        lease.lease_expires = now() + app.lease_seconds;
        transaction.put(key_group_key(key_group), &lease)?;
        assigned = Some(lease);
        Ok(())
    })?;
    Ok(Json(assigned.expect("assignment committed")))
}

pub(crate) async fn poll_sink(
    State(app): State<AppState>,
    Path(sink): Path<String>,
    Json(request): Json<PollSinkRequest>,
) -> Result<impl IntoResponse, ApiError> {
    if request.consumer_id.trim().is_empty() || request.lease_seconds <= 0.0 {
        return Err(ApiError(anyhow!(
            "consumer_id must be non-empty and lease_seconds positive"
        )));
    }
    promote_process_outputs(&app, Some(&sink))?;
    app.store.sync_all_remote()?;
    let timestamp = now();
    let candidate = app
        .store
        .scan::<OutboxMessage>(&outbox_prefix(&sink))?
        .into_iter()
        .find(|(_, message)| {
            message.acked_at.is_none()
                && message
                    .lease_expires
                    .is_none_or(|lease_expires| lease_expires <= timestamp)
        });
    let Some((candidate_key, candidate)) = candidate else {
        return Ok(StatusCode::NO_CONTENT.into_response());
    };
    let shard = usize::try_from(candidate.shard)?;
    let message = app.commit_output(shard, |transaction| {
        let timestamp = now();
        let Some(mut candidate) = transaction.get::<OutboxMessage>(&candidate_key)? else {
            return Ok(None);
        };
        if candidate.acked_at.is_none()
            && candidate
                .lease_expires
                .is_none_or(|lease_expires| lease_expires <= timestamp)
        {
            candidate.lease_owner = Some(request.consumer_id.clone());
            candidate.lease_expires = Some(timestamp + request.lease_seconds);
            candidate.delivery_attempt += 1;
            transaction.put(&candidate_key, &candidate)?;
            return Ok(Some(candidate));
        }
        Ok(None)
    })?;
    Ok(match message {
        Some(message) => Json(message).into_response(),
        None => StatusCode::NO_CONTENT.into_response(),
    })
}

pub(crate) async fn ack_sink_message(
    State(app): State<AppState>,
    Path((sink, message_id)): Path<(String, String)>,
    Json(request): Json<AckSinkRequest>,
) -> Result<impl IntoResponse, ApiError> {
    app.store.sync_all_remote()?;
    let key = outbox_key(&sink, &message_id);
    let existing = app
        .store
        .get::<OutboxMessage>(&key)?
        .ok_or_else(|| anyhow!("outbox message not found: {message_id}"))?;
    let shard = usize::try_from(existing.shard)?;
    let response = app.commit_output(shard, |transaction| {
        let mut message = transaction
            .get::<OutboxMessage>(&key)?
            .ok_or_else(|| anyhow!("outbox message not found: {message_id}"))?;
        if message.acked_at.is_none() {
            if message.lease_owner.as_deref() != Some(request.consumer_id.as_str())
                || message.lease_expires.is_none_or(|expires| expires <= now())
            {
                bail!("outbox message lease is not owned by this consumer");
            }
            message.acked_at = Some(now());
            message.lease_owner = None;
            message.lease_expires = None;
            transaction.put(&key, &message)?;
        }
        Ok(message)
    })?;
    Ok(Json(response))
}
