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

pub(crate) fn recover_process_tasks(app: &AppState, _recover_orphans: bool) -> Result<()> {
    let timestamp = now();
    let mut expired_by_shard = BTreeMap::<usize, Vec<(String, ProcessBatchLease)>>::new();
    for (lease_key, lease) in app
        .store
        .scan::<ProcessBatchLease>(process_batch_lease_prefix())?
    {
        if lease.lease_expires <= timestamp {
            expired_by_shard
                .entry(lease.shard as usize)
                .or_default()
                .push((lease_key, lease));
        }
    }
    for (shard, leases) in expired_by_shard {
        app.commit_shard(shard, |transaction| {
            for (lease_key, lease) in leases {
                for ready_execution in lease.executions {
                    let execution = &ready_execution.execution;
                    transaction.put(
                        process_ready_key(shard, &execution.process_id, execution.sequence),
                        &ready_execution,
                    )?;
                }
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
        if let Err(error) = maintain_event_time(&app) {
            eprintln!("event-time maintenance failed: {error:#}");
        }
        if let Err(error) = renew_key_groups(&app) {
            eprintln!("key-group lease renewal failed: {error:#}");
        }
        let has_remote_owner = app
            .store
            .scan::<KeyGroupLease>("key-group/")
            .is_ok_and(|leases| {
                leases
                    .into_iter()
                    .any(|(_, lease)| lease.lease_expires > now() && lease.owner != app.node_id)
            });
        if !has_remote_owner && let Err(error) = app.store.checkpoint_if_needed(512) {
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
    let remote_owner = app
        .store
        .scan::<KeyGroupLease>("key-group/")?
        .into_iter()
        .any(|(_, lease)| lease.lease_expires > now() && lease.owner != app.node_id);
    if remote_owner {
        return Err(ApiError(anyhow!(
            "remote key-group owners require a distributed checkpoint barrier"
        )));
    }
    Ok((StatusCode::CREATED, Json(app.store.checkpoint()?)))
}

pub(crate) async fn create_checkpoint_barrier(
    State(app): State<AppState>,
) -> Result<(StatusCode, Json<CheckpointBarrier>), ApiError> {
    let _guard = app
        .mutation_lock
        .lock()
        .map_err(|_| anyhow!("mutation lock poisoned"))?;
    if app
        .store
        .scan::<CheckpointBarrier>("checkpoint-barrier/")?
        .into_iter()
        .any(|(_, barrier)| barrier.status == "ALIGNING")
    {
        return Err(ApiError(anyhow!(
            "a checkpoint barrier is already aligning"
        )));
    }
    let mut manifest = app.store.prepare_checkpoint()?;
    let leases = app.store.scan::<KeyGroupLease>("key-group/")?;
    let mut expected_key_group_epochs: BTreeMap<String, BTreeMap<u32, u64>> = BTreeMap::new();
    for (_, lease) in leases {
        if lease.lease_expires > now() {
            expected_key_group_epochs
                .entry(lease.owner)
                .or_default()
                .insert(lease.key_group, lease.epoch);
        }
    }
    let expected_nodes = expected_key_group_epochs
        .keys()
        .cloned()
        .collect::<Vec<_>>();
    let local_epochs = expected_key_group_epochs
        .get(&app.node_id)
        .cloned()
        .unwrap_or_default();
    let local_ack = CheckpointAck {
        node_id: app.node_id.clone(),
        state_handle: manifest.object_path.clone(),
        key_group_epochs: local_epochs,
        acked_at: now(),
    };
    let mut acknowledgements = BTreeMap::new();
    if expected_key_group_epochs.contains_key(&app.node_id) {
        acknowledgements.insert(app.node_id.clone(), local_ack);
    }
    let complete = acknowledgements.len() == expected_nodes.len();
    if complete {
        for (node, ack) in &acknowledgements {
            manifest
                .state_handles
                .insert(node.clone(), ack.state_handle.clone());
        }
        app.store.publish_checkpoint(&manifest)?;
    }
    let barrier = CheckpointBarrier {
        checkpoint_id: manifest.checkpoint_id.clone(),
        sequence: manifest.sequence,
        status: if complete { "COMPLETE" } else { "ALIGNING" }.to_owned(),
        expected_nodes,
        expected_key_group_epochs,
        acknowledgements,
        manifest,
        created_at: now(),
    };
    app.store.commit(vec![Mutation {
        op: "put".to_owned(),
        key: checkpoint_barrier_key(&barrier.checkpoint_id),
        end_key: None,
        value: Some(serde_json::to_value(&barrier)?),
        encoded_value: None,
    }])?;
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
    Json(request): Json<AcknowledgeCheckpointRequest>,
) -> Result<Json<CheckpointBarrier>, ApiError> {
    let _guard = app
        .mutation_lock
        .lock()
        .map_err(|_| anyhow!("mutation lock poisoned"))?;
    let key = checkpoint_barrier_key(&checkpoint_id);
    let mut barrier = app
        .store
        .get::<CheckpointBarrier>(&key)?
        .ok_or_else(|| anyhow!("checkpoint barrier not found: {checkpoint_id}"))?;
    let expected = barrier
        .expected_key_group_epochs
        .get(&node_id)
        .ok_or_else(|| anyhow!("node {node_id} is not part of checkpoint {checkpoint_id}"))?;
    if expected != &request.key_group_epochs {
        return Err(ApiError(anyhow!(
            "checkpoint acknowledgement has stale or incomplete key-group epochs"
        )));
    }
    for (key_group, epoch) in expected {
        let lease = app
            .store
            .get::<KeyGroupLease>(&key_group_key(*key_group))?
            .ok_or_else(|| anyhow!("key group {key_group} is unassigned"))?;
        if lease.owner != node_id || lease.epoch != *epoch || lease.lease_expires <= now() {
            return Err(ApiError(anyhow!(
                "node {node_id} was fenced while checkpoint {checkpoint_id} aligned"
            )));
        }
    }
    barrier.acknowledgements.insert(
        node_id.clone(),
        CheckpointAck {
            node_id,
            state_handle: request.state_handle,
            key_group_epochs: request.key_group_epochs,
            acked_at: now(),
        },
    );
    if barrier.acknowledgements.len() == barrier.expected_nodes.len() {
        for (node, ack) in &barrier.acknowledgements {
            barrier
                .manifest
                .state_handles
                .insert(node.clone(), ack.state_handle.clone());
        }
        app.store.publish_checkpoint(&barrier.manifest)?;
        barrier.status = "COMPLETE".to_owned();
    }
    app.store.commit(vec![Mutation {
        op: "put".to_owned(),
        key,
        end_key: None,
        value: Some(serde_json::to_value(&barrier)?),
        encoded_value: None,
    }])?;
    Ok(Json(barrier))
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
    let mut message = None;
    app.commit(|transaction| {
        let timestamp = now();
        let candidate = transaction
            .scan::<OutboxMessage>(&outbox_prefix(&sink))?
            .into_iter()
            .find(|(_, message)| {
                message.acked_at.is_none()
                    && message
                        .lease_expires
                        .is_none_or(|lease_expires| lease_expires <= timestamp)
            });
        if let Some((key, mut candidate)) = candidate {
            candidate.lease_owner = Some(request.consumer_id.clone());
            candidate.lease_expires = Some(timestamp + request.lease_seconds);
            candidate.delivery_attempt += 1;
            transaction.put(key, &candidate)?;
            message = Some(candidate);
        }
        Ok(())
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
    let mut response = None;
    app.commit(|transaction| {
        let key = outbox_key(&sink, &message_id);
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
            transaction.put(key, &message)?;
        }
        response = Some(message);
        Ok(())
    })?;
    Ok(Json(response.expect("ack committed")))
}
