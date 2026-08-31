use crate::*;
pub(crate) fn encoded(value: &str) -> String {
    URL_SAFE_NO_PAD.encode(value)
}

pub(crate) fn workflow_key(id: &str) -> String {
    format!("workflow/{}", encoded(id))
}
pub(crate) fn workflow_task_key(id: &str) -> String {
    format!("workflow-task/{}", encoded(id))
}

pub(crate) fn workflow_task_token_key(token: &str) -> String {
    format!("workflow-task-token/{token}")
}
pub(crate) fn workflow_deadline_key(id: &str) -> String {
    format!("workflow-deadline/{}", encoded(id))
}
pub(crate) fn workflow_child_prefix(parent_id: &str) -> String {
    format!("workflow-child/{}/", encoded(parent_id))
}
pub(crate) fn workflow_child_key(parent_id: &str, child_id: &str) -> String {
    format!("{}{}", workflow_child_prefix(parent_id), encoded(child_id))
}
pub(crate) fn event_prefix(id: &str) -> String {
    format!("event/{}/", encoded(id))
}
pub(crate) fn activity_key(id: u64) -> String {
    format!("activity/{id:020}")
}
pub(crate) fn timer_key(id: &str, command_id: u64) -> String {
    format!("timer/{}/{command_id:020}", encoded(id))
}
pub(crate) fn stream_config_key(stream: &str) -> String {
    format!("stream/{}/config", encoded(stream))
}
pub(crate) fn stream_state_key(stream: &str) -> String {
    format!("stream/{}/state", encoded(stream))
}
pub(crate) fn stream_partition_key(stream: &str, partition: u32) -> String {
    format!("stream/{}/partition/{partition:010}", encoded(stream))
}
pub(crate) fn stream_record_prefix(stream: &str) -> String {
    format!("stream-record/{}/", encoded(stream))
}
pub(crate) fn stream_record_key(stream: &str, partition: u32, offset: u64) -> String {
    format!(
        "{}{partition:010}/{offset:020}",
        stream_record_prefix(stream)
    )
}
pub(crate) fn stream_batch_key(stream: &str, last_sequence: u64) -> String {
    format!("stream-batch/{}/{last_sequence:020}", encoded(stream),)
}
pub(crate) fn late_record_prefix(stream: &str) -> String {
    format!("late-record/{}/", encoded(stream))
}
pub(crate) fn late_record_key(stream: &str, sequence: u64) -> String {
    format!("{}{sequence:020}", late_record_prefix(stream))
}
pub(crate) fn watermark_timer_prefix(stream: &str) -> String {
    format!("watermark-timer/{}/", encoded(stream))
}
pub(crate) fn watermark_timer_key(stream: &str, workflow_id: &str, command_id: u64) -> String {
    format!(
        "{}{}/{command_id:020}",
        watermark_timer_prefix(stream),
        encoded(workflow_id)
    )
}
pub(crate) fn stream_schedule_key(schedule_id: &str) -> String {
    format!("stream-schedule/{}", encoded(schedule_id))
}
pub(crate) fn window_accumulator_prefix(schedule_id: &str) -> String {
    format!("window-accumulator/{}/", encoded(schedule_id))
}
pub(crate) fn window_accumulator_key(
    schedule_id: &str,
    window_start: f64,
    key: Option<&str>,
) -> String {
    format!(
        "{}{:016x}/{}",
        window_accumulator_prefix(schedule_id),
        ordered_f64_bits(window_start),
        encoded(key.unwrap_or("")),
    )
}
pub(crate) fn window_value_prefix(
    schedule_id: &str,
    window_start: f64,
    key: Option<&str>,
) -> String {
    format!(
        "window-value/{}/{:016x}/{}/",
        encoded(schedule_id),
        ordered_f64_bits(window_start),
        encoded(key.unwrap_or("")),
    )
}
pub(crate) fn window_value_key(
    schedule_id: &str,
    window_start: f64,
    key: Option<&str>,
    value: f64,
) -> String {
    format!(
        "{}{:016x}",
        window_value_prefix(schedule_id, window_start, key),
        ordered_f64_bits(value),
    )
}
pub(crate) fn operator_change_prefix(operator_id: &str) -> String {
    format!("operator-change/{}/", encoded(operator_id))
}
pub(crate) fn operator_edge_key(operator_id: &str) -> String {
    format!("operator-edge/{}", encoded(operator_id))
}
pub(crate) fn operator_edge_pending_prefix(operator_id: &str) -> String {
    format!("operator-edge-pending/{}/", encoded(operator_id))
}
pub(crate) fn checkpoint_barrier_key(checkpoint_id: &str) -> String {
    format!("checkpoint-barrier/{}", encoded(checkpoint_id))
}
pub(crate) fn process_key(process_id: &str) -> String {
    format!("process/{}", encoded(process_id))
}
pub(crate) fn process_stream_prefix(stream: &str) -> String {
    format!("process-stream/{}/", encoded(stream))
}
pub(crate) fn process_stream_key(stream: &str, process_id: &str) -> String {
    format!("{}{}", process_stream_prefix(stream), encoded(process_id))
}
pub(crate) fn process_mailbox_prefix(process_id: &str) -> String {
    format!("process-mailbox/{}/", encoded(process_id))
}
pub(crate) fn process_mailbox_key(process_id: &str, sequence: u64) -> String {
    format!("{}{sequence:020}", process_mailbox_prefix(process_id))
}
pub(crate) fn process_shard_state_key(process_id: &str, shard: usize) -> String {
    format!("process-shard/{}/{shard:04}", encoded(process_id))
}
pub(crate) fn process_shard_mailbox_prefix(process_id: &str, shard: usize) -> String {
    format!("process-shard-mailbox/{}/{shard:04}/", encoded(process_id))
}
pub(crate) fn process_shard_mailbox_key(process_id: &str, shard: usize, sequence: u64) -> String {
    format!(
        "{}{sequence:020}",
        process_shard_mailbox_prefix(process_id, shard)
    )
}
pub(crate) fn process_shard_execution_prefix() -> &'static str {
    "process-shard-execution/"
}
pub(crate) fn process_shard_execution_key(shard: usize, process_id: &str, sequence: u64) -> String {
    format!(
        "{}{shard:04}/{}/{sequence:020}",
        process_shard_execution_prefix(),
        encoded(process_id)
    )
}
pub(crate) fn process_active_key(process_id: &str, key: &str) -> String {
    format!("process-active/{}/{}", encoded(process_id), encoded(key))
}
pub(crate) fn process_execution_key(workflow_id: &str) -> String {
    format!("process-execution/{}", encoded(workflow_id))
}
pub(crate) fn process_batch_lease_key(token: &str) -> String {
    format!("process-batch-lease/{}", encoded(token))
}
pub(crate) fn process_batch_lease_prefix() -> &'static str {
    "process-batch-lease/"
}
pub(crate) fn process_partition_owner_key(shard: usize) -> String {
    format!("process-partition-owner/{shard:04}")
}
pub(crate) fn process_ready_prefix(shard: usize) -> String {
    format!("process-ready/{shard:04}/")
}
pub(crate) fn process_ready_key(shard: usize, process_id: &str, sequence: u64) -> String {
    format!(
        "{}{}/{sequence:020}",
        process_ready_prefix(shard),
        encoded(process_id)
    )
}
pub(crate) fn process_state_key(process_id: &str, key: &str) -> String {
    format!("process-state/{}/{}", encoded(process_id), encoded(key))
}
pub(crate) fn process_output_key(process_id: &str, key: &str) -> String {
    format!("process-output/{}/{}", encoded(process_id), encoded(key))
}

pub(crate) fn append_operator_change(
    transaction: &mut Transaction<'_>,
    operator_id: &str,
    key: Option<String>,
    event_time: f64,
    kind: ChangeKind,
    row: Value,
) -> Result<()> {
    let sequence_key = format!("meta/operator-change/{}", encoded(operator_id));
    let sequence = transaction.get::<u64>(&sequence_key)?.unwrap_or(0) + 1;
    transaction.put(sequence_key, &sequence)?;
    let change = DifferentialChange {
        operator_id: operator_id.to_owned(),
        sequence,
        key,
        event_time,
        kind,
        diff: kind.weight(),
        row,
    };
    transaction.put(
        format!("{}{sequence:020}", operator_change_prefix(operator_id)),
        &change,
    )?;
    if transaction
        .get::<OperatorEdge>(&operator_edge_key(operator_id))?
        .is_some_and(|edge| edge.status == "ACTIVE")
    {
        transaction.put(
            format!(
                "{}{sequence:020}",
                operator_edge_pending_prefix(operator_id)
            ),
            &change,
        )?;
    }
    Ok(())
}

pub(crate) fn window_accumulator_row(accumulator: &WindowAccumulator) -> Value {
    json!({
        "key": accumulator.key,
        "window_start": accumulator.window_start,
        "window_end": accumulator.window_end,
        "count": accumulator.count,
        "sum": accumulator.sum,
        "max": accumulator.max,
    })
}
pub(crate) fn stream_event_id_key(stream: &str, event_id: &str) -> String {
    format!("stream-event-id/{}/{}", encoded(stream), encoded(event_id))
}
pub(crate) fn temporal_join_key(join_id: &str) -> String {
    format!("temporal-join/{}", encoded(join_id))
}
pub(crate) fn temporal_join_probe_prefix(join_id: &str) -> String {
    format!("temporal-join-probe/{}/", encoded(join_id))
}
pub(crate) fn temporal_join_probe_key(join_id: &str, record: &StreamRecord) -> String {
    format!(
        "{}{}/{:010}/{:020}",
        temporal_join_probe_prefix(join_id),
        encoded(record.key.as_deref().expect("probe key validated")),
        record.partition,
        record.offset,
    )
}
pub(crate) fn temporal_join_version_prefix(join_id: &str) -> String {
    format!("temporal-join-version/{}/", encoded(join_id))
}
pub(crate) fn temporal_join_versions_for_key_prefix(join_id: &str, key: &str) -> String {
    format!("{}{}/", temporal_join_version_prefix(join_id), encoded(key))
}
pub(crate) fn temporal_join_version_key(join_id: &str, record: &StreamRecord) -> String {
    format!(
        "{}{:016x}/{:020}",
        temporal_join_versions_for_key_prefix(
            join_id,
            record.key.as_deref().expect("version key validated"),
        ),
        ordered_f64_bits(record.event_time),
        record.sequence,
    )
}
pub(crate) fn temporal_join_output_prefix(join_id: &str) -> String {
    format!("temporal-join-output/{}/", encoded(join_id))
}
pub(crate) fn temporal_join_output_key(join_id: &str, record: &StreamRecord) -> String {
    format!(
        "{}{:010}/{:020}",
        temporal_join_output_prefix(join_id),
        record.partition,
        record.offset,
    )
}
pub(crate) fn interval_join_key(join_id: &str) -> String {
    format!("interval-join/{}", encoded(join_id))
}
pub(crate) fn interval_join_side_prefix(join_id: &str, side: &str) -> String {
    format!("interval-join-{side}/{}/", encoded(join_id))
}
pub(crate) fn interval_join_side_key(join_id: &str, side: &str, record: &StreamRecord) -> String {
    format!(
        "{}{}/{:010}/{:020}",
        interval_join_side_prefix(join_id, side),
        encoded(record.key.as_deref().expect("interval join key validated")),
        record.partition,
        record.offset,
    )
}
pub(crate) fn interval_join_side_key_prefix(join_id: &str, side: &str, key: &str) -> String {
    format!(
        "{}{}/",
        interval_join_side_prefix(join_id, side),
        encoded(key),
    )
}
pub(crate) fn interval_join_output_prefix(join_id: &str) -> String {
    format!("interval-join-output/{}/", encoded(join_id))
}
pub(crate) fn interval_join_output_key(
    join_id: &str,
    left: Option<&StreamRecord>,
    right: Option<&StreamRecord>,
) -> String {
    let identity = |record: Option<&StreamRecord>| {
        record.map_or_else(
            || "none".to_owned(),
            |record| format!("{:010}-{:020}", record.partition, record.offset),
        )
    };
    format!(
        "{}{}-{}",
        interval_join_output_prefix(join_id),
        identity(left),
        identity(right),
    )
}
pub(crate) fn deduplicate_key(operator_id: &str) -> String {
    format!("deduplicate/{}", encoded(operator_id))
}
pub(crate) fn deduplicate_buffer_prefix(operator_id: &str) -> String {
    format!("deduplicate-buffer/{}/", encoded(operator_id))
}
pub(crate) fn deduplicate_buffer_key(operator_id: &str, record: &StreamRecord) -> String {
    format!(
        "{}{:010}/{:020}",
        deduplicate_buffer_prefix(operator_id),
        record.partition,
        record.offset,
    )
}
pub(crate) fn deduplicate_state_prefix(operator_id: &str) -> String {
    format!("deduplicate-state/{}/", encoded(operator_id))
}
pub(crate) fn deduplicate_state_key(operator_id: &str, key: &str) -> String {
    format!("{}{}", deduplicate_state_prefix(operator_id), encoded(key))
}
pub(crate) fn deduplicate_output_prefix(operator_id: &str) -> String {
    format!("deduplicate-output/{}/", encoded(operator_id))
}
pub(crate) fn deduplicate_output_key(operator_id: &str, record: &StreamRecord) -> String {
    format!(
        "{}{:010}/{:020}",
        deduplicate_output_prefix(operator_id),
        record.partition,
        record.offset,
    )
}
pub(crate) fn stream_filter_key(operator_id: &str) -> String {
    format!("stream-filter/{}", encoded(operator_id))
}
pub(crate) fn stream_filter_output_prefix(operator_id: &str) -> String {
    format!("stream-filter-output/{}/", encoded(operator_id))
}
pub(crate) fn stream_filter_output_key(operator_id: &str, record: &StreamRecord) -> String {
    format!(
        "{}{:010}/{:020}",
        stream_filter_output_prefix(operator_id),
        record.partition,
        record.offset,
    )
}
pub(crate) fn key_group_key(key_group: u32) -> String {
    format!("key-group/{key_group:010}")
}

pub(crate) fn source_cursor_key(stream: &str, source_id: &str, partition: u32) -> String {
    format!(
        "source-cursor/{}/{}/{partition:010}",
        encoded(stream),
        encoded(source_id),
    )
}

pub(crate) fn source_lease_key(stream: &str, partition: u32) -> String {
    format!("source-lease/{}/{partition:010}", encoded(stream))
}

pub(crate) fn outbox_prefix(sink: &str) -> String {
    format!("outbox/{}/", encoded(sink))
}

pub(crate) fn outbox_key(sink: &str, message_id: &str) -> String {
    format!("{}{}", outbox_prefix(sink), encoded(message_id))
}

pub(crate) fn key_group_for(key: Option<&str>, _partition: u32, count: u32) -> u32 {
    let mut hash = 0xcbf29ce484222325_u64;
    if let Some(key) = key {
        for byte in key.as_bytes() {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x100000001b3);
        }
    }
    (hash % u64::from(count)) as u32
}

pub(crate) fn owned_key_group_epoch(
    transaction: &Transaction<'_>,
    node_id: &str,
    key_group: u32,
) -> Result<u64> {
    let lease = transaction
        .get::<KeyGroupLease>(&key_group_key(key_group))?
        .ok_or_else(|| anyhow!("key group {key_group} is unassigned"))?;
    if lease.owner != node_id || lease.lease_expires <= now() {
        bail!(
            "key group {key_group} is fenced at epoch {} and owned by {}",
            lease.epoch,
            lease.owner,
        );
    }
    Ok(lease.epoch)
}

pub(crate) fn claim_source_partition(
    transaction: &mut Transaction<'_>,
    stream: &str,
    partition: u32,
    source_id: &str,
    expected_epoch: Option<u64>,
    lease_seconds: f64,
) -> Result<SourceLease> {
    let key = source_lease_key(stream, partition);
    let timestamp = now();
    let mut lease = transaction
        .get::<SourceLease>(&key)?
        .unwrap_or(SourceLease {
            stream: stream.to_owned(),
            partition,
            source_id: source_id.to_owned(),
            epoch: 1,
            lease_expires: timestamp + lease_seconds,
        });
    if lease.lease_expires > timestamp && lease.source_id != source_id {
        bail!(
            "stream {stream} partition {partition} is owned by source {} at epoch {}",
            lease.source_id,
            lease.epoch,
        );
    }
    if let Some(expected_epoch) = expected_epoch {
        if lease.source_id != source_id
            || lease.epoch != expected_epoch
            || lease.lease_expires <= timestamp
        {
            bail!("source lease is expired or fenced; reclaim the partition");
        }
    } else if lease.lease_expires <= timestamp || lease.source_id != source_id {
        lease.source_id = source_id.to_owned();
        lease.epoch += 1;
    }
    lease.lease_expires = lease.lease_expires.max(timestamp + lease_seconds);
    transaction.put(key, &lease)?;
    Ok(lease)
}

pub(crate) fn initialize_key_groups(app: &AppState) -> Result<()> {
    app.commit(|transaction| {
        if let Some(config) = transaction.get::<ClusterConfig>("cluster/config")? {
            if config.key_group_count != app.key_group_count {
                bail!(
                    "configured key-group count {} differs from persisted count {}",
                    app.key_group_count,
                    config.key_group_count,
                );
            }
        } else {
            transaction.put(
                "cluster/config",
                &ClusterConfig {
                    key_group_count: app.key_group_count,
                },
            )?;
        }
        let expires = now() + app.lease_seconds;
        for key_group in 0..app.key_group_count {
            let key = key_group_key(key_group);
            match transaction.get::<KeyGroupLease>(&key)? {
                Some(mut lease) if lease.owner == app.node_id => {
                    lease.lease_expires = expires;
                    transaction.put(key, &lease)?;
                }
                Some(mut lease) if lease.lease_expires <= now() => {
                    lease.owner.clone_from(&app.node_id);
                    lease.epoch += 1;
                    lease.lease_expires = expires;
                    transaction.put(key, &lease)?;
                }
                Some(_) => {}
                None => transaction.put(
                    key,
                    &KeyGroupLease {
                        key_group,
                        owner: app.node_id.clone(),
                        epoch: 1,
                        lease_expires: expires,
                    },
                )?,
            }
        }
        Ok(())
    })
}

pub(crate) fn renew_key_groups(app: &AppState) -> Result<()> {
    app.commit(|transaction| {
        let expires = now() + app.lease_seconds;
        for (key, mut lease) in transaction.scan::<KeyGroupLease>("key-group/")? {
            if lease.owner == app.node_id && lease.lease_expires <= now() + app.lease_seconds / 2.0
            {
                lease.lease_expires = expires;
                transaction.put(key, &lease)?;
            }
        }
        Ok(())
    })
}

pub(crate) fn initialize_process_partitions(app: &AppState) -> Result<()> {
    for shard in 1..app.shard_locks.len() {
        app.commit_shard(shard, |transaction| {
            let key = process_partition_owner_key(shard);
            let current = transaction.get::<ProcessPartitionOwner>(&key)?;
            let epoch = current
                .as_ref()
                .map_or(1, |owner| owner.epoch.saturating_add(1));
            let next_activation_sequence =
                current.map_or(0, |owner| owner.next_activation_sequence);
            transaction.put(
                key,
                &ProcessPartitionOwner {
                    partition_id: u32::try_from(shard)?,
                    owner: app.runtime_id.clone(),
                    epoch,
                    lease_expires: now() + app.lease_seconds,
                    next_activation_sequence,
                },
            )
        })?;
    }
    Ok(())
}

pub(crate) fn next_process_partition_activation(
    transaction: &mut Transaction<'_>,
    runtime_id: &str,
    shard: usize,
) -> Result<(u64, u64)> {
    let key = process_partition_owner_key(shard);
    let mut owner = transaction
        .get::<ProcessPartitionOwner>(&key)?
        .ok_or_else(|| anyhow!("process partition {shard} is unassigned"))?;
    if owner.owner != runtime_id || owner.lease_expires <= now() {
        bail!(
            "process partition {shard} is fenced at epoch {} and owned by {}",
            owner.epoch,
            owner.owner,
        );
    }
    owner.next_activation_sequence = owner
        .next_activation_sequence
        .checked_add(1)
        .ok_or_else(|| anyhow!("process partition {shard} activation sequence exhausted"))?;
    let activation = (owner.epoch, owner.next_activation_sequence);
    transaction.put(key, &owner)?;
    Ok(activation)
}

pub(crate) fn renew_process_partitions(app: &AppState) -> Result<()> {
    for shard in 1..app.shard_locks.len() {
        app.commit_shard(shard, |transaction| {
            let key = process_partition_owner_key(shard);
            let mut owner = transaction
                .get::<ProcessPartitionOwner>(&key)?
                .ok_or_else(|| anyhow!("process partition {shard} is unassigned"))?;
            if owner.owner != app.runtime_id {
                bail!(
                    "process partition {shard} is fenced at epoch {} and owned by {}",
                    owner.epoch,
                    owner.owner,
                );
            }
            if owner.lease_expires <= now() + app.lease_seconds / 2.0 {
                owner.lease_expires = now() + app.lease_seconds;
                transaction.put(key, &owner)?;
            }
            Ok(())
        })?;
    }
    Ok(())
}

pub(crate) fn owned_process_partition_epoch(
    transaction: &Transaction<'_>,
    runtime_id: &str,
    shard: usize,
) -> Result<u64> {
    let owner = transaction
        .get::<ProcessPartitionOwner>(&process_partition_owner_key(shard))?
        .ok_or_else(|| anyhow!("process partition {shard} is unassigned"))?;
    if owner.owner != runtime_id || owner.lease_expires <= now() {
        bail!(
            "process partition {shard} is fenced at epoch {} and owned by {}",
            owner.epoch,
            owner.owner,
        );
    }
    Ok(owner.epoch)
}

pub(crate) fn ordered_f64_bits(value: f64) -> u64 {
    let bits = value.to_bits();
    if bits & (1 << 63) == 0 {
        bits ^ (1 << 63)
    } else {
        !bits
    }
}

pub(crate) fn append_event(
    transaction: &mut Transaction<'_>,
    workflow_id: &str,
    event_type: &str,
    data: Value,
) -> Result<u64> {
    let id = transaction.get::<u64>("meta/event_sequence")?.unwrap_or(0) + 1;
    transaction.put("meta/event_sequence", &id)?;
    transaction.put(
        format!("{}{id:020}", event_prefix(workflow_id)),
        &Event {
            id,
            workflow_id: workflow_id.to_owned(),
            event_type: event_type.to_owned(),
            data,
            created_at: now(),
        },
    )?;
    Ok(id)
}

pub(crate) fn enqueue_workflow(
    transaction: &mut Transaction<'_>,
    workflow: &WorkflowRecord,
) -> Result<()> {
    let key = workflow_task_key(&workflow.workflow_id);
    let current = transaction.get::<WorkflowTask>(&key)?;
    let enqueued_at = current.as_ref().map_or_else(now, |task| task.enqueued_at);
    transaction.put(
        key,
        &WorkflowTask {
            workflow_id: workflow.workflow_id.clone(),
            task_queue: workflow.task_queue.clone(),
            build_id: workflow.build_id.clone(),
            available_at: current
                .as_ref()
                .map_or_else(now, |task| task.available_at.min(now())),
            attempt: current.as_ref().map_or(0, |task| task.attempt),
            lease_owner: current.as_ref().and_then(|task| task.lease_owner.clone()),
            lease_expires: current.as_ref().and_then(|task| task.lease_expires),
            task_token: current.as_ref().and_then(|task| task.task_token.clone()),
            batch_group: current.as_ref().and_then(|task| task.batch_group.clone()),
            batch_max_size: current.as_ref().map_or(1, |task| task.batch_max_size),
            batch_max_delay: current.as_ref().map_or(0.0, |task| task.batch_max_delay),
            enqueued_at,
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn process_partition_epoch_fences_the_previous_runtime() -> Result<()> {
        let root = std::env::temp_dir().join(format!(
            "highwater-process-partition-test-{}",
            Uuid::new_v4()
        ));
        let state_dir = root.join("state");
        let object_dir = root.join("objects");
        let mut app = AppState {
            store: Arc::new(DurableStore::open_sharded(&state_dir, &object_dir, 2)?),
            mutation_lock: Arc::new(Mutex::new(())),
            shard_locks: Arc::new(vec![Mutex::new(()), Mutex::new(())]),
            partition_senders: Arc::new(vec![None, None]),
            node_id: "test".to_owned(),
            runtime_id: "test:first".to_owned(),
            key_group_count: 1,
            lease_seconds: 30.0,
            query_queue: Arc::new(Mutex::new(VecDeque::new())),
            query_results: Arc::new(Mutex::new(HashMap::new())),
        };
        initialize_process_partitions(&app)?;
        let mut first = None;
        app.commit_shard(1, |transaction| {
            first = Some(next_process_partition_activation(
                transaction,
                &app.runtime_id,
                1,
            )?);
            Ok(())
        })?;

        let old_runtime = app.runtime_id.clone();
        app.runtime_id = "test:second".to_owned();
        initialize_process_partitions(&app)?;
        let mut second = None;
        app.commit_shard(1, |transaction| {
            assert!(owned_process_partition_epoch(transaction, &old_runtime, 1).is_err());
            second = Some(next_process_partition_activation(
                transaction,
                &app.runtime_id,
                1,
            )?);
            Ok(())
        })?;

        let (first_epoch, first_activation) = first.expect("first activation");
        let (second_epoch, second_activation) = second.expect("second activation");
        assert!(second_epoch > first_epoch);
        assert!(second_activation > first_activation);

        drop(app);
        fs::remove_dir_all(root)?;
        Ok(())
    }
}
