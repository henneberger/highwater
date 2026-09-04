use crate::*;
pub(crate) fn refresh_declarative_operators(
    transaction: &mut Transaction<'_>,
    record: Option<&StreamRecord>,
) -> Result<()> {
    refresh_stream_filters(transaction, record)?;
    refresh_temporal_joins(transaction, record)?;
    refresh_interval_joins(transaction, record)?;
    refresh_deduplicates(transaction, record)?;
    refresh_processes(transaction, record)
}

pub(crate) fn ensure_process_capacity(transaction: &Transaction<'_>, stream: &str) -> Result<()> {
    for (_, process_id) in transaction.scan::<String>(&process_stream_prefix(stream))? {
        let process = transaction
            .get::<DurableProcess>(&process_key(&process_id))?
            .ok_or_else(|| anyhow!("indexed process missing: {process_id}"))?;
        if process.status == "ACTIVE"
            && process.stream == stream
            && process.pending + process.running + process.retrying >= process.mailbox_capacity
        {
            return Err(StreamCapacityError(format!(
                "process {} mailbox is full ({}/{})",
                process.process_id,
                process.pending + process.running + process.retrying,
                process.mailbox_capacity,
            ))
            .into());
        }
    }
    Ok(())
}

pub(crate) fn process_item_is_eligible(
    transaction: &Transaction<'_>,
    process: &DurableProcess,
    item: &ProcessMailboxItem,
) -> Result<bool> {
    for stream in &process.versioned_streams {
        let config = transaction
            .get::<StreamConfig>(&stream_config_key(stream))?
            .ok_or_else(|| anyhow!("versioned stream missing: {stream}"))?;
        let state = transaction
            .get::<StreamState>(&stream_state_key(stream))?
            .ok_or_else(|| anyhow!("versioned stream state missing: {stream}"))?;
        if !completeness_frontier(&config, &state)
            .is_some_and(|frontier| frontier >= item.event_time)
        {
            return Ok(false);
        }
    }
    if process.event_time_gate == EventTimeGate::Immediate {
        return Ok(true);
    }
    let config = transaction
        .get::<StreamConfig>(&stream_config_key(&process.stream))?
        .ok_or_else(|| anyhow!("process input stream missing: {}", process.stream))?;
    let state = transaction
        .get::<StreamState>(&stream_state_key(&process.stream))?
        .ok_or_else(|| anyhow!("process input state missing: {}", process.stream))?;
    Ok(completeness_frontier(&config, &state).is_some_and(|frontier| frontier >= item.event_time))
}

pub(crate) fn start_process_workflow(
    transaction: &mut Transaction<'_>,
    process: &DurableProcess,
    item: &ProcessMailboxItem,
) -> Result<ProcessExecution> {
    ensure_pending_process_outcome(transaction, &process.process_id, &item.key, &item.record)?;
    let workflow_id = format!(
        "process/{}/{}/{:020}",
        process.process_id,
        encoded(&item.key),
        item.sequence,
    );
    let timestamp = now();
    let prior_state = transaction
        .get::<ProcessStateRecord>(&process_state_key(&process.process_id, &item.key))?;
    let workflow = WorkflowRecord {
        workflow_id: workflow_id.clone(),
        workflow_type: process.workflow_type.clone(),
        status: "RUNNING".to_owned(),
        result: None,
        error: None,
        task_queue: process.task_queue.clone(),
        build_id: Some(process.active_build_id.clone()),
        run_number: 1,
        parent_id: None,
        parent_command_id: None,
        parent_close_policy: None,
        execution_deadline: None,
        created_at: timestamp,
        updated_at: timestamp,
    };
    transaction.put(workflow_key(&workflow_id), &workflow)?;
    append_event(
        transaction,
        &workflow_id,
        "WORKFLOW_STARTED",
        json!({
            "workflow_type": process.workflow_type,
            "args": [{
                "process_id": process.process_id,
                "key": item.key,
                "event_time": item.event_time,
                "record": item.record,
                "state": prior_state.as_ref().map(|state| &state.value),
                "state_version": prior_state.as_ref().map(|state| state.version),
                "target_state_version": process.state_version,
                "build_id": process.active_build_id,
            }],
            "run_number": 1,
            "process": process.process_id,
        }),
    )?;
    enqueue_workflow(transaction, &workflow)?;
    let task_key = workflow_task_key(&workflow_id);
    let mut task = transaction
        .get::<WorkflowTask>(&task_key)?
        .ok_or_else(|| anyhow!("process workflow task missing"))?;
    task.batch_group = Some(process.process_id.clone());
    task.batch_max_size = process.batch_max_size;
    task.batch_max_delay = process.batch_max_delay;
    transaction.put(task_key, &task)?;
    Ok(ProcessExecution {
        process_id: process.process_id.clone(),
        key: item.key.clone(),
        event_time: item.event_time,
        record: item.record.clone(),
        prior_state,
        state_version: process.state_version,
        build_id: process.active_build_id.clone(),
        workflow_id,
        shard: 0,
        available_at: timestamp,
        enqueued_at: timestamp,
        attempt: 0,
        lease_owner: None,
        lease_expires: None,
        task_token: None,
        last_failure: None,
    })
}

pub(crate) fn dispatch_process(
    transaction: &mut Transaction<'_>,
    storage_key: &str,
    process: &mut DurableProcess,
) -> Result<()> {
    if process.status != "ACTIVE" {
        return Ok(());
    }
    let mailbox = transaction.scan_limit::<ProcessMailboxItem>(
        &process_mailbox_prefix(&process.process_id),
        usize::try_from(process.max_concurrent_keys).unwrap_or(usize::MAX),
    )?;
    let mut seen_keys = HashSet::new();
    for (mailbox_key, item) in mailbox {
        if process.running >= u64::from(process.max_concurrent_keys) {
            break;
        }
        if !seen_keys.insert(item.key.clone())
            || transaction
                .get::<ProcessExecution>(&process_active_key(&process.process_id, &item.key))?
                .is_some()
            || !process_item_is_eligible(transaction, process, &item)?
        {
            continue;
        }
        let execution = start_process_workflow(transaction, process, &item)?;
        transaction.put(
            process_active_key(&process.process_id, &item.key),
            &execution,
        )?;
        transaction.put(process_execution_key(&execution.workflow_id), &execution)?;
        transaction.delete(mailbox_key);
        process.pending = process.pending.saturating_sub(1);
        process.running += 1;
    }
    transaction.put(storage_key, process)
}

pub(crate) fn refresh_processes(
    transaction: &mut Transaction<'_>,
    record: Option<&StreamRecord>,
) -> Result<()> {
    let processes = if let Some(record) = record {
        let mut processes = Vec::new();
        for (_, process_id) in transaction.scan::<String>(&process_stream_prefix(&record.stream))? {
            let storage_key = process_key(&process_id);
            if let Some(process) = transaction.get::<DurableProcess>(&storage_key)? {
                processes.push((storage_key, process));
            }
        }
        processes
    } else {
        transaction.scan::<DurableProcess>("process/")?
    };
    let processes = processes
        .into_iter()
        .filter(|(_, process)| process.status == "ACTIVE")
        .collect::<Vec<_>>();
    for (storage_key, mut process) in processes {
        let relevant = record.is_none_or(|record| record.stream == process.stream);
        if let Some(record) = record
            && record.stream == process.stream
        {
            let key = record
                .key
                .clone()
                .filter(|key| !key.is_empty())
                .ok_or_else(|| anyhow!("process input records require a non-empty key"))?;
            ensure_pending_process_outcome(transaction, &process.process_id, &key, record)?;
            let item = ProcessMailboxItem {
                process_id: process.process_id.clone(),
                sequence: record.sequence,
                key,
                event_time: record.event_time,
                record: record.clone(),
            };
            transaction.put(
                process_mailbox_key(&process.process_id, record.sequence),
                &item,
            )?;
            process.pending += 1;
        }
        if relevant && process.running < u64::from(process.max_concurrent_keys) {
            dispatch_process(transaction, &storage_key, &mut process)?;
        } else if relevant {
            transaction.put(storage_key, &process)?;
        }
    }
    Ok(())
}

fn process_event_id(record: &StreamRecord) -> String {
    record.event_id.clone().unwrap_or_else(|| {
        format!(
            "stream:{}:{}:{}",
            record.stream, record.partition, record.offset
        )
    })
}

fn ensure_pending_process_outcome(
    transaction: &mut Transaction<'_>,
    process_id: &str,
    key: &str,
    record: &StreamRecord,
) -> Result<()> {
    let event_id = process_event_id(record);
    let storage_key = process_outcome_key(process_id, key, &event_id);
    if let Some(existing) = transaction.get::<ProcessExecutionOutcome>(&storage_key)? {
        if existing.sequence != record.sequence {
            bail!("event_id was already used with different process event contents");
        }
        return Ok(());
    }
    transaction.put(
        storage_key,
        &ProcessExecutionOutcome {
            process_id: process_id.to_owned(),
            event_id,
            key: key.to_owned(),
            sequence: record.sequence,
            status: "PENDING".to_owned(),
            attempts: 0,
            output_message_ids: Vec::new(),
            failure: None,
            admitted_at: record.ingestion_time,
            updated_at: record.ingestion_time,
        },
    )
}

pub(crate) struct ProcessOutcomeUpdate {
    pub(crate) status: &'static str,
    pub(crate) attempts: u32,
    pub(crate) output_message_ids: Vec<String>,
    pub(crate) failure: Option<String>,
}

pub(crate) fn update_process_outcome(
    transaction: &mut Transaction<'_>,
    process_id: &str,
    key: &str,
    record: &StreamRecord,
    update: ProcessOutcomeUpdate,
) -> Result<()> {
    ensure_pending_process_outcome(transaction, process_id, key, record)?;
    let event_id = process_event_id(record);
    let storage_key = process_outcome_key(process_id, key, &event_id);
    let mut outcome = transaction
        .get::<ProcessExecutionOutcome>(&storage_key)?
        .ok_or_else(|| anyhow!("process execution outcome missing"))?;
    if outcome.status != "PENDING" && outcome.status != update.status {
        bail!(
            "process event {} already reached terminal status {}",
            outcome.event_id,
            outcome.status
        );
    }
    outcome.status = update.status.to_owned();
    outcome.attempts = update.attempts;
    outcome.output_message_ids = update.output_message_ids;
    outcome.failure = update.failure;
    outcome.updated_at = now();
    transaction.put(storage_key, &outcome)
}

fn enqueue_process_output(
    transaction: &mut Transaction<'_>,
    shard: u32,
    process_id: &str,
    key: &str,
    record: &StreamRecord,
    output: &Value,
) -> Result<String> {
    let sink = format!("process:{process_id}");
    let message_id = format!("process:{process_id}:{}", record.sequence);
    let storage_key = outbox_key(&sink, &message_id);
    let message = OutboxMessage {
        sink,
        message_id: message_id.clone(),
        shard: 0,
        workflow_id: process_id.to_owned(),
        payload: json!({
            "process_id": process_id,
            "event_id": process_event_id(record),
            "key": key,
            "input_sequence": record.sequence,
            "event_time": record.event_time,
            "value": output,
        }),
        created_at: now(),
        lease_owner: None,
        lease_expires: None,
        delivery_attempt: 0,
        acked_at: None,
    };
    if shard == 0 {
        if let Some(existing) = transaction.get::<OutboxMessage>(&storage_key)? {
            if existing.message_id != message.message_id || existing.payload != message.payload {
                bail!("process output message identity collision");
            }
        } else {
            transaction.put(storage_key, &message)?;
        }
    } else {
        let pending_key = pending_process_output_key(shard, &message_id);
        let pending = PendingProcessOutput {
            source_shard: shard,
            message,
        };
        if let Some(existing) = transaction.get::<PendingProcessOutput>(&pending_key)? {
            if existing.message.message_id != pending.message.message_id
                || existing.message.payload != pending.message.payload
            {
                bail!("pending process output identity collision");
            }
        } else {
            transaction.put(pending_key, &pending)?;
        }
    }
    Ok(message_id)
}

pub(crate) fn finish_process_execution(
    transaction: &mut Transaction<'_>,
    workflow_id: &str,
    status: &str,
    result: Option<&Value>,
    failure: Option<&str>,
) -> Result<()> {
    let execution_key = process_execution_key(workflow_id);
    let Some(execution) = transaction.get::<ProcessExecution>(&execution_key)? else {
        return Ok(());
    };
    let storage_key = process_key(&execution.process_id);
    let mut process = transaction
        .get::<DurableProcess>(&storage_key)?
        .ok_or_else(|| anyhow!("process missing: {}", execution.process_id))?;
    transaction.delete(process_active_key(&execution.process_id, &execution.key));
    transaction.delete(execution_key);
    if execution.attempt == 0 {
        process.running = process.running.saturating_sub(1);
    } else {
        process.retrying = process.retrying.saturating_sub(1);
    }
    let mut output_message_ids = Vec::new();
    if status == "COMPLETED" {
        let returned = result.cloned().unwrap_or(Value::Null);
        let transition = returned
            .as_object()
            .filter(|value| {
                value
                    .get("__highwater_transition__")
                    .and_then(Value::as_bool)
                    == Some(true)
            })
            .ok_or_else(|| anyhow!("process handler returned an invalid state transition"))?;
        let next_state = transition
            .get("state")
            .cloned()
            .ok_or_else(|| anyhow!("process state transition is missing state"))?;
        let output = transition
            .get("emit")
            .filter(|value| !value.is_null())
            .cloned();
        transaction.put(
            process_state_key(&execution.process_id, &execution.key),
            &ProcessStateRecord {
                version: execution.state_version,
                build_id: execution.build_id.clone(),
                input_sequence: execution.record.sequence,
                event_time: execution.event_time,
                value: next_state,
            },
        )?;
        if let Some(output) = output {
            let output_key = process_output_key(&execution.process_id, &execution.key);
            if let Some(prior_output) = transaction.get::<Value>(&output_key)? {
                append_operator_change(
                    transaction,
                    &execution.process_id,
                    Some(execution.key.clone()),
                    execution.event_time,
                    ChangeKind::UpdateBefore,
                    prior_output,
                )?;
                append_operator_change(
                    transaction,
                    &execution.process_id,
                    Some(execution.key.clone()),
                    execution.event_time,
                    ChangeKind::UpdateAfter,
                    output.clone(),
                )?;
            } else {
                append_operator_change(
                    transaction,
                    &execution.process_id,
                    Some(execution.key.clone()),
                    execution.event_time,
                    ChangeKind::Insert,
                    output.clone(),
                )?;
            }
            transaction.put(output_key, &output)?;
            output_message_ids.push(enqueue_process_output(
                transaction,
                0,
                &execution.process_id,
                &execution.key,
                &execution.record,
                &output,
            )?);
        }
        update_process_outcome(
            transaction,
            &execution.process_id,
            &execution.key,
            &execution.record,
            ProcessOutcomeUpdate {
                status: "COMMITTED",
                attempts: execution.attempt,
                output_message_ids,
                failure: None,
            },
        )?;
        process.completed += 1;
        if process.discard_input_on_success {
            transaction.delete(stream_record_key(
                &process.stream,
                execution.record.partition,
                execution.record.offset,
            ));
            if let Some(event_id) = execution.record.event_id.as_deref() {
                transaction.delete(stream_event_id_key(&process.stream, event_id));
            }
            for (event_key, _) in transaction.scan::<Event>(&event_prefix(workflow_id))? {
                transaction.delete(event_key);
            }
            transaction.delete(workflow_key(workflow_id));
        }
    } else {
        update_process_outcome(
            transaction,
            &execution.process_id,
            &execution.key,
            &execution.record,
            ProcessOutcomeUpdate {
                status: "FAILED",
                attempts: execution.attempt.max(1),
                output_message_ids: Vec::new(),
                failure: Some(failure.unwrap_or(status).to_owned()),
            },
        )?;
        process.failed += 1;
    }
    if transaction.defer_process_dispatch {
        transaction.put(storage_key, &process)
    } else {
        dispatch_process(transaction, &storage_key, &mut process)
    }
}

pub(crate) fn dispatch_sharded_process(
    transaction: &mut Transaction<'_>,
    process: &DurableProcess,
    shard: usize,
    data_shards: usize,
    shard_state: &mut ProcessShardState,
) -> Result<()> {
    if process.status != "ACTIVE" {
        return Ok(());
    }
    let concurrency = sharded_process_concurrency(process, data_shards);
    let mailbox = transaction.scan_limit::<ShardedProcessMailboxItem>(
        &process_shard_mailbox_prefix(&process.process_id, shard),
        concurrency,
    )?;
    let mut seen_keys = HashSet::new();
    for (mailbox_key, item) in mailbox {
        if shard_state.running >= concurrency as u64 {
            break;
        }
        if !seen_keys.insert(item.key.clone()) || shard_state.active_keys.contains(&item.key) {
            continue;
        }
        shard_state.active_keys.insert(item.key.clone());
        let prior_state = transaction
            .get::<ProcessStateRecord>(&process_state_key(&process.process_id, &item.key))?;
        start_sharded_process_execution(transaction, process, shard, item, prior_state)?;
        transaction.delete(mailbox_key);
        shard_state.pending = shard_state.pending.saturating_sub(1);
        shard_state.running += 1;
    }
    Ok(())
}

fn sharded_process_concurrency(process: &DurableProcess, data_shards: usize) -> usize {
    let shard_count = data_shards.max(1) as u32;
    usize::try_from(u64::from(process.max_concurrent_keys).div_ceil(u64::from(shard_count)))
        .unwrap_or(usize::MAX)
        .max(1)
}

fn sharded_retry_concurrency(process: &DurableProcess, data_shards: usize) -> usize {
    let shard_count = data_shards.max(1) as u32;
    usize::try_from(u64::from(process.retry_concurrency).div_ceil(u64::from(shard_count)))
        .unwrap_or(usize::MAX)
        .max(1)
}

fn release_process_execution(shard_state: &mut ProcessShardState, isolated_retry: bool) {
    if isolated_retry {
        shard_state.retry_running = shard_state.retry_running.saturating_sub(1);
    } else {
        shard_state.running = shard_state.running.saturating_sub(1);
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum ProcessFailureDisposition {
    Retry,
    Quarantine,
}

pub(crate) fn process_failure_disposition(
    attempt: u32,
    max_attempts: u32,
) -> ProcessFailureDisposition {
    if attempt >= max_attempts {
        ProcessFailureDisposition::Quarantine
    } else {
        ProcessFailureDisposition::Retry
    }
}

pub(crate) fn retry_or_quarantine_sharded_execution(
    transaction: &mut Transaction<'_>,
    process: &DurableProcess,
    shard: usize,
    mut ready_execution: ProcessReadyExecution,
    failure: String,
    shard_state: &mut ProcessShardState,
) -> Result<()> {
    let execution = &mut ready_execution.execution;
    release_process_execution(shard_state, execution.isolated_retry);
    execution.attempt = execution.attempt.saturating_add(1);
    execution.last_failure = Some(failure.clone());
    if process_failure_disposition(execution.attempt, process.max_attempts)
        == ProcessFailureDisposition::Quarantine
    {
        transaction.put(
            process_quarantine_key(&process.process_id, shard, execution.sequence),
            &ProcessQuarantineRecord {
                process_id: process.process_id.clone(),
                key: execution.key.clone(),
                sequence: execution.sequence,
                event_time: execution.event_time,
                record: execution.record.clone(),
                attempts: execution.attempt,
                failure: failure.clone(),
                quarantined_at: now(),
            },
        )?;
        update_process_outcome(
            transaction,
            &execution.process_id,
            &execution.key,
            &execution.record,
            ProcessOutcomeUpdate {
                status: "FAILED",
                attempts: execution.attempt,
                output_message_ids: Vec::new(),
                failure: Some(failure),
            },
        )?;
        shard_state.active_keys.remove(&execution.key);
        shard_state.failed += 1;
        shard_state.quarantined += 1;
    } else {
        execution.isolated_retry = true;
        execution.available_at =
            now() + (0.1 * 2f64.powi((execution.attempt.saturating_sub(1)) as i32)).min(5.0);
        execution.enqueued_at = execution.available_at;
        update_process_outcome(
            transaction,
            &execution.process_id,
            &execution.key,
            &execution.record,
            ProcessOutcomeUpdate {
                status: "PENDING",
                attempts: execution.attempt,
                output_message_ids: Vec::new(),
                failure: Some(failure),
            },
        )?;
        transaction.put(
            process_ready_key(shard, &process.process_id, execution.sequence),
            &ready_execution,
        )?;
        shard_state.retry_pending += 1;
    }
    Ok(())
}

fn start_sharded_process_execution(
    transaction: &mut Transaction<'_>,
    process: &DurableProcess,
    shard: usize,
    item: ShardedProcessMailboxItem,
    prior_state: Option<ProcessStateRecord>,
) -> Result<()> {
    let timestamp = now();
    let execution = ShardedProcessExecution {
        process_id: process.process_id.clone(),
        sequence: item.sequence,
        key: item.key.clone(),
        event_time: item.event_time,
        record: item.record,
        prior_state,
        state_version: process.state_version,
        build_id: process.active_build_id.clone(),
        shard: u32::try_from(shard)?,
        available_at: timestamp,
        enqueued_at: timestamp,
        attempt: 0,
        isolated_retry: false,
        lease_owner: None,
        lease_expires: None,
        task_token: None,
        last_failure: None,
    };
    let execution_key = process_shard_execution_key(shard, &process.process_id, item.sequence);
    transaction.put(
        process_ready_key(shard, &process.process_id, item.sequence),
        &ProcessReadyExecution {
            execution_key,
            execution,
        },
    )
}

pub(crate) fn sharded_process_envelope(
    execution: &ShardedProcessExecution,
    record: &StreamRecord,
    prior_state: Option<&ProcessStateRecord>,
) -> Value {
    json!({
        "process_id": execution.process_id,
        "key": execution.key,
        "event_time": execution.event_time,
        "record": record,
        "state": prior_state.map(|state| &state.value),
        "state_version": prior_state.map(|state| state.version),
        "target_state_version": execution.state_version,
        "build_id": execution.build_id,
    })
}

pub(crate) fn finish_sharded_process_execution(
    transaction: &mut Transaction<'_>,
    execution: &ShardedProcessExecution,
    input_sequence: u64,
    result: Value,
    shard_state: &mut ProcessShardState,
) -> Result<()> {
    let transition = result
        .as_object()
        .filter(|value| {
            value
                .get("__highwater_transition__")
                .and_then(Value::as_bool)
                == Some(true)
        })
        .ok_or_else(|| anyhow!("process handler returned an invalid state transition"))?;
    let next_state = transition
        .get("state")
        .cloned()
        .ok_or_else(|| anyhow!("process state transition is missing state"))?;
    transaction.put_encoded(
        process_state_key(&execution.process_id, &execution.key),
        &ProcessStateRecord {
            version: execution.state_version,
            build_id: execution.build_id.clone(),
            input_sequence,
            event_time: execution.event_time,
            value: next_state,
        },
    )?;
    let mut output_message_ids = Vec::new();
    if let Some(output) = transition.get("emit").filter(|value| !value.is_null()) {
        let output_key = process_output_key(&execution.process_id, &execution.key);
        let prior_output = transaction.get::<Value>(&output_key)?;
        transaction.put(output_key, output)?;
        let changes = match prior_output {
            Some(prior) => vec![
                (ChangeKind::UpdateBefore, prior),
                (ChangeKind::UpdateAfter, output.clone()),
            ],
            None => vec![(ChangeKind::Insert, output.clone())],
        };
        for (kind, row) in changes {
            shard_state.next_output_sequence += 1;
            let sequence = (u64::from(execution.shard) << 56) | shard_state.next_output_sequence;
            let change = DifferentialChange {
                operator_id: execution.process_id.clone(),
                sequence,
                key: Some(execution.key.clone()),
                event_time: execution.event_time,
                kind,
                diff: kind.weight(),
                row,
            };
            transaction.put(
                format!(
                    "{}{sequence:020}",
                    operator_change_prefix(&execution.process_id)
                ),
                &change,
            )?;
            if transaction
                .get::<OperatorEdge>(&operator_edge_key(&execution.process_id))?
                .is_some_and(|edge| edge.status == "ACTIVE")
            {
                transaction.put(
                    format!(
                        "{}{sequence:020}",
                        operator_edge_pending_prefix(&execution.process_id)
                    ),
                    &change,
                )?;
            }
        }
        output_message_ids.push(enqueue_process_output(
            transaction,
            execution.shard,
            &execution.process_id,
            &execution.key,
            &execution.record,
            output,
        )?);
    }
    update_process_outcome(
        transaction,
        &execution.process_id,
        &execution.key,
        &execution.record,
        ProcessOutcomeUpdate {
            status: "COMMITTED",
            attempts: execution.attempt,
            output_message_ids,
            failure: None,
        },
    )?;
    shard_state.active_keys.remove(&execution.key);
    release_process_execution(shard_state, execution.isolated_retry);
    shard_state.completed += 1;
    Ok(())
}
pub(crate) async fn create_process(
    State(app): State<AppState>,
    Json(request): Json<CreateProcessRequest>,
) -> Result<impl IntoResponse, ApiError> {
    if request.process_id.trim().is_empty()
        || request.stream.trim().is_empty()
        || request.workflow_type.trim().is_empty()
        || request.task_queue.trim().is_empty()
        || request.build_id.trim().is_empty()
    {
        return Err(ApiError(anyhow!(
            "process_id, stream, workflow_type, task_queue, and build_id must not be empty"
        )));
    }
    if request.state_version == 0
        || request.max_concurrent_keys == 0
        || request.mailbox_capacity == 0
        || request.retry_concurrency == 0
        || request.max_attempts == 0
        || request.batch_max_size == 0
        || request.batch_max_size > 16_384
        || !request.batch_max_delay.is_finite()
        || request.batch_max_delay < 0.0
        || request
            .versioned_streams
            .iter()
            .any(|stream| stream.trim().is_empty())
        || request
            .versioned_streams
            .iter()
            .collect::<HashSet<_>>()
            .len()
            != request.versioned_streams.len()
    {
        return Err(ApiError(anyhow!(
            "process state version, concurrency, retry policy, capacity, and batch settings are invalid"
        )));
    }
    let mut process = DurableProcess {
        process_id: request.process_id,
        stream: request.stream,
        workflow_type: request.workflow_type,
        key_field: request.key_field,
        event_time_field: request.event_time_field,
        state_version: request.state_version,
        active_build_id: request.build_id,
        versioned_streams: request.versioned_streams,
        task_queue: request.task_queue,
        event_time_gate: request.event_time_gate,
        max_concurrent_keys: request.max_concurrent_keys,
        mailbox_capacity: request.mailbox_capacity,
        retry_concurrency: request.retry_concurrency,
        max_attempts: request.max_attempts,
        direct_ingress: request.direct_ingress,
        discard_input_on_success: request.discard_input_on_success,
        batch_max_size: request.batch_max_size,
        batch_max_delay: request.batch_max_delay,
        status: "ACTIVE".to_owned(),
        created_at: now(),
        pending: 0,
        running: 0,
        completed: 0,
        failed: 0,
        retrying: 0,
        quarantined: 0,
    };
    let process_id = process.process_id.clone();
    let mut created = false;
    app.commit(|transaction| {
        if transaction
            .get::<StreamConfig>(&stream_config_key(&process.stream))?
            .is_none()
        {
            bail!("process input stream not found: {}", process.stream);
        }
        for stream in &process.versioned_streams {
            if transaction
                .get::<StreamConfig>(&stream_config_key(stream))?
                .is_none()
            {
                bail!("versioned stream not found: {stream}");
            }
            let ready_key = versioned_index_ready_key(stream);
            if transaction.get::<bool>(&ready_key)?.is_none() {
                for (_, record) in
                    transaction.scan::<StreamRecord>(&stream_record_prefix(stream))?
                {
                    if record.key.as_deref().is_some_and(|key| !key.is_empty()) {
                        transaction.put(versioned_record_key(stream, &record), &record)?;
                    }
                }
                transaction.put(ready_key, &true)?;
            }
        }
        let storage_key = process_key(&process.process_id);
        if let Some(mut existing) = transaction.get::<DurableProcess>(&storage_key)? {
            if existing.stream == process.stream
                && existing.workflow_type == process.workflow_type
                && existing.key_field == process.key_field
                && existing.event_time_field == process.event_time_field
                && existing.event_time_gate == process.event_time_gate
                && existing.direct_ingress == process.direct_ingress
            {
                if process.state_version < existing.state_version {
                    bail!(
                        "process {} cannot downgrade state from version {} to {}",
                        process.process_id,
                        existing.state_version,
                        process.state_version,
                    );
                }
                if process.state_version > existing.state_version
                    && !(existing.state_version..process.state_version)
                        .all(|version| request.migrations_from.contains(&version))
                {
                    bail!(
                        "process {} upgrade from state version {} to {} has no declared migration",
                        process.process_id,
                        existing.state_version,
                        process.state_version,
                    );
                }
                if let Some(edge) =
                    transaction.get::<OperatorEdge>(&operator_edge_key(&process.process_id))?
                {
                    let mut inputs = vec![process.stream.clone()];
                    inputs.extend(process.versioned_streams.iter().cloned());
                    if stream_reaches_any(
                        transaction,
                        &edge.output_stream,
                        &inputs,
                        &mut HashSet::new(),
                    )? {
                        bail!("process reference update would create an operator edge cycle");
                    }
                }
                existing.state_version = process.state_version;
                existing.active_build_id = process.active_build_id.clone();
                existing.versioned_streams = process.versioned_streams.clone();
                existing.task_queue = process.task_queue.clone();
                existing.max_concurrent_keys = process.max_concurrent_keys;
                existing.mailbox_capacity = process.mailbox_capacity;
                existing.retry_concurrency = process.retry_concurrency;
                existing.max_attempts = process.max_attempts;
                existing.discard_input_on_success = process.discard_input_on_success;
                existing.batch_max_size = process.batch_max_size;
                existing.batch_max_delay = process.batch_max_delay;
                transaction.put(&storage_key, &existing)?;
                return Ok(());
            }
            bail!(
                "process {} cannot change its input, workflow, key, event-time field, gate, or ingress mode",
                process.process_id
            );
        }
        let records = if process.direct_ingress {
            Vec::new()
        } else {
            transaction
                .scan::<StreamRecord>(&stream_record_prefix(&process.stream))?
                .into_iter()
                .map(|(_, record)| record)
                .collect::<Vec<_>>()
        };
        if records.len() as u64 > process.mailbox_capacity {
            return Err(StreamCapacityError(format!(
                "process {} backfill exceeds mailbox capacity",
                process.process_id
            ))
            .into());
        }
        for record in records {
            let key = record
                .key
                .clone()
                .filter(|key| !key.is_empty())
                .ok_or_else(|| anyhow!("process input records require a non-empty key"))?;
            let item = ProcessMailboxItem {
                process_id: process.process_id.clone(),
                sequence: record.sequence,
                key,
                event_time: record.event_time,
                record,
            };
            transaction.put(
                process_mailbox_key(&process.process_id, item.sequence),
                &item,
            )?;
            process.pending += 1;
        }
        created = true;
        transaction.put(&storage_key, &process)?;
        if !process.direct_ingress {
            transaction.put(
                process_stream_key(&process.stream, &process.process_id),
                &process.process_id,
            )?;
        }
        dispatch_process(transaction, &storage_key, &mut process)
    })?;
    let process = app
        .store
        .get::<DurableProcess>(&process_key(&process_id))?
        .ok_or_else(|| anyhow!("process missing after creation: {process_id}"))?;
    Ok((
        if created {
            StatusCode::CREATED
        } else {
            StatusCode::OK
        },
        Json(process),
    ))
}

pub(crate) fn append_process_shard_records(
    transaction: &mut Transaction<'_>,
    app: &AppState,
    process_id: &str,
    shard: usize,
    records: &[(usize, AppendStreamRecordRequest)],
    detailed: bool,
) -> Result<ProcessIngressResult> {
    let data_shards = app.shard_locks.len().saturating_sub(1).max(1);
    let mut result = ProcessIngressResult {
        responses: Vec::with_capacity(if detailed { records.len() } else { 0 }),
        ..ProcessIngressResult::default()
    };
    let process = transaction
        .get::<DurableProcess>(&process_key(process_id))?
        .ok_or_else(|| anyhow!("process not found: {process_id}"))?;
    if !process.direct_ingress {
        bail!("process {process_id} accepts events only through its input stream");
    }
    let state_key = process_shard_state_key(process_id, shard);
    let mut shard_state = transaction
        .get::<ProcessShardState>(&state_key)?
        .unwrap_or_default();
    let concurrency = sharded_process_concurrency(&process, data_shards);
    let event_keys = records
        .iter()
        .map(|(_, request)| {
            request
                .key
                .as_deref()
                .filter(|key| !key.is_empty())
                .map(str::to_owned)
                .ok_or_else(|| anyhow!("process events require a non-empty key"))
        })
        .collect::<Result<Vec<_>>>()?;
    let event_ids = records
        .iter()
        .map(|(_, request)| {
            request
                .event_id
                .clone()
                .filter(|event_id| !event_id.trim().is_empty())
                .ok_or_else(|| anyhow!("direct process events require a stable event_id"))
        })
        .collect::<Result<Vec<_>>>()?;
    let dedup_keys = event_ids
        .iter()
        .zip(&event_keys)
        .map(|(event_id, key)| {
            format!(
                "process-event/{}/{shard:04}/{}/{}",
                encoded(process_id),
                encoded(key),
                encoded(event_id)
            )
        })
        .collect::<Vec<_>>();
    let deduplicated = transaction.multi_get::<StreamRecord>(&dedup_keys)?;
    let state_keys = event_keys
        .iter()
        .map(|key| process_state_key(process_id, key))
        .collect::<Vec<_>>();
    let prior_states = transaction.multi_get::<ProcessStateRecord>(&state_keys)?;
    let mut unique_new_records = HashSet::new();
    let new_records = dedup_keys
        .iter()
        .zip(&deduplicated)
        .filter(|(key, record)| record.is_none() && unique_new_records.insert((*key).clone()))
        .count() as u64;
    let capacity = process
        .mailbox_capacity
        .div_ceil(u64::try_from(data_shards)?);
    if shard_state.pending
        + shard_state.running
        + shard_state.retry_pending
        + shard_state.retry_running
        + new_records
        > capacity
    {
        return Err(StreamCapacityError(format!(
            "process {process_id} shard {shard} mailbox is full"
        ))
        .into());
    }
    let mut claimed_keys = HashSet::new();
    let mut admitted_dedup = HashMap::new();
    for (position, (index, request)) in records.iter().enumerate() {
        let key = &event_keys[position];
        if !request.event_time.is_finite() {
            bail!("process event time must be finite");
        }
        let event_id = event_ids[position].clone();
        let dedup_key = &dedup_keys[position];
        let duplicate = deduplicated[position]
            .clone()
            .or_else(|| admitted_dedup.get(dedup_key).cloned());
        if let Some(record) = duplicate {
            if record.event_time != request.event_time
                || record.key.as_deref() != Some(key.as_str())
                || record.value != request.value
                || record.kind != request.kind
            {
                bail!("event_id was already used with different process event contents");
            }
            result.duplicates += 1;
            if detailed {
                result.responses.push((
                    *index,
                    json!({
                        "record": &record,
                        "disposition": "duplicate",
                        "watermark_before": null,
                        "watermark": null,
                        "finalized": false,
                    }),
                ));
            }
            continue;
        }
        shard_state.next_sequence += 1;
        if shard_state.next_sequence >= (1_u64 << 56) {
            bail!("process shard sequence space exhausted");
        }
        let sequence = (u64::try_from(shard)? << 56) | shard_state.next_sequence;
        let key_group = key_group_for(Some(key), u32::try_from(shard)?, app.key_group_count);
        let record = StreamRecord {
            stream: process.stream.clone(),
            partition: u32::try_from(shard.saturating_sub(1))?,
            offset: shard_state.next_sequence,
            sequence,
            event_time: request.event_time,
            ingestion_time: now(),
            key: Some(key.clone()),
            value: request.value.clone(),
            kind: request.kind,
            event_id: Some(event_id),
            key_group,
            owner_epoch: 0,
            source_id: request.source_id.clone(),
            source_partition: request.source_partition,
            source_offset: request.source_offset,
            late: false,
            too_late: false,
        };
        let item = ShardedProcessMailboxItem {
            process_id: process_id.to_owned(),
            sequence,
            key: key.clone(),
            event_time: request.event_time,
            record: record.clone(),
        };
        ensure_pending_process_outcome(transaction, process_id, key, &record)?;
        admitted_dedup.insert(dedup_key.clone(), record.clone());
        if shard_state.running < concurrency as u64
            && !shard_state.active_keys.contains(key)
            && claimed_keys.insert(key.clone())
        {
            shard_state.active_keys.insert(key.clone());
            start_sharded_process_execution(
                transaction,
                &process,
                shard,
                item,
                prior_states[position].clone(),
            )?;
            shard_state.running += 1;
        } else {
            transaction.put(
                process_shard_mailbox_key(process_id, shard, sequence),
                &item,
            )?;
            shard_state.pending += 1;
        }
        result.accepted += 1;
        transaction.put(dedup_key, &record)?;
        if detailed {
            result.responses.push((
                *index,
                json!({
                    "record": record,
                    "disposition": "accepted",
                    "watermark_before": null,
                    "watermark": null,
                    "finalized": false,
                }),
            ));
        }
    }
    if shard_state.pending > 0 {
        dispatch_sharded_process(transaction, &process, shard, data_shards, &mut shard_state)?;
    }
    transaction.put(state_key, &shard_state)?;
    Ok(result)
}

type ProcessIngressBatch = (String, Vec<(usize, AppendStreamRecordRequest)>, bool);

pub(crate) fn commit_process_ingress_batch(
    app: &AppState,
    shard: usize,
    requests: &[ProcessIngressBatch],
) -> Result<Vec<ProcessIngressResult>> {
    let mut results = Vec::with_capacity(requests.len());
    app.commit_shard(shard, |transaction| {
        owned_process_partition_epoch(transaction, &app.runtime_id, shard)?;
        for (process_id, records, detailed) in requests {
            results.push(append_process_shard_records(
                transaction,
                app,
                process_id,
                shard,
                records,
                *detailed,
            )?);
        }
        Ok(())
    })?;
    Ok(results)
}

pub(crate) async fn process_partition_loop(
    app: AppState,
    shard: usize,
    mut receiver: mpsc::Receiver<ProcessPartitionCommand>,
) {
    let mut pending = VecDeque::new();
    loop {
        let command = match pending.pop_front() {
            Some(command) => command,
            None => {
                let Some(command) = receiver.recv().await else {
                    break;
                };
                command
            }
        };
        match command {
            ProcessPartitionCommand::Ingress(first) => {
                let mut requests = vec![first];
                let mut records = requests[0].records.len();
                let deadline = tokio::time::sleep(std::time::Duration::from_micros(500));
                tokio::pin!(deadline);
                while records < 10_000 {
                    tokio::select! {
                        biased;
                        command = receiver.recv() => {
                            let Some(command) = command else { break };
                            match command {
                                ProcessPartitionCommand::Ingress(request) => {
                                    records += request.records.len();
                                    requests.push(request);
                                }
                                command => {
                                    pending.push_back(command);
                                    break;
                                }
                            }
                        }
                        () = &mut deadline => break,
                    }
                }
                let inputs = requests
                    .iter()
                    .map(|request| {
                        (
                            request.process_id.clone(),
                            request.records.clone(),
                            request.detailed,
                        )
                    })
                    .collect::<Vec<_>>();
                let task_app = app.clone();
                let committed = tokio::task::spawn_blocking(move || {
                    commit_process_ingress_batch(&task_app, shard, &inputs)
                })
                .await;
                match committed {
                    Ok(Ok(results)) => {
                        for (request, result) in requests.into_iter().zip(results) {
                            let _ = request.response.send(Ok(result));
                        }
                    }
                    Ok(Err(error)) => {
                        let message = format!("{error:#}");
                        for request in requests {
                            let _ = request.response.send(Err(message.clone()));
                        }
                    }
                    Err(error) => {
                        let message = format!("process ingress task failed: {error}");
                        for request in requests {
                            let _ = request.response.send(Err(message.clone()));
                        }
                    }
                }
            }
            ProcessPartitionCommand::Poll { request, response } => {
                let task_app = app.clone();
                let polled = tokio::task::spawn_blocking(move || {
                    poll_process_partition(&task_app, shard, request)
                })
                .await;
                let result = match polled {
                    Ok(Ok(activation)) => Ok(activation),
                    Ok(Err(error)) => Err(format!("{error:#}")),
                    Err(error) => Err(format!("process partition poll failed: {error}")),
                };
                let _ = response.send(result);
            }
            ProcessPartitionCommand::Complete {
                completion,
                response,
            } => {
                let task_app = app.clone();
                let completed = tokio::task::spawn_blocking(move || {
                    complete_process_partition(&task_app, shard, completion)
                })
                .await;
                let result = match completed {
                    Ok(Ok(())) => Ok(()),
                    Ok(Err(error)) => Err(format!("{error:#}")),
                    Err(error) => Err(format!("process partition completion failed: {error}")),
                };
                let _ = response.send(result);
            }
            ProcessPartitionCommand::Renew { renewal, response } => {
                let task_app = app.clone();
                let renewed = tokio::task::spawn_blocking(move || {
                    renew_process_partition_lease(&task_app, shard, renewal)
                })
                .await;
                let result = match renewed {
                    Ok(Ok(lease_expires)) => Ok(lease_expires),
                    Ok(Err(error)) => Err(format!("{error:#}")),
                    Err(error) => Err(format!("process partition renewal failed: {error}")),
                };
                let _ = response.send(result);
            }
        }
    }
}

pub(crate) async fn admit_process_records(
    app: &AppState,
    process_id: &str,
    records: Vec<AppendStreamRecordRequest>,
    detailed: bool,
) -> Result<ProcessIngressResult, ApiError> {
    app.store.sync_remote_shard(0)?;
    let process = app
        .store
        .get::<DurableProcess>(&process_key(process_id))?
        .ok_or_else(|| anyhow!("process not found: {process_id}"))?;
    if process.event_time_gate != EventTimeGate::Immediate {
        return Err(ApiError(anyhow!(
            "event-time-gated processes must publish through their input stream"
        )));
    }
    let mut grouped = BTreeMap::<usize, Vec<(usize, AppendStreamRecordRequest)>>::new();
    for (index, record) in records.into_iter().enumerate() {
        let key = record
            .key
            .as_deref()
            .filter(|key| !key.is_empty())
            .ok_or_else(|| anyhow!("process events require a non-empty key"))?;
        grouped
            .entry(app.process_shard(key))
            .or_default()
            .push((index, record));
    }
    let mut receivers = Vec::new();
    let mut combined = ProcessIngressResult::default();
    for (shard, records) in grouped {
        app.store.sync_remote_shard(shard)?;
        let owner = app
            .store
            .get::<ProcessPartitionOwner>(&process_partition_owner_key(shard))?
            .ok_or_else(|| anyhow!("process partition {shard} is unassigned"))?;
        if owner.owner == app.runtime_id && owner.status == "ACTIVE" {
            let sender = app
                .partition_senders
                .get(shard)
                .and_then(Option::as_ref)
                .ok_or_else(|| anyhow!("process partition {shard} is not running locally"))?;
            let (response, receiver) = oneshot::channel();
            sender
                .send(ProcessPartitionCommand::Ingress(ProcessIngressRequest {
                    process_id: process_id.to_owned(),
                    records,
                    detailed,
                    response,
                }))
                .await
                .map_err(|_| anyhow!("process ingress shard {shard} stopped"))?;
            receivers.push(receiver);
            continue;
        }
        if owner.status != "ACTIVE" || owner.endpoint.is_empty() {
            return Err(ApiError(
                StreamCapacityError(format!("process partition {shard} is moving")).into(),
            ));
        }
        let token = app
            .cluster_token
            .as_deref()
            .ok_or_else(|| anyhow!("remote partition routing is not configured"))?;
        let response = app
            .http_client
            .post(format!(
                "{}/internal/v1/processes/{}/partitions/{shard}/events",
                owner.endpoint.trim_end_matches('/'),
                percent_encoding::utf8_percent_encode(
                    process_id,
                    percent_encoding::NON_ALPHANUMERIC
                )
            ))
            .bearer_auth(token)
            .json(&RemoteProcessIngressRequest { records, detailed })
            .send()
            .await?;
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(ApiError(anyhow!(
                "remote partition {shard} rejected ingress with {status}: {body}"
            )));
        }
        let result: ProcessIngressResult = response.json().await?;
        combined.responses.extend(result.responses);
        combined.accepted += result.accepted;
        combined.duplicates += result.duplicates;
    }
    for receiver in receivers {
        let result = receiver
            .await
            .map_err(|_| anyhow!("process ingress response was dropped"))?
            .map_err(|error| anyhow!(error))?;
        combined.responses.extend(result.responses);
        combined.accepted += result.accepted;
        combined.duplicates += result.duplicates;
    }
    combined.responses.sort_by_key(|(index, _)| *index);
    Ok(combined)
}

pub(crate) async fn append_remote_process_records(
    headers: HeaderMap,
    State(app): State<AppState>,
    Path((process_id, partition)): Path<(String, u32)>,
    Json(request): Json<RemoteProcessIngressRequest>,
) -> Result<impl IntoResponse, ApiError> {
    app.authorize_cluster(&headers)?;
    let shard = partition as usize;
    if shard == 0 || shard >= app.partition_senders.len() {
        return Err(ApiError(anyhow!(
            "process partition {partition} does not exist"
        )));
    }
    app.store.sync_remote_shard(shard)?;
    let owner = app
        .store
        .get::<ProcessPartitionOwner>(&process_partition_owner_key(shard))?
        .ok_or_else(|| anyhow!("process partition {partition} is unassigned"))?;
    if owner.owner != app.runtime_id || owner.status != "ACTIVE" {
        return Err(ApiError(anyhow!("process partition {partition} is fenced")));
    }
    let sender = app.partition_senders[shard]
        .as_ref()
        .ok_or_else(|| anyhow!("process partition {partition} is not running locally"))?;
    let (response, receiver) = oneshot::channel();
    sender
        .send(ProcessPartitionCommand::Ingress(ProcessIngressRequest {
            process_id,
            records: request.records,
            detailed: request.detailed,
            response,
        }))
        .await
        .map_err(|_| anyhow!("process partition {partition} stopped"))?;
    let result = receiver
        .await
        .map_err(|_| anyhow!("process partition ingress response was dropped"))?
        .map_err(anyhow::Error::msg)?;
    Ok(Json(result))
}

pub(crate) async fn append_process_records(
    State(app): State<AppState>,
    Path(process_id): Path<String>,
    Json(request): Json<AppendStreamRecordsRequest>,
) -> Result<impl IntoResponse, ApiError> {
    if request.records.is_empty() || request.records.len() > 10_000 {
        return Err(ApiError(anyhow!(
            "process record batches must contain between 1 and 10000 events"
        )));
    }
    Ok((
        StatusCode::CREATED,
        Json(
            admit_process_records(&app, &process_id, request.records, true)
                .await?
                .responses
                .into_iter()
                .map(|(_, response)| response)
                .collect::<Vec<_>>(),
        ),
    ))
}

pub(crate) fn take_packed<const N: usize>(body: &[u8], cursor: &mut usize) -> Result<[u8; N]> {
    let end = cursor
        .checked_add(N)
        .filter(|end| *end <= body.len())
        .ok_or_else(|| anyhow!("packed process batch is truncated"))?;
    let value = body[*cursor..end].try_into()?;
    *cursor = end;
    Ok(value)
}

pub(crate) fn parse_packed_process_records(body: &[u8]) -> Result<Vec<AppendStreamRecordRequest>> {
    if body.len() < 8 || &body[..4] != b"TCP1" {
        bail!("invalid packed process batch header");
    }
    let mut cursor = 4;
    let count = u32::from_le_bytes(take_packed(body, &mut cursor)?) as usize;
    if count == 0 || count > 100_000 {
        bail!("packed process batches must contain between 1 and 100000 events");
    }
    let mut records = Vec::with_capacity(count);
    for _ in 0..count {
        let event_time = f64::from_le_bytes(take_packed(body, &mut cursor)?);
        let key_len = u32::from_le_bytes(take_packed(body, &mut cursor)?) as usize;
        let value_len = u32::from_le_bytes(take_packed(body, &mut cursor)?) as usize;
        let event_id = Uuid::from_bytes(take_packed(body, &mut cursor)?).to_string();
        let key_end = cursor
            .checked_add(key_len)
            .filter(|end| *end <= body.len())
            .ok_or_else(|| anyhow!("packed process key is truncated"))?;
        let key = std::str::from_utf8(&body[cursor..key_end])?.to_owned();
        cursor = key_end;
        let value_end = cursor
            .checked_add(value_len)
            .filter(|end| *end <= body.len())
            .ok_or_else(|| anyhow!("packed process value is truncated"))?;
        let value = serde_json::from_slice(&body[cursor..value_end])?;
        cursor = value_end;
        records.push(AppendStreamRecordRequest {
            partition: 0,
            event_time,
            key: Some(key),
            value,
            kind: ChangeKind::Upsert,
            event_id: Some(event_id),
            source_id: None,
            source_partition: None,
            source_offset: None,
            source_epoch: None,
            source_checkpoint: None,
        });
    }
    if cursor != body.len() {
        bail!("packed process batch has trailing bytes");
    }
    Ok(records)
}

pub(crate) async fn append_packed_process_records(
    State(app): State<AppState>,
    Path(process_id): Path<String>,
    body: axum::body::Bytes,
) -> Result<impl IntoResponse, ApiError> {
    let records = parse_packed_process_records(&body)?;
    let received = records.len();
    let result = admit_process_records(&app, &process_id, records, false).await?;
    Ok((
        StatusCode::CREATED,
        Json(json!({
            "accepted": result.accepted,
            "duplicates": result.duplicates,
            "received": received,
        })),
    ))
}

pub(crate) async fn poll_process_batch(
    State(app): State<AppState>,
    Json(request): Json<PollRequest>,
) -> Result<impl IntoResponse, ApiError> {
    if request.protocol_version != PROTOCOL_VERSION {
        return Err(ApiError(anyhow!("unsupported protocol version")));
    }
    app.authorize_poll(&request)?;
    let data_shards = app.shard_locks.len().saturating_sub(1);
    if data_shards == 0 {
        return Ok(Json(None::<ProcessActivationBatch>));
    }
    let shard = match request.partition_id {
        Some(partition_id) => {
            let shard = usize::try_from(partition_id)?;
            if shard == 0 || shard > data_shards {
                return Err(ApiError(anyhow!(
                    "process partition {partition_id} is outside the data partitions"
                )));
            }
            shard
        }
        None => 1 + usize::try_from(request.shard_cursor % data_shards as u64)?,
    };
    let sender = app
        .partition_senders
        .get(shard)
        .and_then(Option::as_ref)
        .ok_or_else(|| anyhow!("process partition {shard} is unavailable"))?;
    let (response, receiver) = oneshot::channel();
    sender
        .send(ProcessPartitionCommand::Poll { request, response })
        .await
        .map_err(|_| anyhow!("process partition {shard} stopped"))?;
    let activation = receiver
        .await
        .map_err(|_| anyhow!("process partition poll response was dropped"))?
        .map_err(|error| anyhow!(error))?;
    Ok(Json(activation))
}

pub(crate) fn poll_process_partition(
    app: &AppState,
    shard: usize,
    request: PollRequest,
) -> Result<Option<ProcessActivationBatch>> {
    let data_shards = app.shard_locks.len().saturating_sub(1).max(1);
    let mut activation_batch = None;
    app.commit_shard(shard, |transaction| {
        let timestamp = now();
        let ready = transaction
            .scan_limit::<ProcessReadyExecution>(&process_ready_prefix(shard), 16_384)?;
        let mut processes = HashMap::new();
        let mut selected_process = None;
        let mut candidates = Vec::new();
        for (position, (ready_key, ready_execution)) in ready.into_iter().enumerate() {
            let execution = &ready_execution.execution;
            if execution.available_at > timestamp {
                continue;
            }
            if !processes.contains_key(&execution.process_id) {
                if let Some(process) =
                    transaction.get::<DurableProcess>(&process_key(&execution.process_id))?
                {
                    processes.insert(execution.process_id.clone(), process);
                } else {
                    continue;
                }
            }
            let process = &processes[&execution.process_id];
            if request
                .task_queue
                .as_ref()
                .is_some_and(|queue| queue != &process.task_queue)
                || !request.build_ids.contains(&execution.build_id)
            {
                continue;
            }
            if selected_process.is_none() {
                selected_process = Some(execution.process_id.clone());
            }
            if selected_process.as_deref() == Some(&execution.process_id) {
                candidates.push((position, ready_key, ready_execution));
            }
        }
        let Some(process_id) = selected_process else {
            return Ok(());
        };
        let process = &processes[&process_id];
        let max_size = process.batch_max_size.clamp(1, 16_384) as usize;
        if candidates.len() < max_size
            && timestamp < candidates[0].2.execution.enqueued_at + process.batch_max_delay
        {
            return Ok(());
        }
        let token = Uuid::new_v4().to_string();
        let (owner_epoch, activation_sequence) =
            next_process_partition_activation(transaction, &app.runtime_id, shard)?;
        let lease_expires = timestamp + request.lease_seconds;
        let mut executions = Vec::new();
        let mut envelopes = Vec::new();
        let state_key = process_shard_state_key(&process_id, shard);
        let mut shard_state = transaction
            .get::<ProcessShardState>(&state_key)?
            .unwrap_or_default();
        let retry_slots = sharded_retry_concurrency(process, data_shards)
            .saturating_sub(usize::try_from(shard_state.retry_running).unwrap_or(usize::MAX));
        let mut selected = Vec::with_capacity(max_size);
        let mut selected_retries = 0_usize;
        for mut candidate in candidates {
            let is_retry = candidate.2.execution.attempt > 0;
            if is_retry && selected_retries >= retry_slots {
                continue;
            }
            if is_retry {
                selected_retries += 1;
                if !candidate.2.execution.isolated_retry {
                    shard_state.running = shard_state.running.saturating_sub(1);
                    candidate.2.execution.isolated_retry = true;
                }
            }
            selected.push(candidate);
            if selected.len() == max_size {
                break;
            }
        }
        if selected.is_empty() {
            return Ok(());
        }
        let contiguous = selected.windows(2).all(|pair| pair[1].0 == pair[0].0 + 1);
        if contiguous {
            let first_key = &selected[0].1;
            let end_key = format!("{}\0", selected.last().expect("selected batch").1);
            transaction.delete_range(first_key, end_key);
        }
        for (_, ready_key, ready_execution) in selected {
            let execution = &ready_execution.execution;
            if execution.available_at > timestamp {
                continue;
            }
            if !contiguous {
                transaction.delete(&ready_key);
            }
            envelopes.push(sharded_process_envelope(
                execution,
                &execution.record,
                execution.prior_state.as_ref(),
            ));
            executions.push(ready_execution);
        }
        if !executions.is_empty() {
            shard_state.retry_pending = shard_state
                .retry_pending
                .saturating_sub(u64::try_from(selected_retries)?);
            shard_state.retry_running += u64::try_from(selected_retries)?;
            transaction.put(&state_key, &shard_state)?;
            transaction.put(
                process_batch_lease_key(&token),
                &ProcessBatchLease {
                    process_id: process_id.clone(),
                    shard: shard as u32,
                    owner_epoch,
                    activation_sequence,
                    worker_id: request.worker_id.clone(),
                    lease_expires,
                    executions,
                },
            )?;
            activation_batch = Some(ProcessActivationBatch {
                protocol_version: PROTOCOL_VERSION,
                lease_token: token,
                partition_id: u32::try_from(shard)?,
                owner_epoch,
                activation_sequence,
                lease_expires,
                process_id,
                workflow_type: process.workflow_type.clone(),
                build_id: process.active_build_id.clone(),
                shard: u32::try_from(shard)?,
                envelopes,
            });
        }
        Ok(())
    })?;
    Ok(activation_batch)
}

pub(crate) async fn complete_process_batch(
    State(app): State<AppState>,
    Json(completion): Json<ProcessCompletionBatch>,
) -> Result<impl IntoResponse, ApiError> {
    if completion.items.is_empty() || completion.items.len() > 16_384 {
        return Err(ApiError(anyhow!(
            "process completion batches must contain between 1 and 16384 items"
        )));
    }
    if completion.protocol_version != PROTOCOL_VERSION {
        return Err(ApiError(anyhow!("unsupported protocol version")));
    }
    let shard = usize::try_from(completion.partition_id)?;
    let data_shards = app.shard_locks.len().saturating_sub(1);
    if shard == 0 || shard > data_shards {
        return Err(ApiError(anyhow!(
            "process partition {} is outside the data partitions",
            completion.partition_id,
        )));
    }
    let sender = app
        .partition_senders
        .get(shard)
        .and_then(Option::as_ref)
        .ok_or_else(|| anyhow!("process partition {shard} is unavailable"))?;
    let (response, receiver) = oneshot::channel();
    sender
        .send(ProcessPartitionCommand::Complete {
            completion,
            response,
        })
        .await
        .map_err(|_| anyhow!("process partition {shard} stopped"))?;
    receiver
        .await
        .map_err(|_| anyhow!("process partition completion response was dropped"))?
        .map_err(|error| anyhow!(error))?;
    Ok(Json(json!({})))
}

pub(crate) fn complete_process_partition(
    app: &AppState,
    shard: usize,
    completion: ProcessCompletionBatch,
) -> Result<()> {
    let token = completion.lease_token;
    let lease_key = process_batch_lease_key(&token);
    let lease = app
        .store
        .get::<ProcessBatchLease>(&lease_key)?
        .ok_or_else(|| anyhow!("process task lease lost"))?;
    let process_id = lease.process_id.clone();
    if lease.shard as usize != shard {
        bail!("process task lease lost");
    }
    if completion.items.len() != lease.executions.len() {
        bail!("incomplete process task batch");
    }
    if completion.partition_id != lease.shard
        || completion.owner_epoch != lease.owner_epoch
        || completion.activation_sequence != lease.activation_sequence
    {
        bail!("process task lease lost");
    }
    let data_shards = app.shard_locks.len().saturating_sub(1).max(1);
    app.commit_shard(shard, |transaction| {
        let current_lease = transaction
            .get::<ProcessBatchLease>(&lease_key)?
            .ok_or_else(|| anyhow!("process task lease lost"))?;
        let owner_epoch = owned_process_partition_epoch(transaction, &app.runtime_id, shard)?;
        if current_lease.shard as usize != shard
            || current_lease.process_id != process_id
            || current_lease.owner_epoch != owner_epoch
            || current_lease.owner_epoch != completion.owner_epoch
            || current_lease.activation_sequence != completion.activation_sequence
            || current_lease.executions.len() != completion.items.len()
        {
            bail!("process task lease lost");
        }
        let process = transaction
            .get::<DurableProcess>(&process_key(&process_id))?
            .ok_or_else(|| anyhow!("process not found: {process_id}"))?;
        let state_key = process_shard_state_key(&process_id, shard);
        let mut shard_state = transaction
            .get::<ProcessShardState>(&state_key)?
            .unwrap_or_default();
        for (leased, item) in current_lease.executions.iter().zip(completion.items) {
            let execution = &leased.execution;
            if execution.shard as usize != shard {
                bail!("process task lease lost");
            }
            if let Some(failure) = item.failure {
                retry_or_quarantine_sharded_execution(
                    transaction,
                    &process,
                    shard,
                    leased.clone(),
                    failure,
                    &mut shard_state,
                )?;
                continue;
            }
            finish_sharded_process_execution(
                transaction,
                execution,
                execution.sequence,
                item.result.unwrap_or(Value::Null),
                &mut shard_state,
            )?;
        }
        transaction.delete(&lease_key);
        dispatch_sharded_process(transaction, &process, shard, data_shards, &mut shard_state)?;
        transaction.put(state_key, &shard_state)
    })?;
    Ok(())
}

pub(crate) async fn renew_process_lease(
    State(app): State<AppState>,
    Json(renewal): Json<ProcessLeaseRenewal>,
) -> Result<impl IntoResponse, ApiError> {
    if renewal.protocol_version != PROTOCOL_VERSION {
        return Err(ApiError(anyhow!("unsupported protocol version")));
    }
    if !renewal.extend_seconds.is_finite() || renewal.extend_seconds <= 0.0 {
        return Err(ApiError(anyhow!("extend_seconds must be positive")));
    }
    let shard = usize::try_from(renewal.partition_id)?;
    let data_shards = app.shard_locks.len().saturating_sub(1);
    if shard == 0 || shard > data_shards {
        return Err(ApiError(anyhow!(
            "process partition {} is outside the data partitions",
            renewal.partition_id,
        )));
    }
    let sender = app
        .partition_senders
        .get(shard)
        .and_then(Option::as_ref)
        .ok_or_else(|| anyhow!("process partition {shard} is unavailable"))?;
    let (response, receiver) = oneshot::channel();
    sender
        .send(ProcessPartitionCommand::Renew { renewal, response })
        .await
        .map_err(|_| anyhow!("process partition {shard} stopped"))?;
    let lease_expires = receiver
        .await
        .map_err(|_| anyhow!("process partition renewal response was dropped"))?
        .map_err(|error| anyhow!(error))?;
    Ok(Json(json!({"lease_expires": lease_expires})))
}

pub(crate) fn renew_process_partition_lease(
    app: &AppState,
    shard: usize,
    renewal: ProcessLeaseRenewal,
) -> Result<f64> {
    let lease_key = process_batch_lease_key(&renewal.lease_token);
    let mut lease_expires = 0.0;
    app.commit_shard(shard, |transaction| {
        let owner_epoch = owned_process_partition_epoch(transaction, &app.runtime_id, shard)?;
        let mut lease = transaction
            .get::<ProcessBatchLease>(&lease_key)?
            .ok_or_else(|| anyhow!("process task lease lost"))?;
        if lease.shard as usize != shard
            || lease.owner_epoch != owner_epoch
            || lease.owner_epoch != renewal.owner_epoch
            || lease.activation_sequence != renewal.activation_sequence
        {
            bail!("process task lease lost");
        }
        lease.lease_expires = lease
            .lease_expires
            .max(now() + renewal.extend_seconds.min(300.0));
        lease_expires = lease.lease_expires;
        transaction.put(lease_key, &lease)
    })?;
    Ok(lease_expires)
}

pub(crate) async fn get_process(
    State(app): State<AppState>,
    Path(process_id): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    app.store.sync_all_remote()?;
    let mut process = app
        .store
        .get::<DurableProcess>(&process_key(&process_id))?
        .ok_or_else(|| anyhow!("process not found: {process_id}"))?;
    for (_, shard) in app
        .store
        .scan::<ProcessShardState>(&format!("process-shard/{}/", encoded(&process_id)))?
    {
        process.pending += shard.pending;
        process.running += shard.running;
        process.completed += shard.completed;
        process.failed += shard.failed;
        process.retrying += shard.retry_pending + shard.retry_running;
        process.quarantined += shard.quarantined;
    }
    Ok(Json(process))
}

pub(crate) async fn get_process_quarantine(
    State(app): State<AppState>,
    Path(process_id): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    app.store.sync_all_remote()?;
    if app
        .store
        .get::<DurableProcess>(&process_key(&process_id))?
        .is_none()
    {
        return Err(ApiError(anyhow!("process not found: {process_id}")));
    }
    let records = app
        .store
        .scan::<ProcessQuarantineRecord>(&process_quarantine_prefix(&process_id))?
        .into_iter()
        .map(|(_, record)| record)
        .collect::<Vec<_>>();
    Ok(Json(records))
}

pub(crate) async fn get_process_outcome(
    State(app): State<AppState>,
    Path((process_id, key, event_id)): Path<(String, String, String)>,
) -> Result<impl IntoResponse, ApiError> {
    app.store.sync_remote_shard(0)?;
    let process = app
        .store
        .get::<DurableProcess>(&process_key(&process_id))?
        .ok_or_else(|| anyhow!("process not found: {process_id}"))?;
    let shard = if process.direct_ingress {
        app.process_shard(&key)
    } else {
        0
    };
    app.store.sync_remote_shard(shard)?;
    let outcome = app
        .store
        .get::<ProcessExecutionOutcome>(&process_outcome_key(&process_id, &key, &event_id))?
        .ok_or_else(|| anyhow!("process event not found: {event_id}"))?;
    Ok(Json(outcome))
}

pub(crate) async fn get_process_state(
    State(app): State<AppState>,
    Path((process_id, key)): Path<(String, String)>,
) -> Result<impl IntoResponse, ApiError> {
    app.store.sync_remote_shard(0)?;
    let process = app
        .store
        .get::<DurableProcess>(&process_key(&process_id))?
        .ok_or_else(|| anyhow!("process not found: {process_id}"))?;
    let shard = if process.direct_ingress {
        app.process_shard(&key)
    } else {
        0
    };
    app.store.sync_remote_shard(shard)?;
    let state = app
        .store
        .get::<ProcessStateRecord>(&process_state_key(&process_id, &key))?;
    Ok(Json(json!({
        "process_id": process_id,
        "key": key,
        "state": state.as_ref().map(|state| &state.value),
        "state_version": state.as_ref().map(|state| state.version),
        "build_id": state.as_ref().map(|state| &state.build_id),
        "input_sequence": state.as_ref().map(|state| state.input_sequence),
        "event_time": state.as_ref().map(|state| state.event_time),
    })))
}

pub(crate) async fn complete_process_through(
    State(app): State<AppState>,
    Path(process_id): Path<String>,
    Json(request): Json<AdvanceWatermarkRequest>,
) -> Result<impl IntoResponse, ApiError> {
    app.commit(|transaction| {
        let process = transaction
            .get::<DurableProcess>(&process_key(&process_id))?
            .ok_or_else(|| anyhow!("process not found: {process_id}"))?;
        let config = transaction
            .get::<StreamConfig>(&stream_config_key(&process.stream))?
            .ok_or_else(|| anyhow!("process input stream missing: {}", process.stream))?;
        let watermark = request.event_time + config.allowed_lateness;
        if !request.event_time.is_finite() || !watermark.is_finite() {
            bail!("event-time completeness must be finite");
        }
        let mut state = transaction
            .get::<StreamState>(&stream_state_key(&process.stream))?
            .ok_or_else(|| anyhow!("process input state missing: {}", process.stream))?;
        let mut partitions = load_stream_partitions(transaction, &config)?;
        for partition in &mut partitions {
            if !partition.sealed
                && partition
                    .watermark
                    .is_none_or(|current| current < watermark)
            {
                partition.advance_watermark(watermark, now())?;
                transaction.put(
                    stream_partition_key(&process.stream, partition.partition),
                    partition,
                )?;
            }
        }
        refresh_stream(transaction, &config, &mut state, &mut partitions, now())?;
        refresh_declarative_operators(transaction, None)
    })?;
    get_process(State(app), Path(process_id)).await
}

pub(crate) async fn get_operator_edge(
    State(app): State<AppState>,
    Path(operator_id): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    Ok(Json(
        app.store
            .get::<OperatorEdge>(&operator_edge_key(&operator_id))?
            .ok_or_else(|| anyhow!("operator edge not found: {operator_id}"))?,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retries_leave_normal_concurrency_and_respect_budget() {
        let mut state = ProcessShardState {
            running: 1,
            retry_running: 1,
            ..ProcessShardState::default()
        };

        release_process_execution(&mut state, false);
        assert_eq!(state.running, 0);
        assert_eq!(state.retry_running, 1);
        release_process_execution(&mut state, true);
        assert_eq!(state.running, 0);
        assert_eq!(state.retry_running, 0);
        assert_eq!(
            process_failure_disposition(4, 5),
            ProcessFailureDisposition::Retry
        );
        assert_eq!(
            process_failure_disposition(5, 5),
            ProcessFailureDisposition::Quarantine
        );
    }
}
