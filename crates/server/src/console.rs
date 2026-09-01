use crate::*;

pub(crate) async fn console_overview(
    State(app): State<AppState>,
) -> Result<impl IntoResponse, ApiError> {
    let mut workflows = app
        .store
        .scan::<WorkflowRecord>("workflow/")?
        .into_iter()
        .map(|(_, workflow)| workflow)
        .collect::<Vec<_>>();
    workflows.sort_by(|left, right| right.updated_at.total_cmp(&left.updated_at));
    workflows.truncate(100);
    let workflow_rows = workflows
        .iter()
        .map(|workflow| workflow_summary(&app, workflow))
        .collect::<Result<Vec<_>>>()?;

    let stream_rows = stream_summaries(&app)?;
    let process_rows = process_summaries(&app)?;
    let operator_rows = operator_summaries(&app)?;
    let running = workflows
        .iter()
        .filter(|workflow| workflow.status == "RUNNING")
        .count();
    let failed = workflows
        .iter()
        .filter(|workflow| workflow.status == "FAILED")
        .count();

    Ok(Json(json!({
        "service": "highwater",
        "environment": "demo",
        "generated_at": now(),
        "counts": {
            "workflows": workflows.len(),
            "running_workflows": running,
            "failed_workflows": failed,
            "streams": stream_rows.len(),
            "processes": process_rows.len(),
            "operators": operator_rows.len(),
        },
        "workflows": workflow_rows,
        "streams": stream_rows,
        "processes": process_rows,
        "operators": operator_rows,
    })))
}

pub(crate) async fn console_workflow(
    State(app): State<AppState>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let workflow = app
        .store
        .get::<WorkflowRecord>(&workflow_key(&id))?
        .ok_or_else(|| anyhow!("workflow not found: {id}"))?;
    let events = app
        .store
        .scan::<Event>(&event_prefix(&id))?
        .into_iter()
        .map(|(_, event)| event)
        .collect::<Vec<_>>();
    Ok(Json(json!({
        "workflow": workflow_summary_from_events(&workflow, &events),
        "history": events,
    })))
}

fn workflow_summary(app: &AppState, workflow: &WorkflowRecord) -> Result<Value> {
    let events = app
        .store
        .scan::<Event>(&event_prefix(&workflow.workflow_id))?
        .into_iter()
        .map(|(_, event)| event)
        .collect::<Vec<_>>();
    Ok(workflow_summary_from_events(workflow, &events))
}

fn workflow_summary_from_events(workflow: &WorkflowRecord, events: &[Event]) -> Value {
    let retries = events
        .iter()
        .filter(|event| {
            matches!(
                event.event_type.as_str(),
                "ACTIVITY_RETRY_SCHEDULED" | "WORKFLOW_TASK_FAILED"
            )
        })
        .count();
    json!({
        "workflow_id": workflow.workflow_id,
        "workflow_type": workflow.workflow_type,
        "status": workflow.status,
        "task_queue": workflow.task_queue,
        "build_id": workflow.build_id,
        "run_number": workflow.run_number,
        "parent_id": workflow.parent_id,
        "created_at": workflow.created_at,
        "updated_at": workflow.updated_at,
        "duration_seconds": workflow.updated_at - workflow.created_at,
        "history_events": events.len(),
        "retries": retries,
        "result": workflow.result,
        "error": workflow.error,
    })
}

fn stream_summaries(app: &AppState) -> Result<Vec<Value>> {
    let mut rows = Vec::new();
    for key in app.store.keys("stream/")? {
        if !key.ends_with("/config") {
            continue;
        }
        let config = app
            .store
            .get::<StreamConfig>(&key)?
            .ok_or_else(|| anyhow!("stream configuration disappeared"))?;
        let state = app
            .store
            .get::<StreamState>(&stream_state_key(&config.name))?
            .ok_or_else(|| anyhow!("stream state missing: {}", config.name))?;
        let partitions = (0..config.partitions)
            .map(|partition| {
                app.store
                    .get::<PartitionState>(&stream_partition_key(&config.name, partition))?
                    .ok_or_else(|| anyhow!("stream partition missing: {}:{partition}", config.name))
            })
            .collect::<Result<Vec<_>>>()?;
        let records = partitions
            .iter()
            .map(|partition| partition.next_offset)
            .sum::<u64>();
        rows.push(json!({
            "name": config.name,
            "partitions": config.partitions,
            "records": records,
            "watermark": state.watermark,
            "max_event_time": state.max_event_time,
            "watermark_lag": match (state.max_event_time, state.watermark) {
                (Some(maximum), Some(watermark)) => Some((maximum - watermark).max(0.0)),
                _ => None,
            },
            "finalized": state.finalized,
            "watermark_mode": config.watermark_mode,
            "updated_at": state.updated_at,
        }));
    }
    rows.sort_by(|left, right| {
        left["name"]
            .as_str()
            .unwrap_or_default()
            .cmp(right["name"].as_str().unwrap_or_default())
    });
    Ok(rows)
}

fn process_summaries(app: &AppState) -> Result<Vec<Value>> {
    let mut rows = Vec::new();
    for (_, mut process) in app.store.scan::<DurableProcess>("process/")? {
        for (_, shard) in app.store.scan::<ProcessShardState>(&format!(
            "process-shard/{}/",
            encoded(&process.process_id)
        ))? {
            process.pending += shard.pending;
            process.running += shard.running;
            process.completed += shard.completed;
            process.failed += shard.failed;
        }
        rows.push(json!({
            "process_id": process.process_id,
            "workflow_type": process.workflow_type,
            "stream": process.stream,
            "status": process.status,
            "build_id": process.active_build_id,
            "pending": process.pending,
            "running": process.running,
            "completed": process.completed,
            "failed": process.failed,
            "max_concurrent_keys": process.max_concurrent_keys,
            "mailbox_capacity": process.mailbox_capacity,
            "batch_max_size": process.batch_max_size,
            "batch_max_delay": process.batch_max_delay,
            "event_time_gate": process.event_time_gate,
        }));
    }
    Ok(rows)
}

fn operator_summaries(app: &AppState) -> Result<Vec<Value>> {
    let mut rows = Vec::new();
    for (_, operator) in app.store.scan::<TemporalJoin>("temporal-join/")? {
        rows.push(json!({
            "kind": "temporal_join", "operator_id": operator.join_id,
            "status": operator.status, "input": [operator.probe_stream, operator.version_stream],
            "received": operator.probes_received + operator.versions_received,
            "emitted": operator.probes_emitted, "matched": operator.matches_emitted,
            "workflow_type": operator.workflow_type,
        }));
    }
    for (_, operator) in app.store.scan::<IntervalJoin>("interval-join/")? {
        rows.push(json!({
            "kind": "interval_join", "operator_id": operator.join_id,
            "status": operator.status, "input": [operator.left_stream, operator.right_stream],
            "received": operator.left_received + operator.right_received,
            "emitted": operator.pairs_emitted, "workflow_type": operator.workflow_type,
        }));
    }
    for (_, operator) in app.store.scan::<WindowSchedule>("stream-schedule/")? {
        rows.push(json!({
            "kind": "window", "operator_id": operator.schedule_id,
            "status": operator.status, "input": [operator.stream],
            "emitted": operator.windows_fired, "workflow_type": operator.workflow_type,
            "window_size": operator.window_size, "slide": operator.slide,
        }));
    }
    for (_, operator) in app.store.scan::<StreamFilter>("stream-filter/")? {
        rows.push(json!({
            "kind": "filter", "operator_id": operator.operator_id,
            "status": operator.status, "input": [operator.stream],
            "received": operator.records_received, "emitted": operator.records_emitted,
            "workflow_type": operator.workflow_type,
        }));
    }
    for (_, operator) in app.store.scan::<Deduplicate>("deduplicate/")? {
        rows.push(json!({
            "kind": "deduplicate", "operator_id": operator.operator_id,
            "status": operator.status, "input": [operator.stream],
            "received": operator.records_received, "emitted": operator.records_emitted,
            "suppressed": operator.duplicates_suppressed,
            "workflow_type": operator.workflow_type,
        }));
    }
    Ok(rows)
}
