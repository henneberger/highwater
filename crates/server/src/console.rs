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
    let recovered = workflow_rows
        .iter()
        .filter(|workflow| {
            workflow["status"] == "COMPLETED"
                && workflow["retries"]
                    .as_u64()
                    .is_some_and(|retries| retries > 0)
        })
        .count();
    let durability = durability_summary(&app)?;

    Ok(Json(json!({
        "service": "highwater",
        "environment": "demo",
        "generated_at": now(),
        "counts": {
            "workflows": workflows.len(),
            "running_workflows": running,
            "failed_workflows": failed,
            "recovered_workflows": recovered,
            "streams": stream_rows.len(),
            "processes": process_rows.len(),
            "operators": operator_rows.len(),
        },
        "workflows": workflow_rows,
        "streams": stream_rows,
        "processes": process_rows,
        "operators": operator_rows,
        "durability": durability,
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
        "trace": temporal_join_trace(&app, &workflow.workflow_id)?,
    })))
}

fn durability_summary(app: &AppState) -> Result<Value> {
    let timestamp = now();
    let checkpoint = DurableStore::read_manifest(&app.store.manifest_path)?;
    let owners = app
        .store
        .scan::<ProcessPartitionOwner>("process-partition-owner/")?
        .into_iter()
        .map(|(_, owner)| owner)
        .collect::<Vec<_>>();
    let key_groups = app
        .store
        .scan::<KeyGroupLease>("key-group/")?
        .into_iter()
        .map(|(_, lease)| lease)
        .collect::<Vec<_>>();
    let active_owners = owners
        .iter()
        .filter(|owner| owner.status == "ACTIVE" && owner.lease_expires > timestamp)
        .count();
    let active_key_groups = key_groups
        .iter()
        .filter(|lease| lease.lease_expires > timestamp)
        .count();
    let status = durability_status(
        owners.len(),
        active_owners,
        key_groups.len(),
        active_key_groups,
    );
    Ok(json!({
        "status": status,
        "storage_mode": app.store.durability_mode(),
        "checkpoint": checkpoint.as_ref().map(|manifest| json!({
            "checkpoint_id": manifest.checkpoint_id,
            "sequence": manifest.sequence,
            "created_at": manifest.created_at,
            "age_seconds": (timestamp - manifest.created_at).max(0.0),
            "shards": manifest.shard_sequences.len(),
            "state_handles": manifest.state_handles.len(),
        })),
        "partition_owners": owners.iter().map(|owner| json!({
            "partition": owner.partition_id,
            "node_id": owner.node_id,
            "epoch": owner.epoch,
            "status": owner.status,
            "lease_remaining_seconds": (owner.lease_expires - timestamp).max(0.0),
            "checkpoint_id": owner.checkpoint_id,
        })).collect::<Vec<_>>(),
        "active_partition_owners": active_owners,
        "key_groups": key_groups.len(),
        "active_key_groups": active_key_groups,
        "node_id": app.node_id,
        "region": env::var("FLY_REGION").unwrap_or_else(|_| "local".to_owned()),
    }))
}

fn durability_status(
    partition_owners: usize,
    active_partition_owners: usize,
    key_groups: usize,
    active_key_groups: usize,
) -> &'static str {
    if active_partition_owners == partition_owners && active_key_groups == key_groups {
        "HEALTHY"
    } else {
        "DEGRADED"
    }
}

fn temporal_join_trace(app: &AppState, workflow_id: &str) -> Result<Option<Value>> {
    for (_, join) in app.store.scan::<TemporalJoin>("temporal-join/")? {
        let output = app
            .store
            .scan::<TemporalJoinOutput>(&temporal_join_output_prefix(&join.join_id))?
            .into_iter()
            .map(|(_, output)| output)
            .find(|output| output.workflow_id.as_deref() == Some(workflow_id));
        let Some(output) = output else {
            continue;
        };
        return Ok(Some(json!({
            "source": {
                "stream": output.probe.stream,
                "partition": output.probe.partition,
                "offset": output.probe.offset,
                "event_id": output.probe.event_id,
                "key": output.probe.key,
                "event_time": output.probe.event_time,
                "ingestion_time": output.probe.ingestion_time,
                "late": output.probe.late,
                "too_late": output.probe.too_late,
            },
            "gate": {
                "as_of": output.as_of,
                "release_watermark": output.watermark,
                "decision": "released",
            },
            "operator": {
                "operator_id": join.join_id,
                "kind": "temporal_join",
                "probe_stream": join.probe_stream,
                "version_stream": join.version_stream,
                "join_type": join.join_type,
                "matched": output.version.is_some(),
            },
            "version": output.version.as_ref().map(|version| json!({
                "stream": version.stream,
                "partition": version.partition,
                "offset": version.offset,
                "event_id": version.event_id,
                "event_time": version.event_time,
                "value": version.value,
            })),
        })));
    }
    Ok(None)
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
        let latest_output = app
            .store
            .scan::<TemporalJoinOutput>(&temporal_join_output_prefix(&operator.join_id))?
            .into_iter()
            .map(|(_, output)| output)
            .max_by_key(|output| output.probe.sequence);
        let probe_watermark = app
            .store
            .get::<StreamState>(&stream_state_key(&operator.probe_stream))?
            .and_then(|state| state.watermark);
        let version_watermark = app
            .store
            .get::<StreamState>(&stream_state_key(&operator.version_stream))?
            .and_then(|state| state.watermark);
        rows.push(json!({
            "kind": "temporal_join", "operator_id": operator.join_id,
            "status": operator.status, "input": [operator.probe_stream, operator.version_stream],
            "received": operator.probes_received + operator.versions_received,
            "emitted": operator.probes_emitted, "matched": operator.matches_emitted,
            "workflow_type": operator.workflow_type,
            "join_type": operator.join_type,
            "probe_watermark": probe_watermark,
            "version_watermark": version_watermark,
            "latest_workflow_id": latest_output.and_then(|output| output.workflow_id),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durability_health_requires_every_lease_to_be_active() {
        assert_eq!(durability_status(2, 2, 128, 128), "HEALTHY");
        assert_eq!(durability_status(2, 1, 128, 128), "DEGRADED");
        assert_eq!(durability_status(2, 2, 128, 127), "DEGRADED");
    }
}
