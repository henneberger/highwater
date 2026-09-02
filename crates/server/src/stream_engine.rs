use crate::*;
pub(crate) fn operator_input_streams(
    transaction: &Transaction<'_>,
    operator_id: &str,
) -> Result<Vec<String>> {
    if let Some(process) = transaction.get::<DurableProcess>(&process_key(operator_id))? {
        let mut inputs = vec![process.stream];
        inputs.extend(process.versioned_streams);
        inputs.sort();
        inputs.dedup();
        return Ok(inputs);
    }
    if let Some(schedule) = transaction.get::<WindowSchedule>(&stream_schedule_key(operator_id))? {
        return Ok(vec![schedule.stream]);
    }
    if let Some(filter) = transaction.get::<StreamFilter>(&stream_filter_key(operator_id))? {
        return Ok(vec![filter.stream]);
    }
    if let Some(operator) = transaction.get::<Deduplicate>(&deduplicate_key(operator_id))? {
        return Ok(vec![operator.stream]);
    }
    if let Some(join) = transaction.get::<TemporalJoin>(&temporal_join_key(operator_id))? {
        return Ok(vec![join.probe_stream, join.version_stream]);
    }
    if let Some(join) = transaction.get::<IntervalJoin>(&interval_join_key(operator_id))? {
        return Ok(vec![join.left_stream, join.right_stream]);
    }
    bail!("operator not found: {operator_id}")
}

pub(crate) fn operator_frontier(
    transaction: &Transaction<'_>,
    operator_id: &str,
) -> Result<Option<f64>> {
    if let Some(process) = transaction.get::<DurableProcess>(&process_key(operator_id))?
        && process_has_pending_work(
            process.pending,
            process.running,
            transaction
                .scan::<ProcessShardState>(&format!("process-shard/{}/", encoded(operator_id)))?
                .into_iter()
                .map(|(_, shard)| shard),
        )
    {
        return Ok(None);
    }
    let mut frontier = Some(f64::MAX);
    for stream in operator_input_streams(transaction, operator_id)? {
        let config = transaction
            .get::<StreamConfig>(&stream_config_key(&stream))?
            .ok_or_else(|| anyhow!("operator input stream missing: {stream}"))?;
        let state = transaction
            .get::<StreamState>(&stream_state_key(&stream))?
            .ok_or_else(|| anyhow!("operator input state missing: {stream}"))?;
        frontier = match (frontier, completeness_frontier(&config, &state)) {
            (Some(current), Some(value)) => Some(current.min(value)),
            _ => None,
        };
    }
    Ok(frontier)
}

fn process_has_pending_work(
    pending: u64,
    running: u64,
    shards: impl IntoIterator<Item = ProcessShardState>,
) -> bool {
    pending > 0
        || running > 0
        || shards
            .into_iter()
            .any(|shard| shard.pending > 0 || shard.running > 0)
}

pub(crate) fn stream_reaches_any(
    transaction: &Transaction<'_>,
    stream: &str,
    targets: &[String],
    visited: &mut HashSet<String>,
) -> Result<bool> {
    if targets.iter().any(|target| target == stream) {
        return Ok(true);
    }
    if !visited.insert(stream.to_owned()) {
        return Ok(false);
    }
    for (_, edge) in transaction.scan::<OperatorEdge>("operator-edge/")? {
        if edge.status == "ACTIVE"
            && operator_input_streams(transaction, &edge.operator_id)?
                .iter()
                .any(|input| input == stream)
            && stream_reaches_any(transaction, &edge.output_stream, targets, visited)?
        {
            return Ok(true);
        }
    }
    Ok(false)
}

pub(crate) fn append_internal_stream_change(
    transaction: &mut Transaction<'_>,
    app: &AppState,
    edge: &OperatorEdge,
    change: &DifferentialChange,
) -> Result<()> {
    let config = transaction
        .get::<StreamConfig>(&stream_config_key(&edge.output_stream))?
        .ok_or_else(|| anyhow!("edge output stream missing: {}", edge.output_stream))?;
    ensure_process_capacity(transaction, &edge.output_stream)?;
    let partition = change
        .key
        .as_deref()
        .map_or(0, |key| key_group_for(Some(key), 0, config.partitions));
    let key_group = key_group_for(change.key.as_deref(), partition, app.key_group_count);
    let owner_epoch = owned_key_group_epoch(transaction, &app.node_id, key_group)?;
    let mut partitions = load_stream_partitions(transaction, &config)?;
    let stream_state_key = stream_state_key(&edge.output_stream);
    let mut state = transaction
        .get::<StreamState>(&stream_state_key)?
        .ok_or_else(|| anyhow!("edge output stream state missing: {}", edge.output_stream))?;
    let event_id = format!("operator:{}:{}", edge.operator_id, change.sequence);
    if transaction
        .get::<StreamRecord>(&stream_event_id_key(&edge.output_stream, &event_id))?
        .is_some()
    {
        return Ok(());
    }
    let offset = partitions[partition as usize].observe(change.event_time, 0.0, false, now())?;
    let sequence = transaction.get::<u64>("meta/stream_sequence")?.unwrap_or(0) + 1;
    transaction.put("meta/stream_sequence", &sequence)?;
    let record = StreamRecord {
        stream: edge.output_stream.clone(),
        partition,
        offset,
        sequence,
        event_time: change.event_time,
        ingestion_time: now(),
        key: change.key.clone(),
        value: change.row.clone(),
        kind: change.kind,
        event_id: Some(event_id.clone()),
        key_group,
        owner_epoch,
        source_id: Some(format!("operator:{}", edge.operator_id)),
        source_partition: Some(0),
        source_offset: Some(change.sequence),
        late: state.is_late(change.event_time),
        too_late: state.is_too_late(change.event_time, config.allowed_lateness),
    };
    transaction.put(
        stream_record_key(&edge.output_stream, partition, offset),
        &record,
    )?;
    transaction.put(stream_event_id_key(&edge.output_stream, &event_id), &record)?;
    transaction.put(
        stream_partition_key(&edge.output_stream, partition),
        &partitions[partition as usize],
    )?;
    index_window_aggregates(transaction, &record)?;
    refresh_stream(transaction, &config, &mut state, &mut partitions, now())?;
    refresh_declarative_operators(transaction, Some(&record))
}

pub(crate) fn drain_operator_edges(
    transaction: &mut Transaction<'_>,
    app: &AppState,
) -> Result<()> {
    let edges = transaction
        .scan::<OperatorEdge>("operator-edge/")?
        .into_iter()
        .filter(|(_, edge)| edge.status == "ACTIVE")
        .collect::<Vec<_>>();
    for (storage_key, mut edge) in edges {
        let pending = transaction
            .scan::<DifferentialChange>(&operator_edge_pending_prefix(&edge.operator_id))?;
        for (pending_key, change) in pending {
            append_internal_stream_change(transaction, app, &edge, &change)?;
            transaction.delete(pending_key);
            edge.changes_forwarded += 1;
        }
        if let Some(frontier) = operator_frontier(transaction, &edge.operator_id)? {
            let config = transaction
                .get::<StreamConfig>(&stream_config_key(&edge.output_stream))?
                .ok_or_else(|| anyhow!("edge output stream missing: {}", edge.output_stream))?;
            let mut state = transaction
                .get::<StreamState>(&stream_state_key(&edge.output_stream))?
                .ok_or_else(|| anyhow!("edge output state missing: {}", edge.output_stream))?;
            let mut partitions = load_stream_partitions(transaction, &config)?;
            for partition in &mut partitions {
                if frontier == f64::MAX {
                    partition.sealed = true;
                    partition.idle = false;
                    partition.last_activity_at = now();
                } else {
                    partition.advance_watermark(frontier, now())?;
                }
                transaction.put(
                    stream_partition_key(&edge.output_stream, partition.partition),
                    partition,
                )?;
            }
            refresh_stream(transaction, &config, &mut state, &mut partitions, now())?;
            refresh_declarative_operators(transaction, None)?;
        }
        transaction.put(storage_key, &edge)?;
    }
    Ok(())
}

pub(crate) fn fire_due_watermark_timers(
    transaction: &mut Transaction<'_>,
    config: &StreamConfig,
    state: &StreamState,
) -> Result<()> {
    let due: Vec<(String, WatermarkTimerRecord)> = transaction
        .scan::<WatermarkTimerRecord>(&watermark_timer_prefix(&config.name))?
        .into_iter()
        .filter(|(_, timer)| {
            state.finalized
                || state
                    .watermark
                    .is_some_and(|watermark| watermark >= timer.event_time)
        })
        .collect();
    for (key, timer) in due {
        transaction.delete(key);
        append_event(
            transaction,
            &timer.workflow_id,
            "WATERMARK_TIMER_FIRED",
            json!({
                "command_id": timer.command_id,
                "stream": timer.stream,
                "event_time": timer.event_time,
                "watermark": state.watermark,
                "finalized": state.finalized,
            }),
        )?;
        if let Some(workflow) =
            transaction.get::<WorkflowRecord>(&workflow_key(&timer.workflow_id))?
            && workflow.status == "RUNNING"
        {
            enqueue_workflow(transaction, &workflow)?;
        }
    }
    Ok(())
}

pub(crate) fn index_window_aggregate(
    transaction: &mut Transaction<'_>,
    schedule: &WindowSchedule,
    record: &StreamRecord,
) -> Result<()> {
    if record.event_time < schedule.start_at {
        return Ok(());
    }
    let weight = record.kind.weight();
    let aggregate_value = schedule
        .value_field
        .as_deref()
        .and_then(|field| field_value(&record.value, field))
        .unwrap_or(&record.value);
    let numeric = (schedule.aggregation != WindowAggregation::Count)
        .then(|| {
            aggregate_value.as_f64().ok_or_else(|| {
                anyhow!(
                    "window aggregate {} requires numeric JSON values",
                    schedule.schedule_id,
                )
            })
        })
        .transpose()?;
    let slide = schedule.effective_slide();
    let mut window_start =
        schedule.start_at + ((record.event_time - schedule.start_at) / slide).floor() * slide;
    while window_start >= schedule.start_at
        && record.event_time < window_start + schedule.window_size
    {
        let key =
            window_accumulator_key(&schedule.schedule_id, window_start, record.key.as_deref());
        let previous = transaction.get::<WindowAccumulator>(&key)?;
        let mut accumulator = previous.clone().unwrap_or(WindowAccumulator {
            schedule_id: schedule.schedule_id.clone(),
            stream: schedule.stream.clone(),
            key: record.key.clone(),
            window_start,
            window_end: window_start + schedule.window_size,
            count: 0,
            sum: 0.0,
            max: None,
        });
        accumulator.count += weight;
        if accumulator.count < 0 {
            bail!(
                "window aggregate {} received a retraction without matching state",
                schedule.schedule_id,
            );
        }
        if schedule.aggregation == WindowAggregation::Sum {
            accumulator.sum += numeric.expect("numeric sum value validated") * weight as f64;
        } else if schedule.aggregation == WindowAggregation::Max {
            let numeric = numeric.expect("numeric max value validated");
            let value_key = window_value_key(
                &schedule.schedule_id,
                window_start,
                record.key.as_deref(),
                numeric,
            );
            let mut value_count =
                transaction
                    .get::<WindowValueCount>(&value_key)?
                    .unwrap_or(WindowValueCount {
                        value: numeric,
                        count: 0,
                    });
            value_count.count += weight;
            if value_count.count < 0 {
                bail!(
                    "window max {} received a value retraction without matching state",
                    schedule.schedule_id,
                );
            } else if value_count.count == 0 {
                transaction.delete(value_key);
            } else {
                transaction.put(value_key, &value_count)?;
            }
            accumulator.max = transaction
                .scan::<WindowValueCount>(&window_value_prefix(
                    &schedule.schedule_id,
                    window_start,
                    record.key.as_deref(),
                ))?
                .into_iter()
                .filter(|(_, value)| value.count > 0)
                .map(|(_, value)| value.value)
                .max_by(f64::total_cmp);
        }
        if accumulator.count == 0 {
            if let Some(previous) = previous {
                append_operator_change(
                    transaction,
                    &schedule.schedule_id,
                    previous.key.clone(),
                    record.event_time,
                    ChangeKind::Delete,
                    window_accumulator_row(&previous),
                )?;
            }
            transaction.delete(key);
        } else {
            if let Some(previous) = previous {
                append_operator_change(
                    transaction,
                    &schedule.schedule_id,
                    previous.key.clone(),
                    record.event_time,
                    ChangeKind::UpdateBefore,
                    window_accumulator_row(&previous),
                )?;
                append_operator_change(
                    transaction,
                    &schedule.schedule_id,
                    accumulator.key.clone(),
                    record.event_time,
                    ChangeKind::UpdateAfter,
                    window_accumulator_row(&accumulator),
                )?;
            } else {
                append_operator_change(
                    transaction,
                    &schedule.schedule_id,
                    accumulator.key.clone(),
                    record.event_time,
                    ChangeKind::Insert,
                    window_accumulator_row(&accumulator),
                )?;
            }
            transaction.put(key, &accumulator)?;
        }
        if window_start < schedule.start_at + slide {
            break;
        }
        window_start -= slide;
    }
    Ok(())
}

pub(crate) fn index_window_aggregates(
    transaction: &mut Transaction<'_>,
    record: &StreamRecord,
) -> Result<()> {
    let schedules = transaction
        .scan::<WindowSchedule>("stream-schedule/")?
        .into_iter()
        .map(|(_, schedule)| schedule)
        .filter(|schedule| schedule.stream == record.stream && schedule.status == "ACTIVE")
        .collect::<Vec<_>>();
    for schedule in &schedules {
        index_window_aggregate(transaction, schedule, record)?;
    }
    Ok(())
}

pub(crate) fn fire_due_stream_schedules(
    transaction: &mut Transaction<'_>,
    config: &StreamConfig,
    state: &StreamState,
) -> Result<()> {
    let schedules: Vec<(String, WindowSchedule)> = transaction
        .scan::<WindowSchedule>("stream-schedule/")?
        .into_iter()
        .filter(|(_, schedule)| schedule.stream == config.name && schedule.status == "ACTIVE")
        .collect();
    for (key, mut schedule) in schedules {
        let mut fired = 0;
        while fired < 100 {
            let window_start = schedule.next_window_start;
            let window_end = window_start + schedule.window_size;
            let due = if state.finalized {
                state
                    .max_event_time
                    .is_some_and(|maximum| window_start <= maximum)
            } else {
                state
                    .watermark
                    .is_some_and(|watermark| watermark >= window_end + config.allowed_lateness)
            };
            if !due {
                break;
            }
            let prefix = format!(
                "{}{:016x}/",
                window_accumulator_prefix(&schedule.schedule_id),
                ordered_f64_bits(window_start),
            );
            let accumulators = transaction.scan::<WindowAccumulator>(&prefix)?;
            if accumulators.is_empty() && schedule.emit_empty_windows {
                let accumulator = WindowAccumulator {
                    schedule_id: schedule.schedule_id.clone(),
                    stream: schedule.stream.clone(),
                    key: None,
                    window_start,
                    window_end,
                    count: 0,
                    sum: 0.0,
                    max: None,
                };
                start_window_workflow(
                    transaction,
                    &schedule,
                    state.watermark,
                    state.finalized,
                    &accumulator,
                )?;
                schedule.windows_fired += 1;
            }
            for (accumulator_key, accumulator) in accumulators {
                start_window_workflow(
                    transaction,
                    &schedule,
                    state.watermark,
                    state.finalized,
                    &accumulator,
                )?;
                for (value_key, _) in transaction.scan::<WindowValueCount>(&window_value_prefix(
                    &schedule.schedule_id,
                    accumulator.window_start,
                    accumulator.key.as_deref(),
                ))? {
                    transaction.delete(value_key);
                }
                transaction.delete(accumulator_key);
                schedule.windows_fired += 1;
            }
            schedule.next_window_start += schedule.effective_slide();
            fired += 1;
        }
        transaction.put(key, &schedule)?;
    }
    Ok(())
}

pub(crate) fn refresh_stream(
    transaction: &mut Transaction<'_>,
    config: &StreamConfig,
    state: &mut StreamState,
    partitions: &mut [PartitionState],
    timestamp: f64,
) -> Result<()> {
    let previous_partitions = partitions.to_vec();
    let changed = state.refresh(config, partitions, timestamp);
    for (before, partition) in previous_partitions.iter().zip(partitions.iter()) {
        if before != partition {
            transaction.put(
                stream_partition_key(&config.name, partition.partition),
                partition,
            )?;
        }
    }
    if changed {
        transaction.put(stream_state_key(&config.name), state)?;
        fire_due_watermark_timers(transaction, config, state)?;
        fire_due_stream_schedules(transaction, config, state)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn process_frontier_waits_for_async_work_to_finish() {
        assert!(process_has_pending_work(
            0,
            0,
            [ProcessShardState {
                pending: 1,
                ..ProcessShardState::default()
            }]
        ));
        assert!(process_has_pending_work(
            0,
            1,
            std::iter::empty::<ProcessShardState>()
        ));
        assert!(!process_has_pending_work(
            0,
            0,
            [ProcessShardState {
                completed: 3,
                ..ProcessShardState::default()
            }]
        ));
    }
}
