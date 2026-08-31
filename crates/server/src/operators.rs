use crate::*;
pub(crate) fn load_stream_partitions(
    transaction: &Transaction<'_>,
    config: &StreamConfig,
) -> Result<Vec<PartitionState>> {
    (0..config.partitions)
        .map(|partition| {
            transaction
                .get::<PartitionState>(&stream_partition_key(&config.name, partition))?
                .ok_or_else(|| anyhow!("stream partition {partition} is missing"))
        })
        .collect()
}

pub(crate) fn ensure_watermark_alignment(
    config: &StreamConfig,
    partitions: &[PartitionState],
    partition: u32,
    candidate: f64,
) -> Result<()> {
    let Some(max_drift) = config.alignment_max_drift else {
        return Ok(());
    };
    let others: Vec<&PartitionState> = partitions
        .iter()
        .filter(|state| state.partition != partition && !state.idle && !state.sealed)
        .collect();
    if others.is_empty() || others.iter().any(|state| state.watermark.is_none()) {
        return Ok(());
    }
    let minimum = others
        .iter()
        .filter_map(|state| state.watermark)
        .reduce(f64::min)
        .expect("other partition watermarks checked above");
    if candidate > minimum + max_drift {
        return Err(WatermarkAlignmentError(format!(
            "partition {partition} is watermark-aligned and paused at {}; retry after slower partitions advance",
            minimum + max_drift,
        ))
        .into());
    }
    Ok(())
}

pub(crate) fn start_window_workflow(
    transaction: &mut Transaction<'_>,
    schedule: &WindowSchedule,
    watermark: Option<f64>,
    finalized: bool,
    accumulator: &WindowAccumulator,
) -> Result<()> {
    let key = accumulator.key.as_deref();
    let window_start = accumulator.window_start;
    let window_end = accumulator.window_end;
    let workflow_id = key.map_or_else(
        || {
            format!(
                "stream/{}/{window_start:.6}-{window_end:.6}",
                schedule.schedule_id,
            )
        },
        |key| {
            format!(
                "stream/{}/{}/{window_start:.6}-{window_end:.6}",
                schedule.schedule_id,
                encoded(key),
            )
        },
    );
    if transaction
        .get::<WorkflowRecord>(&workflow_key(&workflow_id))?
        .is_some()
    {
        return Ok(());
    }
    let timestamp = now();
    let workflow = WorkflowRecord {
        workflow_id: workflow_id.clone(),
        workflow_type: schedule.workflow_type.clone(),
        status: "RUNNING".to_owned(),
        result: None,
        error: None,
        task_queue: schedule.task_queue.clone(),
        build_id: None,
        run_number: 1,
        parent_id: None,
        parent_command_id: None,
        parent_close_policy: None,
        execution_deadline: None,
        created_at: timestamp,
        updated_at: timestamp,
    };
    let window = json!({
        "schedule_id": schedule.schedule_id,
        "stream": schedule.stream,
        "key": key,
        "window_start": window_start,
        "window_end": window_end,
        "watermark": watermark,
        "finalized": finalized,
        "aggregation": schedule.aggregation,
        "count": accumulator.count,
        "sum": accumulator.sum,
        "max": accumulator.max,
    });
    transaction.put(workflow_key(&workflow_id), &workflow)?;
    append_event(
        transaction,
        &workflow_id,
        "WORKFLOW_STARTED",
        json!({
            "workflow_type": schedule.workflow_type,
            "args": [window],
            "run_number": 1,
            "stream_schedule": schedule.schedule_id,
        }),
    )?;
    enqueue_workflow(transaction, &workflow)
}

pub(crate) fn start_temporal_join_workflow(
    transaction: &mut Transaction<'_>,
    join: &TemporalJoin,
    probe: &StreamRecord,
    version: Option<&StreamRecord>,
    watermark: Option<f64>,
) -> Result<Option<String>> {
    if join.join_type == TemporalJoinType::Inner && version.is_none() {
        return Ok(None);
    }
    let workflow_id = format!(
        "temporal-join/{}/{}/{:010}/{:020}",
        join.join_id, probe.stream, probe.partition, probe.offset,
    );
    if transaction
        .get::<WorkflowRecord>(&workflow_key(&workflow_id))?
        .is_some()
    {
        return Ok(Some(workflow_id));
    }
    let timestamp = now();
    let workflow = WorkflowRecord {
        workflow_id: workflow_id.clone(),
        workflow_type: join.workflow_type.clone(),
        status: "RUNNING".to_owned(),
        result: None,
        error: None,
        task_queue: join.task_queue.clone(),
        build_id: None,
        run_number: 1,
        parent_id: None,
        parent_command_id: None,
        parent_close_policy: None,
        execution_deadline: None,
        created_at: timestamp,
        updated_at: timestamp,
    };
    let input = json!({
        "join_id": join.join_id,
        "as_of": probe.event_time,
        "watermark": watermark,
        "probe": probe,
        "version": version,
    });
    transaction.put(workflow_key(&workflow_id), &workflow)?;
    append_event(
        transaction,
        &workflow_id,
        "WORKFLOW_STARTED",
        json!({
            "workflow_type": join.workflow_type,
            "args": [input],
            "run_number": 1,
            "temporal_join": join.join_id,
        }),
    )?;
    enqueue_workflow(transaction, &workflow)?;
    Ok(Some(workflow_id))
}

pub(crate) fn index_temporal_join_record(
    transaction: &mut Transaction<'_>,
    join: &mut TemporalJoin,
    record: &StreamRecord,
) -> Result<bool> {
    if record.stream != join.probe_stream && record.stream != join.version_stream {
        return Ok(false);
    }
    record
        .key
        .as_deref()
        .filter(|key| !key.is_empty())
        .ok_or_else(|| anyhow!("temporal join records require a non-empty key"))?;
    if record.stream == join.probe_stream {
        if !record.kind.is_addition() {
            let buffered = transaction
                .scan::<StreamRecord>(&temporal_join_probe_prefix(&join.join_id))?
                .into_iter()
                .find(|(_, candidate)| {
                    candidate.key == record.key
                        && candidate.event_time == record.event_time
                        && candidate.value == record.value
                });
            if let Some((storage_key, _)) = buffered {
                transaction.delete(storage_key);
            }
            join.probes_received += 1;
            return Ok(true);
        }
        transaction.put(temporal_join_probe_key(&join.join_id, record), record)?;
        join.probes_received += 1;
    } else {
        transaction.put(temporal_join_version_key(&join.join_id, record), record)?;
        join.versions_received += 1;
    }
    Ok(true)
}

pub(crate) fn latest_temporal_join_version(
    transaction: &Transaction<'_>,
    join_id: &str,
    probe: &StreamRecord,
) -> Result<Option<StreamRecord>> {
    let key = probe
        .key
        .as_deref()
        .expect("buffered temporal join probe has a key");
    let versions: Vec<StreamRecord> = transaction
        .scan::<StreamRecord>(&temporal_join_versions_for_key_prefix(join_id, key))?
        .into_iter()
        .map(|(_, version)| version)
        .collect();
    Ok(latest_version_as_of(&versions, probe.event_time).cloned())
}

pub(crate) fn cleanup_temporal_join_versions(
    transaction: &mut Transaction<'_>,
    join_id: &str,
    frontier: f64,
) -> Result<()> {
    let versions = transaction.scan::<StreamRecord>(&temporal_join_version_prefix(join_id))?;
    let mut by_key: BTreeMap<String, Vec<(String, StreamRecord)>> = BTreeMap::new();
    for (storage_key, version) in versions {
        by_key
            .entry(version.key.clone().expect("indexed version has a key"))
            .or_default()
            .push((storage_key, version));
    }
    for versions in by_key.values_mut() {
        versions.sort_by(|(_, left), (_, right)| {
            left.event_time
                .total_cmp(&right.event_time)
                .then(left.sequence.cmp(&right.sequence))
        });
        if frontier == f64::MAX {
            for (storage_key, _) in versions.iter() {
                transaction.delete(storage_key);
            }
            continue;
        }
        let retain = versions
            .iter()
            .rposition(|(_, version)| version.event_time <= frontier);
        if let Some(retain) = retain {
            for (storage_key, _) in versions.iter().take(retain) {
                transaction.delete(storage_key);
            }
        }
    }
    Ok(())
}

pub(crate) fn refresh_temporal_joins(
    transaction: &mut Transaction<'_>,
    record: Option<&StreamRecord>,
) -> Result<()> {
    let joins: Vec<(String, TemporalJoin)> = transaction
        .scan::<TemporalJoin>("temporal-join/")?
        .into_iter()
        .filter(|(_, join)| join.status == "ACTIVE")
        .collect();
    for (storage_key, mut join) in joins {
        let mut changed = false;
        if let Some(record) = record {
            changed = index_temporal_join_record(transaction, &mut join, record)?;
        }
        let probe_config = transaction
            .get::<StreamConfig>(&stream_config_key(&join.probe_stream))?
            .ok_or_else(|| anyhow!("probe stream missing: {}", join.probe_stream))?;
        let probe_state = transaction
            .get::<StreamState>(&stream_state_key(&join.probe_stream))?
            .ok_or_else(|| anyhow!("probe stream state missing: {}", join.probe_stream))?;
        let version_config = transaction
            .get::<StreamConfig>(&stream_config_key(&join.version_stream))?
            .ok_or_else(|| anyhow!("version stream missing: {}", join.version_stream))?;
        let version_state = transaction
            .get::<StreamState>(&stream_state_key(&join.version_stream))?
            .ok_or_else(|| anyhow!("version stream state missing: {}", join.version_stream))?;
        let Some(frontier) =
            temporal_join_frontier(&probe_config, &probe_state, &version_config, &version_state)
        else {
            if changed {
                transaction.put(storage_key, &join)?;
            }
            continue;
        };
        let watermark = (frontier != f64::MAX).then_some(frontier);
        let due_probes: Vec<(String, StreamRecord)> = transaction
            .scan::<StreamRecord>(&temporal_join_probe_prefix(&join.join_id))?
            .into_iter()
            .filter(|(_, probe)| probe.event_time <= frontier)
            .collect();
        for (probe_key, probe) in due_probes {
            let version = latest_temporal_join_version(transaction, &join.join_id, &probe)?;
            let workflow_id = start_temporal_join_workflow(
                transaction,
                &join,
                &probe,
                version.as_ref(),
                watermark,
            )?;
            let output = TemporalJoinOutput {
                join_id: join.join_id.clone(),
                as_of: probe.event_time,
                probe: probe.clone(),
                version,
                watermark,
                workflow_id,
            };
            if output.version.is_some() {
                join.matches_emitted += 1;
            }
            join.probes_emitted += 1;
            transaction.put(temporal_join_output_key(&join.join_id, &probe), &output)?;
            if join.join_type == TemporalJoinType::Left || output.version.is_some() {
                append_operator_change(
                    transaction,
                    &join.join_id,
                    probe.key.clone(),
                    probe.event_time,
                    ChangeKind::Insert,
                    json!({"probe": probe, "version": output.version}),
                )?;
            }
            transaction.delete(probe_key);
            changed = true;
        }
        cleanup_temporal_join_versions(transaction, &join.join_id, frontier)?;
        if changed {
            transaction.put(storage_key, &join)?;
        }
    }
    Ok(())
}

pub(crate) fn emit_interval_join_pair(
    transaction: &mut Transaction<'_>,
    join: &IntervalJoin,
    left: &StreamRecord,
    right: &StreamRecord,
) -> Result<bool> {
    let output_key = interval_join_output_key(&join.join_id, Some(left), Some(right));
    if transaction
        .get::<IntervalJoinOutput>(&output_key)?
        .is_some()
    {
        return Ok(false);
    }
    let workflow_id = format!(
        "interval-join/{}/{:010}/{:020}/{:010}/{:020}",
        join.join_id, left.partition, left.offset, right.partition, right.offset,
    );
    let timestamp = now();
    let workflow = WorkflowRecord {
        workflow_id: workflow_id.clone(),
        workflow_type: join.workflow_type.clone(),
        status: "RUNNING".to_owned(),
        result: None,
        error: None,
        task_queue: join.task_queue.clone(),
        build_id: None,
        run_number: 1,
        parent_id: None,
        parent_command_id: None,
        parent_close_policy: None,
        execution_deadline: None,
        created_at: timestamp,
        updated_at: timestamp,
    };
    let input = json!({
        "join_id": join.join_id,
        "left": left,
        "right": right,
        "event_time": left.event_time.max(right.event_time),
    });
    transaction.put(workflow_key(&workflow_id), &workflow)?;
    append_event(
        transaction,
        &workflow_id,
        "WORKFLOW_STARTED",
        json!({
            "workflow_type": join.workflow_type,
            "args": [input],
            "run_number": 1,
            "interval_join": join.join_id,
        }),
    )?;
    enqueue_workflow(transaction, &workflow)?;
    transaction.put(
        output_key,
        &IntervalJoinOutput {
            join_id: join.join_id.clone(),
            left: Some(left.clone()),
            right: Some(right.clone()),
            workflow_id,
        },
    )?;
    append_operator_change(
        transaction,
        &join.join_id,
        left.key.clone(),
        left.event_time.max(right.event_time),
        ChangeKind::Insert,
        json!({"left": left, "right": right}),
    )?;
    Ok(true)
}

pub(crate) fn retract_interval_join_record(
    transaction: &mut Transaction<'_>,
    join: &IntervalJoin,
    side: &str,
    record: &StreamRecord,
) -> Result<()> {
    let key = record
        .key
        .as_deref()
        .expect("interval join retraction key validated");
    let arranged = transaction
        .scan::<StreamRecord>(&interval_join_side_key_prefix(&join.join_id, side, key))?
        .into_iter()
        .find(|(_, candidate)| {
            candidate.event_time == record.event_time && candidate.value == record.value
        });
    let Some((arrangement_key, arranged)) = arranged else {
        return Ok(());
    };
    transaction.delete(arrangement_key);
    for (output_key, output) in
        transaction.scan::<IntervalJoinOutput>(&interval_join_output_prefix(&join.join_id))?
    {
        let matches = if side == "left" {
            output.left.as_ref().is_some_and(|candidate| {
                candidate.partition == arranged.partition && candidate.offset == arranged.offset
            })
        } else {
            output.right.as_ref().is_some_and(|candidate| {
                candidate.partition == arranged.partition && candidate.offset == arranged.offset
            })
        };
        if matches {
            transaction.delete(output_key);
            append_operator_change(
                transaction,
                &join.join_id,
                arranged.key.clone(),
                output
                    .left
                    .as_ref()
                    .map_or(record.event_time, |left| left.event_time)
                    .max(
                        output
                            .right
                            .as_ref()
                            .map_or(record.event_time, |right| right.event_time),
                    ),
                ChangeKind::Delete,
                json!({"left": output.left, "right": output.right}),
            )?;
        }
    }
    Ok(())
}

pub(crate) fn index_interval_join_record(
    transaction: &mut Transaction<'_>,
    join: &mut IntervalJoin,
    record: &StreamRecord,
) -> Result<bool> {
    if join.left_stream == join.right_stream {
        if record.stream != join.left_stream {
            return Ok(false);
        }
        let key = record
            .key
            .as_deref()
            .filter(|key| !key.is_empty())
            .ok_or_else(|| anyhow!("interval join records require a non-empty key"))?;
        if !record.kind.is_addition() {
            retract_interval_join_record(transaction, join, "left", record)?;
            join.left_received += 1;
            join.right_received += 1;
            return Ok(true);
        }
        let prior_records = transaction
            .scan::<StreamRecord>(&interval_join_side_key_prefix(&join.join_id, "left", key))?
            .into_iter()
            .map(|(_, prior)| prior)
            .collect::<Vec<_>>();
        for prior in prior_records {
            if prior.event_time < record.event_time
                && interval_contains(
                    prior.event_time,
                    record.event_time,
                    join.lower_bound,
                    join.upper_bound,
                )
                && emit_interval_join_pair(transaction, join, &prior, record)?
            {
                join.pairs_emitted += 1;
            }
        }
        transaction.put(
            interval_join_side_key(&join.join_id, "left", record),
            record,
        )?;
        join.left_received += 1;
        join.right_received += 1;
        return Ok(true);
    }
    let side = if record.stream == join.left_stream {
        "left"
    } else if record.stream == join.right_stream {
        "right"
    } else {
        return Ok(false);
    };
    let key = record
        .key
        .as_deref()
        .filter(|key| !key.is_empty())
        .ok_or_else(|| anyhow!("interval join records require a non-empty key"))?;
    if !record.kind.is_addition() {
        retract_interval_join_record(transaction, join, side, record)?;
        if side == "left" {
            join.left_received += 1;
        } else {
            join.right_received += 1;
        }
        return Ok(true);
    }
    let opposite = if side == "left" { "right" } else { "left" };
    let candidates: Vec<StreamRecord> = transaction
        .scan::<StreamRecord>(&interval_join_side_key_prefix(&join.join_id, opposite, key))?
        .into_iter()
        .map(|(_, candidate)| candidate)
        .collect();
    for candidate in candidates {
        let (left, right) = if side == "left" {
            (record, &candidate)
        } else {
            (&candidate, record)
        };
        if interval_contains(
            left.event_time,
            right.event_time,
            join.lower_bound,
            join.upper_bound,
        ) && emit_interval_join_pair(transaction, join, left, right)?
        {
            join.pairs_emitted += 1;
        }
    }
    transaction.put(interval_join_side_key(&join.join_id, side, record), record)?;
    if side == "left" {
        join.left_received += 1;
    } else {
        join.right_received += 1;
    }
    Ok(true)
}

pub(crate) fn refresh_interval_joins(
    transaction: &mut Transaction<'_>,
    record: Option<&StreamRecord>,
) -> Result<()> {
    let joins: Vec<(String, IntervalJoin)> = transaction
        .scan::<IntervalJoin>("interval-join/")?
        .into_iter()
        .filter(|(_, join)| join.status == "ACTIVE")
        .collect();
    for (storage_key, mut join) in joins {
        let mut changed = false;
        if let Some(record) = record
            && (record.stream == join.left_stream || record.stream == join.right_stream)
        {
            changed = index_interval_join_record(transaction, &mut join, record)?;
        }
        let left_config = transaction
            .get::<StreamConfig>(&stream_config_key(&join.left_stream))?
            .ok_or_else(|| anyhow!("left stream missing: {}", join.left_stream))?;
        let left_state = transaction
            .get::<StreamState>(&stream_state_key(&join.left_stream))?
            .ok_or_else(|| anyhow!("left stream state missing: {}", join.left_stream))?;
        let right_config = transaction
            .get::<StreamConfig>(&stream_config_key(&join.right_stream))?
            .ok_or_else(|| anyhow!("right stream missing: {}", join.right_stream))?;
        let right_state = transaction
            .get::<StreamState>(&stream_state_key(&join.right_stream))?
            .ok_or_else(|| anyhow!("right stream state missing: {}", join.right_stream))?;
        let left_frontier = completeness_frontier(&left_config, &left_state);
        let right_frontier = completeness_frontier(&right_config, &right_state);
        if let (Some(left_frontier), Some(right_frontier)) = (left_frontier, right_frontier) {
            for (key, left) in transaction
                .scan::<StreamRecord>(&interval_join_side_prefix(&join.join_id, "left"))?
            {
                if right_frontier >= left.event_time + join.upper_bound
                    && left_frontier >= left.event_time
                {
                    transaction.delete(key);
                }
            }
            for (key, right) in transaction
                .scan::<StreamRecord>(&interval_join_side_prefix(&join.join_id, "right"))?
            {
                if left_frontier >= right.event_time - join.lower_bound
                    && right_frontier >= right.event_time
                {
                    transaction.delete(key);
                }
            }
        }
        if changed {
            transaction.put(storage_key, &join)?;
        }
    }
    Ok(())
}

pub(crate) fn emit_deduplicated_record(
    transaction: &mut Transaction<'_>,
    operator: &Deduplicate,
    record: &StreamRecord,
) -> Result<String> {
    let workflow_id = format!(
        "deduplicate/{}/{:010}/{:020}",
        operator.operator_id, record.partition, record.offset,
    );
    let timestamp = now();
    let workflow = WorkflowRecord {
        workflow_id: workflow_id.clone(),
        workflow_type: operator.workflow_type.clone(),
        status: "RUNNING".to_owned(),
        result: None,
        error: None,
        task_queue: operator.task_queue.clone(),
        build_id: None,
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
            "workflow_type": operator.workflow_type,
            "args": [record],
            "run_number": 1,
            "deduplicate": operator.operator_id,
        }),
    )?;
    enqueue_workflow(transaction, &workflow)?;
    Ok(workflow_id)
}

pub(crate) fn index_deduplicate_record(
    transaction: &mut Transaction<'_>,
    operator: &mut Deduplicate,
    record: &StreamRecord,
) -> Result<bool> {
    if record.stream != operator.stream {
        return Ok(false);
    }
    record
        .key
        .as_deref()
        .filter(|key| !key.is_empty())
        .ok_or_else(|| anyhow!("deduplicate records require a non-empty key"))?;
    if !record.kind.is_addition() {
        bail!("deduplicate records must be insert-like changes");
    }
    transaction.put(
        deduplicate_buffer_key(&operator.operator_id, record),
        record,
    )?;
    operator.records_received += 1;
    Ok(true)
}

pub(crate) fn refresh_deduplicates(
    transaction: &mut Transaction<'_>,
    record: Option<&StreamRecord>,
) -> Result<()> {
    let operators: Vec<(String, Deduplicate)> = transaction
        .scan::<Deduplicate>("deduplicate/")?
        .into_iter()
        .filter(|(_, operator)| operator.status == "ACTIVE")
        .collect();
    for (storage_key, mut operator) in operators {
        let mut changed = false;
        if let Some(record) = record
            && record.stream == operator.stream
        {
            changed = index_deduplicate_record(transaction, &mut operator, record)?;
        }
        let config = transaction
            .get::<StreamConfig>(&stream_config_key(&operator.stream))?
            .ok_or_else(|| anyhow!("deduplicate stream missing: {}", operator.stream))?;
        let state = transaction
            .get::<StreamState>(&stream_state_key(&operator.stream))?
            .ok_or_else(|| anyhow!("deduplicate stream state missing: {}", operator.stream))?;
        let Some(frontier) = completeness_frontier(&config, &state) else {
            if changed {
                transaction.put(storage_key, &operator)?;
            }
            continue;
        };
        let mut due: Vec<(String, StreamRecord)> = transaction
            .scan::<StreamRecord>(&deduplicate_buffer_prefix(&operator.operator_id))?
            .into_iter()
            .filter(|(_, record)| record.event_time <= frontier)
            .collect();
        due.sort_by(|(_, left), (_, right)| {
            left.event_time
                .total_cmp(&right.event_time)
                .then(left.sequence.cmp(&right.sequence))
        });
        for (buffer_key, record) in due {
            let key = record
                .key
                .as_deref()
                .expect("buffered deduplicate record has a key");
            let canonical_key = deduplicate_state_key(&operator.operator_id, key);
            let existing = transaction.get::<StreamRecord>(&canonical_key)?;
            let (canonical, workflow_id) = if let Some(canonical) = existing {
                operator.duplicates_suppressed += 1;
                (canonical, None)
            } else {
                let workflow_id = emit_deduplicated_record(transaction, &operator, &record)?;
                transaction.put(canonical_key, &record)?;
                operator.records_emitted += 1;
                (record.clone(), Some(workflow_id))
            };
            transaction.put(
                deduplicate_output_key(&operator.operator_id, &record),
                &DeduplicateOutput {
                    operator_id: operator.operator_id.clone(),
                    canonical: workflow_id.is_some(),
                    canonical_record: canonical,
                    record,
                    workflow_id,
                },
            )?;
            transaction.delete(buffer_key);
            changed = true;
        }
        if state.finalized {
            for (key, _) in transaction
                .scan::<StreamRecord>(&deduplicate_state_prefix(&operator.operator_id))?
            {
                transaction.delete(key);
            }
        }
        if changed {
            transaction.put(storage_key, &operator)?;
        }
    }
    Ok(())
}

pub(crate) fn field_value<'a>(value: &'a Value, field: &str) -> Option<&'a Value> {
    field
        .split('.')
        .try_fold(value, |current, segment| current.get(segment))
}

pub(crate) fn matches_filter(filter: &StreamFilter, record: &StreamRecord) -> bool {
    let Some(value) = field_value(&record.value, &filter.field) else {
        return false;
    };
    match filter.comparison {
        Comparison::Equal => value == &filter.operand,
        Comparison::NotEqual => value != &filter.operand,
        comparison => {
            let Some(left) = value.as_f64() else {
                return false;
            };
            let Some(right) = filter.operand.as_f64() else {
                return false;
            };
            match comparison {
                Comparison::GreaterThan => left > right,
                Comparison::GreaterThanOrEqual => left >= right,
                Comparison::LessThan => left < right,
                Comparison::LessThanOrEqual => left <= right,
                Comparison::Equal | Comparison::NotEqual => unreachable!(),
            }
        }
    }
}

pub(crate) fn emit_stream_filter_record(
    transaction: &mut Transaction<'_>,
    filter: &StreamFilter,
    record: &StreamRecord,
) -> Result<bool> {
    let output_key = stream_filter_output_key(&filter.operator_id, record);
    if transaction
        .get::<StreamFilterOutput>(&output_key)?
        .is_some()
    {
        return Ok(false);
    }
    let workflow_id = format!(
        "stream-filter/{}/{:010}/{:020}",
        filter.operator_id, record.partition, record.offset,
    );
    let timestamp = now();
    let workflow = WorkflowRecord {
        workflow_id: workflow_id.clone(),
        workflow_type: filter.workflow_type.clone(),
        status: "RUNNING".to_owned(),
        result: None,
        error: None,
        task_queue: filter.task_queue.clone(),
        build_id: None,
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
            "workflow_type": filter.workflow_type,
            "args": [record],
            "run_number": 1,
            "stream_filter": filter.operator_id,
        }),
    )?;
    enqueue_workflow(transaction, &workflow)?;
    transaction.put(
        output_key,
        &StreamFilterOutput {
            operator_id: filter.operator_id.clone(),
            record: record.clone(),
            workflow_id,
        },
    )?;
    append_operator_change(
        transaction,
        &filter.operator_id,
        record.key.clone(),
        record.event_time,
        record.kind,
        record.value.clone(),
    )?;
    Ok(true)
}

pub(crate) fn refresh_stream_filters(
    transaction: &mut Transaction<'_>,
    record: Option<&StreamRecord>,
) -> Result<()> {
    let Some(record) = record else {
        return Ok(());
    };
    let filters = transaction
        .scan::<StreamFilter>("stream-filter/")?
        .into_iter()
        .filter(|(_, filter)| filter.status == "ACTIVE" && filter.stream == record.stream)
        .collect::<Vec<_>>();
    for (storage_key, mut filter) in filters {
        filter.records_received += 1;
        if matches_filter(&filter, record)
            && emit_stream_filter_record(transaction, &filter, record)?
        {
            filter.records_emitted += 1;
        }
        transaction.put(storage_key, &filter)?;
    }
    Ok(())
}
