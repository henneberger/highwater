use crate::maintenance::maintain_event_time;
use crate::*;
pub(crate) async fn create_stream(
    State(app): State<AppState>,
    Json(request): Json<CreateStreamRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let timestamp = now();
    let config = StreamConfig {
        name: request.name,
        partitions: request.partitions,
        watermark_mode: request.watermark_mode,
        max_out_of_orderness: request.max_out_of_orderness,
        idle_timeout: request.idle_timeout,
        allowed_lateness: request.allowed_lateness,
        alignment_max_drift: request.alignment_max_drift,
        late_policy: request.late_policy,
        created_at: timestamp,
    };
    config.validate()?;
    app.commit(|transaction| {
        if transaction
            .get::<StreamConfig>(&stream_config_key(&config.name))?
            .is_some()
        {
            bail!("stream already exists: {}", config.name);
        }
        transaction.put(stream_config_key(&config.name), &config)?;
        transaction.put(stream_state_key(&config.name), &StreamState::new(timestamp))?;
        for partition in 0..config.partitions {
            transaction.put(
                stream_partition_key(&config.name, partition),
                &PartitionState::new(partition, timestamp),
            )?;
        }
        Ok(())
    })?;
    Ok((StatusCode::CREATED, Json(config)))
}

pub(crate) async fn get_stream(
    State(app): State<AppState>,
    Path(stream): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let config = app
        .store
        .get::<StreamConfig>(&stream_config_key(&stream))?
        .ok_or_else(|| anyhow!("stream not found: {stream}"))?;
    let state = app
        .store
        .get::<StreamState>(&stream_state_key(&stream))?
        .ok_or_else(|| anyhow!("stream state missing: {stream}"))?;
    let partitions: Vec<PartitionState> = (0..config.partitions)
        .map(|partition| {
            app.store
                .get::<PartitionState>(&stream_partition_key(&stream, partition))?
                .ok_or_else(|| anyhow!("stream partition {partition} is missing"))
        })
        .collect::<Result<_>>()?;
    let blockers = partitions
        .iter()
        .filter(|partition| !partition.idle && !partition.sealed)
        .filter(|partition| state.watermark.is_none() || partition.watermark == state.watermark)
        .map(|partition| partition.partition)
        .collect::<Vec<_>>();
    let stalled_reason = if state.finalized {
        None
    } else if partitions
        .iter()
        .any(|partition| !partition.idle && !partition.sealed && partition.watermark.is_none())
    {
        Some("an active partition has not produced a watermark")
    } else if blockers.is_empty() {
        Some("all unsealed partitions are idle")
    } else {
        Some("waiting for the lowest active partition watermark")
    };
    Ok(Json(json!({
        "config": config,
        "watermark": state.watermark,
        "finalized": state.finalized,
        "max_event_time": state.max_event_time,
        "partitions": partitions,
        "watermark_diagnostics": {
            "mode": config.watermark_mode,
            "blocking_partitions": blockers,
            "completeness_frontier": completeness_frontier(&config, &state),
            "stalled_reason": stalled_reason,
        },
    })))
}

pub(crate) fn append_stream_record_transaction(
    transaction: &mut Transaction<'_>,
    stream: &str,
    request: &AppendStreamRecordRequest,
    node_id: &str,
    key_group_count: u32,
    lease_seconds: f64,
) -> Result<Value> {
    let config = transaction
        .get::<StreamConfig>(&stream_config_key(stream))?
        .ok_or_else(|| anyhow!("stream not found: {stream}"))?;
    if request.partition >= config.partitions {
        bail!("partition {} is outside the stream", request.partition);
    }
    let mut state = transaction
        .get::<StreamState>(&stream_state_key(stream))?
        .ok_or_else(|| anyhow!("stream state missing"))?;
    let mut partitions = load_stream_partitions(transaction, &config)?;
    let key_group = key_group_for(request.key.as_deref(), request.partition, key_group_count);
    let owner_epoch = owned_key_group_epoch(transaction, node_id, key_group)?;
    let source = match (
        request.source_id.as_deref(),
        request.source_partition,
        request.source_offset,
    ) {
        (None, None, None) => None,
        (Some(source_id), Some(partition), Some(offset)) if !source_id.trim().is_empty() => {
            Some((source_id, partition, offset))
        }
        _ => bail!("source_id, source_partition, and source_offset must be provided together"),
    };
    if request
        .source_checkpoint
        .as_ref()
        .is_some_and(|checkpoint| {
            source.is_none() || checkpoint.is_empty() || checkpoint.len() > 4_096
        })
    {
        bail!("source_checkpoint must be 1..4096 bytes and accompanied by source metadata");
    }
    if let Some((source_id, _, _)) = source {
        claim_source_partition(
            transaction,
            stream,
            request.partition,
            source_id,
            request.source_epoch,
            lease_seconds,
        )?;
    }
    let effective_event_id = request.event_id.clone().or_else(|| {
        source.map(|(source_id, partition, offset)| {
            format!("source:{source_id}:{partition}:{offset}")
        })
    });
    if let Some(event_id) = effective_event_id.as_deref() {
        if event_id.trim().is_empty() {
            bail!("event_id must not be empty");
        }
        if let Some(existing) =
            transaction.get::<StreamRecord>(&stream_event_id_key(stream, event_id))?
        {
            if existing.partition != request.partition
                || existing.event_time != request.event_time
                || existing.key != request.key
                || existing.value != request.value
                || existing.kind != request.kind
            {
                bail!("event_id was already used with different record contents");
            }
            return Ok(json!({
                "record": existing,
                "disposition": "duplicate",
                "watermark_before": state.watermark,
                "watermark": state.watermark,
                "finalized": state.finalized,
            }));
        }
    }
    if let Some((source_id, partition, offset)) = source {
        let cursor = transaction
            .get::<SourceCursor>(&source_cursor_key(stream, source_id, partition))?
            .unwrap_or(SourceCursor {
                stream: stream.to_owned(),
                source_id: source_id.to_owned(),
                partition,
                next_offset: 0,
                checkpoint: None,
            });
        if offset != cursor.next_offset {
            bail!(
                "source {source_id} partition {partition} expected offset {}, got {offset}",
                cursor.next_offset,
            );
        }
    }
    ensure_process_capacity(transaction, stream)?;
    let prior_watermark = state.watermark;
    let late = state.is_late(request.event_time);
    let too_late = state.is_too_late(request.event_time, config.allowed_lateness);
    let watermark_delay = match config.watermark_mode {
        WatermarkMode::Bounded => config.max_out_of_orderness,
        WatermarkMode::Monotonic => 0.0,
        WatermarkMode::SourceManaged => config.max_out_of_orderness,
    };
    if config.watermark_mode != WatermarkMode::SourceManaged {
        let candidate = partitions[request.partition as usize]
            .watermark
            .map_or(request.event_time - watermark_delay, |current| {
                current.max(request.event_time - watermark_delay)
            });
        ensure_watermark_alignment(&config, &partitions, request.partition, candidate)?;
    }
    let partition = &mut partitions[request.partition as usize];
    let offset = partition.observe(
        request.event_time,
        watermark_delay,
        config.watermark_mode != WatermarkMode::SourceManaged,
        now(),
    )?;
    let sequence = transaction.get::<u64>("meta/stream_sequence")?.unwrap_or(0) + 1;
    transaction.put("meta/stream_sequence", &sequence)?;
    let record = StreamRecord {
        stream: stream.to_owned(),
        partition: request.partition,
        offset,
        sequence,
        event_time: request.event_time,
        ingestion_time: now(),
        key: request.key.clone(),
        value: request.value.clone(),
        kind: request.kind,
        event_id: effective_event_id.clone(),
        key_group,
        owner_epoch,
        source_id: request.source_id.clone(),
        source_partition: request.source_partition,
        source_offset: request.source_offset,
        late,
        too_late,
    };
    let disposition = if too_late {
        match config.late_policy {
            LatePolicy::Drop => "dropped",
            LatePolicy::SideOutput => {
                transaction.put(late_record_key(stream, sequence), &record)?;
                "side_output"
            }
            LatePolicy::Accept => {
                transaction.put(
                    stream_record_key(stream, request.partition, offset),
                    &record,
                )?;
                "accepted_too_late"
            }
        }
    } else {
        transaction.put(
            stream_record_key(stream, request.partition, offset),
            &record,
        )?;
        if late { "accepted_late" } else { "accepted" }
    };
    if let Some(event_id) = effective_event_id.as_deref() {
        transaction.put(stream_event_id_key(stream, event_id), &record)?;
    }
    if let Some((source_id, partition, offset)) = source {
        let next_offset = offset
            .checked_add(1)
            .ok_or_else(|| anyhow!("source offset overflow"))?;
        transaction.put(
            source_cursor_key(stream, source_id, partition),
            &SourceCursor {
                stream: stream.to_owned(),
                source_id: source_id.to_owned(),
                partition,
                next_offset,
                checkpoint: request.source_checkpoint.clone(),
            },
        )?;
    }
    transaction.put(
        stream_partition_key(stream, request.partition),
        &partitions[request.partition as usize],
    )?;
    let accepted = !too_late || config.late_policy == LatePolicy::Accept;
    if accepted {
        index_window_aggregates(transaction, &record)?;
    }
    refresh_stream(transaction, &config, &mut state, &mut partitions, now())?;
    refresh_declarative_operators(transaction, accepted.then_some(&record))?;
    Ok(json!({
        "record": record,
        "disposition": disposition,
        "watermark_before": prior_watermark,
        "watermark": state.watermark,
        "finalized": state.finalized,
    }))
}

pub(crate) fn record_stream_batch(
    transaction: &mut Transaction<'_>,
    stream: &str,
    responses: &[Value],
) -> Result<()> {
    let sequences = responses
        .iter()
        .filter(|response| response["disposition"] != "duplicate")
        .filter_map(|response| response["record"]["sequence"].as_u64())
        .collect::<Vec<_>>();
    let Some(first_sequence) = sequences.first().copied() else {
        return Ok(());
    };
    let last_sequence = sequences.last().copied().unwrap_or(first_sequence);
    transaction.put(
        stream_batch_key(stream, last_sequence),
        &StreamBatchCommit {
            batch_id: format!("{stream}:{first_sequence}:{last_sequence}"),
            stream: stream.to_owned(),
            first_sequence,
            last_sequence,
            records: sequences.len() as u64,
            committed_at: now(),
        },
    )
}

pub(crate) async fn append_stream_record(
    State(app): State<AppState>,
    Path(stream): Path<String>,
    Json(request): Json<AppendStreamRecordRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let mut response = Value::Null;
    app.commit(|transaction| {
        response = append_stream_record_transaction(
            transaction,
            &stream,
            &request,
            &app.node_id,
            app.key_group_count,
            app.lease_seconds,
        )?;
        record_stream_batch(transaction, &stream, std::slice::from_ref(&response))
    })?;
    Ok((StatusCode::CREATED, Json(response)))
}

pub(crate) async fn append_stream_records(
    State(app): State<AppState>,
    Path(stream): Path<String>,
    Json(request): Json<AppendStreamRecordsRequest>,
) -> Result<impl IntoResponse, ApiError> {
    if request.records.is_empty() || request.records.len() > 1_000 {
        return Err(ApiError(anyhow!(
            "record batches must contain between 1 and 1000 events"
        )));
    }
    let mut responses = Vec::with_capacity(request.records.len());
    app.commit(|transaction| {
        for record in &request.records {
            responses.push(append_stream_record_transaction(
                transaction,
                &stream,
                record,
                &app.node_id,
                app.key_group_count,
                app.lease_seconds,
            )?);
        }
        record_stream_batch(transaction, &stream, &responses)
    })?;
    Ok((StatusCode::CREATED, Json(responses)))
}

pub(crate) async fn advance_stream_watermark(
    State(app): State<AppState>,
    Path((stream, partition)): Path<(String, u32)>,
    Json(request): Json<AdvanceWatermarkRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let mut response = Value::Null;
    app.commit(|transaction| {
        let config = transaction
            .get::<StreamConfig>(&stream_config_key(&stream))?
            .ok_or_else(|| anyhow!("stream not found: {stream}"))?;
        if partition >= config.partitions {
            bail!("partition {partition} is outside the stream");
        }
        let mut state = transaction
            .get::<StreamState>(&stream_state_key(&stream))?
            .ok_or_else(|| anyhow!("stream state missing"))?;
        let mut partitions = load_stream_partitions(transaction, &config)?;
        ensure_watermark_alignment(&config, &partitions, partition, request.event_time)?;
        partitions[partition as usize].advance_watermark(request.event_time, now())?;
        transaction.put(
            stream_partition_key(&stream, partition),
            &partitions[partition as usize],
        )?;
        refresh_stream(transaction, &config, &mut state, &mut partitions, now())?;
        refresh_declarative_operators(transaction, None)?;
        response = json!({"watermark": state.watermark, "finalized": state.finalized});
        Ok(())
    })?;
    Ok(Json(response))
}

pub(crate) async fn seal_stream_partition(
    State(app): State<AppState>,
    Path((stream, partition)): Path<(String, u32)>,
) -> Result<impl IntoResponse, ApiError> {
    let mut response = Value::Null;
    app.commit(|transaction| {
        let config = transaction
            .get::<StreamConfig>(&stream_config_key(&stream))?
            .ok_or_else(|| anyhow!("stream not found: {stream}"))?;
        if partition >= config.partitions {
            bail!("partition {partition} is outside the stream");
        }
        let mut state = transaction
            .get::<StreamState>(&stream_state_key(&stream))?
            .ok_or_else(|| anyhow!("stream state missing"))?;
        let mut partitions = load_stream_partitions(transaction, &config)?;
        partitions[partition as usize].sealed = true;
        partitions[partition as usize].idle = false;
        partitions[partition as usize].last_activity_at = now();
        transaction.put(
            stream_partition_key(&stream, partition),
            &partitions[partition as usize],
        )?;
        refresh_stream(transaction, &config, &mut state, &mut partitions, now())?;
        refresh_declarative_operators(transaction, None)?;
        response = json!({"watermark": state.watermark, "finalized": state.finalized});
        Ok(())
    })?;
    Ok(Json(response))
}

pub(crate) async fn read_stream_records(
    State(app): State<AppState>,
    Path(stream): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    if app
        .store
        .get::<StreamConfig>(&stream_config_key(&stream))?
        .is_none()
    {
        return Err(ApiError(anyhow!("stream not found: {stream}")));
    }
    let mut records: Vec<StreamRecord> = app
        .store
        .scan::<StreamRecord>(&stream_record_prefix(&stream))?
        .into_iter()
        .map(|(_, record)| record)
        .collect();
    records.sort_by_key(|record| record.sequence);
    Ok(Json(records))
}

pub(crate) async fn read_late_stream_records(
    State(app): State<AppState>,
    Path(stream): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let records: Vec<StreamRecord> = app
        .store
        .scan::<StreamRecord>(&late_record_prefix(&stream))?
        .into_iter()
        .map(|(_, record)| record)
        .collect();
    Ok(Json(records))
}

pub(crate) async fn get_source_cursor(
    State(app): State<AppState>,
    Path((stream, source_id, partition)): Path<(String, String, u32)>,
) -> Result<impl IntoResponse, ApiError> {
    let cursor = app
        .store
        .get::<SourceCursor>(&source_cursor_key(&stream, &source_id, partition))?
        .unwrap_or(SourceCursor {
            stream,
            source_id,
            partition,
            next_offset: 0,
            checkpoint: None,
        });
    Ok(Json(cursor))
}

pub(crate) async fn claim_source(
    State(app): State<AppState>,
    Path((stream, partition, source_id)): Path<(String, u32, String)>,
    Json(request): Json<ClaimSourceRequest>,
) -> Result<impl IntoResponse, ApiError> {
    if request.lease_seconds <= 0.0 {
        return Err(ApiError(anyhow!("lease_seconds must be positive")));
    }
    let mut claimed = None;
    app.commit(|transaction| {
        let config = transaction
            .get::<StreamConfig>(&stream_config_key(&stream))?
            .ok_or_else(|| anyhow!("stream not found: {stream}"))?;
        if partition >= config.partitions {
            bail!("partition {partition} is outside the stream");
        }
        claimed = Some(claim_source_partition(
            transaction,
            &stream,
            partition,
            &source_id,
            None,
            request.lease_seconds,
        )?);
        Ok(())
    })?;
    Ok(Json(claimed.expect("source claim committed")))
}

pub(crate) async fn create_window_schedule(
    State(app): State<AppState>,
    Json(request): Json<CreateWindowScheduleRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let schedule = WindowSchedule {
        schedule_id: request.schedule_id,
        stream: request.stream,
        workflow_type: request.workflow_type,
        task_queue: request.task_queue,
        window_size: request.window_size,
        slide: request.slide.unwrap_or(request.window_size),
        start_at: request.start_at,
        next_window_start: request.start_at,
        emit_empty_windows: request.emit_empty_windows,
        aggregation: request.aggregation,
        value_field: request.value_field,
        status: "ACTIVE".to_owned(),
        created_at: now(),
        windows_fired: 0,
    };
    schedule.validate()?;
    let schedule_id = schedule.schedule_id.clone();
    let mut created = false;
    app.commit(|transaction| {
        let config = transaction
            .get::<StreamConfig>(&stream_config_key(&schedule.stream))?
            .ok_or_else(|| anyhow!("stream not found: {}", schedule.stream))?;
        if config.late_policy == LatePolicy::Accept {
            bail!(
                "final window schedules require drop or side_output for data beyond allowed lateness"
            );
        }
        let key = stream_schedule_key(&schedule.schedule_id);
        if let Some(existing) = transaction.get::<WindowSchedule>(&key)? {
            if existing.has_same_spec(&schedule) {
                return Ok(());
            }
            bail!(
                "stream schedule {} already exists with a different specification",
                schedule.schedule_id,
            );
        }
        created = true;
        transaction.put(key, &schedule)?;
        let records: Vec<StreamRecord> = transaction
            .scan::<StreamRecord>(&stream_record_prefix(&schedule.stream))?
            .into_iter()
            .map(|(_, record)| record)
            .collect();
        for record in &records {
            index_window_aggregate(transaction, &schedule, record)?;
        }
        let state = transaction
            .get::<StreamState>(&stream_state_key(&schedule.stream))?
            .ok_or_else(|| anyhow!("stream state missing"))?;
        fire_due_stream_schedules(transaction, &config, &state)
    })?;
    let schedule = app
        .store
        .get::<WindowSchedule>(&stream_schedule_key(&schedule_id))?
        .ok_or_else(|| anyhow!("stream schedule missing after creation: {schedule_id}"))?;
    Ok((
        if created {
            StatusCode::CREATED
        } else {
            StatusCode::OK
        },
        Json(schedule),
    ))
}

pub(crate) async fn get_window_schedule(
    State(app): State<AppState>,
    Path(schedule_id): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let schedule = app
        .store
        .get::<WindowSchedule>(&stream_schedule_key(&schedule_id))?
        .ok_or_else(|| anyhow!("stream schedule not found: {schedule_id}"))?;
    Ok(Json(schedule))
}

pub(crate) async fn create_temporal_join(
    State(app): State<AppState>,
    Json(request): Json<CreateTemporalJoinRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let mut join = TemporalJoin {
        join_id: request.join_id,
        probe_stream: request.probe_stream,
        version_stream: request.version_stream,
        workflow_type: request.workflow_type,
        task_queue: request.task_queue,
        join_type: request.join_type,
        status: "ACTIVE".to_owned(),
        created_at: now(),
        probes_received: 0,
        versions_received: 0,
        probes_emitted: 0,
        matches_emitted: 0,
    };
    join.validate()?;
    let join_id = join.join_id.clone();
    let mut created = false;
    app.commit(|transaction| {
        let probe_config = transaction
            .get::<StreamConfig>(&stream_config_key(&join.probe_stream))?
            .ok_or_else(|| anyhow!("probe stream not found: {}", join.probe_stream))?;
        let version_config = transaction
            .get::<StreamConfig>(&stream_config_key(&join.version_stream))?
            .ok_or_else(|| anyhow!("version stream not found: {}", join.version_stream))?;
        if probe_config.late_policy == LatePolicy::Accept
            || version_config.late_policy == LatePolicy::Accept
        {
            bail!("temporal joins require drop or side_output late policies");
        }
        let storage_key = temporal_join_key(&join.join_id);
        if let Some(existing) = transaction.get::<TemporalJoin>(&storage_key)? {
            if existing.has_same_spec(&join) {
                return Ok(());
            }
            bail!(
                "temporal join {} already exists with a different specification",
                join.join_id,
            );
        }
        created = true;
        let probe_records: Vec<StreamRecord> = transaction
            .scan::<StreamRecord>(&stream_record_prefix(&join.probe_stream))?
            .into_iter()
            .map(|(_, record)| record)
            .collect();
        let version_records: Vec<StreamRecord> = transaction
            .scan::<StreamRecord>(&stream_record_prefix(&join.version_stream))?
            .into_iter()
            .map(|(_, record)| record)
            .collect();
        for record in version_records.iter().chain(probe_records.iter()) {
            index_temporal_join_record(transaction, &mut join, record)?;
        }
        transaction.put(storage_key, &join)?;
        refresh_declarative_operators(transaction, None)
    })?;
    let join = app
        .store
        .get::<TemporalJoin>(&temporal_join_key(&join_id))?
        .ok_or_else(|| anyhow!("temporal join missing after creation: {join_id}"))?;
    Ok((
        if created {
            StatusCode::CREATED
        } else {
            StatusCode::OK
        },
        Json(join),
    ))
}

pub(crate) async fn get_temporal_join(
    State(app): State<AppState>,
    Path(join_id): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let join = app
        .store
        .get::<TemporalJoin>(&temporal_join_key(&join_id))?
        .ok_or_else(|| anyhow!("temporal join not found: {join_id}"))?;
    Ok(Json(join))
}

pub(crate) async fn read_temporal_join_outputs(
    State(app): State<AppState>,
    Path(join_id): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    if app
        .store
        .get::<TemporalJoin>(&temporal_join_key(&join_id))?
        .is_none()
    {
        return Err(ApiError(anyhow!("temporal join not found: {join_id}")));
    }
    let mut outputs: Vec<TemporalJoinOutput> = app
        .store
        .scan::<TemporalJoinOutput>(&temporal_join_output_prefix(&join_id))?
        .into_iter()
        .map(|(_, output)| output)
        .collect();
    outputs.sort_by_key(|output| output.probe.sequence);
    Ok(Json(outputs))
}

pub(crate) async fn create_interval_join(
    State(app): State<AppState>,
    Json(request): Json<CreateIntervalJoinRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let mut join = IntervalJoin {
        join_id: request.join_id,
        left_stream: request.left_stream,
        right_stream: request.right_stream,
        workflow_type: request.workflow_type,
        task_queue: request.task_queue,
        lower_bound: request.lower_bound,
        upper_bound: request.upper_bound,
        join_type: request.join_type,
        status: "ACTIVE".to_owned(),
        created_at: now(),
        left_received: 0,
        right_received: 0,
        pairs_emitted: 0,
    };
    join.validate()?;
    let join_id = join.join_id.clone();
    let mut created = false;
    app.commit(|transaction| {
        let left_config = transaction
            .get::<StreamConfig>(&stream_config_key(&join.left_stream))?
            .ok_or_else(|| anyhow!("left stream not found: {}", join.left_stream))?;
        let right_config = transaction
            .get::<StreamConfig>(&stream_config_key(&join.right_stream))?
            .ok_or_else(|| anyhow!("right stream not found: {}", join.right_stream))?;
        if left_config.late_policy == LatePolicy::Accept
            || right_config.late_policy == LatePolicy::Accept
        {
            bail!("interval joins require drop or side_output late policies");
        }
        let storage_key = interval_join_key(&join.join_id);
        if let Some(existing) = transaction.get::<IntervalJoin>(&storage_key)? {
            if existing.has_same_spec(&join) {
                return Ok(());
            }
            bail!(
                "interval join {} already exists with a different specification",
                join.join_id,
            );
        }
        created = true;
        let mut left_records: Vec<StreamRecord> = transaction
            .scan::<StreamRecord>(&stream_record_prefix(&join.left_stream))?
            .into_iter()
            .map(|(_, record)| record)
            .collect();
        let mut right_records: Vec<StreamRecord> = if join.left_stream == join.right_stream {
            Vec::new()
        } else {
            transaction
                .scan::<StreamRecord>(&stream_record_prefix(&join.right_stream))?
                .into_iter()
                .map(|(_, record)| record)
                .collect()
        };
        left_records.sort_by_key(|record| record.sequence);
        right_records.sort_by_key(|record| record.sequence);
        for record in left_records.iter().chain(&right_records) {
            index_interval_join_record(transaction, &mut join, record)?;
        }
        transaction.put(storage_key, &join)?;
        refresh_interval_joins(transaction, None)
    })?;
    let join = app
        .store
        .get::<IntervalJoin>(&interval_join_key(&join_id))?
        .ok_or_else(|| anyhow!("interval join missing after creation: {join_id}"))?;
    Ok((
        if created {
            StatusCode::CREATED
        } else {
            StatusCode::OK
        },
        Json(join),
    ))
}

pub(crate) async fn get_interval_join(
    State(app): State<AppState>,
    Path(join_id): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let join = app
        .store
        .get::<IntervalJoin>(&interval_join_key(&join_id))?
        .ok_or_else(|| anyhow!("interval join not found: {join_id}"))?;
    Ok(Json(join))
}

pub(crate) async fn read_interval_join_outputs(
    State(app): State<AppState>,
    Path(join_id): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    if app
        .store
        .get::<IntervalJoin>(&interval_join_key(&join_id))?
        .is_none()
    {
        return Err(ApiError(anyhow!("interval join not found: {join_id}")));
    }
    let mut outputs: Vec<IntervalJoinOutput> = app
        .store
        .scan::<IntervalJoinOutput>(&interval_join_output_prefix(&join_id))?
        .into_iter()
        .map(|(_, output)| output)
        .collect();
    outputs.sort_by_key(|output| {
        (
            output.left.as_ref().map_or(u64::MAX, |left| left.sequence),
            output
                .right
                .as_ref()
                .map_or(u64::MAX, |right| right.sequence),
        )
    });
    Ok(Json(outputs))
}

pub(crate) async fn create_deduplicate(
    State(app): State<AppState>,
    Json(request): Json<CreateDeduplicateRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let mut operator = Deduplicate {
        operator_id: request.operator_id,
        stream: request.stream,
        workflow_type: request.workflow_type,
        task_queue: request.task_queue,
        status: "ACTIVE".to_owned(),
        created_at: now(),
        records_received: 0,
        records_emitted: 0,
        duplicates_suppressed: 0,
    };
    operator.validate()?;
    let operator_id = operator.operator_id.clone();
    let mut created = false;
    app.commit(|transaction| {
        let config = transaction
            .get::<StreamConfig>(&stream_config_key(&operator.stream))?
            .ok_or_else(|| anyhow!("deduplicate stream not found: {}", operator.stream))?;
        if config.late_policy == LatePolicy::Accept {
            bail!("deduplicate operators require drop or side_output late policies");
        }
        let storage_key = deduplicate_key(&operator.operator_id);
        if let Some(existing) = transaction.get::<Deduplicate>(&storage_key)? {
            if existing.has_same_spec(&operator) {
                return Ok(());
            }
            bail!(
                "deduplicate operator {} already exists with a different specification",
                operator.operator_id,
            );
        }
        created = true;
        let records: Vec<StreamRecord> = transaction
            .scan::<StreamRecord>(&stream_record_prefix(&operator.stream))?
            .into_iter()
            .map(|(_, record)| record)
            .collect();
        for record in &records {
            index_deduplicate_record(transaction, &mut operator, record)?;
        }
        transaction.put(storage_key, &operator)?;
        refresh_deduplicates(transaction, None)
    })?;
    let operator = app
        .store
        .get::<Deduplicate>(&deduplicate_key(&operator_id))?
        .ok_or_else(|| anyhow!("deduplicate operator missing after creation: {operator_id}"))?;
    Ok((
        if created {
            StatusCode::CREATED
        } else {
            StatusCode::OK
        },
        Json(operator),
    ))
}

pub(crate) async fn get_deduplicate(
    State(app): State<AppState>,
    Path(operator_id): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let operator = app
        .store
        .get::<Deduplicate>(&deduplicate_key(&operator_id))?
        .ok_or_else(|| anyhow!("deduplicate operator not found: {operator_id}"))?;
    Ok(Json(operator))
}

pub(crate) async fn read_deduplicate_outputs(
    State(app): State<AppState>,
    Path(operator_id): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    if app
        .store
        .get::<Deduplicate>(&deduplicate_key(&operator_id))?
        .is_none()
    {
        return Err(ApiError(anyhow!(
            "deduplicate operator not found: {operator_id}"
        )));
    }
    let mut outputs: Vec<DeduplicateOutput> = app
        .store
        .scan::<DeduplicateOutput>(&deduplicate_output_prefix(&operator_id))?
        .into_iter()
        .map(|(_, output)| output)
        .collect();
    outputs.sort_by_key(|output| output.record.sequence);
    Ok(Json(outputs))
}

pub(crate) async fn create_stream_filter(
    State(app): State<AppState>,
    Json(request): Json<CreateStreamFilterRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let mut filter = StreamFilter {
        operator_id: request.operator_id,
        stream: request.stream,
        workflow_type: request.workflow_type,
        task_queue: request.task_queue,
        field: request.field,
        comparison: request.comparison,
        operand: request.operand,
        status: "ACTIVE".to_owned(),
        created_at: now(),
        records_received: 0,
        records_emitted: 0,
    };
    filter.validate()?;
    let operator_id = filter.operator_id.clone();
    let mut created = false;
    app.commit(|transaction| {
        if transaction
            .get::<StreamConfig>(&stream_config_key(&filter.stream))?
            .is_none()
        {
            bail!("filter stream not found: {}", filter.stream);
        }
        let storage_key = stream_filter_key(&filter.operator_id);
        if let Some(existing) = transaction.get::<StreamFilter>(&storage_key)? {
            if existing.has_same_spec(&filter) {
                return Ok(());
            }
            bail!(
                "stream filter {} already exists with a different specification",
                filter.operator_id,
            );
        }
        created = true;
        let records = transaction
            .scan::<StreamRecord>(&stream_record_prefix(&filter.stream))?
            .into_iter()
            .map(|(_, record)| record)
            .collect::<Vec<_>>();
        for record in &records {
            filter.records_received += 1;
            if matches_filter(&filter, record)
                && emit_stream_filter_record(transaction, &filter, record)?
            {
                filter.records_emitted += 1;
            }
        }
        transaction.put(storage_key, &filter)
    })?;
    let filter = app
        .store
        .get::<StreamFilter>(&stream_filter_key(&operator_id))?
        .ok_or_else(|| anyhow!("stream filter missing after creation: {operator_id}"))?;
    Ok((
        if created {
            StatusCode::CREATED
        } else {
            StatusCode::OK
        },
        Json(filter),
    ))
}

pub(crate) async fn get_stream_filter(
    State(app): State<AppState>,
    Path(operator_id): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let filter = app
        .store
        .get::<StreamFilter>(&stream_filter_key(&operator_id))?
        .ok_or_else(|| anyhow!("stream filter not found: {operator_id}"))?;
    Ok(Json(filter))
}

pub(crate) async fn read_stream_filter_outputs(
    State(app): State<AppState>,
    Path(operator_id): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    if app
        .store
        .get::<StreamFilter>(&stream_filter_key(&operator_id))?
        .is_none()
    {
        return Err(ApiError(anyhow!("stream filter not found: {operator_id}")));
    }
    let mut outputs = app
        .store
        .scan::<StreamFilterOutput>(&stream_filter_output_prefix(&operator_id))?
        .into_iter()
        .map(|(_, output)| output)
        .collect::<Vec<_>>();
    outputs.sort_by_key(|output| output.record.sequence);
    Ok(Json(outputs))
}

pub(crate) async fn read_operator_changes(
    State(app): State<AppState>,
    Path(operator_id): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let changes = app
        .store
        .scan::<DifferentialChange>(&operator_change_prefix(&operator_id))?
        .into_iter()
        .map(|(_, change)| change)
        .collect::<Vec<_>>();
    Ok(Json(changes))
}

pub(crate) async fn create_operator_edge(
    State(app): State<AppState>,
    Json(request): Json<CreateOperatorEdgeRequest>,
) -> Result<impl IntoResponse, ApiError> {
    if request.operator_id.trim().is_empty() || request.output_stream.trim().is_empty() {
        return Err(ApiError(anyhow!(
            "operator_id and output_stream must not be empty"
        )));
    }
    let mut created = false;
    app.commit(|transaction| {
        let inputs = operator_input_streams(transaction, &request.operator_id)?;
        if stream_reaches_any(
            transaction,
            &request.output_stream,
            &inputs,
            &mut HashSet::new(),
        )? {
            bail!("operator edge would create a cycle");
        }
        let output = transaction
            .get::<StreamConfig>(&stream_config_key(&request.output_stream))?
            .ok_or_else(|| anyhow!("output stream not found: {}", request.output_stream))?;
        if output.watermark_mode != WatermarkMode::SourceManaged {
            bail!("operator edge output streams must use source-managed watermarks");
        }
        if transaction
            .scan::<OperatorEdge>("operator-edge/")?
            .into_iter()
            .any(|(_, edge)| {
                edge.status == "ACTIVE"
                    && edge.output_stream == request.output_stream
                    && edge.operator_id != request.operator_id
            })
        {
            bail!("an output stream can currently have only one upstream operator edge");
        }
        let key = operator_edge_key(&request.operator_id);
        if let Some(existing) = transaction.get::<OperatorEdge>(&key)? {
            if existing.output_stream == request.output_stream {
                return Ok(());
            }
            bail!(
                "operator {} already has an output edge",
                request.operator_id
            );
        }
        let edge = OperatorEdge {
            operator_id: request.operator_id.clone(),
            output_stream: request.output_stream.clone(),
            status: "ACTIVE".to_owned(),
            created_at: now(),
            changes_forwarded: 0,
        };
        for (_, change) in
            transaction.scan::<DifferentialChange>(&operator_change_prefix(&request.operator_id))?
        {
            transaction.put(
                format!(
                    "{}{:020}",
                    operator_edge_pending_prefix(&request.operator_id),
                    change.sequence
                ),
                &change,
            )?;
        }
        transaction.put(key, &edge)?;
        created = true;
        Ok(())
    })?;
    maintain_event_time(&app)?;
    let edge = app
        .store
        .get::<OperatorEdge>(&operator_edge_key(&request.operator_id))?
        .ok_or_else(|| anyhow!("operator edge missing after creation"))?;
    Ok((
        if created {
            StatusCode::CREATED
        } else {
            StatusCode::OK
        },
        Json(edge),
    ))
}
