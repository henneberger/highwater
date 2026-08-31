use crate::*;
pub async fn run() -> Result<()> {
    let mut listen = "127.0.0.1:7233".to_owned();
    let mut state_dir = PathBuf::from("temporal-code-rust-state");
    let mut object_dir = PathBuf::from("temporal-code-rust-objects");
    let mut node_id = "local".to_owned();
    let mut key_group_count = 128_u32;
    let mut lease_seconds = 15.0_f64;
    let mut log_shards =
        std::thread::available_parallelism().map_or(4, std::num::NonZeroUsize::get) + 1;
    let mut arguments = env::args().skip(1);
    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "--listen" => listen = arguments.next().context("--listen requires a value")?,
            "--state-dir" => {
                state_dir = PathBuf::from(arguments.next().context("--state-dir requires a value")?)
            }
            "--object-store-dir" => {
                object_dir = PathBuf::from(
                    arguments
                        .next()
                        .context("--object-store-dir requires a value")?,
                )
            }
            "--node-id" => node_id = arguments.next().context("--node-id requires a value")?,
            "--key-groups" => {
                key_group_count = arguments
                    .next()
                    .context("--key-groups requires a value")?
                    .parse()
                    .context("--key-groups must be an integer")?;
            }
            "--lease-seconds" => {
                lease_seconds = arguments
                    .next()
                    .context("--lease-seconds requires a value")?
                    .parse()
                    .context("--lease-seconds must be numeric")?;
            }
            "--log-shards" => {
                log_shards = arguments
                    .next()
                    .context("--log-shards requires a value")?
                    .parse()
                    .context("--log-shards must be an integer")?;
            }
            _ => bail!("unknown argument: {argument}"),
        }
    }
    if node_id.trim().is_empty()
        || key_group_count == 0
        || lease_seconds <= 0.0
        || !(2..=256).contains(&log_shards)
    {
        bail!(
            "node-id must be non-empty; key-groups and lease-seconds must be positive; log-shards must be 2..256"
        );
    }
    let mut partition_senders = vec![None];
    let mut partition_receivers = Vec::new();
    for shard in 1..log_shards {
        let (sender, receiver) = mpsc::channel::<ProcessPartitionCommand>(4_096);
        partition_senders.push(Some(sender));
        partition_receivers.push((shard, receiver));
    }
    let runtime_id = format!("{node_id}:{}", Uuid::new_v4());
    let state = AppState {
        store: Arc::new(DurableStore::open_sharded(
            &state_dir,
            &object_dir,
            log_shards,
        )?),
        mutation_lock: Arc::new(Mutex::new(())),
        shard_locks: Arc::new((0..log_shards).map(|_| Mutex::new(())).collect()),
        partition_senders: Arc::new(partition_senders),
        node_id,
        runtime_id,
        key_group_count,
        lease_seconds,
        query_queue: Arc::new(Mutex::new(VecDeque::new())),
        query_results: Arc::new(Mutex::new(HashMap::new())),
    };
    initialize_key_groups(&state)?;
    initialize_process_partitions(&state)?;
    recover_process_tasks(&state, true)?;
    for (shard, receiver) in partition_receivers {
        tokio::spawn(process_partition_loop(state.clone(), shard, receiver));
    }
    tokio::spawn(event_time_maintenance_loop(state.clone()));
    let app = Router::new()
        .route("/workflows", post(start_workflow))
        .route("/workflows/{id}", get(get_workflow))
        .route("/workflows/{id}/history", get(history))
        .route("/workflows/{id}/signals/{name}", post(signal))
        .route("/workflows/{id}/updates/{name}", post(update))
        .route("/workflows/{id}/queries/{name}", post(query_workflow))
        .route("/workflows/{id}/cancel", post(cancel))
        .route("/workflows/{id}/terminate", post(terminate))
        .route("/internal/v1/workflow-tasks/poll", post(poll_workflow))
        .route(
            "/internal/v1/workflow-tasks/poll-batch",
            post(poll_workflow_batch),
        )
        .route(
            "/internal/v1/workflow-tasks/complete",
            post(complete_workflow),
        )
        .route(
            "/internal/v1/workflow-tasks/complete-batch",
            post(complete_workflow_batch),
        )
        .route(
            "/internal/v1/process-tasks/poll-batch",
            post(poll_process_batch),
        )
        .route(
            "/internal/v1/process-tasks/complete-batch",
            post(complete_process_batch),
        )
        .route(
            "/internal/v1/process-tasks/renew",
            post(renew_process_lease),
        )
        .route("/internal/v1/activity-tasks/poll", post(poll_activity))
        .route(
            "/internal/v1/activity-tasks/complete",
            post(complete_activity),
        )
        .route(
            "/internal/v1/activity-tasks/{token}/heartbeat",
            post(heartbeat_activity),
        )
        .route("/internal/v1/query-tasks/poll", post(poll_query))
        .route("/internal/v1/query-tasks/complete", post(complete_query))
        .route("/streams", post(create_stream))
        .route("/streams/{stream}", get(get_stream))
        .route(
            "/streams/{stream}/records",
            get(read_stream_records).post(append_stream_record),
        )
        .route(
            "/streams/{stream}/records/batch",
            post(append_stream_records),
        )
        .route(
            "/streams/{stream}/late-records",
            get(read_late_stream_records),
        )
        .route(
            "/streams/{stream}/sources/{source_id}/partitions/{partition}/cursor",
            get(get_source_cursor),
        )
        .route(
            "/streams/{stream}/partitions/{partition}/sources/{source_id}/claim",
            post(claim_source),
        )
        .route(
            "/streams/{stream}/partitions/{partition}/watermark",
            post(advance_stream_watermark),
        )
        .route(
            "/streams/{stream}/partitions/{partition}/seal",
            post(seal_stream_partition),
        )
        .route("/stream-schedules", post(create_window_schedule))
        .route("/stream-schedules/{schedule_id}", get(get_window_schedule))
        .route("/temporal-joins", post(create_temporal_join))
        .route("/temporal-joins/{join_id}", get(get_temporal_join))
        .route(
            "/temporal-joins/{join_id}/outputs",
            get(read_temporal_join_outputs),
        )
        .route("/interval-joins", post(create_interval_join))
        .route("/interval-joins/{join_id}", get(get_interval_join))
        .route(
            "/interval-joins/{join_id}/outputs",
            get(read_interval_join_outputs),
        )
        .route("/deduplicates", post(create_deduplicate))
        .route("/deduplicates/{operator_id}", get(get_deduplicate))
        .route(
            "/deduplicates/{operator_id}/outputs",
            get(read_deduplicate_outputs),
        )
        .route("/stream-filters", post(create_stream_filter))
        .route("/stream-filters/{operator_id}", get(get_stream_filter))
        .route(
            "/stream-filters/{operator_id}/outputs",
            get(read_stream_filter_outputs),
        )
        .route(
            "/operators/{operator_id}/changes",
            get(read_operator_changes),
        )
        .route("/operator-edges", post(create_operator_edge))
        .route("/operator-edges/{operator_id}", get(get_operator_edge))
        .route("/processes", post(create_process))
        .route("/processes/{process_id}", get(get_process))
        .route(
            "/processes/{process_id}/events",
            post(append_process_records),
        )
        .route(
            "/processes/{process_id}/events/packed",
            post(append_packed_process_records),
        )
        .route("/processes/{process_id}/keys/{key}", get(get_process_state))
        .route(
            "/processes/{process_id}/complete-through",
            post(complete_process_through),
        )
        .route("/admin/checkpoints", post(create_checkpoint))
        .route("/admin/checkpoints/current", get(get_checkpoint_manifest))
        .route(
            "/admin/checkpoint-barriers",
            post(create_checkpoint_barrier),
        )
        .route(
            "/admin/checkpoint-barriers/{checkpoint_id}",
            get(get_checkpoint_barrier),
        )
        .route(
            "/admin/checkpoint-barriers/{checkpoint_id}/acks/{node_id}",
            post(acknowledge_checkpoint_barrier),
        )
        .route(
            "/admin/nodes/{node_id}/checkpoint-barriers",
            get(pending_checkpoint_barriers),
        )
        .route("/admin/key-groups", get(list_key_groups))
        .route(
            "/admin/key-groups/{key_group}/assign",
            post(assign_key_group),
        )
        .route("/sinks/{sink}/poll", post(poll_sink))
        .route(
            "/sinks/{sink}/messages/{message_id}/ack",
            post(ack_sink_message),
        )
        .with_state(state);
    let listener = TcpListener::bind(&listen).await?;
    println!("temporal-code Rust core listening on {listen}");
    axum::serve(listener, app).await?;
    Ok(())
}
