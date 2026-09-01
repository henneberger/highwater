use crate::*;
pub async fn run() -> Result<()> {
    run_with_args(env::args().skip(1)).await
}

pub async fn run_with_args(arguments: impl IntoIterator<Item = String>) -> Result<()> {
    let mut listen = "127.0.0.1:7233".to_owned();
    let mut execution_listen = None;
    let mut state_dir = PathBuf::from("highwater-rust-state");
    let mut object_dir = PathBuf::from("highwater-rust-objects");
    let mut journal_uri = None;
    let mut node_id = "local".to_owned();
    let mut endpoint = String::new();
    let mut control_plane = true;
    let mut owned_partitions: Option<HashSet<usize>> = None;
    let mut execution_identity_file = None;
    let mut cluster_token_file = None;
    let mut api_token_file = None;
    let mut key_group_count = 128_u32;
    let mut lease_seconds = 15.0_f64;
    let mut log_shards =
        std::thread::available_parallelism().map_or(4, std::num::NonZeroUsize::get) + 1;
    let mut arguments = arguments.into_iter();
    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "--listen" => listen = arguments.next().context("--listen requires a value")?,
            "--execution-listen" => {
                execution_listen = Some(
                    arguments
                        .next()
                        .context("--execution-listen requires a value")?,
                );
            }
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
            "--journal" => {
                journal_uri = Some(arguments.next().context("--journal requires a value")?);
            }
            "--node-id" => node_id = arguments.next().context("--node-id requires a value")?,
            "--advertise-endpoint" => {
                endpoint = arguments
                    .next()
                    .context("--advertise-endpoint requires a value")?;
            }
            "--data-plane-only" => control_plane = false,
            "--execution-identity-file" => {
                execution_identity_file = Some(PathBuf::from(
                    arguments
                        .next()
                        .context("--execution-identity-file requires a value")?,
                ));
            }
            "--cluster-token-file" => {
                cluster_token_file = Some(PathBuf::from(
                    arguments
                        .next()
                        .context("--cluster-token-file requires a value")?,
                ));
            }
            "--api-token-file" => {
                api_token_file = Some(PathBuf::from(
                    arguments
                        .next()
                        .context("--api-token-file requires a value")?,
                ));
            }
            "--process-partitions" => {
                let value = arguments
                    .next()
                    .context("--process-partitions requires a value")?;
                owned_partitions = Some(
                    value
                        .split(',')
                        .map(|partition| {
                            partition
                                .parse::<usize>()
                                .context("invalid process partition")
                        })
                        .collect::<Result<HashSet<_>>>()?,
                );
            }
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
    if endpoint.is_empty() {
        endpoint = format!("http://{}", execution_listen.as_deref().unwrap_or(&listen));
    }
    reqwest::Url::parse(&endpoint).context("advertise endpoint must be an absolute URL")?;
    if let Some(partitions) = &owned_partitions
        && partitions
            .iter()
            .any(|partition| *partition == 0 || *partition >= log_shards)
    {
        bail!(
            "process partitions must be between 1 and {}",
            log_shards - 1
        );
    }
    let mut partition_senders = vec![None];
    let mut partition_receivers = Vec::new();
    for shard in 1..log_shards {
        if owned_partitions
            .as_ref()
            .is_some_and(|partitions| !partitions.contains(&shard))
        {
            partition_senders.push(None);
            continue;
        }
        let (sender, receiver) = mpsc::channel::<ProcessPartitionCommand>(4_096);
        partition_senders.push(Some(sender));
        partition_receivers.push((shard, receiver));
    }
    let runtime_id = format!("{node_id}:{}", Uuid::new_v4());
    let cluster_token = cluster_token_file
        .map(fs::read_to_string)
        .transpose()?
        .map(|token| token.trim().to_owned());
    if journal_uri.is_some() && cluster_token.as_ref().is_none_or(|token| token.len() < 32) {
        bail!("remote journals require a cluster token of at least 32 bytes");
    }
    let execution_identities = execution_identity_file
        .map(|path| -> Result<Vec<ExecutionIdentity>> {
            let file: ExecutionIdentityFile = serde_json::from_slice(&fs::read(path)?)?;
            let mut tokens = HashSet::new();
            if file.identities.iter().any(|identity| {
                identity.token.len() < 32
                    || identity.task_queue.trim().is_empty()
                    || identity.build_ids.is_empty()
                    || !tokens.insert(identity.token.clone())
            }) {
                bail!(
                    "execution identities require a unique 32-byte token, task queue, and build IDs"
                );
            }
            Ok(file.identities)
        })
        .transpose()?
        .unwrap_or_default();
    if journal_uri.is_some() && execution_listen.is_none() {
        bail!("remote journals require a separate execution listener");
    }
    if journal_uri.is_some() && execution_identities.is_empty() {
        bail!("remote journals require deployment-scoped execution identities");
    }
    let api_token = api_token_file
        .map(fs::read_to_string)
        .transpose()?
        .map(|token| token.trim().to_owned());
    if api_token.as_ref().is_some_and(|token| token.len() < 32) {
        bail!("API tokens must contain at least 32 bytes");
    }
    if api_token.is_some() && execution_listen.is_none() {
        bail!("--api-token-file requires --execution-listen so worker APIs stay private");
    }
    let state = AppState {
        store: Arc::new(DurableStore::open_sharded_with_journal(
            &state_dir,
            &object_dir,
            log_shards,
            journal_uri.as_deref(),
        )?),
        mutation_lock: Arc::new(Mutex::new(())),
        shard_locks: Arc::new((0..log_shards).map(|_| Mutex::new(())).collect()),
        partition_senders: Arc::new(partition_senders),
        node_id,
        runtime_id,
        endpoint,
        control_plane,
        execution_identities: Arc::new(execution_identities),
        cluster_token,
        http_client: HttpClient::new(),
        key_group_count,
        lease_seconds,
        query_queue: Arc::new(Mutex::new(VecDeque::new())),
        query_results: Arc::new(Mutex::new(HashMap::new())),
    };
    if state.control_plane {
        initialize_key_groups(&state)?;
    }
    initialize_process_partitions(&state)?;
    recover_process_tasks(&state, true)?;
    for (shard, receiver) in partition_receivers {
        tokio::spawn(process_partition_loop(state.clone(), shard, receiver));
    }
    tokio::spawn(event_time_maintenance_loop(state.clone()));
    let internal_app = Router::new()
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
        .route(
            "/internal/v1/processes/{process_id}/partitions/{partition}/events",
            post(append_remote_process_records),
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
        .with_state(state.clone());
    let console_app = Router::new()
        .route("/console/overview", get(console_overview))
        .route("/console/workflows/{id}", get(console_workflow))
        .with_state(state.clone())
        .layer(middleware::from_fn_with_state(
            Arc::new(ConsoleCredentials::from_environment()),
            require_console_login,
        ));
    let public_app = Router::new()
        .route("/workflows", post(start_workflow))
        .route("/workflows/{id}", get(get_workflow))
        .route("/workflows/{id}/history", get(history))
        .route("/workflows/{id}/signals/{name}", post(signal))
        .route("/workflows/{id}/updates/{name}", post(update))
        .route("/workflows/{id}/queries/{name}", post(query_workflow))
        .route("/workflows/{id}/cancel", post(cancel))
        .route("/workflows/{id}/terminate", post(terminate))
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
            "/processes/{process_id}/quarantine",
            get(get_process_quarantine),
        )
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
        .route("/admin/process-partitions", get(list_process_partitions))
        .route(
            "/admin/process-partitions/{partition}/transfer",
            post(transfer_process_partition),
        )
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
    let public_app = if let Some(token) = api_token {
        public_app.layer(middleware::from_fn_with_state(
            Arc::new(token),
            require_bearer,
        ))
    } else {
        public_app
    };
    let public_app = Router::new()
        .route("/health", get(health))
        .merge(public_app)
        .merge(console_app)
        .layer(middleware::from_fn(console_cors));
    let app = if let Some(execution_listen) = execution_listen {
        let listener = TcpListener::bind(&execution_listen).await?;
        println!("highwater execution gateway listening on {execution_listen}");
        tokio::spawn(async move {
            if let Err(error) = axum::serve(listener, internal_app).await {
                eprintln!("execution gateway stopped: {error}");
            }
        });
        public_app
    } else {
        public_app.merge(internal_app)
    };
    let listener = TcpListener::bind(&listen).await?;
    println!("highwater Rust core listening on {listen}");
    axum::serve(listener, app).await?;
    Ok(())
}
