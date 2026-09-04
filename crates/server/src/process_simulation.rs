use crate::*;
use highwater_protocol::ProcessBatchResult;

const EVENTS: usize = 12;
const KEYS: usize = 3;

struct Fixture {
    root: PathBuf,
    app: Option<AppState>,
    previous_time: Option<f64>,
    generation: usize,
}

impl Fixture {
    fn new() -> Result<Self> {
        let previous_time = TEST_TIME.with(|clock| clock.replace(Some(1000.0)));
        let mut fixture = Self {
            root: std::env::temp_dir()
                .join(format!("highwater-process-simulation-{}", Uuid::new_v4())),
            app: None,
            previous_time,
            generation: 0,
        };
        fixture.reopen()?;
        let process: DurableProcess = serde_json::from_value(json!({
            "process_id": "simulation", "stream": "unused", "workflow_type": "Counter",
            "state_version": 1, "active_build_id": "v1", "task_queue": "default",
            "event_time_gate": "immediate", "max_concurrent_keys": KEYS,
            "mailbox_capacity": 100, "retry_concurrency": KEYS, "max_attempts": 3,
            "direct_ingress": true, "batch_max_size": KEYS, "batch_max_delay": 0.0,
            "status": "ACTIVE", "created_at": now(), "pending": 0, "running": 0,
            "completed": 0, "failed": 0
        }))?;
        fixture
            .app()
            .commit(|transaction| transaction.put(process_key("simulation"), &process))?;
        Ok(fixture)
    }

    fn app(&self) -> &AppState {
        self.app.as_ref().unwrap()
    }

    fn advance(&self) {
        TEST_TIME.with(|clock| clock.set(Some(clock.get().unwrap() + 2.0)));
    }

    fn reopen(&mut self) -> Result<()> {
        self.app.take();
        self.generation += 1;
        let (sender, _receiver) = mpsc::channel(1);
        let app = AppState {
            store: Arc::new(DurableStore::open_sharded_with_journal(
                &self.root.join("state"),
                &self.root.join("objects"),
                2,
                None,
            )?),
            mutation_lock: Arc::new(Mutex::new(())),
            shard_locks: Arc::new(vec![Mutex::new(()), Mutex::new(())]),
            partition_senders: Arc::new(vec![None, Some(sender)]),
            node_id: "simulation".to_owned(),
            runtime_id: format!("simulation:{}", self.generation),
            endpoint: "http://simulation".to_owned(),
            control_plane: true,
            execution_identities: Arc::new(Vec::new()),
            cluster_token: None,
            http_client: HttpClient::new(),
            key_group_count: 1,
            lease_seconds: 10000.0,
            query_queue: Arc::new(Mutex::new(VecDeque::new())),
            query_results: Arc::new(Mutex::new(HashMap::new())),
        };
        initialize_process_partitions(&app)?;
        self.app = Some(app);
        Ok(())
    }

    fn admit(&self) -> Result<()> {
        let records = (0..EVENTS)
            .map(|event| {
                Ok((
                    event,
                    serde_json::from_value::<AppendStreamRecordRequest>(json!({
                        "key": format!("key-{}", event % KEYS), "event_id": event.to_string(),
                        "event_time": event as f64, "value": {"amount": 1}
                    }))?,
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        commit_process_ingress_batch(self.app(), 1, &[("simulation".to_owned(), records, true)])?;
        Ok(())
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.app.take();
        TEST_TIME.with(|clock| clock.set(self.previous_time));
        let _ = fs::remove_dir_all(&self.root);
    }
}

#[derive(Default)]
struct Reference {
    attempts: [u32; EVENTS],
    // 0 = pending, 1 = committed, 2 = failed.
    outcomes: [u8; EVENTS],
    totals: [u64; KEYS],
}

impl Reference {
    fn check(&self, fixture: &Fixture) -> Result<()> {
        let store = &fixture.app().store;
        let outcomes = store.scan::<ProcessExecutionOutcome>("process-outcome/")?;
        anyhow::ensure!(
            outcomes.len() == EVENTS,
            "expected one durable outcome per admitted event, found {}",
            outcomes.len()
        );
        let mut output_ids = HashSet::new();
        for (_, outcome) in outcomes {
            let event: usize = outcome.event_id.parse()?;
            let status = ["PENDING", "COMMITTED", "FAILED"][self.outcomes[event] as usize];
            anyhow::ensure!(
                outcome.status == status && outcome.attempts == self.attempts[event],
                "event {event}: expected {status}/{} attempts, got {}/{}",
                self.attempts[event],
                outcome.status,
                outcome.attempts
            );
            anyhow::ensure!(
                outcome.output_message_ids.len() == usize::from(self.outcomes[event] == 1),
                "event {event}: output and outcome disagree"
            );
            output_ids.extend(outcome.output_message_ids);
        }
        for key in 0..KEYS {
            let state = store.get::<ProcessStateRecord>(&process_state_key(
                "simulation",
                &format!("key-{key}"),
            ))?;
            let total = state.map_or(0, |state| state.value["total"].as_u64().unwrap());
            anyhow::ensure!(
                total == self.totals[key],
                "key {key}: expected total {}, got {total}",
                self.totals[key]
            );
        }
        let pending = store.scan::<PendingProcessOutput>(pending_process_output_prefix())?;
        let actual_ids: HashSet<_> = pending
            .iter()
            .map(|(_, pending)| pending.message.message_id.clone())
            .collect();
        anyhow::ensure!(
            actual_ids == output_ids,
            "pending effects differ from committed outcomes"
        );
        let state = store
            .get::<ProcessShardState>(&process_shard_state_key("simulation", 1))?
            .unwrap_or_default();
        anyhow::ensure!(
            state.completed == self.outcomes.iter().filter(|&&status| status == 1).count() as u64,
            "completion count differs from reference"
        );
        anyhow::ensure!(
            state.failed == self.outcomes.iter().filter(|&&status| status == 2).count() as u64,
            "failure count differs from reference"
        );
        Ok(())
    }
}

#[test]
fn seeded_process_histories_preserve_outcomes_and_make_progress() -> Result<()> {
    let seeds = std::env::var("HIGHWATER_PROCESS_SEED")
        .ok()
        .map(|seed| seed.parse::<u64>().map(|seed| vec![seed]))
        .transpose()?
        .map(Ok)
        .unwrap_or_else(|| {
            let count = std::env::var("HIGHWATER_PROCESS_SEED_COUNT")
                .ok()
                .map(|count| count.parse::<u64>())
                .transpose()?
                .unwrap_or(32);
            anyhow::ensure!(count > 0, "process seed count must be positive");
            Ok::<_, anyhow::Error>((0..count).collect())
        })?;
    for seed in seeds {
        let mut trace = Vec::new();
        simulate(seed, &mut trace).with_context(|| {
            format!(
                "process seed={seed}; replay with HIGHWATER_PROCESS_SEED={seed}\n{}",
                trace.join("\n")
            )
        })?;
    }
    Ok(())
}

fn simulate(seed: u64, trace: &mut Vec<String>) -> Result<()> {
    let mut fixture = Fixture::new()?;
    let mut reference = Reference::default();
    fixture.admit()?;
    fixture.admit()?; // Lost admission response followed by producer retry.
    reference.check(&fixture)?;
    let mut random = seed;
    for step in 0..64 {
        fixture.advance();
        let request: PollRequest = serde_json::from_value(json!({
            "protocol_version": PROTOCOL_VERSION, "worker_id": "worker",
            "build_ids": ["v1"], "lease_seconds": 1.0
        }))?;
        let Some(batch) = poll_process_partition(fixture.app(), 1, request)? else {
            reference.check(&fixture)?;
            if reference.outcomes.iter().all(|&status| status != 0) {
                break;
            }
            continue;
        };
        let lease = fixture
            .app()
            .store
            .get::<ProcessBatchLease>(&process_batch_lease_key(&batch.lease_token))?
            .unwrap();
        let mut events = Vec::new();
        let mut items = Vec::new();
        for leased in &lease.executions {
            let execution = &leased.execution;
            let event: usize = execution.record.event_id.as_ref().unwrap().parse()?;
            let key = event % KEYS;
            anyhow::ensure!(
                reference.outcomes[event] == 0,
                "terminal event {event} was invoked again"
            );
            anyhow::ensure!(
                (key..event)
                    .step_by(KEYS)
                    .all(|prior| reference.outcomes[prior] != 0),
                "event {event} overtook an earlier event for its key"
            );
            let prior = execution
                .prior_state
                .as_ref()
                .map_or(0, |state| state.value["total"].as_u64().unwrap());
            anyhow::ensure!(
                prior == reference.totals[key],
                "event {event} received stale state"
            );
            events.push(event);
            items.push(ProcessBatchResult {
                result: Some(json!({
                    "__highwater_transition__": true, "state": {"total": prior + 1},
                    "emit": {"event": event}
                })),
                failure: None,
            });
        }
        let mut completion = ProcessCompletionBatch {
            protocol_version: PROTOCOL_VERSION,
            lease_token: batch.lease_token,
            partition_id: 1,
            owner_epoch: batch.owner_epoch,
            activation_sequence: batch.activation_sequence,
            items,
        };
        random = random
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        // Stop injecting failures after a finite prefix, then require bounded drain.
        let action = if step < 24 { (random >> 32) % 6 } else { 0 };
        let first_event = events[0];
        trace.push(format!("step={step} action={action} events={events:?}"));
        if action == 4 {
            // Reject the last transition after earlier items were staged: the
            // entire batch must roll back, leaving its original lease usable.
            let mut malformed = completion.clone();
            malformed.items.last_mut().unwrap().result = Some(json!({"invalid": true}));
            anyhow::ensure!(
                complete_process_partition(fixture.app(), 1, malformed).is_err(),
                "invalid batch accepted"
            );
            reference.check(&fixture)?;
        }
        match action {
            5 => {
                completion.items[0].failure = Some("isolated poison event".to_owned());
                completion.items[0].result = None;
                complete_process_partition(fixture.app(), 1, completion.clone())?;
            }
            1 => {
                for item in &mut completion.items {
                    item.failure = Some("injected handler failure".to_owned());
                    item.result = None;
                }
                complete_process_partition(fixture.app(), 1, completion.clone())?;
            }
            2 | 3 => {
                if action == 2 {
                    fixture.advance();
                } else {
                    fixture.reopen()?;
                }
                recover_process_tasks(fixture.app(), true)?;
                anyhow::ensure!(
                    complete_process_partition(fixture.app(), 1, completion.clone()).is_err(),
                    "abandoned worker committed after recovery"
                );
            }
            _ => {
                complete_process_partition(fixture.app(), 1, completion.clone())?;
                anyhow::ensure!(
                    complete_process_partition(fixture.app(), 1, completion.clone()).is_err(),
                    "duplicate completion accepted"
                );
            }
        }
        for event in events {
            if (1..=3).contains(&action) || action == 5 && event == first_event {
                reference.attempts[event] += 1;
                if reference.attempts[event] == 3 {
                    reference.outcomes[event] = 2;
                }
            } else {
                reference.outcomes[event] = 1;
                reference.totals[event % KEYS] += 1;
            }
        }
        reference.check(&fixture)?;
        fixture.admit()?;
        reference.check(&fixture)?;
        promote_process_outputs(fixture.app(), None)?;
        promote_process_outputs(fixture.app(), None)?;
    }
    anyhow::ensure!(
        reference.outcomes.iter().all(|&status| status != 0),
        "work failed to reach terminal outcomes after failures stopped"
    );
    fixture.reopen()?;
    reference.check(&fixture)?;
    let state = fixture
        .app()
        .store
        .get::<ProcessShardState>(&process_shard_state_key("simulation", 1))?
        .unwrap();
    anyhow::ensure!(
        state.pending + state.running + state.retry_pending + state.retry_running == 0
            && state.active_keys.is_empty(),
        "terminal workload leaked permits or active keys"
    );
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(check_output_delivery(&mut fixture, &reference))?;
    Ok(())
}

async fn poll_output(fixture: &Fixture) -> Result<Option<OutboxMessage>> {
    let response = poll_sink(
        State(fixture.app().clone()),
        Path("process:simulation".to_owned()),
        Json(PollSinkRequest {
            consumer_id: "sink".to_owned(),
            lease_seconds: 1.0,
        }),
    )
    .await
    .map_err(|error| error.0)?
    .into_response();
    if response.status() == StatusCode::NO_CONTENT {
        return Ok(None);
    }
    anyhow::ensure!(response.status() == StatusCode::OK, "sink polling failed");
    let body = axum::body::to_bytes(response.into_body(), 1024 * 1024).await?;
    Ok(Some(serde_json::from_slice(&body)?))
}

async fn acknowledge(fixture: &Fixture, message: &OutboxMessage, consumer: &str) -> Result<()> {
    ack_sink_message(
        State(fixture.app().clone()),
        Path((message.sink.clone(), message.message_id.clone())),
        Json(AckSinkRequest {
            consumer_id: consumer.to_owned(),
        }),
    )
    .await
    .map_err(|error| error.0)?;
    Ok(())
}

async fn check_output_delivery(fixture: &mut Fixture, reference: &Reference) -> Result<()> {
    let expected: HashSet<_> = reference
        .outcomes
        .iter()
        .enumerate()
        .filter(|(_, status)| **status == 1)
        .map(|(event, _)| event)
        .collect();
    let mut delivered = HashSet::new();
    while let Some(message) = poll_output(fixture).await? {
        anyhow::ensure!(
            acknowledge(fixture, &message, "wrong-consumer")
                .await
                .is_err(),
            "another consumer acknowledged the output"
        );
        // A consumer crashes before acknowledgement. Restart and expire its lease;
        // redelivery must retain the stable identity and exact payload.
        fixture.reopen()?;
        fixture.advance();
        let redelivered = poll_output(fixture)
            .await?
            .ok_or_else(|| anyhow!("unacknowledged output lost on restart"))?;
        anyhow::ensure!(
            message.message_id == redelivered.message_id && message.payload == redelivered.payload,
            "redelivered output changed identity or payload"
        );
        let event = message.payload["value"]["event"]
            .as_u64()
            .ok_or_else(|| anyhow!("output has no event"))? as usize;
        anyhow::ensure!(
            expected.contains(&event) && delivered.insert(event),
            "unexpected or already acknowledged output {event}"
        );
        acknowledge(fixture, &redelivered, "sink").await?;
        acknowledge(fixture, &redelivered, "sink").await?; // Lost acknowledgement response.
        promote_process_outputs(fixture.app(), None)?;
    }
    anyhow::ensure!(
        delivered == expected,
        "delivered events differ from committed events"
    );
    fixture.reopen()?;
    fixture.advance();
    promote_process_outputs(fixture.app(), None)?;
    anyhow::ensure!(
        poll_output(fixture).await?.is_none(),
        "acknowledged output resurrected after restart"
    );
    reference.check(fixture)
}
