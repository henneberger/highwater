use crate::*;
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct WorkflowRecord {
    pub(crate) workflow_id: String,
    pub(crate) workflow_type: String,
    pub(crate) status: String,
    pub(crate) result: Option<Value>,
    pub(crate) error: Option<String>,
    pub(crate) task_queue: String,
    #[serde(default)]
    pub(crate) build_id: Option<String>,
    pub(crate) run_number: u32,
    pub(crate) parent_id: Option<String>,
    pub(crate) parent_command_id: Option<u64>,
    pub(crate) parent_close_policy: Option<String>,
    #[serde(default)]
    pub(crate) execution_deadline: Option<f64>,
    pub(crate) created_at: f64,
    pub(crate) updated_at: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct WorkflowTask {
    pub(crate) workflow_id: String,
    pub(crate) task_queue: String,
    #[serde(default)]
    pub(crate) build_id: Option<String>,
    pub(crate) available_at: f64,
    pub(crate) attempt: u32,
    pub(crate) lease_owner: Option<String>,
    pub(crate) lease_expires: Option<f64>,
    pub(crate) task_token: Option<String>,
    pub(crate) batch_group: Option<String>,
    pub(crate) batch_max_size: u32,
    pub(crate) batch_max_delay: f64,
    pub(crate) enqueued_at: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct WorkflowDeadline {
    pub(crate) workflow_id: String,
    pub(crate) deadline: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ActivityRecord {
    pub(crate) id: u64,
    pub(crate) workflow_id: String,
    pub(crate) command_id: u64,
    pub(crate) name: String,
    pub(crate) args: Vec<Value>,
    pub(crate) task_queue: String,
    pub(crate) attempt: u32,
    pub(crate) max_attempts: u32,
    pub(crate) initial_interval: f64,
    pub(crate) backoff: f64,
    pub(crate) max_interval: f64,
    #[serde(default)]
    pub(crate) schedule_deadline: Option<f64>,
    #[serde(default)]
    pub(crate) start_to_close_timeout: Option<f64>,
    #[serde(default)]
    pub(crate) heartbeat_timeout: Option<f64>,
    pub(crate) available_at: f64,
    pub(crate) lease_owner: Option<String>,
    pub(crate) lease_expires: Option<f64>,
    pub(crate) task_token: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct TimerRecord {
    pub(crate) workflow_id: String,
    pub(crate) command_id: u64,
    pub(crate) fire_at: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct WatermarkTimerRecord {
    pub(crate) stream: String,
    pub(crate) workflow_id: String,
    pub(crate) command_id: u64,
    pub(crate) event_time: f64,
}

#[derive(Debug, Deserialize)]
pub(crate) struct StartRequest {
    pub(crate) workflow_type: String,
    #[serde(default)]
    pub(crate) args: Vec<Value>,
    pub(crate) workflow_id: Option<String>,
    #[serde(default)]
    pub(crate) options: Value,
}

#[derive(Debug, Deserialize)]
pub(crate) struct CreateStreamRequest {
    pub(crate) name: String,
    #[serde(default = "default_stream_partitions")]
    pub(crate) partitions: u32,
    #[serde(default = "default_watermark_mode")]
    pub(crate) watermark_mode: WatermarkMode,
    #[serde(default = "default_max_out_of_orderness")]
    pub(crate) max_out_of_orderness: f64,
    #[serde(default = "default_idle_timeout")]
    pub(crate) idle_timeout: Option<f64>,
    #[serde(default)]
    pub(crate) allowed_lateness: f64,
    #[serde(default)]
    pub(crate) alignment_max_drift: Option<f64>,
    #[serde(default = "default_late_policy")]
    pub(crate) late_policy: LatePolicy,
}

pub(crate) fn default_stream_partitions() -> u32 {
    1
}

pub(crate) fn default_watermark_mode() -> WatermarkMode {
    WatermarkMode::Bounded
}

pub(crate) fn default_max_out_of_orderness() -> f64 {
    5.0
}

pub(crate) fn default_idle_timeout() -> Option<f64> {
    Some(60.0)
}

pub(crate) fn default_late_policy() -> LatePolicy {
    LatePolicy::SideOutput
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct AppendStreamRecordRequest {
    #[serde(default)]
    pub(crate) partition: u32,
    pub(crate) event_time: f64,
    #[serde(default)]
    pub(crate) key: Option<String>,
    pub(crate) value: Value,
    #[serde(default = "default_change_kind")]
    pub(crate) kind: ChangeKind,
    #[serde(default)]
    pub(crate) event_id: Option<String>,
    #[serde(default)]
    pub(crate) source_id: Option<String>,
    #[serde(default)]
    pub(crate) source_partition: Option<u32>,
    #[serde(default)]
    pub(crate) source_offset: Option<u64>,
    #[serde(default)]
    pub(crate) source_epoch: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct AppendStreamRecordsRequest {
    pub(crate) records: Vec<AppendStreamRecordRequest>,
}

pub(crate) fn default_change_kind() -> ChangeKind {
    ChangeKind::Upsert
}

#[derive(Debug, Deserialize)]
pub(crate) struct AdvanceWatermarkRequest {
    pub(crate) event_time: f64,
}

#[derive(Debug, Deserialize)]
pub(crate) struct CreateWindowScheduleRequest {
    pub(crate) schedule_id: String,
    pub(crate) stream: String,
    pub(crate) workflow_type: String,
    pub(crate) window_size: f64,
    #[serde(default)]
    pub(crate) slide: Option<f64>,
    pub(crate) start_at: f64,
    #[serde(default = "default_task_queue")]
    pub(crate) task_queue: String,
    #[serde(default)]
    pub(crate) emit_empty_windows: bool,
    #[serde(default = "default_window_aggregation")]
    pub(crate) aggregation: WindowAggregation,
    #[serde(default)]
    pub(crate) value_field: Option<String>,
}

pub(crate) fn default_window_aggregation() -> WindowAggregation {
    WindowAggregation::Count
}

#[derive(Debug, Deserialize)]
pub(crate) struct CreateTemporalJoinRequest {
    pub(crate) join_id: String,
    pub(crate) probe_stream: String,
    pub(crate) version_stream: String,
    pub(crate) workflow_type: String,
    #[serde(default = "default_task_queue")]
    pub(crate) task_queue: String,
    #[serde(default = "default_temporal_join_type")]
    pub(crate) join_type: TemporalJoinType,
}

#[derive(Debug, Deserialize)]
pub(crate) struct CreateIntervalJoinRequest {
    pub(crate) join_id: String,
    pub(crate) left_stream: String,
    pub(crate) right_stream: String,
    pub(crate) workflow_type: String,
    pub(crate) lower_bound: f64,
    pub(crate) upper_bound: f64,
    #[serde(default = "default_task_queue")]
    pub(crate) task_queue: String,
    #[serde(default = "default_interval_join_type")]
    pub(crate) join_type: IntervalJoinType,
}

#[derive(Debug, Deserialize)]
pub(crate) struct CreateDeduplicateRequest {
    pub(crate) operator_id: String,
    pub(crate) stream: String,
    pub(crate) workflow_type: String,
    #[serde(default = "default_task_queue")]
    pub(crate) task_queue: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct CreateStreamFilterRequest {
    pub(crate) operator_id: String,
    pub(crate) stream: String,
    pub(crate) workflow_type: String,
    pub(crate) field: String,
    pub(crate) comparison: Comparison,
    pub(crate) operand: Value,
    #[serde(default = "default_task_queue")]
    pub(crate) task_queue: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum EventTimeGate {
    Immediate,
    Complete,
}

pub(crate) fn default_process_gate() -> EventTimeGate {
    EventTimeGate::Immediate
}

pub(crate) fn default_process_concurrency() -> u32 {
    64
}

pub(crate) fn default_process_capacity() -> u64 {
    10_000
}

#[derive(Debug, Deserialize)]
pub(crate) struct CreateProcessRequest {
    pub(crate) process_id: String,
    pub(crate) stream: String,
    pub(crate) workflow_type: String,
    #[serde(default)]
    pub(crate) key_field: Option<String>,
    #[serde(default)]
    pub(crate) event_time_field: Option<String>,
    pub(crate) state_version: u32,
    pub(crate) build_id: String,
    #[serde(default)]
    pub(crate) migrations_from: Vec<u32>,
    #[serde(default = "default_task_queue")]
    pub(crate) task_queue: String,
    #[serde(default = "default_process_gate")]
    pub(crate) event_time_gate: EventTimeGate,
    #[serde(default = "default_process_concurrency")]
    pub(crate) max_concurrent_keys: u32,
    #[serde(default = "default_process_capacity")]
    pub(crate) mailbox_capacity: u64,
    #[serde(default = "default_process_batch_size")]
    pub(crate) batch_max_size: u32,
    #[serde(default = "default_process_batch_delay")]
    pub(crate) batch_max_delay: f64,
}

pub(crate) fn default_process_batch_size() -> u32 {
    64
}

pub(crate) fn default_process_batch_delay() -> f64 {
    0.005
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct KeyGroupLease {
    pub(crate) key_group: u32,
    pub(crate) owner: String,
    pub(crate) epoch: u64,
    pub(crate) lease_expires: f64,
}

#[derive(Debug, Deserialize)]
pub(crate) struct AssignKeyGroupRequest {
    pub(crate) owner: String,
    pub(crate) expected_epoch: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ClusterConfig {
    pub(crate) key_group_count: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct SourceCursor {
    pub(crate) stream: String,
    pub(crate) source_id: String,
    pub(crate) partition: u32,
    pub(crate) next_offset: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct SourceLease {
    pub(crate) stream: String,
    pub(crate) partition: u32,
    pub(crate) source_id: String,
    pub(crate) epoch: u64,
    pub(crate) lease_expires: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct StreamBatchCommit {
    pub(crate) batch_id: String,
    pub(crate) stream: String,
    pub(crate) first_sequence: u64,
    pub(crate) last_sequence: u64,
    pub(crate) records: u64,
    pub(crate) committed_at: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct OutboxMessage {
    pub(crate) sink: String,
    pub(crate) message_id: String,
    pub(crate) workflow_id: String,
    pub(crate) payload: Value,
    pub(crate) created_at: f64,
    pub(crate) lease_owner: Option<String>,
    pub(crate) lease_expires: Option<f64>,
    pub(crate) delivery_attempt: u32,
    pub(crate) acked_at: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct WindowValueCount {
    pub(crate) value: f64,
    pub(crate) count: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct DifferentialChange {
    pub(crate) operator_id: String,
    pub(crate) sequence: u64,
    pub(crate) key: Option<String>,
    pub(crate) event_time: f64,
    pub(crate) kind: ChangeKind,
    pub(crate) diff: i64,
    pub(crate) row: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct DurableProcess {
    pub(crate) process_id: String,
    pub(crate) stream: String,
    pub(crate) workflow_type: String,
    #[serde(default)]
    pub(crate) key_field: Option<String>,
    #[serde(default)]
    pub(crate) event_time_field: Option<String>,
    pub(crate) state_version: u32,
    pub(crate) active_build_id: String,
    pub(crate) task_queue: String,
    pub(crate) event_time_gate: EventTimeGate,
    pub(crate) max_concurrent_keys: u32,
    pub(crate) mailbox_capacity: u64,
    pub(crate) batch_max_size: u32,
    pub(crate) batch_max_delay: f64,
    pub(crate) status: String,
    pub(crate) created_at: f64,
    pub(crate) pending: u64,
    pub(crate) running: u64,
    pub(crate) completed: u64,
    pub(crate) failed: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ProcessMailboxItem {
    pub(crate) process_id: String,
    pub(crate) sequence: u64,
    pub(crate) key: String,
    pub(crate) event_time: f64,
    pub(crate) record: StreamRecord,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ShardedProcessMailboxItem {
    pub(crate) process_id: String,
    pub(crate) sequence: u64,
    pub(crate) key: String,
    pub(crate) event_time: f64,
    pub(crate) record: StreamRecord,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ProcessExecution {
    pub(crate) process_id: String,
    pub(crate) key: String,
    pub(crate) event_time: f64,
    pub(crate) record: StreamRecord,
    pub(crate) prior_state: Option<ProcessStateRecord>,
    pub(crate) state_version: u32,
    pub(crate) build_id: String,
    pub(crate) workflow_id: String,
    #[serde(default)]
    pub(crate) shard: u32,
    #[serde(default)]
    pub(crate) available_at: f64,
    #[serde(default)]
    pub(crate) enqueued_at: f64,
    #[serde(default)]
    pub(crate) attempt: u32,
    #[serde(default)]
    pub(crate) lease_owner: Option<String>,
    #[serde(default)]
    pub(crate) lease_expires: Option<f64>,
    #[serde(default)]
    pub(crate) task_token: Option<String>,
    #[serde(default)]
    pub(crate) last_failure: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ShardedProcessExecution {
    pub(crate) process_id: String,
    pub(crate) sequence: u64,
    pub(crate) key: String,
    pub(crate) event_time: f64,
    pub(crate) record: StreamRecord,
    pub(crate) prior_state: Option<ProcessStateRecord>,
    pub(crate) state_version: u32,
    pub(crate) build_id: String,
    pub(crate) shard: u32,
    pub(crate) available_at: f64,
    pub(crate) enqueued_at: f64,
    pub(crate) attempt: u32,
    pub(crate) lease_owner: Option<String>,
    pub(crate) lease_expires: Option<f64>,
    pub(crate) task_token: Option<String>,
    pub(crate) last_failure: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ProcessReadyExecution {
    pub(crate) execution_key: String,
    pub(crate) execution: ShardedProcessExecution,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ProcessBatchLease {
    pub(crate) process_id: String,
    pub(crate) shard: u32,
    #[serde(default)]
    pub(crate) owner_epoch: u64,
    #[serde(default)]
    pub(crate) activation_sequence: u64,
    pub(crate) worker_id: String,
    pub(crate) lease_expires: f64,
    pub(crate) executions: Vec<ProcessReadyExecution>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct ProcessShardState {
    pub(crate) next_sequence: u64,
    pub(crate) next_output_sequence: u64,
    pub(crate) pending: u64,
    pub(crate) running: u64,
    pub(crate) completed: u64,
    pub(crate) failed: u64,
    #[serde(default)]
    pub(crate) active_keys: HashSet<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ProcessPartitionOwner {
    pub(crate) partition_id: u32,
    pub(crate) owner: String,
    pub(crate) epoch: u64,
    pub(crate) lease_expires: f64,
    #[serde(default)]
    pub(crate) next_activation_sequence: u64,
}

pub(crate) struct ProcessIngressRequest {
    pub(crate) process_id: String,
    pub(crate) records: Vec<(usize, AppendStreamRecordRequest)>,
    pub(crate) detailed: bool,
    pub(crate) response: oneshot::Sender<std::result::Result<ProcessIngressResult, String>>,
}

pub(crate) enum ProcessPartitionCommand {
    Ingress(ProcessIngressRequest),
    Poll {
        request: PollRequest,
        response: oneshot::Sender<std::result::Result<Option<ProcessActivationBatch>, String>>,
    },
    Complete {
        completion: ProcessCompletionBatch,
        response: oneshot::Sender<std::result::Result<(), String>>,
    },
    Renew {
        renewal: ProcessLeaseRenewal,
        response: oneshot::Sender<std::result::Result<f64, String>>,
    },
}

#[derive(Default)]
pub(crate) struct ProcessIngressResult {
    pub(crate) responses: Vec<(usize, Value)>,
    pub(crate) accepted: usize,
    pub(crate) duplicates: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct ProcessStateRecord {
    pub(crate) version: u32,
    pub(crate) build_id: String,
    pub(crate) input_sequence: u64,
    pub(crate) event_time: f64,
    pub(crate) value: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct OperatorEdge {
    pub(crate) operator_id: String,
    pub(crate) output_stream: String,
    pub(crate) status: String,
    pub(crate) created_at: f64,
    pub(crate) changes_forwarded: u64,
}

#[derive(Debug, Deserialize)]
pub(crate) struct CreateOperatorEdgeRequest {
    pub(crate) operator_id: String,
    pub(crate) output_stream: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct CheckpointAck {
    pub(crate) node_id: String,
    pub(crate) state_handle: String,
    pub(crate) key_group_epochs: BTreeMap<u32, u64>,
    pub(crate) acked_at: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct CheckpointBarrier {
    pub(crate) checkpoint_id: String,
    pub(crate) sequence: u64,
    pub(crate) status: String,
    pub(crate) expected_nodes: Vec<String>,
    pub(crate) expected_key_group_epochs: BTreeMap<String, BTreeMap<u32, u64>>,
    pub(crate) acknowledgements: BTreeMap<String, CheckpointAck>,
    pub(crate) manifest: CheckpointManifest,
    pub(crate) created_at: f64,
}

#[derive(Debug, Deserialize)]
pub(crate) struct AcknowledgeCheckpointRequest {
    pub(crate) state_handle: String,
    pub(crate) key_group_epochs: BTreeMap<u32, u64>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct PollSinkRequest {
    pub(crate) consumer_id: String,
    #[serde(default = "default_lease_seconds")]
    pub(crate) lease_seconds: f64,
}

#[derive(Debug, Deserialize)]
pub(crate) struct AckSinkRequest {
    pub(crate) consumer_id: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ClaimSourceRequest {
    #[serde(default = "default_lease_seconds")]
    pub(crate) lease_seconds: f64,
}

pub(crate) fn default_lease_seconds() -> f64 {
    30.0
}

pub(crate) fn default_interval_join_type() -> IntervalJoinType {
    IntervalJoinType::Inner
}

pub(crate) fn default_temporal_join_type() -> TemporalJoinType {
    TemporalJoinType::Inner
}

pub(crate) fn default_task_queue() -> String {
    "default".to_owned()
}

pub(crate) fn now() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before epoch")
        .as_secs_f64()
}
