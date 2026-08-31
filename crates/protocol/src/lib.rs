use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

pub const PROTOCOL_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PayloadEncoding {
    Json,
    Protobuf,
    ArrowIpcStream,
    Raw,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Payload {
    pub encoding: PayloadEncoding,
    #[serde(default)]
    pub metadata: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data_base64: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sha256: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    pub id: u64,
    pub workflow_id: String,
    #[serde(rename = "type")]
    pub event_type: String,
    pub data: Value,
    pub created_at: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Command {
    #[serde(rename = "type")]
    pub command_type: String,
    pub attributes: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowActivation {
    pub protocol_version: u32,
    pub task_token: String,
    pub workflow_id: String,
    pub workflow_type: String,
    pub attempt: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub build_id: Option<String>,
    pub history: Vec<Event>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowCompletion {
    pub protocol_version: u32,
    pub task_token: String,
    pub history_event_id: u64,
    pub commands: Vec<Command>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessActivation {
    pub protocol_version: u32,
    pub task_token: String,
    pub process_id: String,
    pub workflow_type: String,
    pub build_id: String,
    pub attempt: u32,
    pub shard: u32,
    pub envelope: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessCompletion {
    pub protocol_version: u32,
    pub task_token: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessActivationBatch {
    pub protocol_version: u32,
    pub lease_token: String,
    pub partition_id: u32,
    pub owner_epoch: u64,
    pub activation_sequence: u64,
    pub lease_expires: f64,
    pub process_id: String,
    pub workflow_type: String,
    pub build_id: String,
    pub shard: u32,
    pub envelopes: Vec<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessBatchResult {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessCompletionBatch {
    pub protocol_version: u32,
    pub lease_token: String,
    pub partition_id: u32,
    pub owner_epoch: u64,
    pub activation_sequence: u64,
    pub items: Vec<ProcessBatchResult>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessLeaseRenewal {
    pub protocol_version: u32,
    pub lease_token: String,
    pub partition_id: u32,
    pub owner_epoch: u64,
    pub activation_sequence: u64,
    pub extend_seconds: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActivityTask {
    pub protocol_version: u32,
    pub task_token: String,
    pub id: u64,
    pub workflow_id: String,
    pub name: String,
    pub args: Vec<Value>,
    pub attempt: u32,
    pub lease_seconds: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_to_close_timeout: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActivityCompletion {
    pub protocol_version: u32,
    pub task_token: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(default)]
    pub non_retryable: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryTask {
    pub protocol_version: u32,
    pub task_token: String,
    pub workflow_id: String,
    pub workflow_type: String,
    pub name: String,
    pub args: Vec<Value>,
    pub history: Vec<Event>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryCompletion {
    pub protocol_version: u32,
    pub task_token: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PollRequest {
    pub protocol_version: u32,
    pub worker_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_token: Option<String>,
    #[serde(default)]
    pub task_queue: Option<String>,
    #[serde(default)]
    pub build_ids: Vec<String>,
    #[serde(default = "default_lease_seconds")]
    pub lease_seconds: f64,
    #[serde(default)]
    pub shard_cursor: u64,
    #[serde(default)]
    pub partition_id: Option<u32>,
}

fn default_lease_seconds() -> f64 {
    30.0
}
