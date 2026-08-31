mod error;
mod keyspace;
mod maintenance;
mod model;
mod operators;
mod process;
mod runtime;
mod state;
mod storage;
mod stream_api;
mod stream_engine;
mod streaming;
mod workflow;

use anyhow::{Context, Result, anyhow, bail};
use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use rocksdb::{DB, IteratorMode, Options, WriteBatch, WriteOptions, checkpoint::Checkpoint};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    env,
    fs::{self, File},
    io::Write,
    path::{Path as FsPath, PathBuf},
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};
use temporal_code_protocol::{
    ActivityCompletion, ActivityTask, Command, Event, PROTOCOL_VERSION, PollRequest,
    ProcessActivationBatch, ProcessCompletionBatch, ProcessLeaseRenewal, QueryCompletion,
    QueryTask, WorkflowActivation, WorkflowCompletion,
};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

use streaming::{
    ChangeKind, Comparison, Deduplicate, DeduplicateOutput, IntervalJoin, IntervalJoinOutput,
    IntervalJoinType, LatePolicy, PartitionState, StreamConfig, StreamFilter, StreamFilterOutput,
    StreamRecord, StreamState, TemporalJoin, TemporalJoinOutput, TemporalJoinType, WatermarkMode,
    WindowAccumulator, WindowAggregation, WindowSchedule, completeness_frontier, interval_contains,
    latest_version_as_of, temporal_join_frontier,
};

use error::*;
use keyspace::*;
use maintenance::*;
use model::*;
use operators::*;
use process::*;
use state::*;
use storage::*;
use stream_api::*;
use stream_engine::*;
use workflow::*;

pub use runtime::run;
