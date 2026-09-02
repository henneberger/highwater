import sys

if sys.version_info < (3, 11):
    raise RuntimeError("Highwater requires Python 3.11 or newer")

from .annotations import workflow
from .activity_context import current_activity, heartbeat
from .errors import (
    ActivityError,
    ChildWorkflowError,
    NonDeterminismError,
    NonRetryableError,
    QueryNotFound,
    StreamBackpressure,
    HighwaterError,
    UpdateNotFound,
    WorkflowCancelled,
    WorkflowFailed,
)
from .model import (
    ActivityOptions,
    ChangeKind,
    Comparison,
    DeduplicateOutput,
    ChildWorkflowOptions,
    Event,
    EventTimeGate,
    IntervalJoinOutput,
    IntervalJoinType,
    LatePolicy,
    ProcessOptions,
    RetryPolicy,
    StreamInfo,
    StreamOptions,
    StreamRecord,
    TemporalJoinOutput,
    TemporalJoinType,
    WindowAggregation,
    WatermarkMode,
    WorkflowOptions,
    WorkflowStatus,
)
from .client import Client, ProcessHandle, RemoteClient, RemoteWorkflowHandle, StreamWriter, WorkflowHandle
from .streaming import ProcessContext, RecordValue, Transition, Versioned, streaming
from .autoscaling import AutoscalingPolicy, ScalingDecision, WorkloadSample, assign_partitions, recommend_replicas
from .replay import ReplayComparison, ReplayDifference, compare_process_builds
from .registry import Registry
from .runtime import continue_as_new, execute_activity, execute_child_workflow, get_version, info, now, sleep, wait_condition, wait_for_watermark

__all__ = [
    "ActivityError", "ActivityOptions", "AutoscalingPolicy", "ChangeKind", "ChildWorkflowError", "ChildWorkflowOptions", "Client", "Comparison", "DeduplicateOutput", "Event", "EventTimeGate", "IntervalJoinOutput", "IntervalJoinType", "NonDeterminismError", "NonRetryableError", "ProcessContext", "ProcessHandle", "ProcessOptions", "QueryNotFound", "RecordValue", "Registry", "ReplayComparison", "ReplayDifference", "ScalingDecision", "Transition", "Versioned", "WorkloadSample",
    "LatePolicy", "RemoteClient", "RemoteWorkflowHandle", "RetryPolicy", "StreamBackpressure", "StreamInfo", "StreamOptions", "StreamRecord", "StreamWriter", "HighwaterError", "TemporalJoinOutput", "TemporalJoinType", "UpdateNotFound", "WatermarkMode", "WindowAggregation",
    "WorkflowCancelled", "WorkflowFailed", "WorkflowHandle", "WorkflowOptions", "WorkflowStatus", "execute_activity",
    "assign_partitions", "compare_process_builds", "continue_as_new", "current_activity", "execute_child_workflow", "get_version", "heartbeat", "info", "now", "recommend_replicas", "sleep", "streaming", "wait_condition", "wait_for_watermark", "workflow",
]
