export type Workflow = {
  workflow_id: string;
  workflow_type: string;
  status: string;
  retries: number;
  history_events: number;
  duration_seconds: number;
  created_at: number;
  updated_at: number;
  task_queue: string;
  build_id?: string;
  result?: unknown;
  error?: string;
};

export type Stream = {
  name: string;
  partitions: number;
  records: number;
  watermark?: number;
  max_event_time?: number;
  watermark_lag?: number;
  watermark_mode: string;
  finalized: boolean;
  updated_at: number;
};

export type Operator = {
  operator_id: string;
  kind: string;
  status: string;
  input: string[];
  received?: number;
  emitted?: number;
  matched?: number;
  suppressed?: number;
  workflow_type: string;
  join_type?: string;
  probe_watermark?: number;
  version_watermark?: number;
  latest_workflow_id?: string;
};

export type Process = {
  process_id: string;
  workflow_type: string;
  stream: string;
  status: string;
  pending: number;
  running: number;
  completed: number;
  failed: number;
  max_concurrent_keys: number;
  mailbox_capacity: number;
  event_time_gate: string;
};

export type Overview = {
  environment: string;
  generated_at: number;
  counts: {
    workflows: number;
    running_workflows: number;
    failed_workflows: number;
    streams: number;
    processes: number;
    operators: number;
    recovered_workflows: number;
  };
  workflows: Workflow[];
  streams: Stream[];
  operators: Operator[];
  processes: Process[];
  durability: {
    status: string;
    storage_mode: string;
    checkpoint?: {
      checkpoint_id: string;
      sequence: number;
      created_at: number;
      age_seconds: number;
      shards: number;
      state_handles: number;
    };
    partition_owners: {
      partition: number;
      node_id: string;
      epoch: number;
      status: string;
      lease_remaining_seconds: number;
      checkpoint_id?: string;
    }[];
    active_partition_owners: number;
    key_groups: number;
    active_key_groups: number;
    node_id: string;
    region: string;
  };
};

export type HistoryEvent = {
  event_id?: number;
  type: string;
  event_type?: string;
  created_at: number;
  data: Record<string, unknown>;
};

export type ExecutionTrace = {
  source: { stream: string; partition: number; offset: number; event_id?: string; key?: string; event_time: number; ingestion_time: number; late: boolean; too_late: boolean };
  gate: { as_of: number; release_watermark?: number; decision: string };
  operator: { operator_id: string; kind: string; probe_stream: string; version_stream: string; join_type: string; matched: boolean };
  version?: { stream: string; partition: number; offset: number; event_id?: string; event_time: number; value: unknown };
};

export type WorkflowDetail = { workflow: Workflow; history: HistoryEvent[]; trace?: ExecutionTrace };
