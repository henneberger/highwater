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
  };
  workflows: Workflow[];
  streams: Stream[];
  operators: Operator[];
  processes: Process[];
};

export type HistoryEvent = {
  event_id?: number;
  type: string;
  event_type?: string;
  created_at: number;
  data: Record<string, unknown>;
};

export type WorkflowDetail = { workflow: Workflow; history: HistoryEvent[] };
