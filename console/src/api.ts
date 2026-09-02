import type { Overview, ProcessDetail, WorkflowDetail } from "./types";

const API = ["localhost", "127.0.0.1"].includes(window.location.hostname)
  && !new URLSearchParams(window.location.search).has("cloud")
  ? "http://127.0.0.1:17333"
  : window.location.origin;

export class ApiError extends Error {
  constructor(message: string, public status?: number) { super(message); }
}

export async function request<T>(path: string, credential: string): Promise<T> {
  let response: Response;
  try {
    response = await fetch(`${API}${path}`, {
      headers: { Authorization: `Basic ${credential}`, Accept: "application/json" },
    });
  } catch {
    throw new ApiError("The cloud API could not be reached. Check your connection and try again.");
  }
  const body = await response.json().catch(() => ({ error: `HTTP ${response.status}` }));
  if (!response.ok) throw new ApiError(body.error || `HTTP ${response.status}`, response.status);
  return body as T;
}

export async function getOverview(credential: string): Promise<Overview> {
  const overview = await request<Overview>("/console/overview", credential);
  overview.counts.recovered_workflows ??= overview.workflows.filter((run) => run.status === "COMPLETED" && run.retries > 0).length;
  overview.durability ??= {
    status: "UNKNOWN",
    storage_mode: "checkpointed",
    partition_owners: [],
    active_partition_owners: 0,
    key_groups: 0,
    active_key_groups: 0,
    node_id: "upgrading",
    region: "unknown",
  };
  return overview;
}
export const getWorkflow = (credential: string, id: string) => request<WorkflowDetail>(`/console/workflows/${encodeURIComponent(id)}`, credential);
export const getProcess = (credential: string, id: string) => request<ProcessDetail>(`/console/processes/${encodeURIComponent(id)}`, credential);
