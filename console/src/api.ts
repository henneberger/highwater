import type { Overview, WorkflowDetail } from "./types";

const API = ["localhost", "127.0.0.1"].includes(window.location.hostname)
  && !new URLSearchParams(window.location.search).has("cloud")
  ? "http://127.0.0.1:17333"
  : "https://api.highwater.cloud";

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

export const getOverview = (credential: string) => request<Overview>("/console/overview", credential);
export const getWorkflow = (credential: string, id: string) => request<WorkflowDetail>(`/console/workflows/${encodeURIComponent(id)}`, credential);
