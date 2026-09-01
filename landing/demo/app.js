const API = ["localhost", "127.0.0.1"].includes(window.location.hostname)
  ? "http://127.0.0.1:17333"
  : "https://api.highwater.cloud";
const loginView = document.querySelector("[data-login]");
const consoleView = document.querySelector("[data-console]");
const loginError = document.querySelector("[data-login-error]");
let credential = sessionStorage.getItem("highwater_demo_login");
let refreshTimer;

const escapeHtml = (value) => String(value ?? "").replace(/[&<>"']/g, (character) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#039;" })[character]);
const formatTime = (seconds) => seconds ? new Date(seconds * 1000).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" }) : "—";
const formatDuration = (seconds) => seconds < 1 ? `${Math.max(0, seconds * 1000).toFixed(0)}ms` : `${seconds.toFixed(2)}s`;
const statusClass = (status) => String(status).toLowerCase();

async function api(path) {
  const response = await fetch(`${API}${path}`, { headers: { Authorization: `Basic ${credential}`, Accept: "application/json" } });
  const body = await response.json().catch(() => ({ error: `HTTP ${response.status}` }));
  if (!response.ok) throw Object.assign(new Error(body.error || `HTTP ${response.status}`), { status: response.status });
  return body;
}

function showLogin(message = "") {
  clearInterval(refreshTimer);
  credential = null;
  sessionStorage.removeItem("highwater_demo_login");
  loginError.textContent = message;
  loginView.hidden = false;
  consoleView.hidden = true;
}

function showConsole() {
  loginView.hidden = true;
  consoleView.hidden = false;
}

function renderStats(data) {
  const values = [
    ["Runs", data.counts.workflows, `${data.counts.running_workflows} currently running`],
    ["Streaming operators", data.counts.operators, "durable, event-time aware"],
    ["Streams", data.counts.streams, "watermarks tracked"],
    ["Keyed processes", data.counts.processes, `${data.counts.failed_workflows} failed runs`],
  ];
  document.querySelector("[data-stats]").innerHTML = values.map(([label, value, note]) => `<article class="stat"><span>${escapeHtml(label)}</span><strong>${value}</strong><small>${escapeHtml(note)}</small></article>`).join("");
}

function renderWorkflows(workflows) {
  const body = document.querySelector("[data-workflows]");
  document.querySelector("[data-workflows-empty]").hidden = workflows.length > 0;
  body.innerHTML = workflows.map((workflow) => `<tr tabindex="0" data-workflow="${escapeHtml(workflow.workflow_id)}"><td><strong>${escapeHtml(workflow.workflow_id)}</strong><small>${escapeHtml(workflow.workflow_type)}</small></td><td><span class="status ${statusClass(workflow.status)}">${escapeHtml(workflow.status)}</span></td><td>${workflow.retries}</td><td>${workflow.history_events}</td><td>${formatDuration(workflow.duration_seconds)}</td><td>${formatTime(workflow.updated_at)}</td></tr>`).join("");
  body.querySelectorAll("[data-workflow]").forEach((row) => {
    const open = () => openWorkflow(row.dataset.workflow);
    row.addEventListener("click", open);
    row.addEventListener("keydown", (event) => { if (event.key === "Enter" || event.key === " ") open(); });
  });
}

function renderOperators(operators) {
  document.querySelector("[data-operator-count]").textContent = `${operators.length} deployed`;
  document.querySelector("[data-operators]").innerHTML = operators.length ? operators.map((operator) => `<article class="list-item"><div><strong>${escapeHtml(operator.operator_id)}</strong><small>${escapeHtml(operator.kind.replaceAll("_", " "))} · ${escapeHtml((operator.input || []).join(" + "))}</small></div><div class="metric">${operator.emitted ?? 0} out<small>${operator.received ?? "—"} in</small></div></article>`).join("") : '<div class="empty">No streaming operators deployed.</div>';
}

function renderStreams(streams) {
  document.querySelector("[data-stream-count]").textContent = `${streams.length} active`;
  document.querySelector("[data-streams]").innerHTML = streams.length ? streams.map((stream) => `<article class="list-item"><div><strong>${escapeHtml(stream.name)}</strong><small>${stream.partitions} partition${stream.partitions === 1 ? "" : "s"} · ${escapeHtml(stream.watermark_mode)}</small></div><div class="metric">wm ${stream.watermark ?? "—"}<small>${stream.records} events</small></div></article>`).join("") : '<div class="empty">No streams created.</div>';
}

function renderProcesses(processes) {
  document.querySelector("[data-process-count]").textContent = `${processes.length} deployed`;
  document.querySelector("[data-processes]").innerHTML = processes.length ? processes.map((process) => `<article class="process"><h3>${escapeHtml(process.process_id)}</h3><p>${escapeHtml(process.workflow_type)} · ${escapeHtml(process.event_time_gate)} gate</p><div class="process-metrics"><span><b>${process.pending}</b>pending</span><span><b>${process.running}</b>running</span><span><b>${process.completed}</b>done</span><span><b>${process.failed}</b>failed</span></div></article>`).join("") : '<div class="empty">No keyed processes deployed.</div>';
}

async function refresh() {
  const button = document.querySelector("[data-refresh]");
  button.disabled = true;
  try {
    const data = await api("/console/overview");
    renderStats(data);
    renderWorkflows(data.workflows);
    renderOperators(data.operators);
    renderStreams(data.streams);
    renderProcesses(data.processes);
    document.querySelector("[data-updated]").textContent = `Updated ${formatTime(data.generated_at)}`;
  } catch (error) {
    if (error.status === 401) showLogin("Your console session has ended.");
    else document.querySelector("[data-updated]").textContent = "Console unavailable";
  } finally {
    button.disabled = false;
  }
}

async function openWorkflow(id) {
  const drawer = document.querySelector("[data-drawer]");
  const scrim = document.querySelector("[data-scrim]");
  drawer.hidden = false;
  scrim.hidden = false;
  document.querySelector("[data-drawer-title]").textContent = id;
  const body = document.querySelector("[data-drawer-body]");
  body.textContent = "Loading history…";
  try {
    const data = await api(`/console/workflows/${encodeURIComponent(id)}`);
    const workflow = data.workflow;
    body.innerHTML = `<div class="drawer-summary"><div><small>Status</small><strong>${escapeHtml(workflow.status)}</strong></div><div><small>Retries</small><strong>${workflow.retries}</strong></div><div><small>Duration</small><strong>${formatDuration(workflow.duration_seconds)}</strong></div></div><div class="timeline">${data.history.map((event) => `<article><strong>${escapeHtml(event.type)}</strong><time>${formatTime(event.created_at)}</time>${Object.keys(event.data || {}).length ? `<pre>${escapeHtml(JSON.stringify(event.data, null, 2))}</pre>` : ""}</article>`).join("")}</div>`;
  } catch (error) {
    body.textContent = error.message;
  }
}

function closeDrawer() {
  document.querySelector("[data-drawer]").hidden = true;
  document.querySelector("[data-scrim]").hidden = true;
}

document.querySelector("[data-login-form]").addEventListener("submit", async (event) => {
  event.preventDefault();
  const data = new FormData(event.currentTarget);
  credential = btoa(`${data.get("username")}:${data.get("password")}`);
  try {
    await api("/console/overview");
    sessionStorage.setItem("highwater_demo_login", credential);
    showConsole();
    await refresh();
    refreshTimer = setInterval(refresh, 5000);
  } catch {
    showLogin("That username or password is not valid.");
  }
});
document.querySelector("[data-logout]").addEventListener("click", () => showLogin());
document.querySelector("[data-refresh]").addEventListener("click", refresh);
document.querySelector("[data-drawer-close]").addEventListener("click", closeDrawer);
document.querySelector("[data-scrim]").addEventListener("click", closeDrawer);
document.querySelectorAll("[data-copy]").forEach((button) => button.addEventListener("click", async () => {
  await navigator.clipboard.writeText(button.dataset.copy);
  button.querySelector("span").textContent = "Copied";
  setTimeout(() => { button.querySelector("span").textContent = "Copy"; }, 1500);
}));

if (credential) {
  showConsole();
  refresh();
  refreshTimer = setInterval(refresh, 5000);
} else {
  showLogin();
}
