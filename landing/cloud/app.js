const addressInput = document.querySelector("[data-address]");
const apiKeyInput = document.querySelector("[data-api-key]");
const serviceState = document.querySelector("[data-service-state]");
const result = document.querySelector("[data-result]");
const resultTitle = document.querySelector("[data-result-title]");

const address = () => addressInput.value.trim().replace(/\/$/, "");

const showResult = (title, value) => {
  resultTitle.textContent = title;
  result.textContent = typeof value === "string" ? value : JSON.stringify(value, null, 2);
};

const request = async (path, options = {}) => {
  const headers = { Accept: "application/json", ...options.headers };
  const apiKey = apiKeyInput.value.trim();
  if (apiKey) headers.Authorization = `Bearer ${apiKey}`;
  const response = await fetch(`${address()}${path}`, { ...options, headers });
  const body = await response.json().catch(() => ({ error: `HTTP ${response.status}` }));
  if (!response.ok) throw new Error(body.error || `HTTP ${response.status}`);
  return body;
};

const checkService = async () => {
  serviceState.className = "service-state";
  serviceState.querySelector("span").textContent = "Checking service";
  try {
    await request("/health");
    serviceState.classList.add("ok");
    serviceState.querySelector("span").textContent = "Service healthy";
  } catch (error) {
    serviceState.classList.add("error");
    serviceState.querySelector("span").textContent = "Service unavailable";
    showResult("Connection failed", error.message);
  }
};

const checkConnection = async (event) => {
  const button = event.currentTarget;
  button.disabled = true;
  try {
    const response = await request("/cloud/status");
    serviceState.className = "service-state ok";
    serviceState.querySelector("span").textContent = "Connected";
    showResult("Connection ready", response);
  } catch (error) {
    showResult("Connection failed", error.message);
  } finally {
    button.disabled = false;
  }
};

document.querySelector("[data-check]").addEventListener("click", checkConnection);

document.querySelector("[data-start-form]").addEventListener("submit", async (event) => {
  event.preventDefault();
  const button = event.submitter;
  const data = new FormData(event.currentTarget);
  let args;
  try {
    args = JSON.parse(data.get("args"));
    if (!Array.isArray(args)) throw new Error("Arguments must be a JSON array.");
  } catch (error) {
    showResult("Invalid arguments", error.message);
    return;
  }
  const workflowId = data.get("workflow_id").trim();
  const body = { workflow_type: data.get("workflow_type").trim(), args };
  if (workflowId) body.workflow_id = workflowId;
  button.disabled = true;
  try {
    const response = await request("/workflows", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    showResult("Workflow started", response);
    document.querySelector("[data-inspect-form] [name=id]").value = response.workflow_id;
  } catch (error) {
    showResult("Start failed", error.message);
  } finally {
    button.disabled = false;
  }
});

document.querySelector("[data-inspect-form]").addEventListener("submit", async (event) => {
  event.preventDefault();
  const button = event.submitter;
  const data = new FormData(event.currentTarget);
  const resource = data.get("resource");
  const id = encodeURIComponent(data.get("id").trim());
  button.disabled = true;
  try {
    showResult(`${resource.slice(0, -1)} · ${decodeURIComponent(id)}`, await request(`/${resource}/${id}`));
  } catch (error) {
    showResult("Lookup failed", error.message);
  } finally {
    button.disabled = false;
  }
});

document.querySelector("[data-clear]").addEventListener("click", () => {
  showResult("Nothing selected", "Connect, start a workflow, or inspect a resource.");
});

checkService();
