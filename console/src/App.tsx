import * as Avatar from "@radix-ui/react-avatar";
import * as DropdownMenu from "@radix-ui/react-dropdown-menu";
import * as Separator from "@radix-ui/react-separator";
import * as Tabs from "@radix-ui/react-tabs";
import { createColumnHelper, flexRender, getCoreRowModel, getFilteredRowModel, useReactTable } from "@tanstack/react-table";
import {
  Activity, ArrowRight, Braces, CheckCircle2, ChevronsUpDown, CircleHelp, Clock3,
  Cloud, Command, Copy, Database, ExternalLink, Gauge, GitBranch, Github, KeyRound,
  Layers3, LogOut, Menu, Network, Play, RadioTower, RefreshCw, Search,
  Settings, ShieldCheck, TerminalSquare, TriangleAlert, Workflow, XCircle,
} from "lucide-react";
import { useCallback, useEffect, useMemo, useState } from "react";
import { Area, AreaChart, CartesianGrid, ResponsiveContainer, Tooltip as ChartTooltip, XAxis, YAxis } from "recharts";
import { toast } from "sonner";
import { ApiError, getOverview, getWorkflow } from "./api";
import { compact, duration, relativeTime, title } from "./lib";
import type { Operator, Overview, Process, Stream as StreamType, Workflow as WorkflowType, WorkflowDetail } from "./types";
import { Badge, Button, Card, Hint, SelectField, Sheet } from "./ui";

type Page = "overview" | "runs" | "streams" | "operators" | "processes" | "quickstart";
type TrendPoint = { time: string; rate: number; total: number };

const nav: { id: Page; label: string; icon: typeof Gauge }[] = [
  { id: "overview", label: "Overview", icon: Gauge },
  { id: "runs", label: "Runs", icon: Workflow },
  { id: "streams", label: "Streams", icon: RadioTower },
  { id: "operators", label: "Operators", icon: GitBranch },
  { id: "processes", label: "Processes", icon: Layers3 },
];

const commandExamples = [
  { label: "Temporal join with retry", command: "highwater example run temporal-order", detail: "Versioned enrichment, event-time gating, and an activity retry" },
  { label: "Keyed account balances", command: "highwater example run account-balance", detail: "Per-key isolation with durable ordered processing" },
  { label: "Durable order", command: "highwater example run order", detail: "A compact durable execution example" },
];

function Login({ onLogin }: { onLogin: (credential: string) => Promise<void> }) {
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  async function submit(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setBusy(true); setError("");
    const data = new FormData(event.currentTarget);
    try { await onLogin(btoa(`${data.get("username")}:${data.get("password")}`)); }
    catch (failure) {
      setError(failure instanceof ApiError && failure.status === 401 ? "That username or password is not valid." : failure instanceof Error ? failure.message : "Unable to sign in.");
    } finally { setBusy(false); }
  }
  return <main className="auth-layout">
    <section className="auth-product">
      <a className="wordmark inverse" href="https://highwater.cloud"><span className="mark"><Waves /></span>Highwater</a>
      <div className="auth-message"><Badge status="healthy">Live demo cluster</Badge><h1>Operate streams<br />like durable programs.</h1><p>Inspect executions, retry history, event-time progress, and per-key state from one control plane.</p></div>
      <div className="auth-proof"><span><ShieldCheck size={17} /> Durable execution</span><span><Clock3 size={17} /> Event-time correctness</span><span><GitBranch size={17} /> Stateful operators</span></div>
    </section>
    <section className="auth-form-wrap"><form className="auth-form" onSubmit={submit}>
      <div className="mobile-wordmark"><span className="mark"><Waves /></span>Highwater</div>
      <p className="overline">Highwater Cloud</p><h2>Sign in to the console</h2><p className="subtle">Monitor the shared demo environment.</p>
      <label>Username<input name="username" defaultValue="demo" autoComplete="username" required /></label>
      <label>Password<input name="password" defaultValue="demo" type="password" autoComplete="current-password" required /></label>
      {error && <div className="form-error"><TriangleAlert size={15} />{error}</div>}
      <Button type="submit" disabled={busy}>{busy ? <><RefreshCw className="spin" size={16} /> Connecting</> : <>Continue <ArrowRight size={16} /></>}</Button>
      <div className="demo-credential"><KeyRound size={15} /><span>Demo credentials</span><code>demo / demo</code></div>
    </form></section>
  </main>;
}

function Waves() { return <svg viewBox="0 0 24 24" aria-hidden="true"><path d="M3 8c3.2 0 3.2-3 6.4-3s3.2 3 6.4 3S19 5 22 5M3 13c3.2 0 3.2-3 6.4-3s3.2 3 6.4 3S19 10 22 10M3 18c3.2 0 3.2-3 6.4-3s3.2 3 6.4 3S19 15 22 15" /></svg>; }

function Shell({ page, setPage, children, onLogout, online, sidebarOpen, setSidebarOpen }: { page: Page; setPage: (page: Page) => void; children: React.ReactNode; onLogout: () => void; online: boolean; sidebarOpen: boolean; setSidebarOpen: (open: boolean) => void }) {
  return <div className="app-shell">
    <aside className={sidebarOpen ? "sidebar open" : "sidebar"}>
      <div className="sidebar-head"><button className="wordmark" onClick={() => setPage("overview")}><span className="mark"><Waves /></span>Highwater</button><button className="mobile-close" onClick={() => setSidebarOpen(false)}><XCircle size={20} /></button></div>
      <button className="workspace-switcher"><span className="workspace-avatar">AC</span><span><strong>Acme Corp</strong><small>Demo workspace</small></span><ChevronsUpDown size={14} /></button>
      <nav className="side-nav"><p>Workspace</p>{nav.map(({ id, label, icon: Icon }) => <button key={id} className={page === id ? "active" : ""} onClick={() => { setPage(id); setSidebarOpen(false); }}><Icon size={17} />{label}</button>)}</nav>
      <nav className="side-nav secondary"><p>Build</p><button className={page === "quickstart" ? "active" : ""} onClick={() => setPage("quickstart")}><TerminalSquare size={17} />Quickstart</button><a href="https://highwater.cloud/docs/"><Braces size={17} />Documentation<ExternalLink size={12} /></a></nav>
      <div className="sidebar-bottom"><div className="cluster-state"><span className={online ? "online" : "offline"} /><div><strong>{online ? "All systems operational" : "Connection interrupted"}</strong><small>sjc · demo</small></div></div><Separator.Root className="separator" /><button><CircleHelp size={17} />Support</button><button><Settings size={17} />Settings</button></div>
    </aside>
    {sidebarOpen && <button className="sidebar-scrim" onClick={() => setSidebarOpen(false)} aria-label="Close navigation" />}
    <section className="workspace">
      <header className="topbar"><button className="menu-button" onClick={() => setSidebarOpen(true)}><Menu size={19} /></button><div className="environment-picker"><span className="environment-dot" />Demo<ChevronDownIcon /></div><div className="topbar-actions"><Hint label="Search resources"><button className="icon-button"><Search size={17} /></button></Hint><Hint label="Documentation"><a className="icon-button" href="https://highwater.cloud/docs/"><CircleHelp size={17} /></a></Hint><DropdownMenu.Root><DropdownMenu.Trigger className="avatar-trigger"><Avatar.Root className="avatar"><Avatar.Fallback>DE</Avatar.Fallback></Avatar.Root></DropdownMenu.Trigger><DropdownMenu.Portal><DropdownMenu.Content className="dropdown" align="end" sideOffset={8}><div className="dropdown-account"><strong>Demo user</strong><small>demo@highwater.cloud</small></div><DropdownMenu.Separator /><DropdownMenu.Item onSelect={onLogout}><LogOut size={15} />Sign out</DropdownMenu.Item></DropdownMenu.Content></DropdownMenu.Portal></DropdownMenu.Root></div></header>
      <div className="page-content">{children}</div>
    </section>
  </div>;
}

function ChevronDownIcon() { return <ChevronsUpDown size={13} />; }

function PageHeader({ eyebrow, title: heading, description, actions }: { eyebrow?: string; title: string; description: string; actions?: React.ReactNode }) {
  return <div className="page-header"><div>{eyebrow && <p className="overline">{eyebrow}</p>}<h1>{heading}</h1><p>{description}</p></div>{actions && <div className="page-actions">{actions}</div>}</div>;
}

function StatusSummary({ data, rate }: { data: Overview; rate: number }) {
  const healthy = data.counts.failed_workflows === 0;
  const items = [
    { label: "Cluster health", value: healthy ? "Operational" : "Needs attention", note: healthy ? "No active failures" : `${data.counts.failed_workflows} failed runs`, icon: healthy ? CheckCircle2 : TriangleAlert, tone: healthy ? "green" : "red" },
    { label: "Event throughput", value: `${compact(rate)}/s`, note: "Observed this session", icon: Activity, tone: "blue" },
    { label: "Running now", value: data.counts.running_workflows.toString(), note: `${data.counts.workflows} total runs`, icon: Play, tone: "amber" },
    { label: "Watermark lag", value: maxWatermarkLag(data.streams), note: `${data.counts.streams} managed streams`, icon: Clock3, tone: "violet" },
  ];
  return <div className="metric-grid">{items.map(({ label, value, note, icon: Icon, tone }) => <Card className="metric-card" key={label}><div className={`metric-icon ${tone}`}><Icon size={18} /></div><div><span>{label}</span><strong>{value}</strong><small>{note}</small></div></Card>)}</div>;
}

function maxWatermarkLag(streams: StreamType[]) {
  const lag = Math.max(0, ...streams.map((stream) => stream.watermark_lag || 0));
  return lag ? duration(lag) : "Caught up";
}

function SectionHeading({ title: heading, detail, action }: { title: string; detail?: string; action?: React.ReactNode }) {
  return <div className="section-heading"><div><h2>{heading}</h2>{detail && <p>{detail}</p>}</div>{action}</div>;
}

function OverviewPage({ data, trend, rate, openRun, setPage }: { data: Overview; trend: TrendPoint[]; rate: number; openRun: (id: string) => void; setPage: (page: Page) => void }) {
  const attention = data.workflows.filter((run) => run.status === "FAILED" || run.retries > 0).slice(0, 4);
  return <>
    <PageHeader eyebrow="Control plane" title="Overview" description="Live operational state for the demo environment." actions={<Button onClick={() => setPage("quickstart")}><TerminalSquare size={16} />Run an example</Button>} />
    <StatusSummary data={data} rate={rate} />
    <div className="overview-grid">
      <Card className="chart-card"><SectionHeading title="Event throughput" detail="Records observed per second across managed streams" /><div className="chart"><ResponsiveContainer width="100%" height="100%"><AreaChart data={trend}><defs><linearGradient id="rateFill" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stopColor="#2e6fe8" stopOpacity={0.28} /><stop offset="100%" stopColor="#2e6fe8" stopOpacity={0} /></linearGradient></defs><CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#e6e8eb" /><XAxis dataKey="time" axisLine={false} tickLine={false} tick={{ fill: "#7a808a", fontSize: 11 }} /><YAxis axisLine={false} tickLine={false} width={38} tick={{ fill: "#7a808a", fontSize: 11 }} /><ChartTooltip contentStyle={{ borderRadius: 8, border: "1px solid #d9dde2", boxShadow: "0 10px 30px #10182818" }} /><Area type="monotone" dataKey="rate" stroke="#2e6fe8" strokeWidth={2} fill="url(#rateFill)" /></AreaChart></ResponsiveContainer></div></Card>
      <Card className="inventory-card"><SectionHeading title="Deployed resources" detail="Objects active in this environment" /><div className="inventory-list"><button onClick={() => setPage("streams")}><span className="resource-icon blue"><Database size={17} /></span><span><strong>{data.counts.streams}</strong>Streams</span><ArrowRight size={15} /></button><button onClick={() => setPage("operators")}><span className="resource-icon violet"><GitBranch size={17} /></span><span><strong>{data.counts.operators}</strong>Operators</span><ArrowRight size={15} /></button><button onClick={() => setPage("processes")}><span className="resource-icon green"><Layers3 size={17} /></span><span><strong>{data.counts.processes}</strong>Processes</span><ArrowRight size={15} /></button></div></Card>
    </div>
    <Card className="table-card"><SectionHeading title="Recent runs" detail="Most recently updated executions" action={<Button variant="ghost" onClick={() => setPage("runs")}>View all <ArrowRight size={14} /></Button>} /><RunsTable runs={data.workflows.slice(0, 7)} openRun={openRun} compactTable /></Card>
    {attention.length > 0 && <Card className="attention-card"><SectionHeading title="Needs attention" detail="Executions with failures or retries" /><div>{attention.map((run) => <button key={run.workflow_id} onClick={() => openRun(run.workflow_id)}><span className={run.status === "FAILED" ? "attention-icon failed" : "attention-icon retried"}>{run.status === "FAILED" ? <XCircle size={16} /> : <RefreshCw size={16} />}</span><span><strong>{run.workflow_type}</strong><small>{run.workflow_id}</small></span><Badge status={run.status}>{run.status === "FAILED" ? "Failed" : `${run.retries} retry`}</Badge><ArrowRight size={15} /></button>)}</div></Card>}
  </>;
}

const column = createColumnHelper<WorkflowType>();
function RunsTable({ runs, openRun, compactTable = false }: { runs: WorkflowType[]; openRun: (id: string) => void; compactTable?: boolean }) {
  const columns = useMemo(() => [
    column.accessor("workflow_id", { header: "Run", cell: ({ row }) => <button className="run-name" onClick={() => openRun(row.original.workflow_id)}><span className="run-icon"><Workflow size={15} /></span><span><strong>{row.original.workflow_type}</strong><small>{row.original.workflow_id}</small></span></button> }),
    column.accessor("status", { header: "Status", cell: (info) => <Badge status={info.getValue()}>{title(info.getValue().toLowerCase())}</Badge> }),
    column.accessor("task_queue", { header: "Task queue", cell: (info) => <code className="table-code">{info.getValue() || "default"}</code> }),
    column.accessor("retries", { header: "Retries", cell: (info) => info.getValue() }),
    column.accessor("duration_seconds", { header: "Duration", cell: (info) => duration(info.getValue()) }),
    column.accessor("updated_at", { header: "Updated", cell: (info) => relativeTime(info.getValue()) }),
  ], [openRun]);
  const table = useReactTable({ data: runs, columns, getCoreRowModel: getCoreRowModel(), getFilteredRowModel: getFilteredRowModel() });
  return <div className="table-scroll"><table className="data-table"><thead>{table.getHeaderGroups().map((group) => <tr key={group.id}>{group.headers.map((header, index) => compactTable && index === 2 ? null : <th key={header.id}>{flexRender(header.column.columnDef.header, header.getContext())}</th>)}</tr>)}</thead><tbody>{table.getRowModel().rows.map((row) => <tr key={row.id}>{row.getVisibleCells().map((cell, index) => compactTable && index === 2 ? null : <td key={cell.id}>{flexRender(cell.column.columnDef.cell, cell.getContext())}</td>)}</tr>)}</tbody></table>{runs.length === 0 && <Empty icon={Workflow} title="No matching runs" detail="Try a different status or search term." />}</div>;
}

function RunsPage({ data, openRun }: { data: Overview; openRun: (id: string) => void }) {
  const [query, setQuery] = useState(""); const [status, setStatus] = useState("all");
  const runs = data.workflows.filter((run) => (status === "all" || run.status === status) && `${run.workflow_id} ${run.workflow_type}`.toLowerCase().includes(query.toLowerCase()));
  return <><PageHeader eyebrow="Execution" title="Runs" description="Inspect durable program executions and their complete event history." /><Card className="resource-panel"><div className="toolbar"><div className="search-field"><Search size={15} /><input value={query} onChange={(event) => setQuery(event.target.value)} placeholder="Search runs" /></div><SelectField label="Filter status" value={status} onValueChange={setStatus} options={[{ value: "all", label: "All statuses" }, { value: "RUNNING", label: "Running" }, { value: "COMPLETED", label: "Completed" }, { value: "FAILED", label: "Failed" }]} /><span className="toolbar-count">{runs.length} runs</span></div><RunsTable runs={runs} openRun={openRun} /></Card></>;
}

function StreamsPage({ streams }: { streams: StreamType[] }) {
  return <><PageHeader eyebrow="Event ingress" title="Streams" description="Inspect partitions, event-time progress, and durable records." /><div className="resource-card-grid">{streams.map((stream) => <Card className="resource-card" key={stream.name}><div className="resource-card-head"><span className="large-resource-icon blue"><Database size={20} /></span><DropdownMenu.Root><DropdownMenu.Trigger className="icon-button"><Menu size={16} /></DropdownMenu.Trigger><DropdownMenu.Portal><DropdownMenu.Content className="dropdown" align="end"><DropdownMenu.Item onSelect={() => navigator.clipboard.writeText(stream.name)}><Copy size={14} />Copy stream name</DropdownMenu.Item></DropdownMenu.Content></DropdownMenu.Portal></DropdownMenu.Root></div><h3>{stream.name}</h3><div className="resource-status"><Badge status={stream.finalized ? "completed" : "healthy"}>{stream.finalized ? "Finalized" : "Active"}</Badge><span>{relativeTime(stream.updated_at)}</span></div><Separator.Root className="separator" /><dl><div><dt>Records</dt><dd>{stream.records.toLocaleString()}</dd></div><div><dt>Partitions</dt><dd>{stream.partitions}</dd></div><div><dt>Watermark</dt><dd>{stream.watermark ?? "—"}</dd></div><div><dt>Lag</dt><dd>{stream.watermark_lag ? duration(stream.watermark_lag) : "Caught up"}</dd></div></dl><div className="resource-footer"><Clock3 size={14} />{title(stream.watermark_mode)} watermark</div></Card>)}</div>{streams.length === 0 && <Card><Empty icon={Database} title="No streams" detail="Create one by running an example from the CLI." /></Card>}</>;
}

function OperatorsPage({ operators }: { operators: Operator[] }) {
  return <><PageHeader eyebrow="Data plane" title="Operators" description="Durable event-time transformations deployed to this environment." /><Card className="resource-panel"><div className="operator-list">{operators.map((operator) => <article className="operator-row" key={operator.operator_id}><span className="large-resource-icon violet"><GitBranch size={19} /></span><div className="operator-identity"><strong>{operator.operator_id}</strong><span><Badge status={operator.status}>{title(operator.status)}</Badge><small>{title(operator.kind)}</small></span></div><div className="operator-flow"><div>{operator.input.map((input) => <code key={input}>{input}</code>)}</div><ArrowRight size={16} /><code>{operator.workflow_type}</code></div><dl><div><dt>Received</dt><dd>{operator.received?.toLocaleString() ?? "—"}</dd></div><div><dt>Emitted</dt><dd>{operator.emitted?.toLocaleString() ?? "—"}</dd></div></dl></article>)}</div>{operators.length === 0 && <Empty icon={GitBranch} title="No operators" detail="Deploy an example to see its streaming topology." />}</Card></>;
}

function ProcessesPage({ processes }: { processes: Process[] }) {
  return <><PageHeader eyebrow="Isolation" title="Processes" description="Per-key durable programs with explicit concurrency and backpressure." /><div className="resource-card-grid">{processes.map((process) => <Card className="resource-card process-card" key={process.process_id}><div className="resource-card-head"><span className="large-resource-icon green"><Layers3 size={20} /></span><Badge status={process.status}>{title(process.status)}</Badge></div><h3>{process.process_id}</h3><p className="resource-description">{process.workflow_type}</p><div className="queue-bar"><span style={{ width: `${Math.min(100, ((process.pending + process.running) / Math.max(1, process.mailbox_capacity)) * 100)}%` }} /></div><div className="queue-label"><span>Mailbox utilization</span><b>{process.pending + process.running} / {process.mailbox_capacity}</b></div><dl><div><dt>Pending</dt><dd>{process.pending}</dd></div><div><dt>Running</dt><dd>{process.running}</dd></div><div><dt>Completed</dt><dd>{process.completed}</dd></div><div><dt>Failed</dt><dd>{process.failed}</dd></div></dl><div className="resource-footer"><Network size={14} />{process.max_concurrent_keys} concurrent keys · {title(process.event_time_gate)} gate</div></Card>)}</div>{processes.length === 0 && <Card><Empty icon={Layers3} title="No keyed processes" detail="Run the account balance example to deploy one." /></Card>}</>;
}

function QuickstartPage() {
  return <><PageHeader eyebrow="Get started" title="Run an example" description="Submit real work through the CLI, then monitor it here." /><div className="quickstart-grid"><Card className="quickstart-main"><div className="step"><span>1</span><div><h3>Install the CLI</h3><p>Install Highwater with Homebrew.</p><CommandBox command="brew install henneberger/tap/highwater" /></div></div><div className="step"><span>2</span><div><h3>Connect to Highwater Cloud</h3><p>The demo CLI profile is already configured for this shared environment.</p><CommandBox command="highwater cloud login" /></div></div><div className="step"><span>3</span><div><h3>Submit an example</h3><p>Pick a workload. It will appear in Runs within seconds.</p><div className="example-list">{commandExamples.map((example) => <div key={example.command}><span className="example-icon"><Command size={16} /></span><div><strong>{example.label}</strong><small>{example.detail}</small><CommandBox command={example.command} /></div></div>)}</div></div></div></Card><aside><Card className="help-card"><Cloud size={20} /><h3>Shared demo environment</h3><p>Resources are visible to every demo user and may be periodically removed.</p></Card><Card className="help-card"><Github size={20} /><h3>See how it works</h3><p>Inspect the SDK, execution engine, and examples in the public repository.</p><a href="https://github.com/henneberger/highwater">Open GitHub <ExternalLink size={13} /></a></Card></aside></div></>;
}

function CommandBox({ command }: { command: string }) {
  async function copy() { await navigator.clipboard.writeText(command); toast.success("Command copied"); }
  return <div className="command-box"><code><span>$</span> {command}</code><button onClick={copy} aria-label="Copy command"><Copy size={14} /></button></div>;
}

function Empty({ icon: Icon, title: heading, detail }: { icon: typeof Workflow; title: string; detail: string }) {
  return <div className="empty"><span><Icon size={21} /></span><strong>{heading}</strong><p>{detail}</p></div>;
}

function RunSheet({ id, credential, onClose }: { id?: string; credential: string; onClose: () => void }) {
  const [detail, setDetail] = useState<WorkflowDetail>(); const [error, setError] = useState("");
  useEffect(() => { if (!id) return; setDetail(undefined); setError(""); getWorkflow(credential, id).then(setDetail).catch((failure) => setError(failure instanceof Error ? failure.message : "Unable to load run")); }, [credential, id]);
  const workflow = detail?.workflow;
  return <Sheet open={Boolean(id)} onOpenChange={(open) => !open && onClose()} title={workflow?.workflow_type || "Run detail"} description={id}>
    {!detail && !error && <div className="sheet-loading"><RefreshCw className="spin" size={20} />Loading event history…</div>}
    {error && <div className="sheet-error"><TriangleAlert size={18} />{error}<Button variant="secondary" onClick={onClose}>Close</Button></div>}
    {detail && <Tabs.Root defaultValue="history" className="run-tabs"><div className="run-summary"><div><span>Status</span><Badge status={workflow!.status}>{title(workflow!.status.toLowerCase())}</Badge></div><div><span>Duration</span><strong>{duration(workflow!.duration_seconds)}</strong></div><div><span>Retries</span><strong>{workflow!.retries}</strong></div><div><span>Events</span><strong>{workflow!.history_events}</strong></div></div><Tabs.List><Tabs.Trigger value="history">Event history</Tabs.Trigger><Tabs.Trigger value="details">Details</Tabs.Trigger></Tabs.List><Tabs.Content value="history"><div className="event-history">{detail.history.map((event, index) => <article key={event.event_id ?? index}><div className="event-rail"><span>{index + 1}</span></div><div className="event-content"><div><strong>{title(event.type || event.event_type)}</strong><time>{new Date(event.created_at * 1000).toLocaleString()}</time></div>{Object.keys(event.data || {}).length > 0 && <details><summary>Event payload</summary><pre>{JSON.stringify(event.data, null, 2)}</pre></details>}</div></article>)}</div></Tabs.Content><Tabs.Content value="details"><dl className="detail-list"><div><dt>Workflow ID</dt><dd><code>{workflow!.workflow_id}</code></dd></div><div><dt>Workflow type</dt><dd>{workflow!.workflow_type}</dd></div><div><dt>Task queue</dt><dd>{workflow!.task_queue || "default"}</dd></div><div><dt>Build ID</dt><dd>{workflow!.build_id || "—"}</dd></div><div><dt>Created</dt><dd>{new Date(workflow!.created_at * 1000).toLocaleString()}</dd></div>{workflow!.error && <div><dt>Error</dt><dd className="error-text">{workflow!.error}</dd></div>}</dl></Tabs.Content></Tabs.Root>}
  </Sheet>;
}

export default function App() {
  const [credential, setCredential] = useState(() => sessionStorage.getItem("highwater_demo_login") || "");
  const [data, setData] = useState<Overview>(); const [error, setError] = useState(""); const [page, setPage] = useState<Page>("overview");
  const [selectedRun, setSelectedRun] = useState<string>(); const [refreshing, setRefreshing] = useState(false); const [trend, setTrend] = useState<TrendPoint[]>([]); const [sidebarOpen, setSidebarOpen] = useState(false);
  const load = useCallback(async (login = credential) => {
    if (!login) return;
    setRefreshing(true);
    try {
      const next = await getOverview(login);
      setError("");
      setData((previous) => {
        const total = next.streams.reduce((sum, stream) => sum + stream.records, 0);
        const oldTotal = previous?.streams.reduce((sum, stream) => sum + stream.records, 0) ?? total;
        const elapsed = previous ? Math.max(1, next.generated_at - previous.generated_at) : 1;
        setTrend((points) => [...points, { time: new Date(next.generated_at * 1000).toLocaleTimeString([], { minute: "2-digit", second: "2-digit" }), rate: Math.max(0, Math.round((total - oldTotal) / elapsed)), total }].slice(-18));
        return next;
      });
      return next;
    } catch (failure) {
      if (failure instanceof ApiError && failure.status === 401) { sessionStorage.removeItem("highwater_demo_login"); setCredential(""); setData(undefined); }
      else setError(failure instanceof Error ? failure.message : "Console unavailable");
      throw failure;
    } finally { setRefreshing(false); }
  }, [credential]);
  useEffect(() => { if (!credential) return; load().catch(() => undefined); const timer = window.setInterval(() => load().catch(() => undefined), 5000); return () => window.clearInterval(timer); }, [credential, load]);
  async function login(next: string) { const result = await getOverview(next); sessionStorage.setItem("highwater_demo_login", next); setCredential(next); setData(result); setError(""); }
  function logout() { sessionStorage.removeItem("highwater_demo_login"); setCredential(""); setData(undefined); }
  if (!credential) return <Login onLogin={login} />;
  const rate = trend.at(-1)?.rate ?? 0;
  return <Shell page={page} setPage={setPage} onLogout={logout} online={!error} sidebarOpen={sidebarOpen} setSidebarOpen={setSidebarOpen}>
    {error && <div className="connection-banner"><TriangleAlert size={15} /><span>{error}</span><button onClick={() => load().catch(() => undefined)}>Retry</button></div>}
    {!data ? <div className="page-loader"><RefreshCw className="spin" />Connecting to the demo cluster…</div> : <>
      <div className="live-refresh"><span className={!error ? "online" : "offline"} />{error ? "Disconnected" : `Live · updated ${relativeTime(data.generated_at)}`}<button onClick={() => load().catch(() => undefined)} disabled={refreshing}><RefreshCw className={refreshing ? "spin" : ""} size={13} />Refresh</button></div>
      {page === "overview" && <OverviewPage data={data} trend={trend} rate={rate} openRun={setSelectedRun} setPage={setPage} />}
      {page === "runs" && <RunsPage data={data} openRun={setSelectedRun} />}
      {page === "streams" && <StreamsPage streams={data.streams} />}
      {page === "operators" && <OperatorsPage operators={data.operators} />}
      {page === "processes" && <ProcessesPage processes={data.processes} />}
      {page === "quickstart" && <QuickstartPage />}
    </>}
    <RunSheet id={selectedRun} credential={credential} onClose={() => setSelectedRun(undefined)} />
  </Shell>;
}
