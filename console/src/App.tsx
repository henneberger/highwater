import * as Avatar from "@radix-ui/react-avatar";
import * as DropdownMenu from "@radix-ui/react-dropdown-menu";
import * as Separator from "@radix-ui/react-separator";
import * as Tabs from "@radix-ui/react-tabs";
import { createColumnHelper, flexRender, getCoreRowModel, getFilteredRowModel, useReactTable } from "@tanstack/react-table";
import {
  Activity, ArrowLeft, ArrowRight, Braces, CheckCircle2, CircleHelp, Clock3,
  Cloud, Command, Copy, Database, ExternalLink, Gauge, GitBranch, Github, KeyRound,
  Layers3, LogOut, Menu, Network, Play, RadioTower, RefreshCw, Search,
  ShieldCheck, TerminalSquare, TriangleAlert, Workflow, XCircle,
} from "lucide-react";
import { useCallback, useEffect, useMemo, useState } from "react";
import { useLocation, useNavigate } from "react-router-dom";
import { Area, AreaChart, CartesianGrid, ResponsiveContainer, Tooltip as ChartTooltip, XAxis, YAxis } from "recharts";
import { toast } from "sonner";
import { ApiError, getOverview, getWorkflow } from "./api";
import { compact, duration, relativeTime, title } from "./lib";
import type { Operator, Overview, Process, Stream as StreamType, Workflow as WorkflowType, WorkflowDetail } from "./types";
import { Badge, Button, Card, Hint, SelectField } from "./ui";

type Page = "overview" | "runs" | "streams" | "operators" | "processes" | "quickstart";
type TrendPoint = { time: string; generatedAt: number; rate: number; total: number };

const paths: Record<Page, string> = {
  overview: "/overview",
  runs: "/runs",
  streams: "/streams",
  operators: "/operators",
  processes: "/processes",
  quickstart: "/quickstart",
};

function pageFromPath(pathname: string): Page {
  if (pathname.startsWith("/runs")) return "runs";
  if (pathname.startsWith("/processes")) return "processes";
  const match = Object.entries(paths).find(([, path]) => path === pathname);
  return match ? match[0] as Page : "overview";
}

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
      <div className="workspace-switcher"><span className="workspace-avatar">HW</span><span><strong>Public sandbox</strong><small>Shared demo workspace</small></span></div>
      <nav className="side-nav"><p>Workspace</p>{nav.map(({ id, label, icon: Icon }) => <button key={id} className={page === id ? "active" : ""} onClick={() => { setPage(id); setSidebarOpen(false); }}><Icon size={17} />{label}</button>)}</nav>
      <nav className="side-nav secondary"><p>Build</p><button className={page === "quickstart" ? "active" : ""} onClick={() => setPage("quickstart")}><TerminalSquare size={17} />Quickstart</button><a href="https://highwater.cloud/docs/"><Braces size={17} />Documentation<ExternalLink size={12} /></a></nav>
      <div className="sidebar-bottom"><div className="cluster-state"><span className={online ? "online" : "offline"} /><div><strong>{online ? "Console connected" : "Connection interrupted"}</strong><small>sjc · public sandbox</small></div></div><Separator.Root className="separator" /><a href="https://calendly.com/henneberger-daniel/30min"><CircleHelp size={17} />Talk to Highwater<ExternalLink size={12} /></a></div>
    </aside>
    {sidebarOpen && <button className="sidebar-scrim" onClick={() => setSidebarOpen(false)} aria-label="Close navigation" />}
    <section className="workspace">
      <header className="topbar"><button className="menu-button" onClick={() => setSidebarOpen(true)} aria-label="Open navigation"><Menu size={19} /></button><div className="environment-picker"><span className="environment-dot" />Demo environment</div><div className="topbar-actions"><Hint label="Documentation"><a className="icon-button" aria-label="Documentation" href="https://highwater.cloud/docs/"><CircleHelp size={17} /></a></Hint><DropdownMenu.Root><DropdownMenu.Trigger className="avatar-trigger" aria-label="User menu"><Avatar.Root className="avatar"><Avatar.Fallback>DE</Avatar.Fallback></Avatar.Root></DropdownMenu.Trigger><DropdownMenu.Portal><DropdownMenu.Content className="dropdown" align="end" sideOffset={8}><div className="dropdown-account"><strong>Demo user</strong><small>demo@highwater.cloud</small></div><DropdownMenu.Separator /><DropdownMenu.Item onSelect={onLogout}><LogOut size={15} />Sign out</DropdownMenu.Item></DropdownMenu.Content></DropdownMenu.Portal></DropdownMenu.Root></div></header>
      <div className="page-content">{children}</div>
    </section>
  </div>;
}

function PageHeader({ eyebrow, title: heading, description, actions }: { eyebrow?: string; title: string; description: string; actions?: React.ReactNode }) {
  return <div className="page-header"><div>{eyebrow && <p className="overline">{eyebrow}</p>}<h1>{heading}</h1><p>{description}</p></div>{actions && <div className="page-actions">{actions}</div>}</div>;
}

function StatusSummary({ data, rate }: { data: Overview; rate: number }) {
  const healthy = data.durability.status === "HEALTHY";
  const activeJobs = data.operators.filter((operator) => operator.status === "ACTIVE").length
    + data.processes.filter((process) => process.status === "ACTIVE").length;
  const liveInputs = data.streams.filter((stream) =>
    !stream.finalized && data.generated_at - stream.updated_at <= 30
  ).length;
  const items = [
    { label: "Durability health", value: healthy ? "Healthy" : title(data.durability.status), note: `${data.durability.active_partition_owners}/${data.durability.partition_owners.length} partition owners active`, icon: healthy ? CheckCircle2 : TriangleAlert, tone: healthy ? "green" : "red" },
    { label: "Event throughput", value: `${rate < 10 ? rate.toFixed(1) : compact(rate)}/s`, note: "60-second observed rate", icon: Activity, tone: "blue" },
    { label: "Streaming jobs", value: activeJobs.toString(), note: `${liveInputs} inputs receiving · ${data.counts.running_workflows} executions in flight`, icon: Play, tone: "amber" },
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

function OverviewPage({ data, trend, rate, openRun, openProcess, setPage }: { data: Overview; trend: TrendPoint[]; rate: number; openRun: (id: string) => void; openProcess: (id: string) => void; setPage: (page: Page) => void }) {
  const attention = data.workflows.filter((run) => run.status === "FAILED").slice(0, 4);
  const recovered = data.workflows.filter((run) => run.status === "COMPLETED" && run.retries > 0).slice(0, 4);
  const latestRecovery = recovered[0];
  return <>
    <PageHeader eyebrow="Control plane" title="Overview" description="Operational state produced by jobs submitted to this environment." actions={<Button onClick={() => latestRecovery ? openRun(latestRecovery.workflow_id) : setPage("quickstart")}><ShieldCheck size={16} />{latestRecovery ? "Inspect a recovery" : "Run an example"}</Button>} />
    <StatusSummary data={data} rate={rate} />
    <div className="overview-grid">
      <Card className="chart-card"><SectionHeading title="Event throughput" detail="Records observed per second across managed streams" /><div className="chart"><ResponsiveContainer width="100%" height="100%"><AreaChart data={trend}><defs><linearGradient id="rateFill" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stopColor="#2e6fe8" stopOpacity={0.28} /><stop offset="100%" stopColor="#2e6fe8" stopOpacity={0} /></linearGradient></defs><CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#e6e8eb" /><XAxis dataKey="time" axisLine={false} tickLine={false} tick={{ fill: "#7a808a", fontSize: 11 }} /><YAxis axisLine={false} tickLine={false} width={38} tick={{ fill: "#7a808a", fontSize: 11 }} /><ChartTooltip contentStyle={{ borderRadius: 8, border: "1px solid #d9dde2", boxShadow: "0 10px 30px #10182818" }} /><Area type="monotone" dataKey="rate" stroke="#2e6fe8" strokeWidth={2} fill="url(#rateFill)" /></AreaChart></ResponsiveContainer></div></Card>
      <Card className="inventory-card"><SectionHeading title="Deployed resources" detail="Objects active in this environment" /><div className="inventory-list"><button onClick={() => setPage("streams")}><span className="resource-icon blue"><Database size={17} /></span><span><strong>{data.counts.streams}</strong>Streams</span><ArrowRight size={15} /></button><button onClick={() => setPage("operators")}><span className="resource-icon violet"><GitBranch size={17} /></span><span><strong>{data.counts.operators}</strong>Operators</span><ArrowRight size={15} /></button><button onClick={() => setPage("processes")}><span className="resource-icon green"><Layers3 size={17} /></span><span><strong>{data.counts.processes}</strong>Processes</span><ArrowRight size={15} /></button></div></Card>
    </div>
    <StreamingJobsPanel processes={data.processes} open={openProcess} />
    <DurabilityPanel data={data} />
    <Card className="table-card"><SectionHeading title="Recent runs" detail="Active streaming jobs and recently updated executions" action={<Button variant="ghost" onClick={() => setPage("runs")}>View all <ArrowRight size={14} /></Button>} /><RunsTable runs={data.workflows.slice(0, 7)} processes={data.processes} openRun={openRun} openProcess={openProcess} compactTable /></Card>
    {attention.length > 0 && <Card className="attention-card"><SectionHeading title="Needs attention" detail="Unresolved execution failures" /><div>{attention.map((run) => <button key={run.workflow_id} onClick={() => openRun(run.workflow_id)}><span className="attention-icon failed"><XCircle size={16} /></span><span><strong>{run.workflow_type}</strong><small>{run.workflow_id}</small></span><Badge status="failed">Failed</Badge><ArrowRight size={15} /></button>)}</div></Card>}
    {recovered.length > 0 && <Card className="attention-card recovered-card"><SectionHeading title="Recovered automatically" detail="Failures Highwater retried without operator intervention" /><div>{recovered.map((run) => <button key={run.workflow_id} onClick={() => openRun(run.workflow_id)}><span className="attention-icon recovered"><ShieldCheck size={16} /></span><span><strong>{run.workflow_type}</strong><small>{run.workflow_id}</small></span><Badge status="completed">{run.retries} retry</Badge><ArrowRight size={15} /></button>)}</div></Card>}
  </>;
}

function StreamingJobsPanel({ processes, open }: { processes: Process[]; open: (id: string) => void }) {
  const jobs = [...processes].sort((left, right) => right.completed - left.completed);
  return <Card className="attention-card recovered-card"><SectionHeading title="Streaming jobs" detail="Continuously running durable programs" /><div>{jobs.map((process) => <button key={process.process_id} onClick={() => open(process.process_id)}><span className="attention-icon recovered"><RadioTower size={16} /></span><span><strong>{process.process_id}</strong><small>{process.workflow_type} · {process.completed.toLocaleString()} events completed</small></span><Badge status={process.status}>{process.status === "ACTIVE" ? "Streaming" : title(process.status)}</Badge><ArrowRight size={15} /></button>)}</div></Card>;
}

function DurabilityPanel({ data }: { data: Overview }) {
  const checkpoint = data.durability.checkpoint;
  return <Card className="durability-card"><SectionHeading title="Durability and ownership" detail="Materialized state, checkpoints, and fenced partition leases" /><div className="durability-grid"><div><span className="resource-icon blue"><Database size={17} /></span><p>Storage mode</p><strong>{title(data.durability.storage_mode)}</strong><small>{data.durability.region} · {data.durability.node_id}</small></div><div><span className="resource-icon violet"><Clock3 size={17} /></span><p>Latest checkpoint</p><strong>{checkpoint ? relativeTime(checkpoint.created_at) : "Pending first checkpoint"}</strong><small>{checkpoint ? `sequence ${checkpoint.sequence.toLocaleString()} · ${checkpoint.shards} shards` : "Checkpointing begins after durable transitions"}</small></div><div><span className="resource-icon green"><ShieldCheck size={17} /></span><p>Fenced ownership</p><strong>{data.durability.active_partition_owners}/{data.durability.partition_owners.length} partitions active</strong><small>{data.durability.active_key_groups}/{data.durability.key_groups} key groups leased</small></div></div></Card>;
}

const column = createColumnHelper<WorkflowType>();
function RunsTable({ runs, processes = [], openRun, openProcess, compactTable = false }: { runs: WorkflowType[]; processes?: Process[]; openRun: (id: string) => void; openProcess?: (id: string) => void; compactTable?: boolean }) {
  const columns = useMemo(() => [
    column.accessor("workflow_id", { header: "Run", cell: ({ row }) => <button className="run-name" onClick={() => openRun(row.original.workflow_id)}><span className="run-icon"><Workflow size={15} /></span><span><strong>{row.original.workflow_type}</strong><small>{row.original.workflow_id}</small></span></button> }),
    column.accessor("status", { header: "Status", cell: (info) => <Badge status={info.getValue()}>{title(info.getValue().toLowerCase())}</Badge> }),
    column.accessor("task_queue", { header: "Task queue", cell: (info) => <code className="table-code">{info.getValue() || "default"}</code> }),
    column.accessor("retries", { header: "Retries", cell: (info) => info.getValue() }),
    column.accessor("duration_seconds", { header: "Duration", cell: (info) => duration(info.getValue()) }),
    column.accessor("updated_at", { header: "Updated", cell: (info) => relativeTime(info.getValue()) }),
  ], [openRun]);
  const table = useReactTable({ data: runs, columns, getCoreRowModel: getCoreRowModel(), getFilteredRowModel: getFilteredRowModel() });
  return <div className="table-scroll"><table className="data-table"><thead>{table.getHeaderGroups().map((group) => <tr key={group.id}>{group.headers.map((header, index) => compactTable && index === 2 ? null : <th key={header.id}>{flexRender(header.column.columnDef.header, header.getContext())}</th>)}</tr>)}</thead><tbody>{processes.map((process) => <tr key={`process-${process.process_id}`}><td><button className="run-name" onClick={() => openProcess?.(process.process_id)}><span className="run-icon"><RadioTower size={15} /></span><span><strong>{process.workflow_type}</strong><small>{process.process_id}</small></span></button></td><td><Badge status={process.status}>{process.status === "ACTIVE" ? "Streaming" : title(process.status)}</Badge></td>{!compactTable && <td><code className="table-code">continuous</code></td>}<td>{process.retrying ?? 0}</td><td>Continuous</td><td>Live</td></tr>)}{table.getRowModel().rows.map((row) => <tr key={row.id}>{row.getVisibleCells().map((cell, index) => compactTable && index === 2 ? null : <td key={cell.id}>{flexRender(cell.column.columnDef.cell, cell.getContext())}</td>)}</tr>)}</tbody></table>{runs.length === 0 && processes.length === 0 && <Empty icon={Workflow} title="No matching runs" detail="Try a different status or search term." />}</div>;
}

function RunsPage({ data, openRun, openProcess }: { data: Overview; openRun: (id: string) => void; openProcess: (id: string) => void }) {
  const [query, setQuery] = useState(""); const [status, setStatus] = useState("all");
  const runs = data.workflows.filter((run) => (status === "all" || run.status === status) && `${run.workflow_id} ${run.workflow_type}`.toLowerCase().includes(query.toLowerCase()));
  return <><PageHeader eyebrow="Execution" title="Runs" description="Inspect continuously running streaming jobs and bounded durable executions." /><Card className="resource-panel"><SectionHeading title="Execution history" detail="Streaming jobs and bounded runs with retained state" /><div className="toolbar"><div className="search-field"><Search size={15} /><input value={query} onChange={(event) => setQuery(event.target.value)} placeholder="Search runs" /></div><SelectField label="Filter status" value={status} onValueChange={setStatus} options={[{ value: "all", label: "All statuses" }, { value: "RUNNING", label: "Running" }, { value: "COMPLETED", label: "Completed" }, { value: "FAILED", label: "Failed" }]} /><span className="toolbar-count">{runs.length + data.processes.length} runs</span></div><RunsTable runs={runs} processes={data.processes} openRun={openRun} openProcess={openProcess} /></Card></>;
}

function StreamsPage({ streams }: { streams: StreamType[] }) {
  return <><PageHeader eyebrow="Event ingress" title="Streams" description="Inspect partitions, event-time progress, and durable records." /><div className="resource-card-grid">{streams.map((stream) => <Card className="resource-card" key={stream.name}><div className="resource-card-head"><span className="large-resource-icon blue"><Database size={20} /></span><DropdownMenu.Root><DropdownMenu.Trigger className="icon-button"><Menu size={16} /></DropdownMenu.Trigger><DropdownMenu.Portal><DropdownMenu.Content className="dropdown" align="end"><DropdownMenu.Item onSelect={() => navigator.clipboard.writeText(stream.name)}><Copy size={14} />Copy stream name</DropdownMenu.Item></DropdownMenu.Content></DropdownMenu.Portal></DropdownMenu.Root></div><h3>{stream.name}</h3><div className="resource-status"><Badge status={stream.finalized ? "completed" : "healthy"}>{stream.finalized ? "Finalized" : "Active"}</Badge><span>{relativeTime(stream.updated_at)}</span></div><Separator.Root className="separator" /><dl><div><dt>Records</dt><dd>{stream.records.toLocaleString()}</dd></div><div><dt>Partitions</dt><dd>{stream.partitions}</dd></div><div><dt>Watermark</dt><dd>{stream.watermark ?? "—"}</dd></div><div><dt>Lag</dt><dd>{stream.watermark_lag ? duration(stream.watermark_lag) : "Caught up"}</dd></div></dl><div className="resource-footer"><Clock3 size={14} />{title(stream.watermark_mode)} watermark</div></Card>)}</div>{streams.length === 0 && <Card><Empty icon={Database} title="No streams" detail="Create one by running an example from the CLI." /></Card>}</>;
}

function OperatorsPage({ operators, openRun }: { operators: Operator[]; openRun: (id: string) => void }) {
  return <><PageHeader eyebrow="Data plane" title="Operators" description="Durable event-time transformations deployed to this environment." /><Card className="resource-panel"><div className="operator-list">{operators.map((operator) => <article className="operator-row" key={operator.operator_id}><span className="large-resource-icon violet"><GitBranch size={19} /></span><div className="operator-identity"><strong>{operator.operator_id}</strong><span><Badge status={operator.status}>{title(operator.status)}</Badge><small>{title(operator.kind)}{operator.join_type ? ` · ${operator.join_type}` : ""}</small></span></div><div className="operator-flow"><div>{operator.input.map((input, index) => <code key={input}>{input}{index === 0 && operator.probe_watermark != null ? ` · wm ${operator.probe_watermark.toFixed(1)}` : ""}{index === 1 && operator.version_watermark != null ? ` · wm ${operator.version_watermark.toFixed(1)}` : ""}</code>)}</div><ArrowRight size={16} /><code>{operator.workflow_type}</code></div><dl><div><dt>Matched</dt><dd>{operator.matched?.toLocaleString() ?? "—"}</dd></div><div><dt>Emitted</dt><dd>{operator.emitted?.toLocaleString() ?? "—"}</dd></div></dl>{operator.latest_workflow_id && <Button variant="secondary" onClick={() => openRun(operator.latest_workflow_id!)}>Trace <ArrowRight size={13} /></Button>}</article>)}</div>{operators.length === 0 && <Empty icon={GitBranch} title="No operators" detail="Deploy an example to see its streaming topology." />}</Card></>;
}

function ProcessesPage({ processes, openProcess }: { processes: Process[]; openProcess: (id: string) => void }) {
  const ordered = [...processes].sort((left, right) => right.completed - left.completed);
  return <><PageHeader eyebrow="Isolation" title="Processes" description="Continuously running durable programs with per-key concurrency and backpressure." /><div className="resource-card-grid">{ordered.map((process) => { const retrying = process.retrying ?? 0; const used = process.pending + process.running + retrying; return <Card className="resource-card process-card" key={process.process_id}><div className="resource-card-head"><span className="large-resource-icon green"><Layers3 size={20} /></span><Badge status={process.status}>{process.status === "ACTIVE" ? "Streaming" : title(process.status)}</Badge></div><h3>{process.process_id}</h3><p className="resource-description">{process.workflow_type}</p><div className="queue-bar"><span style={{ width: `${Math.min(100, (used / Math.max(1, process.mailbox_capacity)) * 100)}%` }} /></div><div className="queue-label"><span>Mailbox utilization</span><b>{used} / {process.mailbox_capacity}</b></div><dl><div><dt>Pending</dt><dd>{process.pending}</dd></div><div><dt>In flight</dt><dd>{process.running}</dd></div><div><dt>Retrying</dt><dd>{retrying}</dd></div><div><dt>Quarantined</dt><dd>{process.quarantined ?? 0}</dd></div><div><dt>Completed</dt><dd>{process.completed}</dd></div><div><dt>Failed</dt><dd>{process.failed}</dd></div></dl><Button variant="secondary" onClick={() => openProcess(process.process_id)}>View job <ArrowRight size={13} /></Button><div className="resource-footer"><Network size={14} />Continuously polling · {process.max_concurrent_keys} normal · {process.retry_concurrency ?? 0} retry slots</div></Card>; })}</div>{processes.length === 0 && <Card><Empty icon={Layers3} title="No keyed processes" detail="Run the account balance example to deploy one." /></Card>}</>;
}

function ProcessDetailPage({ process, onBack }: { process?: Process; onBack: () => void }) {
  if (!process) return <><Button variant="ghost" onClick={onBack}><ArrowLeft size={15} />Back to processes</Button><Card><div className="sheet-error"><TriangleAlert size={18} />Streaming job not found.</div></Card></>;
  const retrying = process.retrying ?? 0;
  const active = process.status === "ACTIVE";
  return <div className="run-detail-page"><button className="back-link" onClick={onBack}><ArrowLeft size={14} />Processes</button><PageHeader eyebrow="Streaming job" title={process.process_id} description={process.workflow_type} actions={<Badge status={process.status}>{active ? "Streaming" : title(process.status)}</Badge>} /><div className="run-summary routed"><div><span>Status</span><strong>{active ? "Continuously running" : title(process.status)}</strong></div><div><span>Completed events</span><strong>{process.completed.toLocaleString()}</strong></div><div><span>In flight</span><strong>{process.running.toLocaleString()}</strong></div><div><span>Pending</span><strong>{process.pending.toLocaleString()}</strong></div><div><span>Failed</span><strong>{process.failed.toLocaleString()}</strong></div></div><Card className="durability-card"><SectionHeading title="Execution controls" detail="Per-key isolation, retry capacity, and durable mailbox limits" /><div className="durability-grid"><div><span className="resource-icon green"><Network size={17} /></span><p>Concurrency</p><strong>{process.max_concurrent_keys.toLocaleString()} keys</strong><small>{process.retry_concurrency.toLocaleString()} isolated retry slots</small></div><div><span className="resource-icon blue"><Database size={17} /></span><p>Durable mailbox</p><strong>{(process.pending + process.running + retrying).toLocaleString()} / {process.mailbox_capacity.toLocaleString()}</strong><small>{process.quarantined.toLocaleString()} quarantined</small></div><div><span className="resource-icon violet"><RefreshCw size={17} /></span><p>Retry policy</p><strong>{process.max_attempts} attempts</strong><small>{retrying.toLocaleString()} retrying now</small></div></div></Card></div>;
}

function QuickstartPage() {
  return <><PageHeader eyebrow="Get started" title="Run an example" description="Submit real work through the CLI, then monitor it here." /><div className="quickstart-grid"><Card className="quickstart-main"><div className="step"><span>1</span><div><h3>Install the CLI</h3><p>Install Highwater with Homebrew.</p><CommandBox command="brew install henneberger/tap/highwater" /></div></div><div className="step"><span>2</span><div><h3>Connect your workspace</h3><p>Use the endpoint and scoped API key issued for your workspace.</p><CommandBox command="export HIGHWATER_ADDRESS=https://api.highwater.cloud" /><CommandBox command="export HIGHWATER_API_KEY=..." /></div></div><div className="step"><span>3</span><div><h3>Submit an example</h3><p>Pick a workload. The CLI returns its resource and run identifiers for direct inspection.</p><div className="example-list">{commandExamples.map((example) => <div key={example.command}><span className="example-icon"><Command size={16} /></span><div><strong>{example.label}</strong><small>{example.detail}</small><CommandBox command={example.command} /></div></div>)}</div></div></div></Card><aside><Card className="help-card"><Cloud size={20} /><h3>Public sandbox</h3><p>This console is read-only. Everything shown here was created by submitting the same examples through the ordinary CLI.</p></Card><Card className="help-card"><Github size={20} /><h3>See how it works</h3><p>Inspect the SDK, execution engine, and examples in the public repository.</p><a href="https://github.com/henneberger/highwater">Open GitHub <ExternalLink size={13} /></a></Card><Card className="help-card"><ShieldCheck size={20} /><h3>Start an isolated pilot</h3><p>Get a dedicated workspace and scoped credentials for your team.</p><a href="https://calendly.com/henneberger-daniel/30min">Talk to Highwater <ExternalLink size={13} /></a></Card></aside></div></>;
}

function CommandBox({ command }: { command: string }) {
  async function copy() { await navigator.clipboard.writeText(command); toast.success("Command copied"); }
  return <div className="command-box"><code><span>$</span> {command}</code><button onClick={copy} aria-label="Copy command"><Copy size={14} /></button></div>;
}

function Empty({ icon: Icon, title: heading, detail }: { icon: typeof Workflow; title: string; detail: string }) {
  return <div className="empty"><span><Icon size={21} /></span><strong>{heading}</strong><p>{detail}</p></div>;
}

function RunDetailPage({ id, credential, onBack }: { id: string; credential: string; onBack: () => void }) {
  const [detail, setDetail] = useState<WorkflowDetail>(); const [error, setError] = useState("");
  useEffect(() => { setDetail(undefined); setError(""); getWorkflow(credential, id).then(setDetail).catch((failure) => setError(failure instanceof Error ? failure.message : "Unable to load run")); }, [credential, id]);
  if (!detail && !error) return <div className="page-loader"><RefreshCw className="spin" size={20} />Loading run history…</div>;
  if (error) return <><Button variant="ghost" onClick={onBack}><ArrowLeft size={15} />Back to runs</Button><Card><div className="sheet-error"><TriangleAlert size={18} />{error}</div></Card></>;
  const workflow = detail!.workflow;
  const started = detail!.history.find((event) => (event.type || event.event_type) === "WORKFLOW_STARTED");
  const inputs = started?.data.args ?? [];
  return <div className="run-detail-page">
    <button className="back-link" onClick={onBack}><ArrowLeft size={14} />Runs</button>
    <PageHeader eyebrow="Run detail" title={workflow.workflow_type} description={workflow.workflow_id} actions={<Badge status={workflow.status}>{title(workflow.status.toLowerCase())}</Badge>} />
    <div className="run-summary routed"><div><span>Status</span><Badge status={workflow.status}>{title(workflow.status.toLowerCase())}</Badge></div><div><span>Duration</span><strong>{duration(workflow.duration_seconds)}</strong></div><div><span>Retries</span><strong>{workflow.retries}</strong></div><div><span>History events</span><strong>{workflow.history_events}</strong></div><div><span>Task queue</span><strong>{workflow.task_queue || "default"}</strong></div></div>
    <div className="io-grid"><JsonPanel label="Input" value={inputs} /><JsonPanel label="Result" value={workflow.result} /></div>
    <Tabs.Root defaultValue="timeline" className="history-workbench">
      <div className="history-heading"><div><h2>Event history</h2><p>Replay-safe execution and streaming decisions in order.</p></div><Tabs.List><Tabs.Trigger value="compact">Compact</Tabs.Trigger><Tabs.Trigger value="timeline">Timeline</Tabs.Trigger><Tabs.Trigger value="full">Full history</Tabs.Trigger>{detail!.trace && <Tabs.Trigger value="trace">Streaming trace</Tabs.Trigger>}</Tabs.List></div>
      <Tabs.Content value="compact"><CompactHistory detail={detail!} /></Tabs.Content>
      <Tabs.Content value="timeline"><ExecutionTimeline detail={detail!} /></Tabs.Content>
      <Tabs.Content value="full"><FullHistory history={detail!.history} /></Tabs.Content>
      {detail!.trace && <Tabs.Content value="trace"><ExecutionTraceView trace={detail!.trace} workflow={workflow} /></Tabs.Content>}
    </Tabs.Root>
  </div>;
}

function JsonPanel({ label, value }: { label: string; value: unknown }) {
  async function copy() { await navigator.clipboard.writeText(JSON.stringify(value, null, 2)); toast.success(`${label} copied`); }
  return <Card className="json-panel"><div><span>{label}</span><button onClick={copy} aria-label={`Copy ${label.toLowerCase()}`}><Copy size={14} /></button></div><pre>{JSON.stringify(value ?? null, null, 2)}</pre></Card>;
}

function CompactHistory({ detail }: { detail: WorkflowDetail }) {
  const retries = detail.history.filter((event) => (event.type || event.event_type) === "ACTIVITY_RETRY_SCHEDULED");
  const activities = detail.history.filter((event) => (event.type || event.event_type) === "ACTIVITY_COMPLETED");
  return <div className="compact-history"><article><CheckCircle2 size={17} /><div><strong>Workflow started</strong><p>{detail.workflow.workflow_type} entered durable execution.</p></div><time>{relativeTime(detail.workflow.created_at)}</time></article>{detail.trace && <article><Clock3 size={17} /><div><strong>Event-time gate released</strong><p>Watermark {detail.trace.gate.release_watermark?.toFixed(1) ?? "final"} passed as-of {detail.trace.gate.as_of.toFixed(1)}.</p></div></article>}{retries.map((event, index) => <article className="retry" key={event.event_id ?? index}><RefreshCw size={17} /><div><strong>Activity recovered automatically</strong><p>Attempt {String(event.data.failed_attempt)} failed; attempt {String(event.data.next_attempt)} was scheduled.</p></div><time>{relativeTime(event.created_at)}</time></article>)}{activities.map((event, index) => <article key={event.event_id ?? index}><CheckCircle2 size={17} /><div><strong>Activity completed</strong><p>Command {String(event.data.command_id)} committed its result.</p></div><time>{relativeTime(event.created_at)}</time></article>)}<article><ShieldCheck size={17} /><div><strong>Workflow {detail.workflow.status.toLowerCase()}</strong><p>Result and event history are durably retained.</p></div><time>{relativeTime(detail.workflow.updated_at)}</time></article></div>;
}

function FullHistory({ history }: { history: WorkflowDetail["history"] }) {
  return <div className="event-history full-page-history">{history.map((event, index) => <article key={event.event_id ?? index}><div className="event-rail"><span>{index + 1}</span></div><div className="event-content"><div><strong>{title(event.type || event.event_type)}</strong><time>{new Date(event.created_at * 1000).toLocaleString()}</time></div>{Object.keys(event.data || {}).length > 0 && <details><summary>Event payload</summary><pre>{JSON.stringify(event.data, null, 2)}</pre></details>}</div></article>)}</div>;
}

type TimelineLane = { label: string; kind: string; commandId?: unknown; start: number; end: number; markers: { at: number; kind: string }[] };

function timelineLanes(detail: WorkflowDetail): TimelineLane[] {
  const events = detail.history;
  const first = events[0]?.created_at ?? detail.workflow.created_at;
  const last = events.at(-1)?.created_at ?? detail.workflow.updated_at;
  const lanes: TimelineLane[] = [{ label: detail.workflow.workflow_type, kind: "workflow", start: first, end: last, markers: [] }];
  for (const event of events) {
    const type = event.type || event.event_type;
    const command = event.data.command_id;
    if (type === "ACTIVITY_SCHEDULED") lanes.push({ label: String(event.data.name || `Activity ${command}`), kind: "activity", commandId: command, start: event.created_at, end: last, markers: [] });
    const lane = lanes.find((candidate) => candidate.kind === "activity" && candidate.commandId === command);
    if (type === "ACTIVITY_RETRY_SCHEDULED" && lane) lane.markers.push({ at: event.created_at, kind: "retry" });
    if ((type === "ACTIVITY_COMPLETED" || type === "ACTIVITY_FAILED") && lane) { lane.end = event.created_at; lane.markers.push({ at: event.created_at, kind: type === "ACTIVITY_FAILED" ? "failed" : "completed" }); }
    if (type === "SIGNAL_RECEIVED") lanes.push({ label: String(event.data.name || "Signal"), kind: "signal", start: event.created_at, end: event.created_at, markers: [{ at: event.created_at, kind: "signal" }] });
  }
  return lanes;
}

function ExecutionTimeline({ detail }: { detail: WorkflowDetail }) {
  const lanes = timelineLanes(detail);
  return <div className="timeline-chart"><TimelineSvg lanes={lanes} /></div>;
}

function TimelineSvg({ lanes }: { lanes: TimelineLane[] }) {
  const width = 1100; const labelWidth = 190; const top = 32; const rowHeight = 58; const height = top + lanes.length * rowHeight + 42;
  const minimum = Math.min(...lanes.map((lane) => lane.start)); const maximum = Math.max(...lanes.map((lane) => lane.end));
  const span = Math.max(.001, maximum - minimum); const x = (value: number) => labelWidth + value / span * (width - labelWidth - 24);
  const ticks = Array.from({ length: 7 }, (_, index) => span * index / 6);
  return <svg viewBox={`0 0 ${width} ${height}`} width={width} height={height} role="img" aria-label="Execution event timeline"><rect width={width} height={height} fill="#11151b" rx={8} /><g transform={`translate(0 ${top})`}>{ticks.map((tick) => <g key={tick}><line x1={x(tick)} x2={x(tick)} y1={0} y2={lanes.length * rowHeight} stroke="#293140" strokeDasharray="3 5" /><text x={x(tick)} y={lanes.length * rowHeight + 24} fill="#929cad" fontSize={9} textAnchor="middle" fontFamily="IBM Plex Mono">{duration(tick)}</text></g>)}{lanes.map((lane, index) => { const y = index * rowHeight + 18; const start = x(lane.start - minimum); const end = x(lane.end - minimum); return <g key={`${lane.kind}-${index}`}><text x={16} y={y + 5} fill="#d8dee8" fontSize={11} fontFamily="IBM Plex Mono">{lane.label.length > 24 ? `${lane.label.slice(0, 24)}…` : lane.label}</text><rect x={start} y={y - (lane.kind === "workflow" ? 5 : 4)} width={Math.max(3, end - start)} height={lane.kind === "workflow" ? 10 : 8} rx={4} fill={lane.kind === "workflow" ? "#4e7cf0" : "#3fab6b"} />{lane.markers.map((marker, markerIndex) => <circle key={markerIndex} cx={x(marker.at - minimum)} cy={y} r={marker.kind === "retry" ? 7 : 6} fill={marker.kind === "retry" ? "#d49331" : marker.kind === "failed" ? "#d85850" : "#6de39a"} stroke="#11151b" strokeWidth={3} />)}</g>; })}</g></svg>;
}

function ExecutionTraceView({ trace, workflow }: { trace: NonNullable<WorkflowDetail["trace"]>; workflow: WorkflowType }) {
  const steps = [
    { icon: Database, label: "Source record", title: trace.source.stream, detail: `partition ${trace.source.partition} · offset ${trace.source.offset}`, meta: `event time ${trace.source.event_time.toFixed(1)}` },
    { icon: Clock3, label: "Event-time gate", title: `Watermark ${trace.gate.release_watermark?.toFixed(1) ?? "final"}`, detail: `released as-of ${trace.gate.as_of.toFixed(1)}`, meta: trace.source.late ? "late record" : "on time" },
    { icon: GitBranch, label: "Temporal join", title: trace.operator.operator_id, detail: trace.operator.matched ? "version matched" : "left join without match", meta: trace.version ? `effective at ${trace.version.event_time.toFixed(1)}` : "no version" },
    { icon: Workflow, label: "Durable execution", title: workflow.workflow_type, detail: `${workflow.retries} retries · ${workflow.history_events} history events`, meta: workflow.status.toLowerCase() },
    { icon: CheckCircle2, label: "Result", title: workflow.status === "COMPLETED" ? "Committed" : title(workflow.status), detail: `completed in ${duration(workflow.duration_seconds)}`, meta: "durable history retained" },
  ];
  return <div className="trace-view"><div className="trace-explainer"><ShieldCheck size={17} /><div><strong>One causal path across streaming and execution</strong><p>The record was held by event time, enriched with the version valid at that instant, and handed to a replay-safe workflow.</p></div></div><div className="trace-steps">{steps.map(({ icon: Icon, label, title: heading, detail, meta }, index) => <article key={label}><div className="trace-marker"><Icon size={16} />{index < steps.length - 1 && <span />}</div><div><small>{label}</small><strong>{heading}</strong><p>{detail}</p><code>{meta}</code></div></article>)}</div>{trace.version && <details className="trace-payload"><summary>Matched version payload</summary><pre>{JSON.stringify(trace.version.value, null, 2)}</pre></details>}</div>;
}

export default function App() {
  const location = useLocation();
  const navigate = useNavigate();
  const page = pageFromPath(location.pathname);
  const selectedRun = location.pathname === "/runs/detail"
    ? new URLSearchParams(location.search).get("run") || undefined
    : undefined;
  const selectedProcess = location.pathname === "/processes/detail"
    ? new URLSearchParams(location.search).get("process") || undefined
    : undefined;
  const [credential, setCredential] = useState(() => sessionStorage.getItem("highwater_demo_login") || "");
  const [data, setData] = useState<Overview>(); const [error, setError] = useState("");
  const [refreshing, setRefreshing] = useState(false); const [trend, setTrend] = useState<TrendPoint[]>([]); const [sidebarOpen, setSidebarOpen] = useState(false);
  const setPage = useCallback((next: Page) => navigate(paths[next]), [navigate]);
  const openRun = useCallback((id: string) => navigate(`/runs/detail?run=${encodeURIComponent(id)}`, {
    state: { from: `${location.pathname}${location.search}` },
  }), [location.pathname, location.search, navigate]);
  const closeRun = useCallback(() => {
    const from = (location.state as { from?: string } | null)?.from;
    navigate(from || paths.runs, { replace: true });
  }, [location.state, navigate]);
  const openProcess = useCallback((id: string) => navigate(`/processes/detail?process=${encodeURIComponent(id)}`, {
    state: { from: `${location.pathname}${location.search}` },
  }), [location.pathname, location.search, navigate]);
  const closeProcess = useCallback(() => {
    const from = (location.state as { from?: string } | null)?.from;
    navigate(from || paths.processes, { replace: true });
  }, [location.state, navigate]);
  const load = useCallback(async (login = credential) => {
    if (!login) return;
    setRefreshing(true);
    try {
      const next = await getOverview(login);
      setError("");
      const total = next.streams.reduce((sum, stream) => sum + stream.records, 0);
      setTrend((points) => {
        const recent = points.filter((point) => next.generated_at - point.generatedAt <= 60);
        const anchor = recent[0];
        const elapsed = anchor ? Math.max(1, next.generated_at - anchor.generatedAt) : 1;
        const rate = anchor ? Math.max(0, (total - anchor.total) / elapsed) : 0;
        return [...recent, { time: new Date(next.generated_at * 1000).toLocaleTimeString([], { minute: "2-digit", second: "2-digit" }), generatedAt: next.generated_at, rate, total }].slice(-18);
      });
      setData(next);
      return next;
    } catch (failure) {
      if (failure instanceof ApiError && failure.status === 401) { sessionStorage.removeItem("highwater_demo_login"); setCredential(""); setData(undefined); }
      else setError(failure instanceof Error ? failure.message : "Console unavailable");
      throw failure;
    } finally { setRefreshing(false); }
  }, [credential]);
  useEffect(() => { if (!credential) return; load().catch(() => undefined); const timer = window.setInterval(() => load().catch(() => undefined), 5000); return () => window.clearInterval(timer); }, [credential, load]);
  useEffect(() => {
    if (location.pathname === "/" || (!Object.values(paths).includes(location.pathname) && location.pathname !== "/runs/detail" && location.pathname !== "/processes/detail")) {
      navigate(paths.overview, { replace: true });
    }
  }, [location.pathname, navigate]);
  async function login(next: string) { const result = await getOverview(next); sessionStorage.setItem("highwater_demo_login", next); setCredential(next); setData(result); setError(""); }
  function logout() { sessionStorage.removeItem("highwater_demo_login"); setCredential(""); setData(undefined); }
  if (!credential) return <Login onLogin={login} />;
  const rate = trend.at(-1)?.rate ?? 0;
  return <Shell page={page} setPage={setPage} onLogout={logout} online={!error} sidebarOpen={sidebarOpen} setSidebarOpen={setSidebarOpen}>
    {error && <div className="connection-banner"><TriangleAlert size={15} /><span>{error}</span><button onClick={() => load().catch(() => undefined)}>Retry</button></div>}
    {!data ? <div className="page-loader"><RefreshCw className="spin" />Connecting to the demo cluster…</div> : <>
      <div className="live-refresh"><span className={!error ? "online" : "offline"} />{error ? "Disconnected" : `Live · updated ${relativeTime(data.generated_at)}`}<button onClick={() => load().catch(() => undefined)} disabled={refreshing}><RefreshCw className={refreshing ? "spin" : ""} size={13} />Refresh</button></div>
      {page === "overview" && <OverviewPage data={data} trend={trend} rate={rate} openRun={openRun} openProcess={openProcess} setPage={setPage} />}
      {page === "runs" && selectedRun && <RunDetailPage id={selectedRun} credential={credential} onBack={closeRun} />}
      {page === "runs" && !selectedRun && <RunsPage data={data} openRun={openRun} openProcess={openProcess} />}
      {page === "streams" && <StreamsPage streams={data.streams} />}
      {page === "operators" && <OperatorsPage operators={data.operators} openRun={openRun} />}
      {page === "processes" && selectedProcess && <ProcessDetailPage process={data.processes.find((process) => process.process_id === selectedProcess)} onBack={closeProcess} />}
      {page === "processes" && !selectedProcess && <ProcessesPage processes={data.processes} openProcess={openProcess} />}
      {page === "quickstart" && <QuickstartPage />}
    </>}
  </Shell>;
}
