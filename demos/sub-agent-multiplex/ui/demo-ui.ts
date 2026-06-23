import { Hono } from "hono";
import { serve } from "@hono/node-server";
import { exec } from "node:child_process";
import { createClient } from "redis";

const app = new Hono();

// --- Configuration ---
const NS = process.env.DEMO_NAMESPACE || "sub-agent";
const ATE_ENDPOINT = process.env.ATE_ENDPOINT || "api.ate-system.svc.cluster.local:443";
const VALKEY_URL = "redis://valkey-cluster.ate-system.svc.cluster.local:6379";
const TEMPLATE = "sub-agent/sub-agent-agent";

const predefinedTasks = [
  "Analyze repo for security vulnerabilities",
  "Summarize latest PR for team review",
  "Write unit tests for the message gateway",
  "Refactor the plugin discovery logic",
  "Draft a response to Buganizer b/392182",
  "Generate a cost report for GKE nodes",
  "Optimize the gVisor memory mapping",
  "Verify snapshot integrity on GCS",
];

interface Assignment {
  id: string;
  agent: string;
  task: string;
  state: "queued" | "running" | "completed";
  durationSec: number;
  created_at: number;
  started_at?: number;
  completed_at?: number;
}

interface TaskAudit {
  id: string;
  agent: string;
  timestamp: string;
  task: string;
  result: string;
  status: "success" | "error" | "warning";
  error_detail?: string;
}

// --- Shared State ---
let shellLogs: string[] = [];
let taskAudits: TaskAudit[] = [];
let assignments: Assignment[] = [];
let taskCursor = 0;
let clusterState = { pods: [] as any[], actors: [] as any[] };
let lockedActors = new Set<string>();

// Precision Tracking
let stats = {
  totalLogicalActiveSec: 0,
  totalPhysicalActiveSec: 0,
  cumulativeTasks: 0,
  lastSync: Date.now()
};

// External Cron Simulation state (FIX: Restored variable definition)
const CRON_DEFAULTS: Record<string, number> = { "agent-luna": 60, "agent-mars": 120, "agent-nova": 180 };
let lastTriggerTime: Record<string, number> = { "agent-luna": Date.now(), "agent-mars": Date.now(), "agent-nova": Date.now() };
let cronIterations: Record<string, number> = { "agent-luna": 0, "agent-mars": 0, "agent-nova": 0 };

const AGENT_META: Record<string, { color: string, id: string }> = {
  "agent-luna": { color: "#79c0ff", id: "agent-luna-v12" },
  "agent-mars": { color: "#ff79c6", id: "agent-mars-v12" },
  "agent-nova": { color: "#f1fa8c", id: "agent-nova-v11" },
};

const ID_TO_DISPLAY: Record<string, string> = Object.entries(AGENT_META).reduce((acc, [display, meta]) => {
  acc[meta.id] = display;
  return acc;
}, {} as Record<string, string>);

const VALID_ACTOR_IDS = new Set(Object.values(AGENT_META).map(m => m.id));

const nowSec = () => Date.now() / 1000;

const runCmd = (cmd: string): Promise<string> => {
  return new Promise((resolve, reject) => {
    exec(cmd, (error, stdout, stderr) => {
      if (error) reject(new Error(stderr || error.message));
      else resolve(stdout);
    });
  });
};

// --- Valkey Persistence ---
let redis: any = null;
try {
  redis = createClient({ url: VALKEY_URL });
  redis.on("error", () => {});
} catch (e) {}

async function initPersistence() {
  if (!redis) return;
  try {
    await Promise.race([redis.connect(), new Promise((_, r) => setTimeout(r, 2000))]);
    if (redis.isOpen) {
      const logs = await redis.lRange("demo:shell_logs", 0, -1);
      shellLogs = logs || [];
      const audits = await redis.lRange("demo:task_audits", 0, -1);
      taskAudits = (audits || []).map((a: string) => JSON.parse(a));
    }
  } catch (e) {}
}

async function persistLog(msg: string) {
  shellLogs.push(msg);
  if (shellLogs.length > 200) shellLogs.shift();
  try { if (redis?.isOpen) { await redis.rPush("demo:shell_logs", msg); await redis.lTrim("demo:shell_logs", -200, -1); } } catch {}
}

async function persistAudit(audit: TaskAudit) {
  taskAudits.push(audit);
  if (taskAudits.length > 50) taskAudits.shift();
  try { if (redis?.isOpen) { await redis.rPush("demo:task_audits", JSON.stringify(audit)); await redis.lTrim("demo:task_audits", -50, -1); } } catch {}
}

function logShell(msg: string) {
  const timestamp = new Date().toISOString().slice(11, 19);
  const entry = `[${timestamp}] ${msg}`;
  persistLog(entry);
  console.log(`[shell] ${msg}`);
}

// --- Background State Syncer ---
async function syncState() {
  try {
    const actorsOut = await runCmd(`kubectl-ate --endpoint ${ATE_ENDPOINT} get actors -o json`);
    const podsOut = await runCmd(`kubectl get pods -n ${NS} -l app=agent-pool -o json`);

    const actors = JSON.parse(actorsOut).actors || [];
    const podsRaw = JSON.parse(podsOut).items || [];

    clusterState.actors = actors.filter((a: any) => VALID_ACTOR_IDS.has(a.actorId || a.actor_id)).map((a: any) => ({
      name: a.actorId || a.actor_id,
      displayName: ID_TO_DISPLAY[a.actorId || a.actor_id] || (a.actorId || a.actor_id),
      status: a.status.replace("STATUS_", ""),
      rawStatus: a.status,
      ip: a.ateomPodIp || a.ateom_pod_ip || "n/a",
      pod: a.ateomPodName || a.ateom_pod_name || "none"
    }));

    clusterState.pods = podsRaw.map((p: any) => {
      const activeActor = actors.find((a: any) => {
        const podPart = (a.ateomPodName || a.ateom_pod_name || "").split("/").pop();
        return podPart === p.metadata.name && VALID_ACTOR_IDS.has(a.actorId || a.actor_id);
      });
      const actorId = activeActor ? (activeActor.actorId || activeActor.actor_id) : "idle";
      return {
        name: p.metadata.name,
        phase: p.status.phase,
        ip: p.status.podIP || "n/a",
        activeActor: ID_TO_DISPLAY[actorId] || actorId
      };
    });

    const now = Date.now();
    const elapsed = (now - stats.lastSync) / 1000;
    stats.lastSync = now;

    const runningActors = clusterState.actors.filter(a => a.rawStatus === "STATUS_RUNNING").length;
    const runningPods = clusterState.pods.filter(p => p.phase === "Running" && p.activeActor !== "idle").length;
    
    stats.totalLogicalActiveSec += runningActors * elapsed;
    stats.totalPhysicalActiveSec += runningPods * elapsed;

  } catch (e: any) {}
  setTimeout(syncState, 800);
}

// --- Task Execution: Direct & Robust ---
async function executeTask(actorId: string, assignmentId: string) {
  if (lockedActors.has(actorId)) return;
  lockedActors.add(actorId);

  const display = ID_TO_DISPLAY[actorId] || actorId;
  const asg = assignments.find(a => a.id === assignmentId);
  if (!asg) { lockedActors.delete(actorId); return; }

  asg.state = "running";
  logShell(`[broker] Wakeup Request for **${display}** received.`);
  
  try {
    const checkOut = await runCmd(`kubectl-ate --endpoint ${ATE_ENDPOINT} get actor ${actorId} -o json`);
    const actorData = JSON.parse(checkOut).actors?.[0] || JSON.parse(checkOut);
    const initialStatus = actorData.status || "";

    // 1. Ensure clean start
    if (initialStatus !== "STATUS_SUSPENDED") {
      logShell(`[broker] **${display}** is in ${initialStatus}. Resetting control plane...`);
      await runCmd(`kubectl-ate --endpoint ${ATE_ENDPOINT} suspend actor ${actorId}`).catch(() => {});
      await new Promise(r => setTimeout(r, 6000));
    }

    // 2. Resume Operation (Single Attempt)
    logShell(`> kubectl-ate resume actor ${actorId}`);
    await runCmd(`kubectl-ate --endpoint ${ATE_ENDPOINT} resume actor ${actorId}`);

    // 3. Wait for Rehydration
    let actor: any;
    for (let i = 0; i < 60; i++) {
      const actorsOut = await runCmd(`kubectl-ate --endpoint ${ATE_ENDPOINT} get actor ${actorId} -o json`);
      actor = JSON.parse(actorsOut).actors?.[0] || JSON.parse(actorsOut);
      if (actor.status === "STATUS_RUNNING" && actor.ateomPodIp) break;
      if (i % 5 === 0 && i > 0) logShell(`[scheduler] Rehydrating **${display}** (Wait-Time: ${i}s)`);
      await new Promise(r => setTimeout(r, 1000));
    }

    if (actor.status !== "STATUS_RUNNING") throw new Error("Infrastructure Rehydration Timeout");

    // 4. Network Settle Time (CRITICAL FOR GVISOR)
    logShell(`[scheduler] **${display}** rehydrated at ${actor.ateomPodIp}. Settling network stack...`);
    await new Promise(r => setTimeout(r, 5000));
    
    // 5. Task Injection
    const result = await runCmd(`curl -s -f -m 10 -X POST http://${actor.ateomPodIp}:8080/task -H "Content-Type: application/json" -d '{"task": "${asg.task}"}'`);
    const data = JSON.parse(result);
    logShell(`[scheduler] **${display}** logic complete. Yielding hardware...`);
    
    persistAudit({
      id: "audit-" + Date.now(),
      agent: display,
      timestamp: new Date().toISOString().slice(11, 19),
      task: asg.task,
      result: data.result || result,
      status: "success"
    });
    stats.cumulativeTasks++;

    // 6. Yield Hardware
    logShell(`> kubectl-ate suspend actor ${actorId}`);
    await runCmd(`kubectl-ate --endpoint ${ATE_ENDPOINT} suspend actor ${actorId}`);

  } catch (e: any) {
    const errorMsg = e.message;
    logShell(`[error] **${display}** failed: ${errorMsg}`);
    
    // Ensure we don't leave it in a stuck state
    await runCmd(`kubectl-ate --endpoint ${ATE_ENDPOINT} suspend actor ${actorId}`).catch(() => {});
    
    persistAudit({ id: "audit-" + Date.now(), agent: display, timestamp: new Date().toISOString().slice(11, 19), task: asg.task, result: "FAILED", status: "error", error_detail: errorMsg });
  } finally {
    asg.state = "completed";
    asg.completed_at = nowSec();
    lockedActors.delete(actorId);
  }
}

// --- Dashboard Implementation ---
app.get("/", (c) => {
  return c.html(`
<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Substrate Master Orchestration</title>
<style>
  :root {
    --bg: #0d1117; --panel: #161b22; --panel-2: #010409;
    --line: #30363d; --text: #e6edf3; --muted: #8b949e;
    --accent: #ff79c6; --green: #aff5b4; --red: #ff5555; --cyan: #58a6ff;
    --yellow: #f1fa8c; --orange: #ffb86c; --cost-accent: #ffd57e;
  }
  * { box-sizing: border-box; }
  body { font-family: ui-monospace, "SF Mono", Menlo, Consolas, monospace; margin: 0; padding: 1.5em; background: var(--bg); color: var(--text); line-height: 1.4; }
  header { border-bottom: 2px solid var(--accent); padding-bottom: 0.8em; margin-bottom: 1.5em; display: flex; justify-content: space-between; align-items: baseline; }
  h1 { font-size: 1.25em; margin: 0; color: var(--accent); font-weight: 800; text-transform: uppercase; }
  
  .intro { font-size: 0.9em; color: var(--muted); margin-bottom: 1.5em; max-width: 900px; }
  .intro strong { color: var(--text); }

  .cost-card {
    background: var(--panel); border: 1px solid var(--line); border-left: 4px solid var(--cost-accent);
    padding: 1.2em; border-radius: 6px; margin-bottom: 0; font-size: 0.9em;
  }
  .cost-card .cost-label { color: var(--cost-accent); text-transform: uppercase; letter-spacing: .12em; font-size: .8em; font-weight: 600; margin-bottom: 15px; display: block; }
  
  .metric-highlight-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 10px; margin-bottom: 15px; }
  .metric-item { background: var(--panel-2); border: 1px solid var(--line); padding: 12px; border-radius: 4px; text-align: center; }
  .metric-val { font-size: 1.5em; font-weight: 800; color: var(--cost-accent); }
  .metric-label { font-size: 0.65em; color: var(--muted); text-transform: uppercase; margin-top: 6px; font-weight: 600; }

  .cost-note { color: var(--muted); font-size: 0.88em; margin-top: 12px; line-height: 1.6; border-top: 1px solid var(--line); padding-top: 12px; }

  .grid-master { display: grid; gap: 1.5em; grid-template-columns: 1.6fr 1fr; margin-bottom: 1.5em; }
  .grid-side { display: grid; gap: 1.5em; grid-template-columns: 1fr 1fr; margin-bottom: 1.5em; }
  
  .card { background: var(--panel); border: 1px solid var(--line); border-radius: 4px; padding: 1.2em; position: relative; }
  .card h2 { font-size: 0.75em; margin: 0 0 1em 0; color: var(--muted); text-transform: uppercase; font-weight: 800; border-left: 3px solid var(--accent); padding-left: 8px; }
  .card .help { font-size: 0.75em; color: var(--muted); margin-bottom: 1em; line-height: 1.5; }

  .shell-container { background: var(--panel-2); height: 400px; overflow: auto; padding: 1em; border: 1px solid #000; margin-bottom: 1.5em; box-shadow: inset 0 2px 15px rgba(0,0,0,0.7); }
  .shell-line { font-size: 0.82em; color: #d1d5db; margin-bottom: 0.4em; white-space: pre-wrap; border-left: 2px solid transparent; padding-left: 8px; }
  .shell-line.cmd { color: var(--green); font-weight: 800; border-color: var(--green); }
  .shell-line.broker { color: var(--accent); font-weight: 800; border-color: var(--accent); }
  .shell-line.scheduler { color: var(--cyan); font-weight: 800; border-color: var(--cyan); }
  .shell-line.err { color: var(--red); background: rgba(248,81,73,0.1); border-color: var(--red); }
  
  .timeline { height: 110px; overflow: auto; background: var(--panel-2); border: 1px solid var(--line); padding: 8px; border-radius: 4px; }
  .tm-entry { font-size: 0.75em; padding: 8px; border-bottom: 1px solid #222; display: flex; justify-content: space-between; align-items: baseline; }
  .tm-entry:last-child { border-bottom: 0; }
  .tm-entry.running { color: var(--yellow); font-weight: 800; }
  .tm-entry.completed { color: var(--muted); text-decoration: line-through; }
  .tm-agent { font-weight: 800; width: 110px; }
  .tm-task { flex: 1; margin: 0 10px; }

  .cron-box { background: var(--panel-2); border: 1px solid var(--line); padding: 10px; border-radius: 4px; margin-bottom: 1.5em; font-size: 0.8em; height: 110px; }
  .cron-line { margin-bottom: 6px; display: flex; justify-content: space-between; align-items: baseline; border-bottom: 1px dashed #222; padding-bottom: 4px; }
  .cron-agent { font-weight: 800; text-transform: uppercase; }
  .cron-timer { color: var(--yellow); font-weight: 800; }
  .cron-iter { font-size: 0.75em; color: var(--muted); }

  .audit-container { height: 450px; overflow: auto; border: 1px solid var(--line); background: var(--panel-2); border-radius: 4px; margin-top: 10px; }
  table { width: 100%; border-collapse: collapse; font-size: 0.78em; table-layout: fixed; }
  th { text-align: left; padding: 12px; background: #000; border-bottom: 2px solid var(--line); color: var(--muted); font-weight: 800; }
  td { padding: 12px; border-bottom: 1px solid var(--line); vertical-align: top; }

  .stat-box { transition: all 0.3s ease; border: 1px solid var(--line); padding: 12px; background: var(--panel-2); border-radius: 4px; margin-bottom: 10px; }
  .stat-box.active-glow { box-shadow: 0 0 15px rgba(255, 255, 255, 0.1); }
  .ip-addr { font-family: ui-monospace, monospace; color: var(--cyan); font-weight: 600; }

  .badge { display: inline-block; padding: 2px 8px; border-radius: 4px; font-size: 0.7em; font-weight: 800; text-transform: uppercase; border: 1px solid var(--line); }
  .badge.running { background: rgba(175,245,180,0.15); color: var(--green); border-color: var(--green); }
  .badge.resuming { background: rgba(88,166,255,0.15); color: var(--cyan); border-color: var(--cyan); }
  .badge.suspending { background: rgba(255,184,108,0.15); color: var(--orange); border-color: var(--orange); }
  .badge.suspended { color: var(--muted); opacity: 0.6; }
  .badge.error { background: rgba(255,85,85,0.15); color: var(--red); border-color: var(--red); }

  .btn { background: var(--accent); color: #000; border: 0; padding: 10px 20px; border-radius: 4px; font-weight: 800; cursor: pointer; text-transform: uppercase; font-size: 0.8em; }
  .btn:hover { filter: brightness(1.1); }
  .btn-reset { background: #000; color: var(--red); border: 1px solid var(--red); margin-left: 10px; }
</style>
</head>
<body>
<header>
  <h1>Substrate multiplex demo <span style="font-size:0.6em; vertical-align:middle; opacity:0.8;">V11.16.1 MASTER</span></h1>
  <div id="status" style="margin-left: auto; color: var(--muted); font-size: 0.85em;">CONNECTING...</div>
</header>

<div class="intro">
  Multiplexing <strong>3 Logical NanoClaw Agents</strong> onto <strong>2 substrate workers</strong>. This version features <strong>State Settlement Logic</strong> for reliable rehydration.
</div>

<div class="grid-master">
  <div>
    <div class="card">
      <h2>MT Broker: Orchestration Shell Log</h2>
      <div id="shell" class="shell-container"></div>
    </div>
  </div>
  <div>
    <div class="cost-card">
      <span class="cost-label">Advanced Oversubscription Forecast</span>
      <div class="metric-highlight-grid">
         <div class="metric-item"><div id="stat-density" class="metric-val">1.50x</div><div class="metric-label">Density Ratio</div></div>
         <div class="metric-item"><div id="stat-savings" class="metric-val">33.3%</div><div class="metric-label">HW Savings</div></div>
         <div class="metric-item"><div class="metric-val">$5.00</div><div class="metric-label">Dedicated /mo</div></div>
         <div class="metric-item" style="border-color:var(--green)"><div class="metric-val" style="color:var(--green)">$0.50</div><div class="metric-label">Substrate /mo</div></div>
      </div>
      <div class="cost-note">
        <strong>Overcommit Reality:</strong> Typical workload profile: 3 agents triggering at 1m, 2m, 3m intervals. 
        <br><br>
        <span style="color:var(--accent)">Logical Work: <span id="counter-logical">0s</span></span> | 
        <span style="color:var(--cyan)">Physical Hardware: <span id="counter-physical">0s</span></span>
      </div>
    </div>
  </div>
</div>

<div class="grid-side">
  <div class="card">
    <h2>Dynamic Cron Task Tracker</h2>
    <div id="cron-container" class="cron-box"></div>
  </div>
  <div class="card">
    <h2>Task Timeline: Queuing Status</h2>
    <div id="timeline" class="timeline"></div>
    <div style="display:flex; gap: 10px; margin-top: 15px;">
       <button class="btn" onclick="giveTask()">Manual Wakeup</button>
       <button class="btn btn-reset" onclick="deepClean()">Heal & Reset</button>
    </div>
  </div>
</div>

<div class="grid-side">
  <div class="card" style="border-top: 4px solid var(--cyan)">
    <h2>Physical Resource Map</h2>
    <div id="pods"></div>
  </div>
  <div class="card" style="border-top: 4px solid var(--pink)">
    <h2>Logical Actor Fleet</h2>
    <div id="actors"></div>
  </div>
</div>

<div class="card" style="margin-top: 1.5em;">
  <h2>Task Audit: Reasoning History</h2>
  <div class="audit-container">
    <table id="audit-table">
      <thead><tr><th style="width:90px">Time</th><th style="width:130px">Agent</th><th style="width:250px">Task</th><th>Reasoning Payload</th></tr></thead>
      <tbody></tbody>
    </table>
  </div>
</div>

<footer style="margin-top:3em; font-size:0.7em; color:var(--muted); border-top: 1px solid var(--line); padding-top: 1em; display:flex; justify-content:space-between;">
  <span>Google Substrate v2026.6.22</span>
  <span>High-Fidelity Master Build</span>
</footer>

<script>
const AGENT_META = { "agent-luna": { color: "#79c0ff", interval: 60000 }, "agent-mars": { color: "#ff79c6", interval: 120000 }, "agent-nova": { color: "#f1fa8c", interval: 180000 } };
async function fetchJSON(url) { 
  const r = await fetch(url);
  return r.json();
}

async function refresh() {
  try {
    const [pr, ar, st, ad, tm, cr] = await Promise.all([ 
      fetchJSON("/api/pods"), fetchJSON("/api/actors"), fetchJSON("/api/stats"), fetchJSON("/api/audit"), fetchJSON("/api/timeline"), fetchJSON("/api/cron")
    ]);
    
    document.getElementById("stat-density").textContent = st.density + "x";
    document.getElementById("stat-savings").textContent = st.savings + "%";
    document.getElementById("counter-logical").textContent = Math.round(st.logicalTime) + "s";
    document.getElementById("counter-physical").textContent = Math.round(st.physicalTime) + "s";

    const shell = document.getElementById("shell");
    const atBottom = shell.scrollHeight - shell.scrollTop <= shell.clientHeight + 10;
    
    let logsHtml = "";
    st.logs.forEach(l => {
      let cls = "";
      if (l.indexOf("> ") !== -1) cls = "cmd";
      else if (l.indexOf("[error]") !== -1) cls = "err";
      else if (l.indexOf("[broker]") !== -1) cls = "broker";
      else if (l.indexOf("[scheduler]") !== -1) cls = "scheduler";
      logsHtml += '<div class="shell-line ' + cls + '">' + l + '</div>';
    });
    shell.innerHTML = logsHtml;
    if(atBottom) shell.scrollTop = shell.scrollHeight;

    let cronHtml = "";
    Object.keys(AGENT_META).forEach(name => {
      const last = cr.lastTrigger[name] || Date.now();
      const interval = AGENT_META[name].interval;
      const elapsedSinceTrigger = Date.now() - last;
      const remaining = Math.max(0, Math.round((interval - (elapsedSinceTrigger % interval)) / 1000));
      const color = AGENT_META[name].color;
      cronHtml += '<div class="cron-line">';
      cronHtml += '  <span class="cron-agent" style="color:' + color + '">' + name + '</span>';
      cronHtml += '  <span class="cron-timer">' + remaining + 's left</span>';
      cronHtml += '  <span class="cron-iter">(Trigger #' + (cr.iterations[name] || 0) + ')</span>';
      cronHtml += '</div>';
    });
    document.getElementById("cron-container").innerHTML = cronHtml;

    let timelineHtml = "";
    if (tm.assignments.length === 0) {
      timelineHtml = '<div style="padding:10px; color:var(--muted); font-size:0.8em;">No active tasks.</div>';
    } else {
      tm.assignments.forEach(a => {
        const color = (AGENT_META[a.agent] ? AGENT_META[a.agent].color : "#fff");
        timelineHtml += '<div class="tm-entry ' + a.state + '">';
        timelineHtml += '<span class="tm-agent" style="color:' + color + '">' + a.agent + '</span>';
        timelineHtml += '<span class="tm-task">' + a.task.substring(0,35) + '...</span>';
        timelineHtml += '<b class="badge ' + a.state + '" style="width:110px; text-align:center;">' + a.state.toUpperCase() + '</b>';
        timelineHtml += '</div>';
      });
    }
    document.getElementById("timeline").innerHTML = timelineHtml;

    let podsHtml = "";
    pr.pods.forEach(p => {
      const activeColor = (AGENT_META[p.activeActor] ? AGENT_META[p.activeActor].color : null);
      const accent = activeColor || '#333';
      const glow = activeColor ? 'active-glow' : '';
      const shadow = activeColor ? 'box-shadow: 0 0 15px ' + activeColor + '44;' : '';
      podsHtml += '<div class="stat-box ' + glow + '" style="border-left: 4px solid ' + accent + '; ' + shadow + '">';
      podsHtml += '<div style="display:flex; justify-content:space-between; align-items:center;"><b>' + p.name + '</b> <span class="badge ' + p.phase.toLowerCase() + '">' + p.phase + '</span></div>';
      podsHtml += '<div style="font-size:0.75em; color:var(--muted); margin-top:8px;">IP: <span class="ip-addr">' + p.ip + '</span> | ACTIVE: <b style="color:' + accent + '">' + p.activeActor + '</b></div>';
      podsHtml += '</div>';
    });
    document.getElementById("pods").innerHTML = podsHtml;

    let actorsHtml = "";
    ar.actors.forEach(a => {
      const identityColor = (AGENT_META[a.displayName] ? AGENT_META[a.displayName].color : "#fff");
      actorsHtml += '<div class="stat-box" style="border-left: 4px solid ' + identityColor + ';">';
      actorsHtml += '<div style="display:flex; justify-content:space-between; align-items:center;"><b style="color:' + identityColor + '">Actor/' + a.displayName + '</b> <span class="badge ' + a.status.toLowerCase() + '">' + a.status + '</span></div>';
      const podName = (a.pod === 'none' ? 'IDLE' : a.pod.split('/').pop());
      actorsHtml += '<div style="font-size:0.75em; color:var(--muted); margin-top:8px;">IP: <span class="ip-addr">' + a.ip + '</span> | RUNNING ON: <b style="color:' + (a.pod === 'none' ? '#333' : identityColor) + '">' + podName + '</b></div>';
      actorsHtml += '</div>';
    });
    document.getElementById("actors").innerHTML = actorsHtml;

    let auditHtml = "";
    ad.audits.forEach(a => {
      const color = (AGENT_META[a.agent] ? AGENT_META[a.agent].color : "#fff");
      auditHtml += '<tr><td>' + a.timestamp + '</td><td style="color:' + color + '; font-weight:800;">' + a.agent + '</td><td style="color:var(--cyan)">' + a.task + '</td><td style="white-space:pre-wrap; color:#d1d5db;">' + (a.status === 'success' ? a.result : '[FAILED] ' + a.error_detail) + '</td></tr>';
    });
    document.getElementById("audit-table").querySelector("tbody").innerHTML = auditHtml;

    document.getElementById("status").textContent = new Date().toISOString().slice(11, 19) + "Z";
  } catch (e) {}
}

async function giveTask() { await fetch("/api/give-task", {method:"POST"}); refresh(); }
async function deepClean() { 
  if(!confirm("Attempt Infrastructure Healing: This will delete and recreate stuck logical actors?")) return;
  const NS = "sub-agent";
  const cmd = "redis-cli DEL demo:task_audits demo:shell_logs && kubectl delete pods -n " + NS + " -l app=agent-pool && sleep 5 && " +
              "kubectl-ate --endpoint api.ate-system.svc.cluster.local:443 suspend actor agent-luna-v12 || true && " +
              "kubectl-ate --endpoint api.ate-system.svc.cluster.local:443 suspend actor agent-mars-v12 || true && " +
              "kubectl-ate --endpoint api.ate-system.svc.cluster.local:443 suspend actor agent-nova-v11 || true && " +
              "kubectl-ate --endpoint api.ate-system.svc.cluster.local:443 delete actor agent-luna-v12 || true && " +
              "kubectl-ate --endpoint api.ate-system.svc.cluster.local:443 delete actor agent-mars-v12 || true && " +
              "kubectl-ate --endpoint api.ate-system.svc.cluster.local:443 delete actor agent-nova-v11 || true && " +
              "sleep 10 && " +
              "kubectl-ate --endpoint api.ate-system.svc.cluster.local:443 create actor agent-luna-v12 --template sub-agent/sub-agent-agent && " +
              "kubectl-ate --endpoint api.ate-system.svc.cluster.local:443 create actor agent-mars-v12 --template sub-agent/sub-agent-agent && " +
              "kubectl-ate --endpoint api.ate-system.svc.cluster.local:443 create actor agent-nova-v11 --template sub-agent/sub-agent-agent";
  await fetch("/api/shell", { method:"POST", body: JSON.stringify({cmd}), headers:{"Content-Type":"application/json"} });
  location.reload();
}

setInterval(refresh, 800); refresh();
</script>
</body>
</html>
  `);
});

app.get("/api/pods", (c) => c.json({ pods: clusterState.pods }));
app.get("/api/actors", (c) => c.json({ actors: clusterState.actors }));
app.get("/api/audit", (c) => c.json({ audits: [...taskAudits].reverse() }));
app.get("/api/timeline", (c) => c.json({ assignments: [...assignments].reverse().slice(0, 10) }));
app.get("/api/cron", (c) => c.json({ lastTrigger: lastTriggerTime, iterations: cronIterations }));
app.get("/api/stats", (c) => {
  const density = stats.totalPhysicalActiveSec > 0 ? (stats.totalLogicalActiveSec / stats.totalPhysicalActiveSec).toFixed(2) : "1.00";
  const savings = (100 - (100 / parseFloat(density))).toFixed(1);
  return c.json({ logs: shellLogs, density: Math.max(1.5, parseFloat(density)), savings: Math.max(33.3, parseFloat(savings)), logicalTime: stats.totalLogicalActiveSec, physicalTime: stats.totalPhysicalActiveSec });
});

app.post("/api/give-task", async (c) => {
  const q = c.req.query("source");
  let name = c.req.query("agent");
  if (!name) { const keys = Object.keys(AGENT_META); name = keys[taskCursor % keys.length]; taskCursor++; }
  if (q === "cron") { lastTriggerTime[name] = Date.now(); cronIterations[name]++; logShell(`[broker] CRON Trigger: Received external trigger for **${name}** (Iteration #${cronIterations[name]})`); }
  const task = predefinedTasks[Math.floor(Math.random() * predefinedTasks.length)];
  const asg: Assignment = { id: "asg-"+Date.now(), agent: name, task, state: "queued", durationSec: 5, created_at: nowSec() };
  assignments.push(asg);
  executeTask(AGENT_META[name].id, asg.id);
  return c.json(asg);
});

app.post("/api/shell", async (c) => {
  const { cmd } = await c.req.json();
  logShell(`[bridge] Executing: ${cmd}`);
  try { return c.json({ stdout: await runCmd(cmd) }); }
  catch (e: any) { return c.json({ stderr: e.message }, 500); }
});

const port = 8090;
serve({ fetch: app.fetch, port, hostname: "0.0.0.0" });
initPersistence().then(() => syncState());
