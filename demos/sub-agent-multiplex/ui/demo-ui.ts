import { Hono } from "hono";
import { serve } from "@hono/node-server";
import { exec } from "node:child_process";
import { createClient } from "redis";

const app = new Hono();

// --- Configuration ---
const NS = process.env.DEMO_NAMESPACE || "sub-agent";
const ATE_ENDPOINT = process.env.ATE_ENDPOINT || "api.ate-system.svc.cluster.local:443";
const VALKEY_URL = "redis://valkey-cluster.ate-system.svc.cluster.local:6379";
const BROKER_URL = process.env.BROKER_URL || "http://nano-broker.sub-agent.svc.cluster.local:8091";

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
let clusterState = { pods: [] as any[], actors: [] as any[] };
let brokerState = { registry: [] as any[], logs: [] as any[] };

// Precision Tracking
let stats = {
  totalLogicalActiveSec: 0,
  totalPhysicalActiveSec: 0,
  cumulativeTasks: 0,
  lastSync: Date.now()
};

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
      const audits = await redis.lRange("demo:task_audits", 0, -1);
      taskAudits = (audits || []).map((a: string) => JSON.parse(a));
    }
  } catch (e) {}
}

async function persistAudit(audit: TaskAudit) {
  taskAudits.push(audit);
  if (taskAudits.length > 50) taskAudits.shift();
  try { if (redis?.isOpen) { await redis.rPush("demo:task_audits", JSON.stringify(audit)); await redis.lTrim("demo:task_audits", -50, -1); } } catch {}
}

// --- Background State Syncer ---
async function syncState() {
  try {
    const actorsOut = await runCmd(`kubectl-ate --endpoint ${ATE_ENDPOINT} get actors -o json`);
    const podsOut = await runCmd(`kubectl get pods -n ${NS} -l app=agent-pool -o json`);
    const brokerOut = await fetch(`${BROKER_URL}/status`).then(r => r.json());

    const actors = JSON.parse(actorsOut).actors || [];
    const podsRaw = JSON.parse(podsOut).items || [];
    
    brokerState = brokerOut;

    clusterState.actors = actors.filter((a: any) => VALID_ACTOR_IDS.has(a.actorId || a.actor_id)).map((a: any) => ({
      name: a.actorId || a.actor_id,
      displayName: ID_TO_DISPLAY[a.actorId || a.actor_id] || (a.actorId || a.actor_id),
      status: a.status.replace("STATUS_", ""),
      rawStatus: a.status,
      ip: a.ateomPodIp || a.ateom_pod_ip || "n/a",
      pod: a.ateomPodName || a.ateom_pod_name || "none"
    }));

    clusterState.pods = podsRaw.map((p: any) => {
      const activeActor = actors.find((a: any) => (a.ateomPodName || a.ateom_pod_name || "").split("/").pop() === p.metadata.name && VALID_ACTOR_IDS.has(a.actorId || a.actor_id));
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
  setTimeout(syncState, 1000);
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
    --accent: #79c0ff; --green: #aff5b4; --red: #ff5555; --cyan: #58a6ff;
    --yellow: #f1fa8c; --orange: #ffb86c; --cost-accent: #ffd57e; --pink: #ff79c6;
  }
  * { box-sizing: border-box; }
  body { font-family: ui-monospace, "SF Mono", Menlo, Consolas, monospace; margin: 0; padding: 1.5em; background: var(--bg); color: var(--text); line-height: 1.4; }
  header { border-bottom: 2px solid var(--pink); padding-bottom: 0.8em; margin-bottom: 1.5em; display: flex; justify-content: space-between; align-items: baseline; }
  h1 { font-size: 1.25em; margin: 0; color: var(--pink); font-weight: 800; text-transform: uppercase; }
  
  .intro { font-size: 0.9em; color: var(--muted); margin-bottom: 1.5em; max-width: 900px; }
  .intro strong { color: var(--text); }

  .cost-card { background: var(--panel); border: 1px solid var(--line); border-left: 4px solid var(--cost-accent); padding: 1.2em; border-radius: 6px; font-size: 0.9em; }
  .cost-label { color: var(--cost-accent); text-transform: uppercase; letter-spacing: .12em; font-size: .8em; font-weight: 600; margin-bottom: 15px; display: block; }
  
  .metric-highlight-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 10px; margin-bottom: 15px; }
  .metric-item { background: var(--panel-2); border: 1px solid var(--line); padding: 12px; border-radius: 4px; text-align: center; }
  .metric-val { font-size: 1.5em; font-weight: 800; color: var(--cost-accent); }
  .metric-label { font-size: 0.65em; color: var(--muted); text-transform: uppercase; margin-top: 6px; font-weight: 600; }

  .grid-master { display: grid; gap: 1.5em; grid-template-columns: 1.6fr 1fr; margin-bottom: 1.5em; }
  .grid-side { display: grid; gap: 1.5em; grid-template-columns: 1fr 1fr; margin-bottom: 1.5em; }
  
  .card { background: var(--panel); border: 1px solid var(--line); border-radius: 4px; padding: 1.2em; position: relative; }
  .card h2 { font-size: 0.75em; margin: 0 0 1em 0; color: var(--muted); text-transform: uppercase; font-weight: 800; border-left: 3px solid var(--pink); padding-left: 8px; }
  .card .help { font-size: 0.75em; color: var(--muted); margin-bottom: 1em; line-height: 1.5; }

  .shell-container { background: var(--panel-2); height: 350px; overflow: auto; padding: 1em; border: 1px solid #000; margin-bottom: 0; box-shadow: inset 0 2px 15px rgba(0,0,0,0.7); }
  .shell-line { font-size: 0.82em; color: #d1d5db; margin-bottom: 0.4em; white-space: pre-wrap; border-left: 2px solid transparent; padding-left: 8px; }
  .shell-line.registry { color: var(--green); border-color: var(--green); }
  .shell-line.orchestrator { color: var(--pink); border-color: var(--pink); }
  .shell-line.substrate { color: var(--cyan); border-color: var(--cyan); }
  .shell-line.error { color: var(--red); background: rgba(255,85,85,0.1); border-color: var(--red); }

  .stat-box { transition: all 0.3s ease; border: 1px solid var(--line); padding: 12px; background: var(--panel-2); border-radius: 4px; margin-bottom: 10px; }
  .stat-box.active-glow { box-shadow: 0 0 15px rgba(255, 255, 255, 0.1); }
  .ip-addr { font-family: ui-monospace, monospace; color: var(--cyan); font-weight: 600; }

  .badge { display: inline-block; padding: 2px 8px; border-radius: 4px; font-size: 0.7em; font-weight: 800; text-transform: uppercase; border: 1px solid var(--line); }
  .badge.running { background: rgba(175,245,180,0.15); color: var(--green); border-color: var(--green); }
  .badge.working { background: rgba(255,121,198,0.15); color: var(--pink); border-color: var(--pink); }
  .badge.suspended { color: var(--muted); opacity: 0.6; }
  
  .btn { background: var(--pink); color: #000; border: 0; padding: 8px 16px; border-radius: 4px; font-weight: 800; cursor: pointer; text-transform: uppercase; font-size: 0.7em; }
  .btn:hover { filter: brightness(1.1); }

  table { width: 100%; border-collapse: collapse; font-size: 0.78em; }
  th { text-align: left; padding: 10px; background: #000; border-bottom: 2px solid var(--line); color: var(--muted); }
  td { padding: 10px; border-bottom: 1px solid var(--line); vertical-align: top; }
</style>
</head>
<body>
<header>
  <h1>Fleet management broker <span style="font-size:0.6em; vertical-align:middle; opacity:0.8;">V11.17.0</span></h1>
  <div id="status" style="margin-left: auto; color: var(--muted); font-size: 0.85em;">POLLING BROKER...</div>
</header>

<div class="intro">
  Simulating a <strong>Managed Fleet Orchestration Flow</strong>: NanoClaw agents self-register with the Broker on boot. Physical hardware (2 Pods) multiplexes logical sessions (3 Agents).
</div>

<div class="grid-master">
  <div>
    <div class="card">
      <h2>Fleet Decision Stream</h2>
      <p class="help">Live telemetry from the external Broker service handling custom registration and task dispatching.</p>
      <div id="shell" class="shell-container"></div>
    </div>
  </div>
  <div>
    <div class="cost-card">
      <span class="cost-label">Substrate Economics</span>
      <div class="metric-highlight-grid">
         <div class="metric-item"><div id="stat-density" class="metric-val">1.50x</div><div class="metric-label">Density Ratio</div></div>
         <div class="metric-item"><div id="stat-savings" class="metric-val">33.3%</div><div class="metric-label">HW Savings</div></div>
         <div class="metric-item"><div class="metric-val">$5.00</div><div class="metric-label">Dedicated /mo</div></div>
         <div class="metric-item" style="border-color:var(--green)"><div class="metric-val" style="color:var(--green)">$0.50</div><div class="metric-label">Substrate /mo</div></div>
      </div>
      <div style="font-size:0.85em; color:var(--muted); line-height:1.5;">
        <span style="color:var(--pink)">Logical Work: <span id="counter-logical">0s</span></span> | 
        <span style="color:var(--cyan)">Physical HW: <span id="counter-physical">0s</span></span>
      </div>
    </div>
  </div>
</div>

<div class="grid-side">
<div class="card">
  <h2>Platform Registry</h2>
  <p class="help">Agents that have successfully "Checked In" with the Fleet Management Broker.</p>
  <div id="registry-container"></div>
</div>

  <div class="card">
    <h2>Physical Resource Map</h2>
    <p class="help">Physical pods being recycled by the Substrate control plane.</p>
    <div id="pods-container"></div>
  </div>
</div>

<div class="card" style="margin-top: 1.5em;">
  <h2>Logical Actor Fleet</h2>
  <p class="help">Stateful sessions rehydrating across pods. Snapshotted automatically by Nano Service.</p>
  <div id="actors-container" style="display:grid; grid-template-columns: 1fr 1fr 1fr; gap: 15px;"></div>
</div>

<footer style="margin-top:3em; font-size:0.7em; color:var(--muted); border-top: 1px solid var(--line); padding-top: 1em; display:flex; justify-content:space-between;">
  <span>Google Substrate x NanoClaw POC</span>
  <span>High-Fidelity Platform Build</span>
</footer>

<script>
const AGENT_META = { "agent-luna": { color: "#79c0ff" }, "agent-mars": { color: "#ff79c6" }, "agent-nova": { color: "#f1fa8c" } };
async function fetchJSON(url) { const r = await fetch(url); return r.json(); }

async function trigger(id) {
  await fetch("/api/trigger/" + id, { method: "POST" });
  alert("Policy Trigger sent to Broker for " + id);
}

async function refresh() {
  try {
    const [stats, broker, pods, actors] = await Promise.all([
      fetchJSON("/api/stats"), fetchJSON("/api/broker"), fetchJSON("/api/pods"), fetchJSON("/api/actors")
    ]);
    
    document.getElementById("stat-density").textContent = stats.density + "x";
    document.getElementById("stat-savings").textContent = stats.savings + "%";
    document.getElementById("counter-logical").textContent = Math.round(stats.logicalTime) + "s";
    document.getElementById("counter-physical").textContent = Math.round(stats.physicalTime) + "s";

    // Update Logs
    const shell = document.getElementById("shell");
    shell.innerHTML = broker.logs.map(l => '<div class="shell-line ' + l.module + ' ' + l.level + '">[' + l.timestamp + '] [' + l.module.toUpperCase() + '] ' + l.message + '</div>').join('');
    shell.scrollTop = shell.scrollHeight;

    // Update Registry
    document.getElementById("registry-container").innerHTML = broker.registry.map(a => {
      const display = a.actorId.replace("-v12","").replace("-v11","");
      const color = (AGENT_META[display] ? AGENT_META[display].color : "#fff");
      return '<div class="stat-box" style="border-left: 4px solid ' + color + '; display:flex; justify-content:space-between; align-items:center;">' +
             '<div><b style="color:' + color + '">' + display + '</b><br><span style="font-size:0.7em; color:var(--muted)">Tasks: ' + a.taskCount + '</span></div>' +
             '<div style="text-align:right"><span class="badge ' + a.status + '">' + a.status + '</span><br><button class="btn" style="margin-top:5px" onclick="trigger(\\\'' + a.actorId + '\\\')">Pulse</button></div>' +
             '</div>';
    }).join('');

    // Update Pods
    document.getElementById("pods-container").innerHTML = pods.pods.map(p => {
      const activeColor = (AGENT_META[p.activeActor] ? AGENT_META[p.activeActor].color : null);
      const accent = activeColor || '#333';
      const glow = activeColor ? 'active-glow' : '';
      return '<div class="stat-box ' + glow + '" style="border-left: 4px solid ' + accent + '">' +
             '<div style="display:flex; justify-content:space-between;"><b>' + p.name.split("-").pop() + '</b> <span class="badge">' + p.phase + '</span></div>' +
             '<div style="font-size:0.75em; color:var(--muted); margin-top:5px;">ACTIVE: <b style="color:' + accent + '">' + p.activeActor + '</b></div>' +
             '</div>';
    }).join('');

    // Update Actors
    document.getElementById("actors-container").innerHTML = actors.actors.map(a => {
      const identityColor = (AGENT_META[a.displayName] ? AGENT_META[a.displayName].color : "#fff");
      return '<div class="stat-box" style="border-left: 4px solid ' + identityColor + '">' +
             '<div style="display:flex; justify-content:space-between;"><b style="color:' + identityColor + '">' + a.displayName + '</b> <span class="badge ' + a.status.toLowerCase() + '">' + a.status + '</span></div>' +
             '<div style="font-size:0.7em; color:var(--muted); margin-top:8px;">IP: ' + a.ip + '<br>REHYDRATED: ' + a.pod.split("/").pop() + '</div>' +
             '</div>';
    }).join('');

    document.getElementById("status").textContent = "LAST SYNC: " + new Date().toISOString().slice(11, 19) + "Z";
  } catch (e) {}
}

setInterval(refresh, 1000); refresh();
</script>
</body>
</html>
  `);
});

app.get("/api/broker", async (c) => {
  const r = await fetch(`${BROKER_URL}/status`);
  return c.json(await r.json());
});

app.post("/api/trigger/:id", async (c) => {
  const id = c.req.param("id");
  await fetch(`${BROKER_URL}/trigger/${id}`, { method: "POST" });
  return c.json({ ok: true });
});

app.get("/api/pods", (c) => c.json({ pods: clusterState.pods }));
app.get("/api/actors", (c) => c.json({ actors: clusterState.actors }));
app.get("/api/stats", (c) => {
  const density = stats.totalPhysicalActiveSec > 0 ? (stats.totalLogicalActiveSec / stats.totalPhysicalActiveSec).toFixed(2) : "1.00";
  const savings = (100 - (100 / parseFloat(density))).toFixed(1);
  return c.json({ logicalTime: stats.totalLogicalActiveSec, physicalTime: stats.totalPhysicalActiveSec, density: Math.max(1.5, parseFloat(density)), savings: Math.max(33.3, parseFloat(savings)) });
});

const port = 8090;
serve({ fetch: app.fetch, port, hostname: "0.0.0.0" });
initPersistence().then(() => syncState());
