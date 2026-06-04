import { Hono } from "hono";
import { serve } from "@hono/node-server";
import { exec } from "node:child_process";

const app = new Hono();

// --- Configuration ---
const DEMO_NAMESPACE = process.env.DEMO_NAMESPACE || "openclaw";

// --- State Management ---
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
  source: "manual" | "broker";
}

let assignments: Assignment[] = [];
let taskCursor = 0;

const AGENT_META: Record<string, { color: string, emoji: string, id: string }> = {
  "Claw-Luna": { color: "#79c0ff", emoji: "🟦", id: "agent-luna" },
  "Claw-Mars": { color: "#ff79c6", emoji: "🟪", id: "agent-mars" },
  "Claw-Nova": { color: "#f1fa8c", emoji: "🟨", id: "agent-nova" },
};

const ID_TO_DISPLAY: Record<string, string> = Object.entries(AGENT_META).reduce((acc, [display, meta]) => {
  acc[meta.id] = display;
  return acc;
}, {} as Record<string, string>);

// --- External Broker (Multi-Tenant Scheduler) ---
interface BrokerEvent {
  agentId: string;
  intervalSec: number;
  lastWakeup: number;
  nextWakeup: number;
  enabled: boolean;
}

const brokerRegistry: Record<string, BrokerEvent> = {
  "agent-luna": { agentId: "agent-luna", intervalSec: 45, lastWakeup: 0, nextWakeup: Date.now() / 1000 + 45, enabled: false },
  "agent-mars": { agentId: "agent-mars", intervalSec: 90, lastWakeup: 0, nextWakeup: Date.now() / 1000 + 90, enabled: false },
  "agent-nova": { agentId: "agent-nova", intervalSec: 120, lastWakeup: 0, nextWakeup: Date.now() / 1000 + 120, enabled: false },
};

// --- Economic Precision Tracking ---
let stats = {
  totalLogicalActiveSec: 0,
  totalPhysicalActiveSec: 0,
  startTime: Date.now() / 1000,
};

// --- Shared State Cache ---
let clusterState = {
  pods: [] as any[],
  actors: [] as any[],
};

const agentLocks: Record<string, boolean> = {};
const podLogLocks: Record<string, boolean> = {};
let shellLogs: string[] = [];

const nowSec = () => Date.now() / 1000;

function logShell(msg: string) {
  const timestamp = new Date().toISOString().slice(11, 19);
  shellLogs.push(`[${timestamp}] ${msg}`);
  if (shellLogs.length > 50) shellLogs.shift();
  console.log(`[shell] ${msg}`);
}

const runCmd = (cmd: string): Promise<string> => {
  return new Promise((resolve, reject) => {
    exec(cmd, (error, stdout, stderr) => {
      if (error) reject(new Error(stderr || error.message));
      else resolve(stdout);
    });
  });
};

let commandQueue: { cmd: string, resolve: (val: string) => void, reject: (err: any) => void }[] = [];
let isProcessingQueue = false;

const enqueueLifecycleCmd = (cmd: string): Promise<string> => {
  logShell(`> ${cmd}`);
  return new Promise((resolve, reject) => {
    commandQueue.push({ cmd, resolve, reject });
    processQueue();
  });
};

async function processQueue() {
  if (isProcessingQueue || commandQueue.length === 0) return;
  isProcessingQueue = true;

  const { cmd, resolve, reject } = commandQueue.shift()!;
  
  exec(cmd, (error, stdout, stderr) => {
    isProcessingQueue = false;
    if (error) {
      logShell(`Error: ${stderr || error.message}`);
      reject(new Error(stderr || error.message));
    } else {
      logShell(stdout.trim() || "OK");
      resolve(stdout);
    }
    setTimeout(processQueue, 50); 
  });
}

// --- Background State Syncer ---
async function syncState() {
  try {
    const [actorsOut, podsOut] = await Promise.all([
      runCmd("kubectl-ate get actors -o json"),
      runCmd(`kubectl get pods -n ${DEMO_NAMESPACE} -l app=agent-pool -o json`)
    ]);

    const actors = JSON.parse(actorsOut).actors || [];
    const podsRaw = JSON.parse(podsOut).items || [];

    clusterState.actors = actors.filter((a: any) => (a.actorId || a.actor_id).startsWith("agent-")).map((a: any) => {
      const id = a.actorId || a.actor_id;
      return {
        name: id,
        displayName: ID_TO_DISPLAY[id] || id,
        template: a.actorTemplateName || a.actor_template_name || "openclaw-agent",
        phase: a.status.replace("STATUS_", ""),
        pod: a.ateomPodName || a.ateom_pod_name || "none",
        ip: a.ateomPodIp || a.ateom_pod_ip || "n/a",
        status: a.status
      };
    });

    clusterState.pods = podsRaw.map((p: any) => {
      const activeActor = actors.find((a: any) => (a.ateomPodName || a.ateom_pod_name) === p.metadata.name);
      const actorId = activeActor ? (activeActor.actorId || activeActor.actor_id) : "idle";
      return {
        name: p.metadata.name,
        phase: p.status.phase,
        ready: p.status.containerStatuses?.[0]?.ready || false,
        ip: p.status.podIP,
        activeActor: ID_TO_DISPLAY[actorId] || "idle"
      };
    });

    // Update stats
    const runningActors = clusterState.actors.filter(a => a.status === "STATUS_RUNNING").length;
    const runningPods = clusterState.pods.filter(p => p.phase === "Running" && p.activeActor !== "idle").length;
    stats.totalLogicalActiveSec += runningActors * 0.4; // 400ms tick
    stats.totalPhysicalActiveSec += runningPods * 0.4;

  } catch (e) {
    console.error("[syncer] Sync error:", e);
  }
  setTimeout(syncState, 400); 
}

// --- Multi-Tenant Broker & High-Utilization Scheduler ---
async function schedulerLoop() {
  const now = nowSec();
  const activeAssignments = assignments.filter(a => a.state !== "completed");
  const queuedAgents = new Set(activeAssignments.filter(a => a.state === "queued").map(a => AGENT_META[a.agent].id));

  // 1. BROKER: Check external timers
  for (const [id, event] of Object.entries(brokerRegistry)) {
    if (event.enabled && now >= event.nextWakeup) {
      const display = ID_TO_DISPLAY[id];
      logShell(`[broker] Timer fired for ${display}. Registering passive wake-up.`);
      const asg: Assignment = {
        id: "broker-" + Date.now(),
        agent: display,
        task: "External Trigger: Periodic health check and reasoning sync",
        state: "queued",
        durationSec: 5,
        created_at: now,
        source: "broker",
      };
      assignments.push(asg);
      event.lastWakeup = now;
      event.nextWakeup = now + event.intervalSec;
    }
  }

  // 2. PROGRESS: Move assignments through lifecycle
  for (const a of activeAssignments) {
    const actorMeta = AGENT_META[a.agent];
    const actor = clusterState.actors.find((act: any) => act.name === actorMeta.id);
    
    if (!actor) continue;
    if (agentLocks[actor.name]) continue;

    const status = actor.status;

    // Transition: Queued -> Running (Confirmed by Substrate)
    if (a.state === "queued" && status === "STATUS_RUNNING") {
      a.state = "running";
      a.started_at = now;
      logShell(`[scheduler] ${actor.displayName} is awake. Injecting task payload.`);
      const ms = a.durationSec * 1000;
      // In a real setup, we'd POST to the agent here.
      // For this PoC, we mimic the injection.
      runCmd(`curl -s -X POST http://${actor.ip}:8080/task -d '{"durationMs": ${ms}}'`).catch(() => {});
      continue; 
    }
    
    // Transition: Running -> Completed
    if (a.state === "running" && a.started_at && (now - a.started_at) > a.durationSec) {
      a.state = "completed";
      a.completed_at = now;
      logShell(`[scheduler] ${actor.displayName} task complete.`);
      
      const moreTasksForMe = activeAssignments.some(other => other.agent === a.agent && other.state !== "completed" && other.id !== a.id);
      if (!moreTasksForMe) {
        logShell(`[scheduler] ${actor.displayName} idle. Waiting for Substrate auto-suspend.`);
        // Note: We don't manually suspend here if we want to show Substrate's idleInterval working.
        // But for the demo UI, we want it fast.
        agentLocks[actor.name] = true;
        enqueueLifecycleCmd(`kubectl-ate suspend actor ${actor.name}`).finally(() => { agentLocks[actor.name] = false; });
      }
      continue;
    }

    // Demand: Resume
    if (a.state === "queued" && status === "STATUS_SUSPENDED") {
      agentLocks[actor.name] = true;
      enqueueLifecycleCmd(`kubectl-ate resume actor ${actor.name}`).catch(async (e: any) => {
        if (e.message.includes("no free workers available")) {
          const contender = clusterState.actors.find((act: any) => {
            if (act.status !== "STATUS_RUNNING" || agentLocks[act.name]) return false;
            const actHasWork = activeAssignments.some(asg => AGENT_META[asg.agent].id === act.name);
            return !actHasWork;
          });
          
          if (contender) {
             logShell(`[scheduler] Hardware full. Preempting idle ${contender.displayName} for active ${actor.displayName}`);
             agentLocks[contender.name] = true;
             await enqueueLifecycleCmd(`kubectl-ate suspend actor ${contender.name}`).finally(() => { agentLocks[contender.name] = false; });
          }
        }
      }).finally(() => { agentLocks[actor.name] = false; });
      continue;
    }
  }

  // 3. CLEANUP: Proactive preemption
  if (queuedAgents.size > 0) {
    for (const actor of clusterState.actors) {
      const hasAnyTasks = activeAssignments.some(a => AGENT_META[a.agent].id === actor.name);
      if (actor.status === "STATUS_RUNNING" && !hasAnyTasks && !agentLocks[actor.name]) {
        logShell(`[scheduler] Proactive Preemption: Evicting idle ${actor.displayName}`);
        agentLocks[actor.name] = true;
        enqueueLifecycleCmd(`kubectl-ate suspend actor ${actor.name}`).finally(() => { agentLocks[actor.name] = false; });
      }
    }
  }
  
  setTimeout(schedulerLoop, 500); 
}

syncState();
schedulerLoop();

// --- Dashboard Implementation ---

app.get("/", (c) => {
  return c.html(`
<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>OpenClaw Substrate Demo</title>
<style>
  :root {
    --bg: #0e1116; --panel: #161b22; --panel-2: #0d1117;
    --line: #30363d; --text: #e6edf3; --muted: #8b949e; --dim: #6e7681;
    --accent: #ff79c6; --green: #aff5b4; --green-bg: #1f3d1c;
    --yellow: #f1fa8c; --yellow-bg: #2d3a13; --red: #ff5555; --red-bg: #3d1c1c;
    --orange: #ffb86c; --orange-bg: #442a15;
    --blue: #79c0ff; --blue-bg: #142a44;
    --cost-accent: #ffd57e;
  }
  * { box-sizing: border-box; }
  body {
    font-family: ui-monospace, "SF Mono", Menlo, Consolas, monospace;
    margin: 0; padding: 1.25em;
    background: var(--bg); color: var(--text);
    line-height: 1.4;
  }
  header { display: flex; justify-content: space-between; align-items: baseline; margin-bottom: .5em; flex-wrap: wrap; }
  h1 { font-size: 1.2em; margin: 0; color: var(--accent); }
  #status { color: var(--dim); font-size: .85em; }
  
  .grid-top { display: grid; gap: 1em; grid-template-columns: 1.5fr 1fr; margin-bottom: 1em; }
  .grid-mid { display: grid; gap: 1em; grid-template-columns: 1fr 1fr; margin-bottom: 1em; }
  @media (max-width: 900px) { .grid-top, .grid-mid { grid-template-columns: 1fr; } }

  .card {
    background: var(--panel); border: 1px solid var(--line);
    padding: .9em; border-radius: 6px;
  }
  .card h2 {
    font-size: .8em; margin: 0 0 .5em 0;
    color: var(--muted); text-transform: uppercase; letter-spacing: .12em;
  }
  .card .help {
    color: var(--dim); font-size: .8em; line-height: 1.5;
    margin: 0 0 .8em 0;
  }
  
  /* Shell Console */
  .shell-container { background: #000; border-radius: 4px; padding: .6em; height: 250px; overflow-y: auto; font-size: .8em; border: 1px solid var(--line); }
  .shell-line { color: #ccc; margin-bottom: 2px; }
  .shell-line.cmd { color: var(--green); font-weight: bold; }

  /* Economic precision panel */
  .stats-grid { display: grid; grid-template-columns: 1fr 1fr; gap: .8em; margin-top: .5em; }
  .stat-box { background: var(--panel-2); padding: .6em; border-radius: 4px; border: 1px solid var(--line); }
  .stat-val { font-size: 1.1em; font-weight: bold; color: var(--cost-accent); }
  .stat-label { font-size: .7em; color: var(--dim); text-transform: uppercase; margin-top: 2px; }

  /* Broker Registry */
  .broker-row { display: flex; align-items: center; justify-content: space-between; padding: .4em 0; border-bottom: 1px solid var(--line); font-size: .85em; }
  .broker-row:last-child { border-bottom: 0; }
  .toggle { cursor: pointer; padding: .2em .5em; border-radius: 3px; font-size: .8em; background: var(--line); }
  .toggle.on { background: var(--green-bg); color: var(--green); }

  .badge {
    display: inline-block; padding: .1em .55em;
    border-radius: 999px; font-size: .75em;
    background: var(--panel-2); color: var(--muted);
    border: 1px solid var(--line); text-transform: uppercase;
  }
  .badge.running { background: var(--green-bg); color: var(--green); border-color: transparent; }
  .badge.resuming { background: var(--yellow-bg); color: var(--yellow); border-color: transparent; }
  .badge.suspending { background: var(--orange-bg); color: var(--orange); border-color: transparent; }
  .badge.suspended { background: var(--panel-2); color: var(--dim); border-color: var(--line); }

  .task-row { display: flex; align-items: baseline; gap: .55em; padding: .3em 0; border-bottom: 1px dashed var(--line); font-size: .88em; }
  .task-state { flex-shrink: 0; min-width: 5.5em; display: inline-block; padding: .1em .55em; border-radius: 999px; font-size: .72em; text-align: center; }
  .task-state.queued { background: var(--yellow-bg); color: var(--yellow); }
  .task-state.running { background: var(--green-bg); color: var(--green); }
  .task-state.completed { background: var(--panel-2); color: var(--dim); }
  .task-source { font-size: .7em; padding: 2px 4px; border-radius: 3px; margin-right: 4px; }
  .source-manual { background: var(--blue-bg); color: var(--blue); }
  .source-broker { background: var(--orange-bg); color: var(--orange); }

  .btn {
    font-family: inherit; font-size: .85em; cursor: pointer;
    background: var(--accent); color: var(--bg);
    border: 0; border-radius: 4px; padding: .4em .8em; font-weight: 600;
  }
  .btn:hover { filter: brightness(1.08); }
</style>
</head>
<body>
<header>
  <h1>OpenClaw on Substrate: MT Broker & Economic Precision</h1>
  <div id="status">connecting…</div>
</header>

<div class="grid-top">
  <div class="card">
    <h2>MT Broker: Orchestration Shell Logs</h2>
    <div class="help">Raw shell commands issued by the external Multi-Tenant Scheduling Service. This decoupling allows agents to remain passive while Substrate manages the physical lifecycle.</div>
    <div id="shell-console" class="shell-container"></div>
  </div>
  
  <div class="card">
    <h2>Economic Precision Analytics</h2>
    <div class="help">Live infrastructure savings comparing dedicated VMs vs. Substrate-multiplexed actors. Data represents Amin's scaling targets.</div>
    <div class="stats-grid">
      <div class="stat-box">
        <div id="stat-oversubscription" class="stat-val">1.50x</div>
        <div class="stat-label">Density Ratio</div>
      </div>
      <div id="stat-savings" class="stat-box">
        <div class="stat-val">33.3%</div>
        <div class="stat-label">Infra Savings</div>
      </div>
      <div class="stat-box">
        <div id="stat-dedicated-cost" class="stat-val">$5.00</div>
        <div class="stat-label">Traditional / mo</div>
      </div>
      <div class="stat-box">
        <div id="stat-substrate-cost" class="stat-val">$0.50</div>
        <div class="stat-label">Substrate / mo</div>
      </div>
    </div>
    <div style="margin-top: 1em; padding-top: 1em; border-top: 1px solid var(--line); display: flex; gap: 0.5em;">
       <button class="btn" style="background:var(--blue); color:#000;" onclick="giveTask()">Manual Task</button>
       <button class="btn" style="background:var(--red); color:#fff;" onclick="resetDashboard()">Deep Reset</button>
    </div>
  </div>
</div>

<div class="grid-mid">
  <div class="card">
    <h2>External Cron Registry</h2>
    <div class="help">Passive agents register their desired duty-cycle with this external service. The broker wakes them only when work is due.</div>
    <div id="broker-list"></div>
  </div>
  <div class="card">
    <h2>Event Timeline</h2>
    <div class="help">History of all logical task injections (Broker triggers vs. Manual inputs).</div>
    <div id="task-list"></div>
  </div>
</div>

<div class="grid-mid">
  <div class="card">
    <h2>Worker Pods</h2>
    <div id="pods">loading…</div>
  </div>
  <div class="card">
    <h2>Logical Actors</h2>
    <div id="actors">loading…</div>
  </div>
</div>

<footer style="margin-top: 2em; padding-top: 1em; border-top: 1px solid var(--line); color: var(--dim); font-size: 0.75em; display: flex; justify-content: space-between;">
  <span>Google Claw v2026.3.14 (OSS Build)</span>
  <span>Agent Substrate | MT Broker Architecture</span>
</footer>

<script>
const REFRESH_MS = 600;
const AGENT_META = {
  "Claw-Luna": { color: "#79c0ff", emoji: "🟦", id: "agent-luna" },
  "Claw-Mars": { color: "#ff79c6", emoji: "🟪", id: "agent-mars" },
  "Claw-Nova": { color: "#f1fa8c", emoji: "🟨", id: "agent-nova" },
};

async function fetchJSON(url) {
  const r = await fetch(url, { cache: "no-store" });
  if (!r.ok) throw new Error(url + " " + r.status);
  return r.json();
}

async function refresh() {
  try {
    const [pr, ar, ts, br, st] = await Promise.all([
      fetchJSON("/api/pods"),
      fetchJSON("/api/actors"),
      fetchJSON("/api/task-status"),
      fetchJSON("/api/broker"),
      fetchJSON("/api/stats")
    ]);

    // Render Stats
    document.getElementById("stat-oversubscription").textContent = st.density + "x";
    document.getElementById("stat-savings").querySelector(".stat-val").textContent = st.savings + "%";
    
    // Render Shell
    const shell = document.getElementById("shell-console");
    const wasAtBottom = shell.scrollHeight - shell.scrollTop <= shell.clientHeight + 10;
    shell.innerHTML = st.logs.map(l => {
      const isCmd = l.includes("> ");
      return \`<div class="shell-line \${isCmd ? 'cmd' : ''}">\${escapeHtml(l)}</div>\`;
    }).join("");
    if (wasAtBottom) shell.scrollTop = shell.scrollHeight;

    // Render Broker
    document.getElementById("broker-list").innerHTML = Object.values(br.registry).map(e => {
       const meta = AGENT_META[ID_TO_DISPLAY[e.agentId]] || { emoji: '◽' };
       const timeToWake = Math.max(0, Math.round(e.nextWakeup - Date.now()/1000));
       return \`
         <div class="broker-row">
           <span>\${meta.emoji} <b>\${e.agentId}</b> (Every \${e.intervalSec}s)</span>
           <div>
             <span style="margin-right: 1em; color: var(--dim);">Wake in \${timeToWake}s</span>
             <button class="toggle \${e.enabled ? 'on' : ''}" onclick="toggleBroker('\${e.agentId}')">
               \${e.enabled ? 'ENABLED' : 'DISABLED'}
             </button>
           </div>
         </div>
       \`;
    }).join("");

    // Render Pods
    document.getElementById("pods").innerHTML = pr.pods.map(p => {
        const meta = AGENT_META[p.activeActor] || { color: 'var(--line)' };
        return \`<div class="stat-box" style="margin-bottom:5px; border-left:3px solid \${meta.color}">
          <div style="display:flex; justify-content:space-between">
            <span style="font-size:0.8em; font-weight:bold;">\${p.name}</span>
            <span class="badge \${p.phase.toLowerCase()}">\${p.phase}</span>
          </div>
          <div style="font-size:0.7em; color:var(--muted)">Active: \${p.activeActor}</div>
        </div>\`;
    }).join("");

    // Render Actors
    document.getElementById("actors").innerHTML = ar.actors.map(a => {
        const meta = AGENT_META[a.displayName] || { color: 'var(--line)' };
        return \`<div class="stat-box" style="margin-bottom:5px; border-left:3px solid \${meta.color}">
          <div style="display:flex; justify-content:space-between">
            <span style="font-size:0.8em; font-weight:bold;">\${a.displayName}</span>
            <span class="badge \${a.phase.toLowerCase()}">\${a.phase}</span>
          </div>
          <div style="font-size:0.7em; color:var(--muted)">Pod: \${a.pod}</div>
        </div>\`;
    }).join("");

    // Render Tasks
    document.getElementById("task-list").innerHTML = ts.assignments.slice(0,6).map(a => {
      const state = a.state.toLowerCase();
      return \`
        <div class="task-row">
          <span class="task-state \${state}">\${state}</span>
          <span class="task-source source-\${a.source}">\${a.source}</span>
          <span style="font-size:0.8em; color:var(--accent);">\${a.agent}</span>
          <span style="font-size:0.75em; color:var(--dim); white-space:nowrap; overflow:hidden; text-overflow:ellipsis;">\${a.task}</span>
        </div>
      \`;
    }).join("");

    document.getElementById("status").innerHTML = new Date().toISOString().slice(11, 19) + "Z";
  } catch (e) { console.error(e); }
}

const ID_TO_DISPLAY = { "agent-luna": "Claw-Luna", "agent-mars": "Claw-Mars", "agent-nova": "Claw-Nova" };
function escapeHtml(s) { return String(s).replace(/[&<>"']/g, c => ({"&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;"}[c])); }

async function toggleBroker(id) {
  await fetch("/api/broker/toggle/" + id, { method: "POST" });
  refresh();
}

async function giveTask() {
  await fetch("/api/give-task", { method: "POST" });
  refresh();
}

async function resetDashboard() {
  await fetch("/api/reset", { method: "POST" });
  refresh();
}

setInterval(refresh, REFRESH_MS);
refresh();
</script>
</body>
</html>
  `);
});

// --- API Implementation ---

app.get("/api/pods", (c) => c.json({ pods: clusterState.pods }));
app.get("/api/actors", (c) => c.json({ actors: clusterState.actors }));
app.get("/api/task-status", (c) => c.json({ assignments: [...assignments].reverse() }));
app.get("/api/broker", (c) => c.json({ registry: brokerRegistry }));
app.get("/api/stats", (c) => {
  const logical = stats.totalLogicalActiveSec;
  const physical = stats.totalPhysicalActiveSec;
  const density = physical > 0 ? (logical / physical).toFixed(2) : "1.00";
  const savings = (100 - (100 / parseFloat(density))).toFixed(1);
  return c.json({
    density: Math.max(1, parseFloat(density)),
    savings: Math.max(0, parseFloat(savings)),
    logs: shellLogs,
  });
});

app.post("/api/broker/toggle/:id", (c) => {
  const id = c.req.param("id");
  if (brokerRegistry[id]) brokerRegistry[id].enabled = !brokerRegistry[id].enabled;
  return c.json({ success: true });
});

app.post("/api/reset", async (c) => {
  assignments = [];
  stats.totalLogicalActiveSec = 0;
  stats.totalPhysicalActiveSec = 0;
  shellLogs = [];
  for (const id of ["agent-luna", "agent-mars", "agent-nova"]) {
     enqueueLifecycleCmd(`kubectl-ate suspend actor ${id}`).catch(() => {});
  }
  return c.json({ success: true });
});

app.post("/api/give-task", async (c) => {
  const agentDisplays = Object.keys(AGENT_META);
  const targetAgentDisplay = agentDisplays[taskCursor % agentDisplays.length];
  taskCursor++;
  const asg: Assignment = {
    id: "manual-" + Date.now(),
    agent: targetAgentDisplay,
    task: predefinedTasks[Math.floor(Math.random() * predefinedTasks.length)],
    state: "queued",
    durationSec: 5,
    created_at: nowSec(),
    source: "manual",
  };
  assignments.push(asg);
  return c.json(asg);
});

const port = process.env.PORT ? parseInt(process.env.PORT) : 8090;
serve({ fetch: app.fetch, port });
