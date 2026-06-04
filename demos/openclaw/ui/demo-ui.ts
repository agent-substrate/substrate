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

// --- Shared State Cache ---
let clusterState = {
  pods: [] as any[],
  actors: [] as any[],
};

const agentLocks: Record<string, boolean> = {};
const podLogLocks: Record<string, boolean> = {};

const nowSec = () => Date.now() / 1000;

// Operation Queue for STATE-CHANGING commands only (resume/suspend)
let commandQueue: { cmd: string, resolve: (val: string) => void, reject: (err: any) => void }[] = [];
let isProcessingQueue = false;

const runCmd = (cmd: string): Promise<string> => {
  return new Promise((resolve, reject) => {
    exec(cmd, (error, stdout, stderr) => {
      if (error) reject(new Error(stderr || error.message));
      else resolve(stdout);
    });
  });
};

const enqueueLifecycleCmd = (cmd: string): Promise<string> => {
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
    if (error) reject(new Error(stderr || error.message));
    else resolve(stdout);
    setTimeout(processQueue, 20); 
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
  } catch (e) {
    console.error("[syncer] Sync error:", e);
  }
  setTimeout(syncState, 400); 
}

// --- High-Utilization Substrate Scheduler ---
async function schedulerLoop() {
  const activeAssignments = assignments.filter(a => a.state !== "completed");
  const queuedAgents = new Set(activeAssignments.filter(a => a.state === "queued").map(a => AGENT_META[a.agent].id));

  // 1. PROGRESS: Move assignments through lifecycle FIRST
  for (const a of activeAssignments) {
    const actorMeta = AGENT_META[a.agent];
    const actor = clusterState.actors.find((act: any) => act.name === actorMeta.id);
    
    if (!actor) continue;
    if (agentLocks[actor.name]) continue;

    const status = actor.status;

    // Transition: Queued -> Running (Confirmed by Substrate)
    if (a.state === "queued" && status === "STATUS_RUNNING") {
      a.state = "running";
      a.started_at = nowSec();
      const ms = a.durationSec * 1000;
      runCmd(`curl -s -X POST http://${actor.ip}:8080/task -d '{"durationMs": ${ms}}'`).catch(() => {});
      continue; 
    }
    
    // Transition: Running -> Completed
    if (a.state === "running" && a.started_at && (nowSec() - a.started_at) > a.durationSec) {
      a.state = "completed";
      a.completed_at = nowSec();
      // Only suspend if no other tasks are queued for THIS agent
      const moreTasksForMe = activeAssignments.some(other => other.agent === a.agent && other.state !== "completed" && other.id !== a.id);
      if (!moreTasksForMe) {
        console.log(`[scheduler] Agent ${actor.displayName} finished all tasks. Suspending.`);
        agentLocks[actor.name] = true;
        enqueueLifecycleCmd(`kubectl-ate suspend actor ${actor.name}`).finally(() => { agentLocks[actor.name] = false; });
      }
      continue;
    }

    // Demand: Resume
    if (a.state === "queued" && status === "STATUS_SUSPENDED") {
      console.log(`[scheduler] Agent ${actor.displayName} has queued tasks. Resuming.`);
      agentLocks[actor.name] = true;
      enqueueLifecycleCmd(`kubectl-ate resume actor ${actor.name}`).catch(async (e: any) => {
        if (e.message.includes("no free workers available")) {
          // Preempt a running agent that has ZERO tasks (queued or running)
          const contender = clusterState.actors.find((act: any) => {
            if (act.status !== "STATUS_RUNNING" || agentLocks[act.name]) return false;
            const actHasWork = activeAssignments.some(asg => AGENT_META[asg.agent].id === act.name);
            return !actHasWork;
          });
          
          if (contender) {
             console.log(`[scheduler] Contention! Preempting idle ${contender.displayName} for busy ${actor.displayName}`);
             agentLocks[contender.name] = true;
             await enqueueLifecycleCmd(`kubectl-ate suspend actor ${contender.name}`).finally(() => { agentLocks[contender.name] = false; });
          } else {
             // Fallback: If ALL running agents are busy, we just have to wait for one to finish.
          }
        }
      }).finally(() => { agentLocks[actor.name] = false; });
      continue;
    }
  }

  // 2. CLEANUP: Suspend agents that are RUNNING but have ZERO tasks (queued or running), IF others are waiting
  if (queuedAgents.size > 0) {
    for (const actor of clusterState.actors) {
      const hasAnyTasks = activeAssignments.some(a => AGENT_META[a.agent].id === actor.name);
      if (actor.status === "STATUS_RUNNING" && !hasAnyTasks && !agentLocks[actor.name]) {
        console.log(`[scheduler] Proactive Cleanup: ${actor.displayName} is idle, freeing pod for queued tasks.`);
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
  .intro {
    color: var(--muted); font-size: .85em; line-height: 1.55;
    max-width: 78em; margin: 0 0 1em 0;
  }
  .intro strong { color: var(--text); }
  .cost-card {
    background: var(--panel); border: 1px solid var(--line);
    border-left: 3px solid var(--cost-accent);
    padding: .7em .9em; border-radius: 6px;
    margin-bottom: 1.1em;
    display: flex; flex-wrap: wrap; gap: .35em 1.6em; align-items: baseline;
    font-size: .82em;
  }
  .cost-card .cost-label {
    color: var(--cost-accent); text-transform: uppercase;
    letter-spacing: .12em; font-size: .78em; font-weight: 600;
  }
  .cost-card .cost-item { color: var(--text); }
  .cost-card .cost-item b { color: var(--cost-accent); font-weight: 600; }
  .cost-card .cost-note { color: var(--dim); flex-basis: 100%; font-size: .85em; }
  
  .grid {
    display: grid; gap: 1em; grid-template-columns: 1fr 1fr;
    margin-bottom: 1em;
  }
  @media (max-width: 720px) { .grid { grid-template-columns: 1fr; } }
  .card {
    background: var(--panel); border: 1px solid var(--line);
    padding: .9em; border-radius: 6px;
  }
  .card h2 {
    font-size: .8em; margin: 0 0 .35em 0;
    color: var(--muted); text-transform: uppercase; letter-spacing: .12em;
  }
  .card .help {
    color: var(--dim); font-size: .8em; line-height: 1.5;
    margin: 0 0 .8em 0;
  }
  .row { padding: .35em 0.5em; border-bottom: 1px solid var(--line); margin-bottom: 0.25em; border-left: 3px solid transparent; }
  .row:last-child { border-bottom: 0; }
  .row .top { display: flex; align-items: center; gap: .5em; }
  .row .name { color: var(--text); font-weight: bold; }
  .row .meta { color: var(--dim); font-size: .8em; padding-left: 0; margin-top: .1em; }
  .badge {
    display: inline-block; padding: .1em .55em;
    border-radius: 999px; font-size: .75em;
    background: var(--panel-2); color: var(--muted);
    border: 1px solid var(--line); text-transform: uppercase; letter-spacing: .05em;
  }
  .badge.running { background: var(--green-bg); color: var(--green); border-color: transparent; }
  .badge.resuming { background: var(--yellow-bg); color: var(--yellow); border-color: transparent; }
  .badge.suspending { background: var(--orange-bg); color: var(--orange); border-color: transparent; }
  .badge.suspended { background: var(--panel-2); color: var(--dim); border-color: var(--line); }

  .section-heading {
    font-size: .8em; color: var(--muted); text-transform: uppercase;
    letter-spacing: .12em; margin: 1em 0 .25em 0;
  }
  .section-help {
    color: var(--dim); font-size: .8em; line-height: 1.5;
    max-width: 78em; margin: 0 0 .6em 0;
  }
  .pod-card {
    background: var(--panel); border: 1px solid var(--line);
    padding: .9em; border-radius: 6px; margin-bottom: 1em;
  }
  .pod-card h3 { font-size: .9em; margin: 0; color: var(--accent); }
  pre.logs {
    background: var(--panel-2); padding: .6em; border-radius: 4px;
    max-height: 14em; overflow: auto; font-size: .78em;
    margin: 0; white-space: pre-wrap; color: var(--text);
  }
  .task-card {
    background: var(--panel); border: 1px solid var(--line);
    border-left: 3px solid var(--accent);
    padding: .8em .9em; border-radius: 6px;
    margin-bottom: 1.1em;
  }
  .task-card .row-top { display: flex; align-items: center; gap: 1em; flex-wrap: wrap; margin-bottom: .6em; }
  .task-card h2 { font-size: .8em; margin: 0; color: var(--muted); text-transform: uppercase; letter-spacing: .12em; }
  .give-task-btn {
    font-family: inherit; font-size: .92em; cursor: pointer;
    background: var(--accent); color: var(--bg);
    border: 0; border-radius: 4px; padding: .45em 1em; font-weight: 600;
  }
  .give-task-btn:hover { filter: brightness(1.08); }
  .give-task-btn:disabled { opacity: .55; cursor: not-allowed; }
  .task-list { display: flex; flex-direction: column; gap: .35em; }
  .task-row {
    display: flex; align-items: baseline; gap: .55em;
    padding: .3em 0; border-bottom: 1px dashed var(--line); font-size: .88em;
  }
  .task-row:last-child { border-bottom: 0; }
  .task-state {
    flex-shrink: 0; min-width: 5.5em;
    display: inline-block; padding: .1em .55em;
    border-radius: 999px; font-size: .72em; text-align: center;
    background: var(--panel-2); color: var(--muted);
    border: 1px solid var(--line); text-transform: uppercase;
  }
  .task-state.queued    { background: var(--yellow-bg); color: var(--yellow); border-color: transparent; }
  .task-state.running   { background: var(--green-bg);  color: var(--green);  border-color: transparent; }
  .task-state.completed { background: var(--panel-2);   color: var(--dim);    border-color: var(--line); }
  .task-agent { color: var(--accent); font-weight: 600; flex-shrink: 0; }
  .task-text { color: var(--text); flex: 1; }
  .task-meta { color: var(--dim); font-size: .78em; flex-shrink: 0; }
  .task-empty { color: var(--dim); font-size: .88em; padding: .3em 0; }
</style>
</head>
<body>
<header>
  <h1>OpenClaw multiplex demo</h1>
  <div id="status">connecting…</div>
</header>

<p class="intro">
  This demo runs <strong>3 OpenClaw agents</strong> on
  <strong>2 substrate worker pods</strong>. Each agent maintains its own in-memory context and state. 
  While an agent is idle, <strong>substrate</strong> can suspend it
  (snapshot its process state, free its pod) and let a different agent borrow
  that pod &mdash; that&rsquo;s the <strong>multiplex</strong>. The dashboard
  refreshes every second; pods, actors, and logs are all live so you
  can watch the rotation happen.
</p>

<div class="cost-card">
  <span class="cost-label">Approximate cost while running</span>
  <span class="cost-item">GCP infrastructure: <b>~$0.40/hr</b></span>
  <span class="cost-item">OpenClaw (3 agents): <b>~$1/hr</b> simulated</span>
  <span class="cost-item">Total: <b>~$1.40/hr</b> typical</span>
  <span class="cost-note">
    GCP figure is one n2-standard-8 VM in us-central1. OpenClaw figure
    assumes lightweight reasoning multiplexed across 2 pods. Substrate enables 
    us to host 1.5x more agents on the same hardware without losing state.
  </span>
</div>

<div class="task-card">
  <div class="row-top">
    <h2>Tasks</h2>
    <div style="display: flex; gap: 0.5em;">
      <button id="give-task-btn" class="give-task-btn" type="button">Give a task</button>
      <button id="pulse-tasks-btn" class="give-task-btn" style="background: var(--cost-accent); color: var(--bg);" type="button">Pulse (10 Tasks)</button>
      <button id="reset-dashboard-btn" class="give-task-btn" style="background: var(--red); color: var(--text);" type="button">Reset Dashboard</button>
    </div>
    <span id="task-feedback" style="color: var(--dim); font-size: .82em;"></span>
  </div>
  <p class="help" style="color: var(--dim); font-size: .8em; margin: 0 0 .65em 0;">
    Click <strong>Give a task</strong> to assign a randomly chosen task to a
    randomly chosen agent. Each task moves through three states &mdash;
    <span class="task-state queued" style="vertical-align: baseline;">queued</span>
    while the agent is suspended,
    <span class="task-state running" style="vertical-align: baseline;">running</span>
    while it owns a worker pod, and
    <span class="task-state completed" style="vertical-align: baseline;">completed</span>
    once it finishes and substrate suspends it again.
  </p>
  <div id="task-list" class="task-list">
    <div class="task-empty">no tasks yet &mdash; click Give a task to start</div>
  </div>
</div>

<div class="grid">
  <div class="card">
    <h2>Worker pods</h2>
    <p class="help">
      The pool of substrate-managed pods that actually host running agents. The
      WorkerPool is configured with <strong>2 replicas</strong> for
      <strong>3 agents</strong>, so substrate is forced to share &mdash; at any
      moment at most 2 agents own pods, and the third is suspended waiting its
      turn. Substrate rotates ownership as agents transition between active and
      idle phases.
    </p>
    <div id="pods">loading…</div>
  </div>
  <div class="card">
    <h2>Actors &amp; templates</h2>
    <p class="help">
      <strong>ActorTemplates</strong> define each agent (container image,
      prompt, idle interval). <strong>Actors</strong> are the live instances
      bound &mdash; or not &mdash; to a worker pod. Watch the
      <code>phase</code>: <strong>Running</strong> means the actor is on a pod
      executing right now; <strong>Suspended</strong> means its state is stored and
      it&rsquo;s waiting for a free pod; <strong>Resuming</strong> /
      <strong>Suspending</strong> are the substrate transitions in between.
    </p>
    <div id="actors">loading…</div>
  </div>
</div>

<h2 class="section-heading">Live logs (per pod, last 25 lines)</h2>
<p class="section-help">
  Each card below tails the logs of one worker pod. Because substrate moves
  agents between pods, the log stream you see in a single card switches
  ownership over time.
</p>
<div id="pod-logs"></div>

<footer style="margin-top: 3em; padding-top: 1em; border-top: 1px solid var(--line); color: var(--dim); font-size: 0.75em; display: flex; justify-content: space-between;">
  <span>Google Claw v2026.3.14</span>
  <span>Agent Substrate PoC</span>
</footer>

<script>
const REFRESH_MS = 600;
const AGENT_META = {
  "Claw-Luna": { color: "#79c0ff", emoji: "🟦" },
  "Claw-Mars": { color: "#ff79c6", emoji: "🟪" },
  "Claw-Nova": { color: "#f1fa8c", emoji: "🟨" },
};

let isRefreshing = false;

function badgeFor(state) {
  const lc = (state || "").toLowerCase();
  if (lc.includes("running") || lc === "ready") return "running";
  if (lc.includes("resuming")) return "resuming";
  if (lc.includes("suspending")) return "suspending";
  if (lc.includes("suspended")) return "suspended";
  return "pending";
}

function escapeHtml(s) {
  return String(s).replace(/[&<>"']/g, c => ({
    "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;"
  }[c]));
}

async function fetchJSON(url) {
  const r = await fetch(url, { cache: "no-store" });
  if (!r.ok) throw new Error(url + " " + r.status);
  return r.json();
}

async function refresh() {
  if (isRefreshing) return;
  isRefreshing = true;

  try {
    const [pr, ar, ts] = await Promise.all([
      fetchJSON("/api/pods"),
      fetchJSON("/api/actors"),
      fetchJSON("/api/task-status")
    ]);

    if (pr.pods) {
      const podsEl = document.getElementById("pods");
      podsEl.innerHTML = pr.pods.map(p => {
        const meta = AGENT_META[p.activeActor] || { color: 'var(--line)', emoji: '◽' };
        return \`
          <div class="row" style="border-left-color: \${meta.color}">
            <div class="top">
              <span class="badge \${badgeFor(p.phase)}">\${escapeHtml(p.phase)}</span>
              <span class="name">\${escapeHtml(p.name)}</span>
            </div>
            <div class="meta" style="color: \${meta.color}; font-weight: bold;">
              \${meta.emoji} \${escapeHtml(p.activeActor || 'idle')} | ip=\${escapeHtml(p.ip || 'n/a')}
            </div>
          </div>
        \`;
      }).join("");
      
      const logsContainer = document.getElementById("pod-logs");
      const wantedIds = new Set(pr.pods.map(p => "log-" + p.name));
      for (const child of [...logsContainer.children]) {
        if (!wantedIds.has(child.id)) child.remove();
      }
      for (const p of pr.pods) {
        const id = "log-" + p.name;
        let card = document.getElementById(id);
        if (!card) {
          card = document.createElement("div");
          card.className = "pod-card";
          card.id = id;
          card.innerHTML = \`<h3>\${escapeHtml(p.name)}</h3><pre class="logs"></pre>\`;
          logsContainer.appendChild(card);
        }
        fetchJSON("/api/logs/" + encodeURIComponent(p.name)).then(lr => {
           if (lr.logs) card.querySelector("pre.logs").textContent = lr.logs;
        }).catch(() => {});
      }
    }

    if (ar.actors) {
      const actorsEl = document.getElementById("actors");
      actorsEl.innerHTML = ar.actors.map(a => {
        const meta = AGENT_META[a.displayName] || { color: 'var(--line)', emoji: '◽' };
        return \`
          <div class="row" style="border-left-color: \${meta.color}">
            <div class="top">
              <span class="badge \${badgeFor(a.phase)}">\${escapeHtml(a.phase)}</span>
              <span class="name" style="color: \${meta.color}">\${meta.emoji} Actor/\${escapeHtml(a.displayName)}</span>
            </div>
            <div class="meta">Template: \${escapeHtml(a.template)} | id=\${escapeHtml(a.name)}</div>
          </div>
        \`;
      }).join("");
    }

    if (ts.assignments) {
      renderTaskList(ts.assignments);
    }

    const status = document.getElementById("status");
    status.innerHTML = (pr.pods?.length || 0) + " pods · " + (ar.actors?.length || 0) + " actors · " + new Date().toISOString().slice(11, 19) + "Z";

  } catch (e) {
    console.error("Refresh failed", e);
  } finally {
    isRefreshing = false;
  }
}

function renderTaskList(assignments) {
  const el = document.getElementById("task-list");
  if (!assignments || assignments.length === 0) {
    el.innerHTML = '<div class="task-empty">no tasks yet &mdash; click Give a task to start</div>';
    return;
  }
  const show = assignments.slice(0, 8);
  el.innerHTML = show.map(a => {
    const state = (a.state || "queued").toLowerCase();
    const meta = AGENT_META[a.agent] || { color: 'var(--muted)', emoji: '' };
    let metaTxt = "";
    if (state === "completed" && a.completed_at) {
      const d = new Date(a.completed_at * 1000);
      metaTxt = "completed at " + d.toISOString().slice(11, 19) + "Z";
    }
    
    return \`
      <div class="task-row">
        <span class="task-state \${escapeHtml(state)}">\${escapeHtml(state)}</span>
        <span class="task-agent" style="color: \${meta.color}">\${meta.emoji} \${escapeHtml(a.agent)}</span>
        <span class="task-text">\${escapeHtml(a.task)}</span>
        <span class="task-meta">\${escapeHtml(metaTxt)}</span>
      </div>
    \`;
  }).join("");
}

async function giveTask() {
  const btn = document.getElementById("give-task-btn");
  btn.disabled = true;
  try {
    await fetch("/api/give-task", { method: "POST", cache: "no-store" });
    refresh();
  } finally {
    setTimeout(() => { btn.disabled = false; }, 400);
  }
}

async function pulseTasks() {
  const btn = document.getElementById("pulse-tasks-btn");
  btn.disabled = true;
  try {
    for (let i = 0; i < 10; i++) {
      await fetch("/api/give-task", { method: "POST", cache: "no-store" });
      await new Promise(r => setTimeout(r, 400)); 
    }
    refresh();
  } finally {
    setTimeout(() => { btn.disabled = false; }, 400);
  }
}

async function resetDashboard() {
  const btn = document.getElementById("reset-dashboard-btn");
  const feedback = document.getElementById("task-feedback");
  btn.disabled = true;
  feedback.textContent = "Cleaning up environment...";
  try {
    await fetch("/api/reset", { method: "POST", cache: "no-store" });
    await new Promise(r => setTimeout(r, 1000));
    refresh();
    feedback.textContent = "Environment ready.";
    setTimeout(() => { feedback.textContent = ""; }, 3000);
  } finally {
    setTimeout(() => { btn.disabled = false; }, 400);
  }
}

document.getElementById("give-task-btn").addEventListener("click", giveTask);
document.getElementById("pulse-tasks-btn").addEventListener("click", pulseTasks);
document.getElementById("reset-dashboard-btn").addEventListener("click", resetDashboard);

refresh();
setInterval(refresh, REFRESH_MS);
</script>
</body>
</html>
  `);
});

// API Implementation

app.get("/api/pods", async (c) => {
  return c.json({ pods: clusterState.pods });
});

app.get("/api/actors", async (c) => {
  return c.json({ actors: clusterState.actors });
});

app.get("/api/logs/:pod", async (c) => {
  const pod = c.req.param("pod");
  if (podLogLocks[pod]) return c.json({ logs: "(fetching...)" });
  podLogLocks[pod] = true;
  try {
    const logs = await runCmd("kubectl logs -n " + DEMO_NAMESPACE + " " + pod + " --tail=25");
    return c.json({ logs: logs });
  } catch (e: any) {
    return c.json({ logs: "(error fetching logs)" });
  } finally {
    podLogLocks[pod] = false;
  }
});

app.get("/api/task-status", async (c) => {
  return c.json({ assignments: [...assignments].reverse() });
});

app.post("/api/reset", async (c) => {
  assignments = [];
  // Proactively suspend all actors via the authenticated service account
  for (const id of ["agent-luna", "agent-mars", "agent-nova"]) {
     enqueueLifecycleCmd(`kubectl-ate suspend actor ${id}`).catch(() => {});
  }
  return c.json({ success: true });
});

app.post("/api/give-task", async (c) => {
  try {
    const agentDisplays = Object.keys(AGENT_META);
    const targetAgentDisplay = agentDisplays[taskCursor % agentDisplays.length];
    taskCursor++;
    const targetTask = predefinedTasks[Math.floor(Math.random() * predefinedTasks.length)];
    const durationSec = Math.floor(Math.random() * 4) + 3; // Random 3-6s
    const asg: Assignment = {
      id: "asg-" + Date.now() + "-" + Math.floor(Math.random()*1000),
      agent: targetAgentDisplay,
      task: targetTask,
      state: "queued",
      durationSec: durationSec,
      created_at: nowSec(),
    };
    assignments.push(asg);
    if (assignments.length > 50) assignments.shift();
    return c.json(asg);
  } catch (e: any) {
    return c.json({ error: e.message }, 500);
  }
});

const port = process.env.PORT ? parseInt(process.env.PORT) : 8090;
serve({ fetch: app.fetch, port });
