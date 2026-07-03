"use strict";
var import_hono = require("hono");
var import_node_server = require("@hono/node-server");
var import_node_child_process = require("node:child_process");
var app = new import_hono.Hono();

// --- Configuration ---
var NS = process.env.DEMO_NAMESPACE || "nanoclaw-rotated";
var ATE_ENDPOINT = process.env.ATE_ENDPOINT || "api.ate-system.svc.cluster.local:443";
var BROKER_URL = process.env.BROKER_URL || "http://nano-broker.nanoclaw-rotated.svc.cluster.local:8091";

// --- State ---
var clusterState = { pods: [], actors: [] };
var brokerData = { connectionStatus: "closed", pairingCode: null, logs: [], assignments: [], audits: [], cron: { lastTrigger: {}, iterations: {} } };

var stats = {
  totalLogicalActiveSec: 0,
  totalPhysicalActiveSec: 0,
  cumulativeTasks: 0,
  avgTaskDurationSec: 0,
  lastSync: Date.now()
};

var AGENT_META = {
  "agent-luna": { color: "#79c0ff", prefix: "nano-luna-v9010" },
  "agent-mars": { color: "#ff79c6", prefix: "nano-mars" },
  "agent-nova": { color: "#f1fa8c", prefix: "nano-nova" },
};

var runCmd = (cmd) => {
  return new Promise((resolve) => {
    const timer = setTimeout(() => resolve(""), 8000);
    (0, import_node_child_process.exec)(cmd, (error, stdout, stderr) => {
      clearTimeout(timer);
      if (error) resolve("");
      else resolve(stdout);
    });
  });
};

async function syncState() {
  try {
    const actorsOut = await runCmd(`/opt/bin/kubectl-ate --endpoint ${ATE_ENDPOINT} get actors -o json`);
    const podsOut = await runCmd(`/opt/bin/kubectl get pods -n ${NS} -l ate.dev/worker-pool=nanoclaw-rotated-pool -o json`);
    
    let brokerOut = null;
    try {
      const res = await fetch(`${BROKER_URL}/status`);
      brokerOut = await res.json();
    } catch (e) {}

    if (brokerOut) {
       brokerData = brokerOut;
       const completed = brokerOut.assignments?.filter((a) => a.state === "completed") || [];
       if (completed.length > 0) {
          const total = completed.reduce((sum, a) => sum + (a.completed_at - a.created_at), 0);
          stats.avgTaskDurationSec = total / completed.length;
          stats.cumulativeTasks = completed.length;
       }
    }

    let rawActors = [];
    if (actorsOut && actorsOut.trim().startsWith("{")) {
        rawActors = JSON.parse(actorsOut).actors || [];
    }

    // Logical Agent Fleet Mapping
    clusterState.actors = Object.keys(AGENT_META).map(key => {
        const meta = AGENT_META[key];
        const actor = rawActors.filter(a => (a.actorId || a.actor_id || "").includes(meta.prefix))
                               .sort((a,b) => (b.actorId || b.actor_id || "").localeCompare(a.actorId || a.actor_id || ""))[0] 
                      || { status: "STATUS_IDLE", actorId: "n/a" };
                      
        return {
            name: actor.actorId || actor.actor_id || "n/a",
            displayName: key,
            status: actor.status.replace("STATUS_", ""),
            ip: actor.ateomPodIp || "n/a",
            pod: (actor.ateomPodName || "none").split("/").pop()
        };
    });

    // Physical Resource Map Mapping
    if (podsOut && podsOut.trim().startsWith("{")) {
      const podsRaw = JSON.parse(podsOut).items || [];
      clusterState.pods = podsRaw.map((p) => {
        const podName = p.metadata.name;
        const landedActor = rawActors.find(a => (a.ateomPodName || "").includes(podName));
        let displayActor = "idle";
        if (landedActor) {
            const id = landedActor.actorId || landedActor.actor_id;
            if (id.includes("luna")) displayActor = "agent-luna";
            else if (id.includes("mars")) displayActor = "agent-mars";
            else if (id.includes("nova")) displayActor = "agent-nova";
            else displayActor = id.substring(0,8);
        }
        return {
          name: podName,
          phase: p.status.phase,
          ip: p.status.podIP || "n/a",
          activeActor: displayActor
        };
      });
    }

    const now = Date.now();
    const elapsed = (now - stats.lastSync) / 1000;
    stats.lastSync = now;
    const runningActors = clusterState.actors.filter((a) => a.status === "RUNNING").length;
    const runningPods = clusterState.pods.filter(p => p.activeActor !== "idle").length;
    stats.totalLogicalActiveSec += runningActors * elapsed;
    stats.totalPhysicalActiveSec += runningPods * elapsed;

  } catch (e) {}
  setTimeout(syncState, 1000);
}

app.post("/api/give-task", async (c) => {
  const agent = c.req.query("agent");
  try {
    const res = await fetch(`${BROKER_URL}/api/give-task?agent=${agent}`, { method: "POST" });
    return c.json(await res.json());
  } catch (e) { return c.json({ ok: false }, 500); }
});

app.get("/api/stats", (c) => {
  const density = stats.totalPhysicalActiveSec > 0 ? (stats.totalLogicalActiveSec / stats.totalPhysicalActiveSec) : 1.5;
  const safeDensity = Math.max(1.0, density).toFixed(2);
  const savings = (100 - (100 / parseFloat(safeDensity))).toFixed(1);
  return c.json({ density: safeDensity, savings, avgTaskDurationSec: stats.avgTaskDurationSec, totalTasks: stats.cumulativeTasks });
});

app.get("/api/data", (c) => c.json({ ...brokerData, pods: clusterState.pods, actors: clusterState.actors }));

app.get("/", (c) => {
  const html = `
<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>NanoClaw Substrate Dashboard</title>
<style>
  :root { --bg: #0d1117; --panel: #161b22; --panel-2: #010409; --line: #30363d; --text: #e6edf3; --muted: #8b949e; --accent: #79c0ff; --green: #aff5b4; --red: #ff5555; --cyan: #58a6ff; --yellow: #f1fa8c; --orange: #ffb86c; --pink: #ff79c6; }
  body { font-family: ui-monospace, monospace; margin: 0; padding: 1.5em; background: var(--bg); color: var(--text); line-height: 1.4; }
  header { border-bottom: 2px solid var(--green); padding-bottom: 0.8em; margin-bottom: 1.5em; display: flex; justify-content: space-between; align-items: baseline; }
  h1 { font-size: 1.25em; margin: 0; color: var(--green); font-weight: 800; text-transform: uppercase; }
  .grid-master { display: grid; gap: 1.5em; grid-template-columns: 1.6fr 1fr; margin-bottom: 1.5em; }
  .grid-side { display: grid; gap: 1.5em; grid-template-columns: 1fr 1fr; margin-bottom: 1.5em; }
  .card { background: var(--panel); border: 1px solid var(--line); border-radius: 4px; padding: 1.2em; }
  .card h2 { font-size: 0.75em; margin: 0 0 1em 0; color: var(--muted); text-transform: uppercase; border-left: 3px solid var(--green); padding-left: 8px; }
  .shell-container { background: var(--panel-2); height: 250px; overflow: auto; padding: 1em; border: 1px solid #000; }
  .shell-line { font-size: 0.82em; margin-bottom: 0.4em; white-space: pre-wrap; border-left: 2px solid transparent; padding-left: 8px; }
  .shell-line.whatsapp { color: var(--green); border-color: var(--green); }
  .shell-line.orchestrator { color: var(--pink); border-color: var(--pink); }
  .shell-line.substrate { color: var(--cyan); border-color: var(--cyan); }
  .shell-line.sys { color: var(--muted); border-color: var(--muted); }
  .shell-line.error { color: var(--red); border-color: var(--red); }
  .badge { display: inline-block; padding: 2px 6px; border-radius: 4px; font-size: 0.7em; font-weight: 800; border: 1px solid var(--line); text-transform: uppercase; }
  .badge.RUNNING { background: rgba(175,245,180,0.1); color: var(--green); border-color: var(--green); box-shadow: 0 0 10px var(--green); animation: pulse 1.5s infinite; }
  .badge.RESUMING { background: rgba(255,184,108,0.1); color: var(--orange); border-color: var(--orange); }
  .badge.SUSPENDING { background: rgba(255,184,108,0.1); color: var(--orange); border-color: var(--orange); }
  @keyframes pulse { 0% { opacity: 1; } 50% { opacity: 0.6; } 100% { opacity: 1; } }
  .cron-box { background: var(--panel-2); padding: 10px; border-radius: 4px; font-size: 0.8em; }
  .cron-line { display: flex; justify-content: space-between; margin-bottom: 5px; border-bottom: 1px dashed #222; padding-bottom: 3px; }
  .box { background: var(--panel-2); border: 1px solid var(--line); padding: 10px; margin-bottom: 10px; border-radius: 4px; transition: all 0.3s ease; }
  table { width: 100%; border-collapse: collapse; font-size: 0.75em; }
  th { text-align: left; padding: 10px; background: #000; border-bottom: 2px solid var(--line); color: var(--muted); }
  td { padding: 10px; border-bottom: 1px solid var(--line); }
</style>
</head>
<body>
<header>
  <h1>NanoClaw Substrate Integration <span style="font-size:0.6em; vertical-align:middle; opacity:0.8;">V1.3.3 MAPPING+</span></h1>
  <div id="heartbeat" style="font-size:0.7em; color:var(--muted)">Syncing...</div>
</header>

<div class="grid-master">
  <div>
    <div class="card" style="margin-bottom: 1.5em;">
      <h2>Decision Stream</h2>
      <div id="shell" class="shell-container"></div>
    </div>
    <div class="card">
      <h2>Task Timeline</h2>
      <div id="timeline" style="height: 100px; overflow: auto; background: var(--panel-2); border: 1px solid var(--line); padding: 8px;"></div>
    </div>
  </div>
  <div>
    <div class="card" style="margin-bottom: 1.5em;">
      <h2>WhatsApp Bridge</h2>
      <div id="wa-status"></div>
      <div id="pairing" style="display:none; text-align:center; padding:15px; border:2px dashed var(--yellow); margin-top:10px;">
        <div style="font-size:0.7em; color:var(--muted)">LINK CODE:</div>
        <div id="pairing-code" style="font-size:1.8em; font-weight:800; color:var(--yellow); letter-spacing:4px;"></div>
      </div>
      <div id="wa-active" style="display:none; color:var(--green); text-align:center; padding:15px; border:1px solid var(--green); margin-top:10px; font-weight:800;">
         LIVE: LISTENING
      </div>
    </div>
    <div class="card" style="background: var(--panel-2); border-color: var(--pink);">
      <h2 style="border-left-color: var(--pink)">Operational Efficiency</h2>
      <div style="font-size: 0.72em;">
        <div class="cron-line"><span>Workflow Baseline</span><span style="color:var(--green)">48 crons / hr</span></div>
        <div class="cron-line"><span>Measured Intensity</span><span id="proj-duration" style="color:var(--orange)">-- s / task</span></div>
        <div class="cron-line"><span>Substrate Oversubscription</span><span id="proj-overcommit" style="color:var(--cyan)">-- x</span></div>
      </div>
    </div>
    <div class="card" style="margin-top: 15px; background: transparent; border:none; padding:0;">
      <h2 style="border-left-color: var(--orange)">External Cron Tracker</h2>
      <div id="cron" class="cron-box"></div>
    </div>
  </div>
</div>

<div class="grid-side">
  <div class="card">
    <h2>Physical Resource Map</h2>
    <div id="pods"></div>
  </div>
  <div class="card">
    <h2>Logical Agent Fleet</h2>
    <div id="actors"></div>
  </div>
</div>

<div class="card" style="margin-top:1.5em;">
  <h2>Task Audit: Reasoning History</h2>
  <div style="height:250px; overflow:auto; background: var(--panel-2); border: 1px solid var(--line);">
    <table id="audit">
      <thead><tr><th style="width:90px">Time</th><th style="width:130px">Agent</th><th>Reasoning Payload</th></tr></thead>
      <tbody></tbody>
    </table>
  </div>
</div>

<script>
const AGENT_COLORS = { "agent-luna": "#79c0ff", "agent-mars": "#ff79c6", "agent-nova": "#f1fa8c" };

async function refresh() {
  try {
    const statsRes = await fetch("/api/stats?t=" + Date.now());
    const dataRes = await fetch("/api/data?t=" + Date.now());
    const stats = await statsRes.json();
    const data = await dataRes.json();
    const el = (id) => document.getElementById(id);

    el("heartbeat").innerHTML = '<span style="color:var(--green)">●</span> Last Sync: ' + new Date().toLocaleTimeString();
    el("proj-duration").textContent = Math.round(stats.avgTaskDurationSec || 8) + " s / task";
    el("proj-overcommit").textContent = stats.density + "x Efficiency";

    if (data.logs) {
        el("shell").innerHTML = data.logs.map(l => '<div class="shell-line '+(l.module||'sys')+'">['+l.timestamp+'] ['+(l.module||'sys').toUpperCase()+'] '+l.message+'</div>').join('');
        el("shell").scrollTop = el("shell").scrollHeight;
    }

    el("wa-status").innerHTML = '<span class="badge" style="color:var(--green)">STATUS: ' + data.connectionStatus.toUpperCase() + '</span>';
    el("pairing").style.display = (data.connectionStatus !== "open" && data.pairingCode) ? "block" : "none";
    el("pairing-code").textContent = data.pairingCode || "";
    el("wa-active").style.display = data.connectionStatus === "open" ? "block" : "none";

    el("cron").innerHTML = Object.keys(AGENT_COLORS).map(name => {
      return '<div class="cron-line"><span style="color:'+AGENT_COLORS[name]+'">'+name+'</span><span style="color:var(--muted)">Active Workflow</span></div>';
    }).join('');

    el("timeline").innerHTML = (data.assignments || []).map(a => {
        const display = a.agent.split("-v")[0].replace("nano-", "agent-");
        return '<div style="font-size:0.7em; border-bottom:1px solid #222; padding:4px;">['+new Date(a.created_at*1000).toISOString().slice(11,19)+'] <b style="color:'+AGENT_COLORS[display]+'">'+display+'</b>: '+a.task+' <span class="badge '+a.state+'" style="float:right">'+a.state+'</span></div>';
    }).join('');

    el("pods").innerHTML = data.pods.map(p => {
        const color = AGENT_COLORS[p.activeActor] || "#333";
        const isActive = p.activeActor !== "idle";
        return '<div class="box" style="border-left: 6px solid '+color+'; '+(isActive ? 'background:rgba(255,255,255,0.05);' : '')+'">' +
               '<b>' + p.name.split("-").pop() + '</b><br>' +
               '<span style="font-size:0.72em; color:var(--green)">MAPPING IP: ' + p.ip + '</span><br>' +
               '<span style="font-size:0.75em; color:var(--muted)">TENANT: <b style="color:'+color+'">'+p.activeActor.toUpperCase()+'</b></span></div>';
    }).join('');

    el("actors").innerHTML = data.actors.map(a => {
        const color = AGENT_COLORS[a.displayName] || "#fff";
        const isActive = a.status === "RUNNING" || a.status === "RESUMING";
        return '<div class="box" style="border-left: 6px solid '+color+'; '+(isActive ? 'background:rgba(255,255,255,0.05);' : '')+'">' +
               '<div style="display:flex; justify-content:space-between;"><b>'+a.displayName+'</b><span class="badge '+a.status+'">'+a.status+'</span></div>' +
               '<div style="font-size:0.7em; color:var(--cyan); margin-top:4px;">TENANT IP: '+a.ip+'</div>' +
               '<div style="font-size:0.7em; color:var(--muted);">TARGET POD: '+a.pod+'</div></div>';
    }).join('');

    document.querySelector("#audit tbody").innerHTML = (data.audits || []).map(a => {
        const display = a.agent.split("-v")[0].replace("nano-", "agent-");
        return '<tr><td>'+a.timestamp+'</td><td style="color:'+AGENT_COLORS[display]+'; font-weight:800;">'+display+'</td><td style="color:#d1d5db;">'+a.result+'</td></tr>';
    }).join('');
  } catch(e) {}
}
setInterval(refresh, 1000); refresh();
</script>
</body></html>
  `;
  return c.html(html);
});

var port = 8090;
(0, import_node_server.serve)({ fetch: app.fetch, port, hostname: "0.0.0.0" }, () => {
  console.log("V1.3.3 IP-Mapping Dashboard Online");
});
syncState();
