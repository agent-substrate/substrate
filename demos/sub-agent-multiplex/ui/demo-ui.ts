import { Hono } from "hono";
import { serve } from "@hono/node-server";
import { exec } from "node:child_process";

const app = new Hono();

// --- Configuration ---
const NS = process.env.DEMO_NAMESPACE || "sub-agent";
const ATE_ENDPOINT = process.env.ATE_ENDPOINT || "api.ate-system.svc.cluster.local:443";
const BROKER_URL = "http://nano-broker.sub-agent.svc.cluster.local:8091";

// --- Types ---
interface BrokerStatus {
  connectionStatus: string;
  pairingCode: string | null;
  registeredAgents: any[];
  logs: any[];
  assignments: any[];
  audits: any[];
  cron?: { lastTrigger: any, iterations: any };
}

// --- State ---
let clusterState = { pods: [] as any[], actors: [] as any[] };
let brokerData: BrokerStatus = { connectionStatus: "closed", pairingCode: null, registeredAgents: [], logs: [], assignments: [], audits: [] };

// Precision Tracking
let stats = {
  totalLogicalActiveSec: 0,
  totalPhysicalActiveSec: 0,
  cumulativeTasks: 0,
  lastSync: Date.now()
};

const AGENT_META: Record<string, { color: string, id: string, interval: number }> = {
  "agent-luna": { color: "#79c0ff", id: "agent-luna-v14", interval: 60000 },
  "agent-mars": { color: "#ff79c6", id: "agent-mars-v12", interval: 120000 },
  "agent-nova": { color: "#f1fa8c", id: "agent-nova-v11", interval: 180000 },
};

const ID_TO_DISPLAY: Record<string, string> = Object.entries(AGENT_META).reduce((acc, [display, meta]) => {
  acc[meta.id] = display;
  return acc;
}, {} as Record<string, string>);

const VALID_ACTOR_IDS = new Set(Object.values(AGENT_META).map(m => m.id));

const runCmd = (cmd: string): Promise<string> => {
  return new Promise((resolve) => {
    const timer = setTimeout(() => resolve(""), 8000);
    exec(cmd, (error, stdout, stderr) => {
      clearTimeout(timer);
      if (error) {
        console.error(`[CMD ERROR] ${cmd}: ${stderr || error.message}`);
        resolve("");
      } else resolve(stdout);
    });
  });
};

const fetchWithTimeout = async (url: string, timeout = 3000) => {
  const controller = new AbortController();
  const id = setTimeout(() => controller.abort(), timeout);
  try {
    const response = await fetch(url, { signal: controller.signal });
    clearTimeout(id);
    return response;
  } catch (e) {
    clearTimeout(id);
    throw e;
  }
};

// --- Background Syncer ---
async function syncState() {
  try {
    const actorsOut = await runCmd(`kubectl-ate --endpoint ${ATE_ENDPOINT} get actors -o json`);
    const podsOut = await runCmd(`kubectl get pods -n ${NS} -l app=agent-pool -o json`);
    
    let brokerOut: any = null;
    try {
      const res = await fetchWithTimeout(`${BROKER_URL}/status`);
      brokerOut = await res.json();
    } catch (e) {
       console.error("Broker Sync Error:", (e as any).message);
    }

    if (brokerOut) brokerData = brokerOut;

    let actors: any[] = [];
    if (actorsOut && actorsOut.trim().startsWith("{")) {
      try {
        const allActors = JSON.parse(actorsOut).actors || [];
        actors = allActors.filter((a: any) => VALID_ACTOR_IDS.has(a.actorId || a.actor_id));
        clusterState.actors = actors.map((a: any) => ({
          name: a.actorId || a.actor_id,
          displayName: ID_TO_DISPLAY[a.actorId || a.actor_id] || (a.actorId || a.actor_id),
          status: a.status.replace("STATUS_", ""),
          rawStatus: a.status,
          ip: a.ateomPodIp || "n/a",
          pod: (a.ateomPodName || "none").split("/").pop()
        }));
      } catch (e) { console.error("Actors JSON Parse Error"); }
    }

    if (podsOut && podsOut.trim().startsWith("{")) {
      try {
        const podsRaw = JSON.parse(podsOut).items || [];
        clusterState.pods = podsRaw.map((p: any) => {
          const activeActor = actors.find((a: any) => (a.ateomPodName || "").split("/").pop() === p.metadata.name);
          const actorId = activeActor ? (activeActor.actorId || activeActor.actor_id) : "idle";
          return {
            name: p.metadata.name,
            phase: p.status.phase,
            ip: p.status.podIP || "n/a",
            activeActor: ID_TO_DISPLAY[actorId] || actorId
          };
        });
      } catch (e) { console.error("Pods JSON Parse Error"); }
    }

    const now = Date.now();
    const elapsed = (now - stats.lastSync) / 1000;
    stats.lastSync = now;
    const runningActors = actors.filter((a: any) => a.status === "STATUS_RUNNING").length;
    const runningPods = clusterState.pods.filter(p => p.phase === "Running" && p.activeActor !== "idle").length;
    
    stats.totalLogicalActiveSec += runningActors * elapsed;
    stats.totalPhysicalActiveSec += runningPods * elapsed;

  } catch (e: any) {
    console.error("Critical Sync Error:", e.message);
  }
  setTimeout(syncState, 2000);
}

// --- API Endpoints ---

app.post("/api/give-task", async (c) => {
  const agent = c.req.query("agent");
  const source = c.req.query("source") || "manual";
  try {
    const res = await fetch(`${BROKER_URL}/api/give-task?agent=${agent}&source=${source}`, { method: "POST" });
    return c.json(await res.json());
  } catch (e: any) {
    return c.json({ ok: false, error: e.message }, 500);
  }
});

app.post("/api/deep-clean", async (c) => {
  try {
    await runCmd(`kubectl-ate --endpoint ${ATE_ENDPOINT} delete actor agent-luna-v14`).catch(() => {});
    await runCmd(`kubectl-ate --endpoint ${ATE_ENDPOINT} delete actor agent-mars-v12`).catch(() => {});
    await runCmd(`kubectl-ate --endpoint ${ATE_ENDPOINT} delete actor agent-nova-v11`).catch(() => {});
    await runCmd(`kubectl patch workerpool agent-pool -n ${NS} --type='merge' -p '{"spec":{"replicas":2}}'`);
    await runCmd(`kubectl delete pods -n ${NS} -l app=agent-pool`);
    
    setTimeout(async () => {
       await runCmd(`kubectl-ate --endpoint ${ATE_ENDPOINT} create actor agent-luna-v14 --template sub-agent/sub-agent-agent`).catch(() => {});
       await runCmd(`kubectl-ate --endpoint ${ATE_ENDPOINT} create actor agent-mars-v12 --template sub-agent/sub-agent-agent`).catch(() => {});
       await runCmd(`kubectl-ate --endpoint ${ATE_ENDPOINT} create actor agent-nova-v11 --template sub-agent/sub-agent-agent`).catch(() => {});
    }, 10000);

    return c.json({ ok: true, status: "Wipe sequence initiated." });
  } catch (e: any) {
    return c.json({ ok: false, error: e.message }, 500);
  }
});

app.get("/api/stats", (c) => {
  const density = stats.totalPhysicalActiveSec > 0 ? (stats.totalLogicalActiveSec / stats.totalPhysicalActiveSec) : 1.5;
  const safeDensity = Math.max(1.0, density).toFixed(2);
  const savings = (100 - (100 / parseFloat(safeDensity))).toFixed(1);
  return c.json({ logicalTime: stats.totalLogicalActiveSec, physicalTime: stats.totalPhysicalActiveSec, density: safeDensity, savings });
});

app.get("/api/data", (c) => c.json({ ...brokerData, pods: clusterState.pods, actors: clusterState.actors }));

app.get("/", (c) => {
  const html = `
<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Substrate x WhatsApp Master</title>
<style>
  :root { --bg: #0d1117; --panel: #161b22; --panel-2: #010409; --line: #30363d; --text: #e6edf3; --muted: #8b949e; --accent: #79c0ff; --green: #aff5b4; --red: #ff5555; --cyan: #58a6ff; --yellow: #f1fa8c; --orange: #ffb86c; --cost-accent: #ffd57e; --pink: #ff79c6; }
  * { box-sizing: border-box; }
  body { font-family: ui-monospace, monospace; margin: 0; padding: 1.5em; background: var(--bg); color: var(--text); line-height: 1.4; }
  header { border-bottom: 2px solid var(--green); padding-bottom: 0.8em; margin-bottom: 1.5em; display: flex; justify-content: space-between; align-items: baseline; }
  h1 { font-size: 1.25em; margin: 0; color: var(--green); font-weight: 800; text-transform: uppercase; }
  
  .grid-master { display: grid; gap: 1.5em; grid-template-columns: 1.6fr 1fr; margin-bottom: 1.5em; }
  .grid-side { display: grid; gap: 1.5em; grid-template-columns: 1fr 1fr; margin-bottom: 1.5em; }
  
  .card { background: var(--panel); border: 1px solid var(--line); border-radius: 4px; padding: 1.2em; position: relative; }
  .card h2 { font-size: 0.75em; margin: 0 0 1em 0; color: var(--muted); text-transform: uppercase; font-weight: 800; border-left: 3px solid var(--green); padding-left: 8px; }

  .shell-container { background: var(--panel-2); height: 250px; overflow: auto; padding: 1em; border: 1px solid #000; box-shadow: inset 0 2px 15px rgba(0,0,0,0.7); }
  .shell-line { font-size: 0.82em; color: #d1d5db; margin-bottom: 0.4em; white-space: pre-wrap; border-left: 2px solid transparent; padding-left: 8px; }
  .shell-line.whatsapp { color: var(--green); border-color: var(--green); }
  .shell-line.orchestrator { color: var(--pink); border-color: var(--pink); }
  .shell-line.substrate { color: var(--cyan); border-color: var(--cyan); }
  .shell-line.alert { color: var(--yellow); border-color: var(--yellow); }
  .shell-line.error { color: var(--red); border-color: var(--red); }
  
  .metric-highlight-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 10px; margin-bottom: 15px; }
  .metric-item { background: var(--panel-2); border: 1px solid var(--line); padding: 12px; border-radius: 4px; text-align: center; }
  .metric-val { font-size: 1.5em; font-weight: 800; color: var(--cost-accent); }

  .badge { display: inline-block; padding: 2px 8px; border-radius: 4px; font-size: 0.7em; font-weight: 800; text-transform: uppercase; border: 1px solid var(--line); }
  .badge.open { background: rgba(175,245,180,0.1); color: var(--green); border-color: var(--green); }
  .badge.closed { background: rgba(255,85,85,0.1); color: var(--red); border-color: var(--red); }
  .badge.running { background: rgba(175,245,180,0.1); color: var(--green); border-color: var(--green); }
  .badge.resuming { background: rgba(255,184,108,0.1); color: var(--orange); border-color: var(--orange); }
  
  .pairing-box { background: #000; border: 2px dashed var(--yellow); padding: 15px; text-align: center; margin-top: 10px; border-radius: 4px; }
  .pairing-code { font-size: 2em; font-weight: 800; color: var(--yellow); letter-spacing: 5px; }

  .timeline { height: 150px; overflow: auto; background: var(--panel-2); border: 1px solid var(--line); padding: 8px; border-radius: 4px; }
  .tm-entry { font-size: 0.72em; padding: 6px; border-bottom: 1px solid #222; display: flex; justify-content: space-between; align-items: baseline; }
  .tm-agent { font-weight: 800; width: 100px; }

  .cron-box { background: var(--panel-2); padding: 10px; border-radius: 4px; font-size: 0.8em; }
  .cron-line { display: flex; justify-content: space-between; margin-bottom: 5px; border-bottom: 1px dashed #222; padding-bottom: 3px; }

  .stat-box { transition: all 0.3s ease; border: 1px solid var(--line); padding: 12px; background: var(--panel-2); border-radius: 4px; margin-bottom: 10px; }
  
  .audit-container { height: 300px; overflow: auto; border: 1px solid var(--line); background: var(--panel-2); border-radius: 4px; margin-top: 10px; }
  table { width: 100%; border-collapse: collapse; font-size: 0.78em; table-layout: fixed; }
  th { text-align: left; padding: 12px; background: #000; border-bottom: 2px solid var(--line); color: var(--muted); }
  td { padding: 12px; border-bottom: 1px solid var(--line); vertical-align: top; }
  .btn { border:none; padding: 4px 8px; border-radius: 4px; cursor: pointer; font-size: 0.7em; font-weight: 800; }
</style>
</head>
<body>
<header>
  <h1>Fleet Management Master <span style="font-size:0.6em; vertical-align:middle; opacity:0.8;">V1.1.27 STABLE</span></h1>
  <div id="heartbeat" style="font-size:0.7em; color:var(--muted)">Initializing...</div>
  <button class="btn" style="background:var(--red); color:#fff;" onclick="deepClean()">Deep Clean</button>
</header>

<div class="grid-master">
  <div class="card">
    <h2>Fleet Decision Stream</h2>
    <div id="shell" class="shell-container"></div>
  </div>
  <div class="card">
    <h2>WhatsApp Bridge</h2>
    <div id="wa-status"></div>
    <div id="pairing-section" style="display:none;">
      <div class="pairing-box">
        <div style="font-size:0.7em; color:var(--muted); margin-bottom:8px;">LINK DEVICE: ENTER CODE ON YOUR PHONE</div>
        <div id="pairing-code" class="pairing-code">---- ----</div>
      </div>
    </div>
    <div id="wa-active" style="display:none; color:var(--green); font-size:0.9em; font-weight:800; text-align:center; padding:15px; border:1px solid var(--green); border-radius:4px; margin-top:10px;">
       LIVE CONNECTION: LISTENING
    </div>
    <div class="card" style="margin-top: 15px; background: transparent; border:none; padding:0;">
      <h2 style="border-left-color: var(--yellow)">Managed Fleet Economics</h2>
      <div class="metric-highlight-grid">
         <div class="metric-item"><div id="stat-density" class="metric-val">1.50x</div><div style="font-size:0.6em;color:var(--muted)">OVERSUBSCRIPTION (3:2)</div></div>
         <div class="metric-item"><div id="stat-savings" class="metric-val">33.3%</div><div style="font-size:0.6em;color:var(--muted)">COST REDUCTION</div></div>
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
    <h2>Logical Actor Fleet</h2>
    <div id="actors" style="display:grid; grid-template-columns: 1fr; gap:10px;"></div>
  </div>
</div>

<div class="card" style="margin-bottom: 1.5em;">
  <h2>Task Timeline</h2>
  <div id="timeline" class="timeline"></div>
</div>

<div class="card">
  <h2>Task Audit: Reasoning History</h2>
  <div class="audit-container">
    <table id="audit-table">
      <thead><tr><th style="width:90px">Time</th><th style="width:130px">Agent</th><th style="width:200px">Task</th><th>Reasoning Payload</th></tr></thead>
      <tbody></tbody>
    </table>
  </div>
</div>

<script>
const AGENT_META = { 
  "agent-luna": { color: "#79c0ff", interval: 60000 }, 
  "agent-mars": { color: "#ff79c6", interval: 120000 }, 
  "agent-nova": { color: "#f1fa8c", interval: 180000 } 
};

async function trigger(agentKey) { 
  try {
    await fetch("/api/give-task?agent=" + agentKey, { method: "POST" }); 
  } catch(e) { console.error("Trigger fail", e); }
}

async function deepClean() { 
  if(confirm("Initiate Hard Hardware Reset? This will wipe all actors and workers.")) { 
    try {
      await fetch("/api/deep-clean", { method: "POST" }); 
    } catch(e) { console.error("Clean fail", e); }
  } 
}

window.onerror = function(msg, url, line) {
  console.error("GLOBAL ERROR:", msg, "line:", line);
};

async function refresh() {
  try {
    const statsRes = await fetch("/api/stats?t=" + Date.now());
    const dataRes = await fetch("/api/data?t=" + Date.now());
    if (!statsRes.ok || !dataRes.ok) return;
    
    const stats = await statsRes.json();
    const data = await dataRes.json();
    if (!data) return;

    const el = (id) => document.getElementById(id);

    if (el("heartbeat")) el("heartbeat").innerHTML = '<span style="color:var(--green)">●</span> Last Sync: ' + new Date().toLocaleTimeString();
    if (el("stat-density")) el("stat-density").textContent = stats.density + "x";
    if (el("stat-savings")) el("stat-savings").textContent = stats.savings + "%";

    const shell = el("shell");
    if (data.logs && shell) {
      shell.innerHTML = data.logs.map(l => '<div class="shell-line ' + (l.module || 'info') + ' ' + (l.level || 'info') + '">[' + l.timestamp + '] [' + (l.module || 'SYS').toUpperCase() + '] ' + l.message + '</div>').join('');
      shell.scrollTop = shell.scrollHeight;
    }

    const waStatus = el("wa-status");
    if (waStatus) {
      waStatus.innerHTML = '<span class="badge ' + data.connectionStatus + '">Status: ' + data.connectionStatus + '</span>';
    }

    if (data.connectionStatus !== "open" && data.pairingCode) {
      if (el("pairing-section")) el("pairing-section").style.display = "block";
      if (el("wa-active")) el("wa-active").style.display = "none";
      if (el("pairing-code")) el("pairing-code").textContent = data.pairingCode;
    } else {
      if (el("pairing-section")) el("pairing-section").style.display = "none";
      if (el("wa-active")) el("wa-active").style.display = data.connectionStatus === "open" ? "block" : "none";
    }

    const cronBox = el("cron");
    if (data.cron && cronBox) {
      cronBox.innerHTML = Object.keys(AGENT_META).map(name => {
        const remaining = Math.max(0, Math.round((AGENT_META[name].interval - (Date.now() - (data.cron.lastTrigger[name]||0)) % AGENT_META[name].interval) / 1000));
        return '<div class="cron-line"><span style="color:'+AGENT_META[name].color+'">'+name+'</span><span>'+remaining+'s left</span></div>';
      }).join('');
    }

    const timeline = el("timeline");
    if (data.assignments && timeline) {
      timeline.innerHTML = data.assignments.map(a => {
        const display = a.agent.split("-v")[0];
        const color = (AGENT_META[display] ? AGENT_META[display].color : "#fff");
        return '<div class="tm-entry" title="' + a.task + '"><span class="tm-agent" style="color:' + color + '">' + display + '</span><span style="flex:1; margin:0 10px; color:var(--muted); white-space: nowrap; overflow: hidden; text-overflow: ellipsis;">' + a.task.substring(0,50) + '</span><b class="badge">' + a.state.toUpperCase() + '</b></div>';
      }).join('') || '<div style="padding:10px; color:var(--muted)">No tasks.</div>';
    }

    const podsBox = el("pods");
    if (data.pods && podsBox) {
      podsBox.innerHTML = data.pods.map(p => {
        const accent = (AGENT_META[p.activeActor] ? AGENT_META[p.activeActor].color : '#333');
        return '<div class="stat-box" style="border-left: 4px solid ' + accent + '">' +
               '<b>' + p.name + '</b><br>' +
               '<span style="font-size:0.7em; color:var(--muted)">IP: ' + p.ip + '</span><br>' +
               '<span style="font-size:0.75em; color:var(--muted); margin-top:5px; display:block;">ACTIVE: <b style="color:' + accent + '">' + p.activeActor + '</b></span></div>';
      }).join('');
    }

    const actorsBox = el("actors");
    if (actorsBox) {
      actorsBox.innerHTML = Object.keys(AGENT_META).map(name => {
        const a = (data.actors || []).find((x) => x.displayName === name) || { displayName: name, status: "IDLE", ip: "n/a", pod: "none" };
        const color = AGENT_META[name].color;
        const triggerFn = "trigger('" + name + "')";
        return '<div class="stat-box" style="border-left: 4px solid ' + color + '">' +
               '<div style="display:flex; justify-content:space-between;"><b>' + a.displayName + '</b> <span class="badge ' + a.status.toLowerCase() + '">' + a.status + '</span></div>' +
               '<div style="display:flex; justify-content:space-between; align-items:flex-end; margin-top:5px;">' +
               '<div style="font-size:0.7em; color:var(--muted);">IP: ' + a.ip + '<br>POD: ' + a.pod + '</div>' +
               '<button class="btn" style="background:'+color+'; color:#000;" onclick="' + triggerFn + '">Pulse</button></div></div>';
      }).join('');
    }

    const auditBody = document.querySelector("#audit-table tbody");
    if (data.audits && auditBody) {
      auditBody.innerHTML = data.audits.map(a => {
        const display = a.agent.split("-v")[0];
        const color = (AGENT_META[display] ? AGENT_META[display].color : "#fff");
        return '<tr><td>' + a.timestamp + '</td><td style="color:' + color + '; font-weight:800;">' + display + '</td><td style="color:var(--cyan)">' + a.task.substring(0,30) + '</td><td style="white-space:pre-wrap; color:#d1d5db;">' + a.result.substring(0,100) + '</td></tr>';
      }).join('');
    }

  } catch (e) {
    console.error("UI Refresh Error:", e);
  }
}
setInterval(refresh, 1000); refresh();
</script>
</body></html>
  `;
  return c.html(html);
});

const port = 8090;
serve({ fetch: app.fetch, port, hostname: "0.0.0.0" });
syncState();
console.log(`Managed Dashboard active on port ${port}`);
