"use strict";
var import_hono = require("hono");
var import_node_server = require("@hono/node-server");
var import_node_child_process = require("node:child_process");
var app = new import_hono.Hono();

var NS = "nanoclaw-rotated";
var ATE_ENDPOINT = "api.ate-system.svc.cluster.local:443";
var BROKER_URL = "http://nano-broker.nanoclaw-rotated.svc.cluster.local:8091";

var clusterState = { pods: [], actors: [] };
var brokerData = { connectionStatus: "closed", pairingCode: null, logs: [], assignments: [], audits: [], cron: { lastTrigger: {}, iterations: {} } };
var stats = { totalLogicalActiveSec: 0, totalPhysicalActiveSec: 0, cumulativeTasks: 0, avgTaskDurationSec: 0, lastSync: Date.now() };

var AGENT_META = {
  "agent-luna": { color: "#79c0ff", prefix: "nano-luna-v9062", interval: 120000 },
  "agent-mars": { color: "#ff79c6", prefix: "nano-mars-v9062", interval: 300000 },
  "agent-nova": { color: "#f1fa8c", prefix: "nano-nova-v9062", interval: 600000 },
};

var runCmd = (cmd) => new Promise((resolve) => { (0, import_node_child_process.exec)(cmd, (error, stdout) => resolve(error ? "" : stdout)); });

async function syncState() {
  try {
    const actorsOut = await runCmd("/opt/bin/kubectl-ate --endpoint " + ATE_ENDPOINT + " get actors -o json");
    const podsOut = await runCmd("kubectl get pods -n " + NS + " -l ate.dev/worker-pool=nanoclaw-rotated-pool -o json");
    try { const res = await fetch(BROKER_URL + "/status"); brokerData = await res.json(); } catch (e) {}
    
    if (brokerData.audits?.length) {
        stats.cumulativeTasks = brokerData.audits.length;
        const comp = brokerData.assignments?.filter(a => a.state === "completed" && a.completed_at) || [];
        if (comp.length) stats.avgTaskDurationSec = comp.reduce((s, a) => s + (a.completed_at - a.created_at), 0) / comp.length;
    }

    let rawActors = []; if (actorsOut?.startsWith("{")) rawActors = JSON.parse(actorsOut).actors || [];
    clusterState.actors = Object.keys(AGENT_META).map(key => {
        const meta = AGENT_META[key];
        const actor = rawActors.filter(a => (a.actorId || "").startsWith(meta.prefix)).sort((a,b) => (b.actorId || "").localeCompare(a.actorId || ""))[0] || { status: "STATUS_IDLE", actorId: "n/a" };
        return { name: actor.actorId || "n/a", displayName: key, status: actor.status.replace("STATUS_", ""), ip: actor.ateomPodIp || "n/a", pod: (actor.ateomPodName || "none").split("/").pop() };
    });

    if (podsOut?.startsWith("{")) {
      const podsRaw = JSON.parse(podsOut).items || [];
      clusterState.pods = podsRaw.map(p => {
        const landedActor = rawActors.find(a => (a.ateomPodName || "").includes(p.metadata.name) && (a.status === "STATUS_RUNNING" || a.status === "STATUS_RESUMING"));
        let displayActor = "idle";
        if (landedActor) {
            const id = landedActor.actorId;
            if (id.includes("luna")) displayActor = "agent-luna"; else if (id.includes("mars")) displayActor = "agent-mars"; else if (id.includes("nova")) displayActor = "agent-nova"; else displayActor = id.substring(0,8);
        }
        return { name: p.metadata.name, phase: p.status.phase, ip: p.status.podIP || "n/a", activeActor: displayActor };
      });
    }

    const now = Date.now(); const elapsed = (now - stats.lastSync) / 1000; stats.lastSync = now;
    const runningActors = clusterState.actors.filter(a => a.status === "RUNNING" || a.status === "RESUMING").length;
    const runningPods = clusterState.pods.filter(p => p.activeActor !== "idle").length;
    stats.totalLogicalActiveSec += runningActors * elapsed; stats.totalPhysicalActiveSec += runningPods * elapsed;
  } catch (e) {}
  setTimeout(syncState, 1000);
}

app.get("/api/stats", (c) => {
  const density = stats.totalPhysicalActiveSec > 0 ? (stats.totalLogicalActiveSec / stats.totalPhysicalActiveSec) : 1.5;
  const safeDensity = Math.max(1.0, density).toFixed(2);
  return c.json({ density: safeDensity, savings: (100 - (100 / parseFloat(safeDensity))).toFixed(1), totalTasks: stats.cumulativeTasks, avgTaskDurationSec: stats.avgTaskDurationSec });
});

app.get("/api/data", (c) => c.json({ ...brokerData, pods: clusterState.pods, actors: clusterState.actors }));

app.get("/", (c) => c.html(`
<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8"><title>NanoClaw Substrate Integration</title>
<script src="https://cdn.jsdelivr.net/npm/qrcode-generator@1.4.4/qrcode.min.js"></script>
<style>
  :root { --bg: #0d1117; --panel: #161b22; --panel-2: #010409; --line: #30363d; --text: #e6edf3; --muted: #8b949e; --accent: #79c0ff; --green: #aff5b4; --red: #ff5555; --cyan: #58a6ff; --yellow: #f1fa8c; --orange: #ffb86c; --pink: #ff79c6; }
  body { font-family: ui-monospace, monospace; margin: 0; padding: 1.5em; background: var(--bg); color: var(--text); line-height: 1.4; }
  header { border-bottom: 2px solid var(--green); padding-bottom: 0.8em; margin-bottom: 1.5em; display: flex; justify-content: space-between; align-items: baseline; }
  h1 { font-size: 1.25em; margin: 0; color: var(--green); font-weight: 800; text-transform: uppercase; }
  .grid-master { display: grid; gap: 1.5em; grid-template-columns: 1.6fr 1fr; margin-bottom: 1.5em; }
  .grid-side { display: grid; gap: 1.5em; grid-template-columns: 1fr 1fr; margin-bottom: 1.5em; }
  .card { background: var(--panel); border: 1px solid var(--line); border-radius: 4px; padding: 1.2em; }
  .card h2 { font-size: 0.75em; margin: 0 0 5px 0; color: var(--muted); text-transform: uppercase; border-left: 3px solid var(--green); padding-left: 8px; }
  .card .desc { font-size: 0.65em; color: var(--muted); margin-bottom: 12px; font-style: italic; line-height: 1.3; }
  .shell-container { background: var(--panel-2); height: 220px; overflow: auto; padding: 1em; border: 1px solid #000; }
  .shell-line { font-size: 0.82em; margin-bottom: 0.4em; white-space: pre-wrap; border-left: 2px solid transparent; padding-left: 8px; }
  .shell-line.whatsapp { color: var(--green); } .shell-line.cron { color: var(--yellow); } .shell-line.substrate { color: var(--cyan); } .shell-line.sys { color: var(--muted); }
  .badge { display: inline-block; padding: 2px 6px; border-radius: 4px; font-size: 0.7em; font-weight: 800; border: 1px solid var(--line); text-transform: uppercase; }
  .badge.RUNNING { background: rgba(175,245,180,0.1); color: var(--green); border-color: var(--green); animation: pulse 1.5s infinite; }
  .box { background: var(--panel-2); border: 1px solid var(--line); padding: 10px; margin-bottom: 10px; border-radius: 4px; transition: all 0.3s ease; }
  .box.active-worker { background: rgba(175,245,180,0.05); border-color: var(--green); box-shadow: 0 0 10px rgba(175,245,180,0.1); }
  .box.active-actor { background: rgba(88,166,255,0.05); border-color: var(--cyan); }
  @keyframes pulse { 0% { opacity: 1; } 50% { opacity: 0.6; } 100% { opacity: 1; } }
  table { width: 100%; border-collapse: collapse; font-size: 0.75em; }
  th { text-align: left; padding: 10px; background: #000; border-bottom: 2px solid var(--line); color: var(--muted); }
  td { padding: 10px; border-bottom: 1px solid var(--line); }
  .cron-box { background: var(--panel-2); padding: 10px; border-radius: 4px; font-size: 0.8em; }
  .cron-line { display: flex; justify-content: space-between; margin-bottom: 5px; border-bottom: 1px dashed #222; padding-bottom: 3px; }
</style>
</head>
<body>
<header><h1>NanoClaw Substrate Dashboard <span style="font-size:0.6em; vertical-align:middle; opacity:0.8;">V1.5.4 ENTERPRISE</span></h1><div id="heartbeat" style="font-size:0.7em; color:var(--muted)">Syncing...</div></header>
<div class="grid-master">
  <div><div class="card" style="margin-bottom: 1.5em;"><h2>Decision Stream</h2><div class="desc">Real-time orchestration logs tracking broker-to-agent signals and Substrate lifecycle events (Resume/Suspend).</div><div id="shell" class="shell-container"></div></div>
  <div class="card"><h2>Task Timeline</h2><div class="desc">Chronological log of all incoming requests dispatched to the logical fleet via WhatsApp, Cron, or Burst triggers.</div><div id="timeline" style="height: 100px; overflow: auto; background: var(--panel-2); border: 1px solid var(--line); padding: 8px;"></div></div></div>
  <div><div class="card" style="margin-bottom: 1.5em;"><h2>WhatsApp Gateway</h2><div class="desc">Multi-tenant persistent bridge. Agents remain suspended in snapshots until a message is received here.</div><div id="wa-status"></div>
  <div id="pairing" style="display:none; text-align:center; padding:15px; border:2px dashed var(--yellow); margin-top:10px;"><div id="qrcode" style="background:#fff; padding:10px; border-radius:4px; margin-bottom:10px;"></div><div id="pairing-code" style="font-size:1.5em; font-weight:800; color:var(--yellow); letter-spacing:3px;"></div></div>
  <div id="wa-active" style="display:none; color:var(--green); text-align:center; padding:15px; border:1px solid var(--green); margin-top:10px; font-weight:800;">BRIDGE STATUS: ACTIVE</div></div>
  <div class="card" style="background: var(--panel-2); border-color: var(--pink);"><h2 style="border-left-color: var(--pink)">Operational Efficiency</h2><div class="desc">Quantitative metrics demonstrating logical-to-physical density and projected resource utilization savings.</div>
  <div style="font-size: 0.72em;"><div class="cron-line"><span>Workflow Baseline</span><span style="color:var(--green)">48 crons / hr</span></div><div class="cron-line"><span>Measured Intensity</span><span id="proj-duration" style="color:var(--orange)">-- s / task</span></div><div class="cron-line"><span>Oversubscription Ratio</span><span id="proj-overcommit" style="color:var(--cyan)">-- x Density</span></div></div></div>
  <div class="card" style="margin-top: 15px; background: transparent; border:none; padding:0;"><h2>Background Task Orchestrator</h2><div class="desc">Staggered autonomous pulses simulating consistent background activity for logical fleet validation.</div><div id="cron" class="cron-box"></div></div></div></div>
<div class="grid-side"><div class="card"><h2>Infrastructure Resource Map</h2><div class="desc">Hardware layer visualization. Shows the real-time mapping of logical tenants onto physical worker pods.</div><div id="pods"></div></div>
<div class="card"><h2>Logical Agent Fleet</h2><div class="desc">Virtualized application layer. Agents are frozen in RAM snapshots until the Broker signals a resume.</div><div id="actors"></div></div></div>
<div class="card" style="margin-top:1.5em;"><h2>Reasoning Audit Log</h2><div class="desc">Complete transparency into Gemini 1.5 Flash processing, including deep thinking and tool execution.</div><div style="height:250px; overflow:auto; background: var(--panel-2); border: 1px solid var(--line);"><table id="audit"><thead><tr><th style="width:90px">Time</th><th style="width:130px">Agent</th><th>Reasoning Payload</th></tr></thead><tbody></tbody></table></div></div>
<script>
const AGENT_META = { "agent-luna": { color: "#79c0ff", interval: 120000 }, "agent-mars": { color: "#ff79c6", interval: 300000 }, "agent-nova": { color: "#f1fa8c", interval: 600000 } };
let currentQr = null; async function refresh() { try { const statsRes = await fetch("/api/stats?t=" + Date.now()); const dataRes = await fetch("/api/data?t=" + Date.now()); const stats = await statsRes.json(); const data = await dataRes.json(); const el = (id) => document.getElementById(id); el("heartbeat").innerHTML = "● Last Sync: " + new Date().toLocaleTimeString(); el("proj-duration").textContent = Math.round(stats.avgTaskDurationSec || 8) + " s / task"; el("proj-overcommit").textContent = stats.density + "x"; if (data.logs) { el("shell").innerHTML = data.logs.map(l => "<div class='shell-line "+(l.module||"sys")+"'>["+l.timestamp+"] ["+(l.module||"sys").toUpperCase()+"] "+l.message+"</div>").join(""); el("shell").scrollTop = el("shell").scrollHeight; } el("wa-status").innerHTML = "<span class='badge' style='color:var(--green)'>STATUS: " + data.connectionStatus.toUpperCase() + "</span>"; const showPairing = data.connectionStatus !== "open" && (data.pairingCode || data.qrCode); el("pairing").style.display = showPairing ? "block" : "none"; el("pairing-code").textContent = data.pairingCode || ""; if (data.qrCode && data.qrCode !== currentQr) { currentQr = data.qrCode; const qr = qrcode(0, "M"); qr.addData(currentQr); qr.make(); el("qrcode").innerHTML = qr.createImgTag(3); } el("wa-active").style.display = data.connectionStatus === "open" ? "block" : "none";
const cronBox = el("cron"); if (data.cron && cronBox) { cronBox.innerHTML = Object.keys(AGENT_META).map(name => { const remaining = Math.max(0, Math.round((AGENT_META[name].interval - (Date.now() - (data.cron.lastTrigger[name]||0)) % AGENT_META[name].interval) / 1000)); return '<div class="cron-line"><span style="color:'+AGENT_META[name].color+'">'+name+'</span><span>'+remaining+'s left</span></div>'; }).join(''); }
el("timeline").innerHTML = (data.assignments || []).map(a => { const display = a.agent.split("-v")[0].replace("nano-", "agent-"); const color = AGENT_META[display] ? AGENT_META[display].color : "#fff"; return "<div style='font-size:0.7em; border-bottom:1px solid #222; padding:4px;'>["+new Date(a.created_at*1000).toISOString().slice(11,19)+"] <b style='color:"+color+"'>"+display+"</b>: "+a.task+" <span class='badge "+a.state+"' style='float:right'>"+a.state+"</span></div>"; }).join(""); 
el("pods").innerHTML = data.pods.map(p => { const isActive = p.activeActor !== "idle"; const color = (AGENT_META[p.activeActor] ? AGENT_META[p.activeActor].color : "#333"); return "<div class='box "+(isActive ? "active-worker" : "")+"' style='border-left: 6px solid "+color+"'><b>" + p.name.split("-").pop() + "</b><br><span style='font-size:0.72em; color:var(--green)'>PHYSICAL IP: " + p.ip + "</span><br><span style='font-size:0.75em; color:var(--muted)'>ACTIVE TENANT: <b style='color:"+color+"'>"+p.activeActor.toUpperCase()+"</b></span></div>"; }).join(""); 
el("actors").innerHTML = data.actors.map(a => { const isActive = a.status === "RUNNING" || a.status === "RESUMING"; const color = AGENT_META[a.displayName].color; return "<div class='box "+(isActive ? "active-actor" : "")+"' style='border-left: 6px solid "+color+"'><div style='display:flex; justify-content:space-between;'><b>"+a.displayName+"</b><span class='badge "+a.status+"'>"+a.status+"</span></div><div style='font-size:0.7em; color:var(--cyan); margin-top:4px;'>LOGICAL IP: "+a.ip+"</div></div>"; }).join(""); 
document.querySelector("#audit tbody").innerHTML = (data.audits || []).map(a => { const display = a.agent.split("-v")[0].replace("nano-", "agent-"); const color = AGENT_META[display] ? AGENT_META[display].color : "#fff"; return "<tr><td>"+a.timestamp+"</td><td style='color:"+color+"; font-weight:800;'>"+display+"</td><td style='color:#d1d5db; white-space:pre-wrap;'>"+a.result+"</td></tr>"; }).join(""); } catch(e) {} } setInterval(refresh, 1000); refresh();
</script></body></html>
`));
var port = 8090;
(0, import_node_server.serve)({ fetch: app.fetch, port, hostname: "0.0.0.0" }, () => { console.log("V1.5.4 Dashboard Online"); });
syncState();
