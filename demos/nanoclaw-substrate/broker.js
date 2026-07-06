"use strict";
var import_hono = require("hono");
var import_node_server = require("@hono/node-server");
var import_node_child_process = require("node:child_process");
var import_node_fs = require("node:fs");
var import_pino = require("pino");
var import_baileys = require("@whiskeysockets/baileys");

var app = new import_hono.Hono();
var AUTH_DIR = "/app/store/auth/persistent_v5"; 
var PHONE_NUMBER = "16503360539";
var TEMPLATE = "nanoclaw-rotated/nanoclaw-v6-ubuntu-v62"; 
var ATE_ENDPOINT = "api.ate-system.svc.cluster.local:443";
var GEMINI_KEY = "REDACTED"; 

var connectionStatus = "closed";
var pairingCode = null;
var qrCode = null;
var agentsList = ["nano-luna-v9062", "nano-mars-v9062", "nano-nova-v9062"];
var activeProcessors = new Set();
var taskQueues = {};
var pendingTasks = {};
var taskResults = {};
var globalSock = null;
var roundRobinIndex = 0;

var brokerLogs = [];
var assignments = [];
var taskAudits = [];

var lastTriggerTime = { "agent-luna": Date.now(), "agent-mars": Date.now(), "agent-nova": Date.now() };
var cronIterations = { "agent-luna": 0, "agent-mars": 0, "agent-nova": 0 };

var log = (module, message, level = "info") => {
  const ts = new Date().toISOString().slice(11, 19);
  const entry = { timestamp: ts, module, message, level };
  brokerLogs.push(entry);
  if (brokerLogs.length > 100) brokerLogs.shift();
  console.log("[" + ts + "] [" + module + "] " + message);
};

var AGENT_TASKS = [
  "Analyze this repository's Dockerfiles for root-privilege vulnerabilities.",
  "Trace the dependency graph of this TypeScript project.",
  "Review GDPR data-sovereignty requirements for multi-page contracts.",
  "Scan 500 lines of production logs for commit correlation.",
  "Generate OpenAPI 3.0 specs for authentication endpoints."
];

// Load the "Modified NanoClaw" logic from disk
const FINAL_AGENT_SCRIPT = import_node_fs.readFileSync(__dirname + "/agent.js", "utf8");

app.get("/agent.js", (c) => c.text(FINAL_AGENT_SCRIPT));

app.get("/api/get-task", (c) => {
    const agentEntry = Object.entries(pendingTasks)[0];
    if (agentEntry) {
        const [actorId, task] = agentEntry;
        log("debug", "Assigning: " + actorId);
        delete pendingTasks[actorId];
        return c.json({ actorId, task });
    }
    return c.json({ task: null });
});

app.post("/api/task-result", async (c) => {
    const data = await c.req.json();
    log("substrate", "Result Push: " + (data.actorId || "unknown"));
    taskResults[data.actorId] = data.result;
    return c.json({ ok: true });
});

function setupCron(agentKey, interval) {
  setInterval(() => {
    const actorId = agentKey.includes("luna") ? "nano-luna-v9062" : agentKey.includes("mars") ? "nano-mars-v9062" : "nano-nova-v9062";
    cronIterations[agentKey]++;
    lastTriggerTime[agentKey] = Date.now();
    log("cron", `Triggering ${agentKey} (Pulse #${cronIterations[agentKey]})`);
    queueTask(actorId, `[CRON] Pulse ${cronIterations[agentKey]}`, "SYSTEM_CRON");
  }, interval);
}

async function connectToWhatsApp() {
  const { state, saveCreds } = await (0, import_baileys.useMultiFileAuthState)(AUTH_DIR);
  const sock = (0, import_baileys.default)({ auth: state, logger: (0, import_pino.pino)({ level: "silent" }), browser: ["Chrome (Linux)", "Chrome", "114.0.0.0"] });
  if (!sock.authState.creds.registered) {
    setTimeout(async () => {
      try {
        if (connectionStatus === "open") return;
        const code = await sock.requestPairingCode(PHONE_NUMBER);
        pairingCode = code;
        log("whatsapp", "NEW LINK CODE: " + pairingCode);
      } catch (e) { log("whatsapp", "Pair Fail: " + e.message); }
    }, 35000);
  }
  sock.ev.on("connection.update", (u) => { 
    const { connection, lastDisconnect, qr } = u;
    if (connection) connectionStatus = connection;
    if (qr) qrCode = qr;
    if (connection === "open") { log("whatsapp", "WHATSAPP BRIDGE LIVE"); pairingCode = null; qrCode = null; }
  });
  sock.ev.on("creds.update", saveCreds);
  sock.ev.on("messages.upsert", async (m) => {
    if (m.type !== 'notify') return;
    const msg = m.messages[0];
    if (!msg.message) return;
    const text = (msg.message.conversation || msg.message.extendedTextMessage?.text || "").toLowerCase();
    if (!text || text.includes("✅")) return;
    const from = msg.key.remoteJid;
    log("whatsapp", "Received: " + text);
    if (text.startsWith("/burst")) {
      const count = Math.min(parseInt(text.split(" ")[1]) || 5, 15);
      await sock.sendMessage(from, { text: "🚀 Queuing " + count + " tasks..." });
      for (let i=0; i<count; i++) queueTask(agentsList[i % 3], "[BURST] " + AGENT_TASKS[Math.floor(Math.random() * AGENT_TASKS.length)], from);
      return;
    }
    queueTask(agentsList[roundRobinIndex++ % 3], text, from);
  });
  globalSock = sock;
}

function queueTask(actorId, task, sender) {
  if (!taskQueues[actorId]) taskQueues[actorId] = [];
  taskQueues[actorId].push({ task, sender });
  assignments.push({ id: Date.now() + Math.random(), agent: actorId, task, state: "queued", created_at: Date.now()/1000 });
  if (assignments.length > 50) assignments.shift();
  processQueue(actorId);
}

async function processQueue(actorId) {
  if (activeProcessors.has(actorId) || !taskQueues[actorId]?.length) return;
  activeProcessors.add(actorId);
  const { task, sender } = taskQueues[actorId].shift();
  const asg = assignments.find(a => a.agent === actorId && a.state === "queued");
  if (asg) asg.state = "running";
  try {
      pendingTasks[actorId] = task;
      taskResults[actorId] = null;
      (0, import_node_child_process.exec)("/tmp/kubectl-ate --endpoint " + ATE_ENDPOINT + " resume actor " + actorId);
      for (let i=0; i<90; i++) {
          if (taskResults[actorId]) break;
          await new Promise(r => setTimeout(r, 1000));
      }
      if (asg) { asg.state = "completed"; asg.completed_at = Date.now()/1000; }
      taskAudits.push({ id: Date.now(), agent: actorId, timestamp: new Date().toISOString().slice(11,19), task, result: taskResults[actorId] || "Timeout", status: "success" });
      if (taskAudits.length > 50) taskAudits.shift();
      if (globalSock && sender && sender.includes("@") && !task.includes("[CRON]")) await globalSock.sendMessage(sender, { text: "✅ " + actorId + ":\n" + (taskResults[actorId] || "Timeout") });
      (0, import_node_child_process.exec)("/tmp/kubectl-ate --endpoint " + ATE_ENDPOINT + " suspend actor " + actorId);
  } catch (e) {}
  activeProcessors.delete(actorId);
  setTimeout(() => processQueue(actorId), 1000);
}

app.get("/status", (c) => c.json({ connectionStatus, pairingCode, qrCode, logs: brokerLogs, assignments: [...assignments].reverse(), audits: taskAudits, cron: { lastTrigger: lastTriggerTime, iterations: cronIterations } }));

(0, import_node_server.serve)({ fetch: app.fetch, port: 8091, hostname: "0.0.0.0" }, () => {
  log("sys", "NanoClaw Broker v67 Online");
  connectToWhatsApp().catch(e => log("error", "Init Fail: " + e.message));
  setupCron("agent-luna", 120000); setupCron("agent-mars", 300000); setupCron("agent-nova", 600000);
});
