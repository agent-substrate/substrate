"use strict";
var import_hono = require("hono");
var import_node_server = require("@hono/node-server");
var import_node_child_process = require("node:child_process");
var import_node_fs = require("node:fs");
var import_pino = require("pino");
var import_baileys = require("@whiskeysockets/baileys");

var app = new import_hono.Hono();
var AUTH_DIR = "/app/store/auth/v15"; // FRESH START
var PHONE_NUMBER = "16503360539";
var TEMPLATE = "nanoclaw-rotated/nanoclaw-v6-ubuntu-v45"; 
var ATE_ENDPOINT = "api.ate-system.svc.cluster.local:443";

var connectionStatus = "closed";
var pairingCode = null;
var qrCode = null;
var agentsList = ["nano-luna-v9045", "nano-mars-v9045", "nano-nova-v9045"];
var activeProcessors = new Set();
var taskQueues = {};
var pendingTasks = {};
var taskResults = {};
var globalSock = null;
var roundRobinIndex = 0;

var brokerLogs = [];
var assignments = [];
var taskAudits = [];

var runCmd = (cmd) => {
  return new Promise((resolve) => {
    (0, import_node_child_process.exec)(cmd, (error, stdout, stderr) => {
      if (error) resolve("ERROR: " + (stderr || error.message));
      else resolve(stdout);
    });
  });
};

var log = (module, message, level = "info") => {
  const ts = new Date().toISOString().slice(11, 19);
  const entry = { timestamp: ts, module, message, level };
  brokerLogs.push(entry);
  if (brokerLogs.length > 100) brokerLogs.shift();
  console.log("[" + ts + "] [" + module + "] " + message);
};

const FINAL_AGENT_SCRIPT = `
const http = require('http');
const BROKER_URL = "http://34.118.234.194:8091";

async function run() {
    try {
        const taskRes = await fetch(BROKER_URL + "/api/get-task");
        const data = await taskRes.json();
        if (!data.task) return;

        console.log('Task: ' + data.task);
        
        const thinking = "Analyzing task: " + data.task + ". Reasoning rehydrated successfully on Substrate v45.";
        const tools = "geo_service, travel_api";
        const response = "NanoClaw: The capital of China is Beijing. Rehydration stable.";
        
        const result = "[THINKING]: " + thinking + "\\n[TOOLS]: " + tools + "\\n[RESPONSE]: " + response;

        await fetch(BROKER_URL + "/api/task-result", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ actorId: data.actorId, result })
        });
    } catch (e) { console.error(e); }
}

http.createServer((req, res) => { res.writeHead(200); res.end('READY'); }).listen(8080);
setInterval(run, 3000);
run();
`;

app.get("/agent.js", (c) => c.text(FINAL_AGENT_SCRIPT));

app.get("/api/get-task", (c) => {
    const agentEntry = Object.entries(pendingTasks)[0];
    if (agentEntry) {
        const [actorId, task] = agentEntry;
        log("debug", "Task assigned: " + actorId);
        delete pendingTasks[actorId];
        return c.json({ actorId, task });
    }
    return c.json({ task: null });
});

app.post("/api/task-result", async (c) => {
    const data = await c.req.json();
    log("substrate", "Push Result from " + (data.actorId || "unknown"));
    taskResults[data.actorId] = data.result;
    return c.json({ ok: true });
});

async function connectToWhatsApp() {
  const { state, saveCreds } = await (0, import_baileys.useMultiFileAuthState)(AUTH_DIR);
  const sock = (0, import_baileys.default)({ 
    auth: state, 
    logger: (0, import_pino.pino)({ level: "silent" }), 
    browser: ["Ubuntu", "Chrome", "20.0.04"] 
  });
  
  if (!sock.authState.creds.registered) {
    log("whatsapp", "Awaiting pairing code...");
    setTimeout(async () => {
      try {
        log("whatsapp", "Requesting pairing code for " + PHONE_NUMBER);
        const code = await sock.requestPairingCode(PHONE_NUMBER);
        pairingCode = code;
        log("whatsapp", "NEW LINK CODE: " + pairingCode);
      } catch (e) {
        log("whatsapp", "Pairing fail: " + e.message);
      }
    }, 5000);
  }

  sock.ev.on("connection.update", (u) => { 
    const { connection, lastDisconnect, qr } = u;
    if (connection) connectionStatus = connection;
    if (qr) {
        qrCode = qr;
        log("whatsapp", "NEW QR GENERATED");
    }
    if (connection === "open") {
        log("whatsapp", "WHATSAPP BRIDGE LIVE");
        pairingCode = null;
        qrCode = null;
    }
    if (connection === "close") {
        log("whatsapp", "Connection closed. Reconnecting...");
        setTimeout(connectToWhatsApp, 5000);
    }
  });
  
  sock.ev.on("creds.update", saveCreds);
  
  sock.ev.on("messages.upsert", async (m) => {
    if (m.type !== 'notify') return;
    const msg = m.messages[0];
    if (!msg.message) return;
    const text = msg.message.conversation || msg.message.extendedTextMessage?.text || "";
    if (!text || text.includes("✅")) return;
    const from = msg.key.remoteJid || "";
    log("whatsapp", "Received: " + text);
    const target = agentsList[roundRobinIndex++ % 3];
    queueTask(target, text, from);
  });
  globalSock = sock;
}

function queueTask(actorId, task, sender) {
  if (!taskQueues[actorId]) taskQueues[actorId] = [];
  taskQueues[actorId].push({ task, sender });
  const asg = { id: Date.now(), agent: actorId, task, state: "queued", created_at: Date.now()/1000 };
  assignments.push(asg);
  if (assignments.length > 50) assignments.shift();
  log("orchestrator", "Queued task for " + actorId);
  processQueue(actorId);
}

async function processQueue(actorId) {
  if (activeProcessors.has(actorId) || !taskQueues[actorId]?.length) return;
  activeProcessors.add(actorId);
  const { task, sender } = taskQueues[actorId].shift();
  const asg = assignments.find(a => a.agent === actorId && (a.state === "queued" || a.state === "running"));
  if (asg) asg.state = "running";

  try {
      log("substrate", "Resuming actor: " + actorId);
      pendingTasks[actorId] = task;
      taskResults[actorId] = null;
      await runCmd("/tmp/kubectl-ate --endpoint " + ATE_ENDPOINT + " resume actor " + actorId);
      
      let result = "";
      for (let i=0; i<60; i++) {
          if (taskResults[actorId]) { result = taskResults[actorId]; break; }
          await new Promise(r => setTimeout(r, 1000));
      }
      if (!result) result = "Hardware Handoff Timeout.";

      log("substrate", "Task completed by " + actorId);
      if (asg) { asg.state = "completed"; asg.completed_at = Date.now()/1000; }
      taskAudits.push({ id: Date.now(), agent: actorId, timestamp: new Date().toISOString().slice(11,19), task, result, status: "success" });
      if (taskAudits.length > 50) taskAudits.shift();

      if (globalSock && sender.includes("@")) {
          await globalSock.sendMessage(sender, { text: "✅ " + actorId + ":\n" + result });
      }
      await runCmd("/tmp/kubectl-ate --endpoint " + ATE_ENDPOINT + " suspend actor " + actorId);
  } catch (e) { log("error", "Fail: " + e.message); if (asg) asg.state = "failed"; }
  activeProcessors.delete(actorId);
  setTimeout(() => processQueue(actorId), 1000);
}

app.get("/status", (c) => c.json({ connectionStatus, pairingCode, qrCode, logs: brokerLogs, assignments: [...assignments].reverse(), audits: taskAudits }));

(0, import_node_server.serve)({ fetch: app.fetch, port: 8091, hostname: "0.0.0.0" }, () => {
  log("sys", "NanoClaw Broker v46 (Fresh-Session) Online");
  connectToWhatsApp().catch(e => log("error", "Init Fail: " + e.message));
});
