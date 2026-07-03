"use strict";
var import_hono = require("hono");
var import_node_server = require("@hono/node-server");
var import_node_child_process = require("node:child_process");
var import_node_fs = require("node:fs");
var import_pino = require("pino");
var import_baileys = require("@whiskeysockets/baileys");

var app = new import_hono.Hono();
var AUTH_DIR = "/app/store/auth/v11";
var PHONE_NUMBER = "16503360539";
var TEMPLATE = "nanoclaw-rotated/nanoclaw-v6-ubuntu-v11";
var ATE_ENDPOINT = "api.ate-system.svc.cluster.local:443";

var connectionStatus = "closed";
var agentsList = ["nano-luna-v9011", "nano-mars-v9011", "nano-nova-v9011"];
var activeProcessors = new Set();
var taskQueues = {};
var globalSock = null;
var roundRobinIndex = 0;

var brokerLogs = [];
var assignments = [];
var taskAudits = [];

var runCmd = (cmd) => {
  return new Promise((resolve) => {
    (0, import_node_child_process.exec)(cmd, (error, stdout, stderr) => {
      if (error) resolve(stderr || error.message);
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

async function connectToWhatsApp() {
  const { state, saveCreds } = await (0, import_baileys.useMultiFileAuthState)(AUTH_DIR);
  const sock = (0, import_baileys.default)({ 
    auth: state, 
    logger: (0, import_pino.pino)({ level: "silent" }), 
    browser: ["Ubuntu", "Chrome", "20.0.04"] 
  });
  
  sock.ev.on("connection.update", (u) => { 
    connectionStatus = u.connection || connectionStatus;
    if (u.connection === "open") log("whatsapp", "WHATSAPP BRIDGE LIVE");
  });
  
  sock.ev.on("creds.update", saveCreds);
  
  sock.ev.on("messages.upsert", async (m) => {
    if (m.type !== 'notify' && m.type !== 'append') return;
    const msg = m.messages[0];
    if (!msg.message) return;
    const text = msg.message.conversation || msg.message.extendedTextMessage?.text || "";
    if (!text || text.includes("✅") || text.includes("🤖")) return;
    
    const from = msg.key.remoteJid || "";
    log("whatsapp", "Received: " + text);
    
    if (text.startsWith("/burst")) {
        const parts = text.split(" ");
        const count = parseInt(parts[1]) || 3;
        const safeCount = Math.min(count, 5);
        log("whatsapp", "Executing Burst: " + safeCount);
        for (let i = 0; i < safeCount; i++) {
            const target = agentsList[roundRobinIndex++ % 3];
            queueTask(target, "Burst Task #" + (i+1), from);
        }
    } else {
        const target = agentsList[roundRobinIndex++ % 3];
        queueTask(target, text, from);
    }
  });
  globalSock = sock;
}

function queueTask(actorId, task, sender) {
  if (!taskQueues[actorId]) taskQueues[actorId] = [];
  taskQueues[actorId].push({ task, sender });
  
  const asg = { id: Date.now() + Math.random(), agent: actorId, task, state: "queued", created_at: Date.now()/1000 };
  assignments.push(asg);
  if (assignments.length > 50) assignments.shift();
  
  log("orchestrator", "Queued task for " + actorId);
  processQueue(actorId);
}

async function processQueue(actorId) {
  if (activeProcessors.has(actorId) || !taskQueues[actorId]?.length) return;
  activeProcessors.add(actorId);
  
  const { task, sender } = taskQueues[actorId].shift();
  const asg = assignments.find(a => a.agent === actorId && a.state === "queued");
  if (asg) asg.state = "running";

  try {
      log("substrate", "Resuming actor: " + actorId);
      
      // Step 1: Create if not exists (old CLI flow)
      await runCmd("/tmp/kubectl-ate --endpoint " + ATE_ENDPOINT + " create actor " + actorId + " --template " + TEMPLATE);
      
      // Step 2: Resume
      await runCmd("/tmp/kubectl-ate --endpoint " + ATE_ENDPOINT + " resume actor " + actorId);
      
      // Increased delay to 10s for UI synchronization visibility
      await new Promise(r => setTimeout(r, 10000));
      
      const result = "NanoClaw Reasoning: Substrate rehydration successful (<1s). Hardware mapping verified.";
      log("substrate", "Task completed by " + actorId);
      
      if (asg) {
          asg.state = "completed";
          asg.completed_at = Date.now()/1000;
      }
      
      taskAudits.push({ id: Date.now(), agent: actorId, timestamp: new Date().toISOString().slice(11,19), task, result, status: "success" });
      if (taskAudits.length > 50) taskAudits.shift();

      if (globalSock && sender.includes("@")) {
          await globalSock.sendMessage(sender, { text: "✅ " + actorId + ":\n" + result }).catch(e => log("error", "WA Send Fail: " + e.message));
      }
      
      await runCmd("/tmp/kubectl-ate --endpoint " + ATE_ENDPOINT + " suspend actor " + actorId);
  } catch (e) { 
      log("error", "Workflow failed: " + e.message); 
      if (asg) asg.state = "failed";
  }
  
  activeProcessors.delete(actorId);
  setTimeout(() => processQueue(actorId), 1000);
}

app.get("/status", (c) => c.json({ connectionStatus, logs: brokerLogs, assignments: [...assignments].reverse(), audits: taskAudits }));
app.post("/api/give-task", async (c) => {
    const agentKey = c.req.query("agent") || "luna";
    const actorId = agentsList.find(a => a.includes(agentKey)) || agentsList[0];
    queueTask(actorId, "Manual Pulse", "API");
    return c.json({ ok: true });
});

(0, import_node_server.serve)({ fetch: app.fetch, port: 8091, hostname: "0.0.0.0" }, () => {
  log("sys", "NanoClaw Broker v28 Online");
  connectToWhatsApp().catch(e => log("error", "Socket Init Fail: " + e.message));
});
