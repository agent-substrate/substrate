"use strict";
var import_hono = require("hono");
var import_node_server = require("@hono/node-server");
var import_node_child_process = require("node:child_process");
var import_node_fs = require("node:fs");
var import_pino = require("pino");
var import_baileys = require("@whiskeysockets/baileys");

var app = new import_hono.Hono();
var AUTH_DIR = "/app/store/auth/persistent_demo_v3"; 
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

// --- AGENT-LIKE COMPLEX TASKS ---
var AGENT_TASKS = [
  "Analyze this repository's Dockerfiles for root-privilege vulnerabilities and generate a hardened OCI-compliant manifest.",
  "Trace the dependency graph of this TypeScript project and identify circular imports that will break the build in a ESM environment.",
  "Review these three multi-page vendor contracts and flag any clauses that conflict with GDPR data-sovereignty requirements.",
  "Scan 500 lines of recent production logs and correlate them with CI/CD deployment timestamps to isolate the commit causing the 502 errors.",
  "Crawl this internal API documentation and generate a structured JSON specification (OpenAPI 3.0) for the authentication endpoints.",
  "Perform a cross-service trace analysis to identify N+1 query bottlenecks in the logical GraphQL resolver layer.",
  "Audit the Kubernetes resource requests vs actual usage for this fleet and suggest a VPA-aligned optimization strategy."
];

const FINAL_AGENT_SCRIPT = `
const http = require('http');
const BROKER_URL = "http://34.118.234.194:8091";
const GEMINI_KEY = "REDACTED";

async function run() {
    try {
        const taskRes = await fetch(BROKER_URL + "/api/get-task");
        const data = await taskRes.json();
        if (!data.task) return;

        const rawTask = data.task;
        const task = rawTask.includes(':') ? rawTask.split(':').pop().trim() : rawTask;
        console.log('REAL LIVE Reasoning for: ' + task);

        let result = "";
        try {
            const prompt = "You are a NanoClaw Agent on Substrate. Task: " + task + ". Output exactly in this format: [THINKING]: your logic [TOOLS]: tools used [RESPONSE]: your answer";
            const geminiRes = await fetch('https://generativelanguage.googleapis.com/v1beta/models/gemini-flash-latest:generateContent?key=' + GEMINI_KEY, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ contents: [{ parts: [{ text: prompt }] }] })
            });
            const gData = await geminiRes.json();
            if (gData.candidates?.[0]?.content?.parts[0]?.text) {
                result = gData.candidates[0].content.parts[0].text;
            } else {
                result = "[THINKING]: Model failed.\\n[TOOLS]: gemini_api\\n[RESPONSE]: Error: " + (gData.error?.message || "No response");
            }
        } catch (e) {
            result = "[THINKING]: Network error.\\n[TOOLS]: fetch\\n[RESPONSE]: Error: " + e.message;
        }

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
    queueTask(actorId, `Pulse ${cronIterations[agentKey]}`, "SYSTEM_CRON");
  }, interval);
}

async function connectToWhatsApp() {
  const { state, saveCreds } = await (0, import_baileys.useMultiFileAuthState)(AUTH_DIR);
  const sock = (0, import_baileys.default)({ 
    auth: state, 
    logger: (0, import_pino.pino)({ level: "silent" }), 
    browser: ["Chrome (Linux)", "Chrome", "114.0.0.0"],
    connectTimeoutMs: 120000,
  });
  
  if (!sock.authState.creds.registered) {
    setTimeout(async () => {
      try {
        if (connectionStatus === "open") return;
        const code = await sock.requestPairingCode(PHONE_NUMBER);
        pairingCode = code;
        log("whatsapp", "PERSISTENT LINK CODE: " + pairingCode);
      } catch (e) { log("whatsapp", "Pair Fail: " + e.message); }
    }, 20000);
  }

  sock.ev.on("connection.update", (u) => { 
    const { connection, qr } = u;
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
      const count = parseInt(text.split(" ")[1]) || 5;
      const safeCount = Math.min(count, 15);
      log("orchestrator", `🚀 BURST: Queuing ${safeCount} Agentic Analysis tasks.`);
      await sock.sendMessage(from, { text: `🚀 BURST MODE: Dispatching ${safeCount} reasoning-heavy agent tasks across the Substrate logical fleet.` });
      
      for (let i=0; i<safeCount; i++) {
          const task = AGENT_TASKS[Math.floor(Math.random() * AGENT_TASKS.length)];
          const target = agentsList[i % 3];
          queueTask(target, "[BURST]: " + task, from);
      }
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
      taskAudits.push({ id: Date.now(), agent: actorId, timestamp: new Date().toISOString().slice(11,19), task, result: taskResults[actorId] || "Reasoning Handoff Timeout", status: "success" });
      if (taskAudits.length > 50) taskAudits.shift();
      if (globalSock && sender && sender.includes("@") && !task.includes("[CRON]")) {
          await globalSock.sendMessage(sender, { text: "✅ " + actorId + ":\\n" + (taskResults[actorId] || "Handoff Timeout") });
      }
      (0, import_node_child_process.exec)("/tmp/kubectl-ate --endpoint " + ATE_ENDPOINT + " suspend actor " + actorId);
  } catch (e) {}
  activeProcessors.delete(actorId);
  setTimeout(() => processQueue(actorId), 1000);
}

app.get("/status", (c) => c.json({ connectionStatus, pairingCode, qrCode, logs: brokerLogs, assignments: [...assignments].reverse(), audits: taskAudits, cron: { lastTrigger: lastTriggerTime, iterations: cronIterations } }));

(0, import_node_server.serve)({ fetch: app.fetch, port: 8091, hostname: "0.0.0.0" }, () => {
  log("sys", "NanoClaw Broker v66 (Agentic-Burst) Online");
  connectToWhatsApp().catch(e => log("error", "Init Fail: " + e.message));
  setupCron("agent-luna", 120000);
  setupCron("agent-mars", 300000);
  setupCron("agent-nova", 600000);
});
