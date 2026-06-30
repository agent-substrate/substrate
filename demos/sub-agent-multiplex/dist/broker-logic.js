"use strict";
var __create = Object.create;
var __defProp = Object.defineProperty;
var __getOwnPropDesc = Object.getOwnPropertyDescriptor;
var __getOwnPropNames = Object.getOwnPropertyNames;
var __getProtoOf = Object.getPrototypeOf;
var __hasOwnProp = Object.prototype.hasOwnProperty;
var __copyProps = (to, from, except, desc) => {
  if (from && typeof from === "object" || typeof from === "function") {
    for (let key of __getOwnPropNames(from))
      if (!__hasOwnProp.call(to, key) && key !== except)
        __defProp(to, key, { get: () => from[key], enumerable: !(desc = __getOwnPropDesc(from, key)) || desc.enumerable });
  }
  return to;
};
var __toESM = (mod, isNodeMode, target) => (target = mod != null ? __create(__getProtoOf(mod)) : {}, __copyProps(
  // If the importer is in node compatibility mode or this is not an ESM
  // file that has been converted to a CommonJS file using a Babel-
  // compatible transform (i.e. "__esModule" has not been set), then set
  // "default" to the CommonJS "module.exports" for node compatibility.
  isNodeMode || !mod || !mod.__esModule ? __defProp(target, "default", { value: mod, enumerable: true }) : target,
  mod
));

// demos/sub-agent-multiplex/broker/server.ts
var import_hono = require("hono");
var import_node_server = require("@hono/node-server");
var import_node_child_process = require("node:child_process");
var import_node_fs = __toESM(require("node:fs"));
var import_pino = require("pino");
var import_baileys = __toESM(require("@whiskeysockets/baileys"));
var app = new import_hono.Hono();
var logger = (0, import_pino.pino)({ level: "info" });
var ATE_ENDPOINT = process.env.ATE_ENDPOINT || "api.ate-system.svc.cluster.local:443";
var AUTH_DIR = "/app/store/auth/v1";
var PHONE_NUMBER = "16503360539";
var TEMPLATE = "sub-agent/sub-agent-agent";
var pairingCode = null;
var connectionStatus = "closed";
var roundRobinIndex = 0;
var agentsList = ["agent-luna-v14", "agent-mars-v12", "agent-nova-v11"];
var registry = {};
var brokerLogs = [];
var assignments = [];
var taskAudits = [];
var globalSock = null;
var taskQueues = {
  "agent-luna-v14": [],
  "agent-mars-v12": [],
  "agent-nova-v11": []
};
var activeProcessors = /* @__PURE__ */ new Set();
var lastTriggerTime = { "agent-luna": Date.now(), "agent-mars": Date.now(), "agent-nova": Date.now() };
var cronIterations = { "agent-luna": 0, "agent-mars": 0, "agent-nova": 0 };
var runCmd = (cmd) => {
  return new Promise((resolve) => {
    const timer = setTimeout(() => resolve(""), 12e3);
    (0, import_node_child_process.exec)(cmd, (error, stdout, stderr) => {
      clearTimeout(timer);
      if (error) {
        console.error(`[CMD ERROR] ${cmd}: ${stderr || error.message}`);
        resolve(stderr || error.message);
      } else resolve(stdout);
    });
  });
};
var log = (module2, message, level = "info") => {
  const entry = { timestamp: (/* @__PURE__ */ new Date()).toISOString().slice(11, 19), module: module2, message, level };
  brokerLogs.push(entry);
  if (brokerLogs.length > 200) brokerLogs.shift();
  console.log(`[${entry.timestamp}] [${module2}] ${message}`);
};
async function connectToWhatsApp() {
  const { state, saveCreds } = await (0, import_baileys.useMultiFileAuthState)(AUTH_DIR);
  const { version, isLatest } = await (0, import_baileys.fetchLatestWaWebVersion)({});
  connectionStatus = "connecting";
  log("whatsapp", `Initializing WhatsApp v${version.join(".")}`);
  const sock = (0, import_baileys.default)({
    version,
    printQRInTerminal: false,
    auth: state,
    logger: (0, import_pino.pino)({ level: "silent" }),
    browser: import_baileys.Browsers.ubuntu("Chrome")
  });
  if (!sock.authState.creds.registered) {
    log("whatsapp", `Requesting pairing code for ${PHONE_NUMBER}...`);
    setTimeout(async () => {
      try {
        const code = await sock.requestPairingCode(PHONE_NUMBER);
        pairingCode = code;
        log("whatsapp", `NEW LINK CODE: ${pairingCode}`);
      } catch (e) {
        log("whatsapp", `Pairing failed: ${e.message}`, "error");
      }
    }, 5e3);
  }
  sock.ev.on("creds.update", saveCreds);
  sock.ev.on("connection.update", (update) => {
    const { connection, lastDisconnect } = update;
    if (connection === "close") {
      connectionStatus = "closed";
      const statusCode = lastDisconnect?.error?.output?.statusCode;
      const shouldReconnect = statusCode !== import_baileys.DisconnectReason.loggedOut;
      log("whatsapp", `Connection closed (${statusCode}). Reconnecting: ${shouldReconnect}`, shouldReconnect ? "info" : "error");
      if (shouldReconnect) setTimeout(connectToWhatsApp, 3e3);
      else pairingCode = "LOGGED_OUT";
    } else if (connection === "open") {
      connectionStatus = "open";
      pairingCode = null;
      log("whatsapp", "WHATSAPP BRIDGE LIVE.");
      sock.sendMessage(`${PHONE_NUMBER}@s.whatsapp.net`, { text: "\u{1F916} Substrate Fleet Broker (V1.1.6-Queued) is now ONLINE." });
    }
  });
  sock.ev.on("messages.upsert", async (m) => {
    const msg = m.messages[0];
    if (!msg.message) return;
    const from = msg.key.remoteJid || "";
    const text = msg.message.conversation || msg.message.extendedTextMessage?.text || "";
    const name = msg.pushName || "User";
    const fromMe = msg.key.fromMe;
    if (text.startsWith("\u{1F916}") || text.startsWith("\u2705")) return;
    if (text.toLowerCase().startsWith("/burst")) {
      const count = parseInt(text.split(" ")[1]) || 5;
      const safeCount = Math.min(count, 15);
      log("whatsapp", `\u{1F680} Burst Triggered: Queuing ${safeCount} tasks...`);
      await sock.sendMessage(from, { text: `\u{1F680} BURST MODE: Queuing ${safeCount} tasks across the fleet. Check Dashboard for Timeline state!` });
      for (let i = 0; i < safeCount; i++) {
        const agents = ["agent-luna-v14", "agent-mars-v12", "agent-nova-v11"];
        queueTask(agents[i % agents.length], `Burst Task #${i + 1}: Multi-tenant pressure test`, from);
      }
      return;
    }
    log("whatsapp", `\u{1F4E9} Msg from ${name} (${from}): "${text}"`);
    await sock.sendMessage(from, { text: `\u{1F916} Message received! Task queued for execution.` });
    const targetAgent = agentsList[roundRobinIndex % agentsList.length];
    roundRobinIndex++;
    queueTask(targetAgent, text, from);
  });
  globalSock = sock;
  return sock;
}
function queueTask(actorId, task, sender) {
  if (!taskQueues[actorId]) taskQueues[actorId] = [];
  taskQueues[actorId].push({ task, sender });
  const asg = { id: "asg-" + Date.now() + "-" + Math.random().toString(36).substr(2, 5), agent: actorId, task, state: "queued", created_at: Date.now() / 1e3 };
  assignments.push(asg);
  if (assignments.length > 50) assignments.shift();
  processQueue(actorId);
}
async function processQueue(actorId) {
  if (activeProcessors.has(actorId)) return;
  if (taskQueues[actorId].length === 0) return;
  activeProcessors.add(actorId);
  const { task, sender } = taskQueues[actorId].shift();
  const asg = assignments.find((a) => a.task === task && a.agent === actorId && a.state === "queued");
  if (asg) asg.state = "running";
  try {
    await executeWorkflow(actorId, task, sender);
  } catch (e) {
    log("substrate", `Queue execution failed for ${actorId}: ${e}`, "error");
  } finally {
    if (asg) {
      asg.state = "completed";
      asg.completed_at = Date.now() / 1e3;
    }
    activeProcessors.delete(actorId);
    setTimeout(() => processQueue(actorId), 1e3);
  }
}
async function executeWorkflow(actorId, task, sender) {
  log("orchestrator", `Starting workflow for ${actorId}...`);
  let resumed = false;
  for (let i = 0; i < 3; i++) {
    try {
      log("substrate", `> kubectl-ate resume actor ${actorId} (Attempt ${i + 1}/3)`);
      const out = await runCmd(`kubectl-ate --endpoint ${ATE_ENDPOINT} resume actor ${actorId}`);
      if (out.includes("error") || out.includes("FailedPrecondition") || out.includes("Internal")) {
        throw new Error(out);
      }
      resumed = true;
      break;
    } catch (e) {
      log("substrate", `Resume conflict: ${e.message.split("\n")[0]}. Self-healing identity...`, "warn");
      await runCmd(`kubectl-ate --endpoint ${ATE_ENDPOINT} delete actor ${actorId}`).catch(() => {
      });
      await (0, import_baileys.delay)(4e3);
      await runCmd(`kubectl-ate --endpoint ${ATE_ENDPOINT} create actor ${actorId} --template ${TEMPLATE}`);
      await (0, import_baileys.delay)(2e3);
    }
  }
  if (!resumed) throw new Error("Hardware Lockout: Could not resume after 3 healing attempts.");
  let actorIP = "";
  for (let i = 0; i < 40; i++) {
    const out = await runCmd(`kubectl-ate --endpoint ${ATE_ENDPOINT} get actor ${actorId} -o json`);
    if (out && out.trim().startsWith("{")) {
      const actor = JSON.parse(out).actors?.[0] || JSON.parse(out);
      if (actor.status === "STATUS_RUNNING" && actor.ateomPodIp) {
        actorIP = actor.ateomPodIp;
        break;
      }
    }
    await (0, import_baileys.delay)(2e3);
  }
  if (!actorIP) throw new Error("Rehydration Timeout: IP never assigned.");
  log("substrate", `Actor ${actorId} ready at ${actorIP}. Warm-up...`);
  await (0, import_baileys.delay)(6e3);
  const result = await runCmd(`curl -s -f -m 15 -X POST http://${actorIP}:8080/task -H "Content-Type: application/json" -d '${JSON.stringify({ task, sender })}'`);
  const data = result.startsWith("{") ? JSON.parse(result) : { result: "Task Finished" };
  log("orchestrator", `Task Success for ${actorId}.`);
  taskAudits.push({ id: "audit-" + Date.now(), agent: actorId, timestamp: (/* @__PURE__ */ new Date()).toISOString().slice(11, 19), task, result: data.result || result, status: "success" });
  if (taskAudits.length > 50) taskAudits.shift();
  if (globalSock && connectionStatus === "open" && sender.includes("@")) {
    await globalSock.sendMessage(sender, { text: `\u2705 Task completed by **${actorId}**!

${data.result || "Check dashboard for details."}` }).catch((e) => console.error("WA Send Error:", e));
  }
  log("substrate", `> kubectl-ate suspend actor ${actorId}`);
  await runCmd(`kubectl-ate --endpoint ${ATE_ENDPOINT} suspend actor ${actorId}`);
  await (0, import_baileys.delay)(2e3);
}
app.post("/register", async (c) => {
  const { actorId } = await c.req.json();
  registry[actorId] = { actorId, lastSeen: Date.now() };
  log("registry", `Agent **${actorId}** online.`);
  return c.json({ status: "registered", broker: "V1.1.6-Queued" });
});
app.post("/api/give-task", async (c) => {
  const agentKey = c.req.query("agent") || "agent-mars";
  const actorId = agentKey.includes("luna") ? "agent-luna-v14" : agentKey.includes("nova") ? "agent-nova-v11" : "agent-mars-v12";
  log("alert", `Manual trigger for **${agentKey}**.`);
  queueTask(actorId, "Manual Pulse", "DASHBOARD");
  return c.json({ ok: true });
});
app.post("/send-message", async (c) => {
  const { to, text } = await c.req.json();
  if (globalSock && connectionStatus === "open") {
    await globalSock.sendMessage(to, { text });
    return c.json({ ok: true });
  }
  return c.json({ ok: false, error: "WhatsApp disconnected" }, 500);
});
app.get("/dist/agent.js", async (c) => {
  try {
    const content = import_node_fs.default.readFileSync("/app/dist/broker.js", "utf8");
    const agentPath = "/app/dist/agent.js";
    if (import_node_fs.default.existsSync(agentPath)) {
      return c.body(import_node_fs.default.readFileSync(agentPath, "utf8"), 200, { "Content-Type": "application/javascript" });
    }
    return c.text("Agent file not found", 404);
  } catch (e) {
    return c.text("Error reading agent file", 500);
  }
});
app.get("/status", (c) => c.json({
  connectionStatus,
  pairingCode,
  registeredAgents: Object.values(registry),
  logs: brokerLogs,
  assignments: [...assignments].reverse(),
  audits: taskAudits,
  cron: { lastTrigger: lastTriggerTime, iterations: cronIterations }
}));
var port = 8091;
(0, import_node_server.serve)({ fetch: app.fetch, port, hostname: "0.0.0.0" }, async () => {
  if (!import_node_fs.default.existsSync(AUTH_DIR)) import_node_fs.default.mkdirSync(AUTH_DIR, { recursive: true });
  await connectToWhatsApp();
});
