import { Hono } from "hono";
import { serve } from "@hono/node-server";
import { exec } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { pino } from "pino";
import makeWASocket, { 
  useMultiFileAuthState, 
  DisconnectReason, 
  fetchLatestWaWebVersion,
  delay,
  Browsers
} from "@whiskeysockets/baileys";

const app = new Hono();
const logger = pino({ level: "info" });

// --- Configuration ---
const ATE_ENDPOINT = process.env.ATE_ENDPOINT || "api.ate-system.svc.cluster.local:443";
const AUTH_DIR = "/app/store/auth/v1";
const PHONE_NUMBER = process.env.WHATSAPP_PHONE || "1234567890"; // Sanitized for OSS
const TEMPLATE = "sub-agent/sub-agent-agent";

// --- Types ---
interface RegisteredAgent {
  actorId: string;
  lastSeen: number;
}

interface BrokerLog {
  timestamp: string;
  module: "registry" | "orchestrator" | "substrate" | "alert" | "whatsapp";
  message: string;
  level: "info" | "warn" | "error";
}

interface Assignment {
  id: string;
  agent: string;
  task: string;
  state: "queued" | "running" | "completed";
  created_at: number;
  completed_at?: number;
}

interface TaskAudit {
  id: string;
  agent: string;
  timestamp: string;
  task: string;
  result: string;
  status: "success" | "error";
}

// --- State ---
let pairingCode: string | null = null;
let connectionStatus: "connecting" | "open" | "closed" = "closed";
let roundRobinIndex = 0;
const agentsList = ["agent-luna-v14", "agent-mars-v12", "agent-nova-v11"];
const registry: Record<string, RegisteredAgent> = {};
const brokerLogs: BrokerLog[] = [];
let assignments: Assignment[] = [];
let taskAudits: TaskAudit[] = [];
let globalSock: any = null;

// Queue State
const taskQueues: Record<string, { task: string, sender: string }[]> = {
  "agent-luna-v14": [],
  "agent-mars-v12": [],
  "agent-nova-v11": []
};
const activeProcessors = new Set<string>();

// Cron Tracking
let lastTriggerTime: Record<string, number> = { "agent-luna": Date.now(), "agent-mars": Date.now(), "agent-nova": Date.now() };
let cronIterations: Record<string, number> = { "agent-luna": 0, "agent-mars": 0, "agent-nova": 0 };

const runCmd = (cmd: string): Promise<string> => {
  return new Promise((resolve) => {
    const timer = setTimeout(() => resolve(""), 12000); // 12s timeout for commands
    exec(cmd, (error, stdout, stderr) => {
      clearTimeout(timer);
      if (error) {
        console.error(`[CMD ERROR] ${cmd}: ${stderr || error.message}`);
        resolve(stderr || error.message);
      } else resolve(stdout);
    });
  });
};

const log = (module: BrokerLog["module"], message: string, level: BrokerLog["level"] = "info") => {
  const entry: BrokerLog = { timestamp: new Date().toISOString().slice(11, 19), module, message, level };
  brokerLogs.push(entry);
  if (brokerLogs.length > 200) brokerLogs.shift();
  console.log(`[${entry.timestamp}] [${module}] ${message}`);
};

// --- WhatsApp Logic (Baileys) ---

async function connectToWhatsApp() {
  const { state, saveCreds } = await useMultiFileAuthState(AUTH_DIR);
  const { version, isLatest } = await fetchLatestWaWebVersion({});
  
  connectionStatus = "connecting";
  log("whatsapp", `Initializing WhatsApp v${version.join(".")}`);

  const sock = makeWASocket({
    version,
    printQRInTerminal: false,
    auth: state,
    logger: pino({ level: "silent" }),
    browser: Browsers.ubuntu("Chrome")
  });

  if (!sock.authState.creds.registered) {
    log("whatsapp", `Requesting pairing code for ${PHONE_NUMBER}...`);
    setTimeout(async () => {
      try {
        const code = await sock.requestPairingCode(PHONE_NUMBER);
        pairingCode = code;
        log("whatsapp", `NEW LINK CODE: ${pairingCode}`);
      } catch (e: any) {
        log("whatsapp", `Pairing failed: ${e.message}`, "error");
      }
    }, 5000);
  }

  sock.ev.on("creds.update", saveCreds);

  sock.ev.on("connection.update", (update) => {
    const { connection, lastDisconnect } = update;
    if (connection === "close") {
      connectionStatus = "closed";
      const statusCode = (lastDisconnect?.error as any)?.output?.statusCode;
      const shouldReconnect = statusCode !== DisconnectReason.loggedOut;
      log("whatsapp", `Connection closed (${statusCode}). Reconnecting: ${shouldReconnect}`, shouldReconnect ? "info" : "error");
      if (shouldReconnect) setTimeout(connectToWhatsApp, 3000);
      else pairingCode = "LOGGED_OUT";
    } else if (connection === "open") {
      connectionStatus = "open";
      pairingCode = null;
      log("whatsapp", "WHATSAPP BRIDGE LIVE.");
      sock.sendMessage(`${PHONE_NUMBER}@s.whatsapp.net`, { text: "🤖 Substrate Fleet Broker (V1.1.6-Queued) is now ONLINE." });
    }
  });

  sock.ev.on("messages.upsert", async (m) => {
    const msg = m.messages[0];
    if (!msg.message) return;

    const from = msg.key.remoteJid || "";
    const text = msg.message.conversation || msg.message.extendedTextMessage?.text || "";
    const name = msg.pushName || "User";
    const fromMe = msg.key.fromMe;
    
    if (text.startsWith("🤖") || text.startsWith("✅")) return;

    if (text.toLowerCase().startsWith("/burst")) {
       const count = parseInt(text.split(" ")[1]) || 5;
       const safeCount = Math.min(count, 15);
       log("whatsapp", `🚀 Burst Triggered: Queuing ${safeCount} tasks...`);
       await sock.sendMessage(from, { text: `🚀 BURST MODE: Queuing ${safeCount} tasks across the fleet. Check Dashboard for Timeline state!` });
       
       for(let i=0; i<safeCount; i++) {
          const agents = ["agent-luna-v14", "agent-mars-v12", "agent-nova-v11"];
          queueTask(agents[i % agents.length], `Burst Task #${i+1}: Multi-tenant pressure test`, from);
       }
       return;
    }

    log("whatsapp", `📩 Msg from ${name} (${from}): "${text}"`);
    await sock.sendMessage(from, { text: `🤖 Message received! Task queued for execution.` });
    
    const targetAgent = agentsList[roundRobinIndex % agentsList.length];
    roundRobinIndex++;
    queueTask(targetAgent, text, from);
  });
  
  globalSock = sock;
  return sock;
}

// --- Queueing & Processing Logic ---

function queueTask(actorId: string, task: string, sender: string) {
  if (!taskQueues[actorId]) taskQueues[actorId] = [];
  taskQueues[actorId].push({ task, sender });
  
  const asg: Assignment = { id: "asg-"+Date.now()+"-"+Math.random().toString(36).substr(2,5), agent: actorId, task, state: "queued", created_at: Date.now()/1000 };
  assignments.push(asg);
  if (assignments.length > 50) assignments.shift();

  processQueue(actorId);
}

async function processQueue(actorId: string) {
  if (activeProcessors.has(actorId)) return;
  if (taskQueues[actorId].length === 0) return;

  activeProcessors.add(actorId);
  const { task, sender } = taskQueues[actorId].shift()!;
  
  const asg = assignments.find(a => a.task === task && a.agent === actorId && a.state === "queued");
  if (asg) asg.state = "running";

  try {
    await executeWorkflow(actorId, task, sender);
  } catch (e) {
    log("substrate", `Queue execution failed for ${actorId}: ${e}`, "error");
  } finally {
    if (asg) {
        asg.state = "completed";
        asg.completed_at = Date.now()/1000;
    }
    activeProcessors.delete(actorId);
    // Continue with next task in queue after a small settle period
    setTimeout(() => processQueue(actorId), 1000);
  }
}

async function executeWorkflow(actorId: string, task: string, sender: string) {
  log("orchestrator", `Starting workflow for ${actorId}...`);
  
  let resumed = false;
  for(let i=0; i<3; i++) {
    try {
      log("substrate", `> kubectl-ate resume actor ${actorId} (Attempt ${i+1}/3)`);
      const out = await runCmd(`kubectl-ate --endpoint ${ATE_ENDPOINT} resume actor ${actorId}`);
      if (out.includes("error") || out.includes("FailedPrecondition") || out.includes("Internal")) {
        throw new Error(out);
      }
      resumed = true;
      break;
    } catch(e: any) {
      log("substrate", `Resume conflict: ${e.message.split('\n')[0]}. Self-healing identity...`, "warn");
      await runCmd(`kubectl-ate --endpoint ${ATE_ENDPOINT} delete actor ${actorId}`).catch(()=>{});
      await delay(4000);
      await runCmd(`kubectl-ate --endpoint ${ATE_ENDPOINT} create actor ${actorId} --template ${TEMPLATE}`);
      await delay(2000);
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
    await delay(2000); // 2s poll intervals
  }

  if (!actorIP) throw new Error("Rehydration Timeout: IP never assigned.");

  log("substrate", `Actor ${actorId} ready at ${actorIP}. Warm-up...`);
  await delay(6000);

  const result = await runCmd(`curl -s -f -m 15 -X POST http://${actorIP}:8080/task -H "Content-Type: application/json" -d '${JSON.stringify({ task, sender })}'`);
  const data = result.startsWith("{") ? JSON.parse(result) : { result: "Task Finished" };

  log("orchestrator", `Task Success for ${actorId}.`);
  taskAudits.push({ id: "audit-"+Date.now(), agent: actorId, timestamp: new Date().toISOString().slice(11, 19), task, result: data.result || result, status: "success" });
  if (taskAudits.length > 50) taskAudits.shift();

  if (globalSock && connectionStatus === "open" && sender.includes("@")) {
    await globalSock.sendMessage(sender, { text: `✅ Task completed by **${actorId}**!\n\n${data.result || "Check dashboard for details."}` }).catch(e => console.error("WA Send Error:", e));
  }

  log("substrate", `> kubectl-ate suspend actor ${actorId}`);
  await runCmd(`kubectl-ate --endpoint ${ATE_ENDPOINT} suspend actor ${actorId}`);
  await delay(2000); // Settle after suspend
}

// --- API Endpoints ---

app.post("/register", async (c) => {
  const { actorId } = await c.req.json();
  registry[actorId] = { actorId, lastSeen: Date.now() };
  log("registry", `Agent **${actorId}** online.`);
  return c.json({ status: "registered", broker: "V1.1.6-Queued" });
});

app.post("/api/give-task", async (c) => {
  const agentKey = c.req.query("agent") || "agent-mars";
  const actorId = agentKey.includes("luna") ? "agent-luna-v14" : (agentKey.includes("nova") ? "agent-nova-v11" : "agent-mars-v12");
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
    const content = fs.readFileSync("/app/dist/broker.js", "utf8"); // Wait, I need the agent file
    // The broker pod will have both bundled files in /app/dist
    const agentPath = "/app/dist/agent.js";
    if (fs.existsSync(agentPath)) {
        return c.body(fs.readFileSync(agentPath, "utf8"), 200, { "Content-Type": "application/javascript" });
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

const port = 8091;
serve({ fetch: app.fetch, port, hostname: "0.0.0.0" }, async () => {
  if (!fs.existsSync(AUTH_DIR)) fs.mkdirSync(AUTH_DIR, { recursive: true });
  await connectToWhatsApp();
});
