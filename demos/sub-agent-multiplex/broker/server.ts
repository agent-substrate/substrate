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
const PHONE_NUMBER = process.env.WHATSAPP_PHONE || "1234567890"; // Use WHATSAPP_PHONE env var
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
const registry: Record<string, RegisteredAgent> = {};
const activeTasks = new Set<string>();
const brokerLogs: BrokerLog[] = [];
let assignments: Assignment[] = [];
let taskAudits: TaskAudit[] = [];
let globalSock: any = null;

// Cron Tracking
let lastTriggerTime: Record<string, number> = { "agent-luna": Date.now(), "agent-mars": Date.now(), "agent-nova": Date.now() };
let cronIterations: Record<string, number> = { "agent-luna": 0, "agent-mars": 0, "agent-nova": 0 };

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

const log = (module: BrokerLog["module"], message: string, level: BrokerLog["level"] = "info") => {
  const entry: BrokerLog = { timestamp: new Date().toISOString().slice(11, 19), module, message, level };
  brokerLogs.push(entry);
  if (brokerLogs.length > 150) brokerLogs.shift();
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

  // Handle Pairing Code
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
      // Send heart-beat to user
      sock.sendMessage(`${PHONE_NUMBER}@s.whatsapp.net`, { text: "🤖 Substrate Fleet Broker is now ONLINE and listening for your messages." });
    }
  });

  sock.ev.on("messages.upsert", async (m) => {
    const msg = m.messages[0];
    if (!msg.message) return;

    const from = msg.key.remoteJid || "";
    const text = msg.message.conversation || msg.message.extendedTextMessage?.text || "";
    const name = msg.pushName || "User";
    const fromMe = msg.key.fromMe;
    
    log("whatsapp", `📩 Msg Event [Me:${fromMe}] from ${name} (${from}): "${text}"`);
    
    // PREVENT INFINITE LOOP: Ignore our own robot ACKs
    if (text.startsWith("🤖") || text.startsWith("✅")) {
       return;
    }

    // Auto-ACK
    await sock.sendMessage(from, { text: `🤖 Message acknowledged! (Event Sync: ${fromMe ? 'Internal' : 'External'})` });

    // Orchestrate
    await orchestrateActor("agent-mars-v12", text, from, sock);
  });
  
  globalSock = sock;
  return sock;
}

// --- Orchestration Logic ---
async function orchestrateActor(actorId: string, task: string, sender: string, sock: any) {
  if (activeTasks.has(actorId)) {
    log("orchestrator", `Actor ${actorId} busy. Skipping task: ${task}`);
    return;
  }
  activeTasks.add(actorId);

  const asg: Assignment = { id: "asg-"+Date.now(), agent: actorId, task, state: "queued", created_at: Date.now()/1000 };
  assignments.push(asg);
  if (assignments.length > 20) assignments.shift();

  try {
    asg.state = "running";
    log("orchestrator", `Resuming ${actorId} for ${sender}...`);
    
    // Try resume with aggressive retry/healing
    let resumed = false;
    for(let i=0; i<3; i++) {
        try {
            await runCmd(`kubectl-ate --endpoint ${ATE_ENDPOINT} resume actor ${actorId}`);
            resumed = true;
            break;
        } catch(e) {
            log("substrate", `Resume Attempt ${i+1} failed. Resetting identity...`, "warn");
            await runCmd(`kubectl-ate --endpoint ${ATE_ENDPOINT} delete actor ${actorId}`).catch(()=>{});
            await delay(3000);
            await runCmd(`kubectl-ate --endpoint ${ATE_ENDPOINT} create actor ${actorId} --template ${TEMPLATE}`);
        }
    }

    if (!resumed) throw new Error("Hard Rehydration Failure");
    
    let actorIP = "";
    for (let i = 0; i < 40; i++) {
      const out = await runCmd(`kubectl-ate --endpoint ${ATE_ENDPOINT} get actor ${actorId} -o json`);
      const actor = JSON.parse(out).actors?.[0] || JSON.parse(out);
      if (actor.status === "STATUS_RUNNING" && actor.ateomPodIp) { actorIP = actor.ateomPodIp; break; }
      await delay(1000);
    }
    if (!actorIP) throw new Error("IP Assignment Timeout");
    
    log("substrate", `Actor ${actorId} rehydrated at ${actorIP}. Settling network...`);
    await delay(6000); // 6s warm-up
    
    const payload = JSON.stringify({ task, sender, source: "whatsapp" });
    const result = await runCmd(`curl -s -f -m 10 -X POST http://${actorIP}:8080/task -H "Content-Type: application/json" -d '${payload}'`);
    const data = JSON.parse(result);
    
    log("orchestrator", `Task SUCCESS for ${actorId}.`);
    
    taskAudits.push({ id: "audit-"+Date.now(), agent: actorId, timestamp: new Date().toISOString().slice(11, 19), task, result: data.result || "Success", status: "success" });
    if (taskAudits.length > 50) taskAudits.shift();

    await sock.sendMessage(sender, { text: `✅ Task completed! Check the dashboard for the full reasoning payload.` });
    
    log("substrate", `Suspending ${actorId} (Idle).`);
    await runCmd(`kubectl-ate --endpoint ${ATE_ENDPOINT} suspend actor ${actorId}`);

  } catch (e: any) {
    log("substrate", `Workflow failed: ${e.message}`, "error");
    taskAudits.push({ id: "audit-"+Date.now(), agent: actorId, timestamp: new Date().toISOString().slice(11,19), task, result: e.message, status: "error" });
  } finally {
    asg.state = "completed";
    asg.completed_at = Date.now()/1000;
    activeTasks.delete(actorId);
  }
}

// --- API Endpoints ---

app.post("/register", async (c) => {
  const { actorId } = await c.req.json();
  registry[actorId] = { actorId, lastSeen: Date.now() };
  log("registry", `Agent **${actorId}** online.`);
  return c.json({ status: "registered", broker: "V1.1-WhatsApp-Live" });
});

app.post("/api/give-task", async (c) => {
  const agentKey = c.req.query("agent") || "agent-mars";
  const source = c.req.query("source") || "manual";
  
  if (source === "cron") {
     return c.json({ ok: false, error: "Cron disabled for stability" });
  }

  const actorId = agentKey.includes("luna") ? "agent-luna-v12" : (agentKey.includes("nova") ? "agent-nova-v11" : "agent-mars-v12");
  log("alert", `Manual trigger for **${agentKey}**.`);
  orchestrateActor(actorId, "Manual Pulse", "DASHBOARD", globalSock);
  return c.json({ ok: true });
});

app.post("/send-message", async (c) => {
  const { to, text } = await c.req.json();
  log("whatsapp", `Outbound WhatsApp to ${to}`);
  if (globalSock && connectionStatus === "open") {
    await globalSock.sendMessage(to, { text });
    return c.json({ ok: true });
  }
  return c.json({ ok: false, error: "WhatsApp disconnected" }, 500);
});

app.get("/status", (c) => c.json({ 
  connectionStatus, 
  pairingCode, 
  registeredAgents: Object.values(registry),
  logs: brokerLogs,
  assignments,
  audits: taskAudits,
  cron: { lastTrigger: lastTriggerTime, iterations: cronIterations }
}));

const port = 8091;
serve({ fetch: app.fetch, port, hostname: "0.0.0.0" }, async () => {
  if (!fs.existsSync(AUTH_DIR)) fs.mkdirSync(AUTH_DIR, { recursive: true });
  await connectToWhatsApp();
});
