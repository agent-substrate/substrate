import { Hono } from "hono";
import { serve } from "@hono/node-server";
import { exec } from "node:child_process";

const app = new Hono();

// --- Configuration ---
const ATE_ENDPOINT = process.env.ATE_ENDPOINT || "api.ate-system.svc.cluster.local:443";
const AGENT_TASKS = [
  "Inventory reconciliation audit",
  "Security patch verification",
  "Log aggregation summary",
  "API endpoint health check",
  "Database index optimization analysis"
];

// --- Types ---
interface RegisteredAgent {
  actorId: string;
  lastSeen: number;
  status: "idle" | "working" | "error";
  taskCount: number;
}

interface BrokerLog {
  timestamp: string;
  module: "registry" | "orchestrator" | "substrate";
  message: string;
  level: "info" | "warn" | "error";
}

// --- State ---
const registry: Record<string, RegisteredAgent> = {};
const logs: BrokerLog[] = [];
let isOrchestrating = false;

// --- Helpers ---
const log = (module: BrokerLog["module"], message: string, level: BrokerLog["level"] = "info") => {
  const entry: BrokerLog = {
    timestamp: new Date().toISOString().slice(11, 19),
    module,
    message,
    level
  };
  logs.push(entry);
  if (logs.length > 100) logs.shift();
  console.log(`[${entry.timestamp}] [${module}] ${message}`);
};

const runCmd = (cmd: string): Promise<string> => {
  return new Promise((resolve, reject) => {
    exec(cmd, (error, stdout, stderr) => {
      if (error) reject(new Error(stderr || error.message));
      else resolve(stdout);
    });
  });
};

// --- API Endpoints ---

// NanoClaw Agents call this on boot
app.post("/register", async (c) => {
  const { actorId } = await c.req.json();
  if (!actorId) return c.json({ error: "actorId required" }, 400);

  registry[actorId] = {
    actorId,
    lastSeen: Date.now(),
    status: registry[actorId]?.status || "idle",
    taskCount: registry[actorId]?.taskCount || 0
  };

  log("registry", `Agent **${actorId}** self-registered successfully.`);
  return c.json({ status: "registered", broker: "FleetBroker-v1" });
});

// Dashboard polls this for "Platform View"
app.get("/status", (c) => {
  return c.json({
    registry: Object.values(registry),
    logs: logs,
    orchestrating: isOrchestrating
  });
});

// Trigger a manual task from Dashboard
app.post("/trigger/:actorId", async (c) => {
  const actorId = c.req.param("actorId");
  if (!registry[actorId]) return c.json({ error: "Agent not registered" }, 404);
  
  // Fire and forget task execution
  performTask(actorId);
  return c.json({ status: "triggered" });
});

// --- Orchestration Logic ---

async function performTask(actorId: string) {
  if (registry[actorId].status === "working") {
    log("orchestrator", `Task skipped for ${actorId}: already working.`, "warn");
    return;
  }

  const task = AGENT_TASKS[Math.floor(Math.random() * AGENT_TASKS.length)];
  registry[actorId].status = "working";
  log("orchestrator", `Policy Trigger: Dispatching '${task}' to **${actorId}**.`);

  try {
    // 1. Substrate Resume
    log("substrate", `> kubectl-ate resume actor ${actorId}`);
    await runCmd(`kubectl-ate --endpoint ${ATE_ENDPOINT} resume actor ${actorId}`);

    // 2. Wait for Rehydration
    let actorIP = "";
    for (let i = 0; i < 30; i++) {
      const out = await runCmd(`kubectl-ate --endpoint ${ATE_ENDPOINT} get actor ${actorId} -o json`);
      const actor = JSON.parse(out).actors?.[0] || JSON.parse(out);
      if (actor.status === "STATUS_RUNNING" && actor.ateomPodIp) {
        actorIP = actor.ateomPodIp;
        break;
      }
      await new Promise(r => setTimeout(r, 1000));
    }

    if (!actorIP) throw new Error("Rehydration Timeout");

    // 3. Execute NanoClaw Task
    log("substrate", `Connected to ${actorId} at ${actorIP}. Injecting payload...`);
    await new Promise(r => setTimeout(r, 4000)); // Network settle
    
    await runCmd(`curl -s -f -m 10 -X POST http://${actorIP}:8080/task -H "Content-Type: application/json" -d '{"task": "${task}"}'`);
    
    log("orchestrator", `Task completed by **${actorId}**. Platform yielding hardware.`);
    registry[actorId].taskCount++;

    // 4. Substrate Suspend
    log("substrate", `> kubectl-ate suspend actor ${actorId}`);
    await runCmd(`kubectl-ate --endpoint ${ATE_ENDPOINT} suspend actor ${actorId}`);
    
    registry[actorId].status = "idle";
  } catch (e: any) {
    log("substrate", `Orchestration failed for ${actorId}: ${e.message}`, "error");
    registry[actorId].status = "error";
    // Safety yield
    await runCmd(`kubectl-ate --endpoint ${ATE_ENDPOINT} suspend actor ${actorId}`).catch(() => {});
  }
}

// Simulated Customer Policy: Every 2 minutes, pick a registered agent to work
async function runAutoPolicy() {
  if (!isOrchestrating) return;

  const activeAgents = Object.keys(registry);
  if (activeAgents.length > 0) {
    const pick = activeAgents[Math.floor(Math.random() * activeAgents.length)];
    performTask(pick);
  }

  setTimeout(runAutoPolicy, 120000); // 2 minute cycle
}

// --- Start Server ---
const port = 8091;
serve({ fetch: app.fetch, port, hostname: "0.0.0.0" });

log("registry", "Fleet Management Broker active on port 8091.");
isOrchestrating = true;
runAutoPolicy();
