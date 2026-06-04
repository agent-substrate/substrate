import { Hono } from "hono";
import { serve } from "@hono/node-server";

const app = new Hono();

// Persistent state that should survive suspend/resume via Substrate
let taskCounter = 0;
const actorId = process.env.ATE_ACTOR_ID || "unknown";

// --- Substrate Demo API ---

// T1: Standard Counter Demo
app.get("/v1/counter", (c) => {
  taskCounter++;
  console.log(`[substrate/actor-wrapper] GET /v1/counter called. New count: ${taskCounter}`);
  return c.text(`counter: ${taskCounter}\n`);
});

// T2: Agent Developer Experience / Secret Agent Demo
app.post("/v1/agent-secret", async (c) => {
  const body = await c.req.text();
  taskCounter++;
  const identity = `AGENT-${actorId.slice(0, 4).toUpperCase()}`;
  console.log(`[substrate/actor-wrapper] POST /v1/agent-secret called with: "${body}". Identity: ${identity}`);
  
  // Return identity and current session
  const response = `Identity: ${identity} | Session: "${actorId}" | TaskCount: ${taskCounter}\n`;
  
  return c.text(response);
});

// --- Legacy PoC Endpoints ---

app.get("/state", (c) => {
  return c.json({
    actorId,
    taskCounter,
    uptime: Math.floor(process.uptime()),
    status: "healthy",
  });
});

app.post("/task", async (c) => {
  const body = await c.req.json();
  const durationMs = body.durationMs || 1000;
  
  taskCounter++;
  console.log(`[substrate/actor-wrapper] Starting task on ${actorId}. Counter: ${taskCounter}. Simulating ${durationMs}ms work...`);
  
  await new Promise((resolve) => setTimeout(resolve, durationMs));
  
  console.log(`[substrate/actor-wrapper] Task on ${actorId} completed.`);
  return c.json({
    success: true,
    actorId,
    taskCounter,
  });
});

const port = process.env.PORT ? parseInt(process.env.PORT) : 8080;
console.log(`[substrate/actor-wrapper] Actor ${actorId} starting. Current counter: ${taskCounter}`);
console.log(`[substrate/actor-wrapper] OpenClaw Actor listening on port ${port}`);

serve({
  fetch: app.fetch,
  port,
});

// Periodic logging to show the actor is alive in logs
setInterval(() => {
  console.log(`[substrate/actor-wrapper] Actor ${actorId} heartbeat. State: count=${taskCounter}, uptime=${Math.floor(process.uptime())}s`);
}, 10000);
