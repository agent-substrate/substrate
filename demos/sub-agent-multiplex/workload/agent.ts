import { Hono } from "hono";
import { serve } from "@hono/node-server";

/**
 * OpenClaw Stateful Agent
 * 
 * This class represents the logical agent. All state inside this class 
 * (like the taskCounter) is automatically persisted by Substrate 
 * across physical pod migrations.
 */
class ClawAgent {
  private taskCounter: number = 0;
  private readonly actorId: string;

  constructor() {
    this.actorId = process.env.ATE_ACTOR_ID || "unknown";
    console.log(`[ClawAgent] Identity ${this.actorId} initialized.`);
  }

  public async performTask(durationMs: number) {
    this.taskCounter++;
    console.log(`[ClawAgent] Starting task. Counter: ${this.taskCounter}. Working for ${durationMs}ms...`);
    await new Promise((resolve) => setTimeout(resolve, durationMs));
    console.log(`[ClawAgent] Task completed.`);
    return { success: true, count: this.taskCounter };
  }

  public getSecret(body: string) {
    this.taskCounter++;
    const identity = `AGENT-${this.actorId.slice(0, 4).toUpperCase()}`;
    return `Identity: ${identity} | Session: "${this.actorId}" | TaskCount: ${this.taskCounter} | Input: ${body}\n`;
  }

  public getStatus() {
    return {
      actorId: this.actorId,
      taskCounter: this.taskCounter,
      uptime: Math.floor(process.uptime()),
      status: "healthy",
    };
  }

  public incrementCounter() {
    this.taskCounter++;
    return this.taskCounter;
  }
}

const agent = new ClawAgent();
const app = new Hono();

// --- Substrate Demo API ---

// T1: Standard Counter Demo
app.get("/v1/counter", (c) => {
  const count = agent.incrementCounter();
  return c.text(`counter: ${count}\n`);
});

// T2: Agent Developer Experience / Secret Agent Demo
app.post("/v1/agent-secret", async (c) => {
  const body = await c.req.text();
  return c.text(agent.getSecret(body));
});

// --- Lifecycle & Health Endpoints ---

app.get("/state", (c) => {
  return c.json(agent.getStatus());
});

app.post("/task", async (c) => {
  const body = await c.req.json();
  const result = await agent.performTask(body.durationMs || 1000);
  return c.json({ ...result, actorId: agent.getStatus().actorId });
});

const port = process.env.PORT ? parseInt(process.env.PORT) : 8080;
console.log(`[agent] OpenClaw Actor starting on port ${port}`);

serve({
  fetch: app.fetch,
  port,
});

// Periodic heartbeat
setInterval(() => {
  const status = agent.getStatus();
  console.log(`[agent] Heartbeat: count=${status.taskCounter}, uptime=${status.uptime}s`);
}, 10000);
