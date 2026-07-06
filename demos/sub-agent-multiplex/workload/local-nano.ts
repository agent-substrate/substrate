import { Hono } from "hono";

/**
 * Local NanoClaw Infrastructure Proxy
 * 
 * This service runs inside the Actor container. It is SUBSTRATE-AWARE.
 * Its job is to:
 * 1. Accept local registrations from AgentApp.
 * 2. Forward those registrations to the External Platform Broker.
 * 3. Provide an external trigger endpoint for the Broker.
 * 4. Perform "Synthetic Injection" of tasks into the local queue.
 * 5. Provide a "WhatsApp Skill" endpoint for the agent to reply.
 */
class LocalNanoProxy {
  private readonly actorId: string;
  private readonly brokerUrl: string;
  private localQueue: any[] = [];

  constructor() {
    this.actorId = process.env.ATE_ACTOR_ID || "unknown";
    this.brokerUrl = process.env.BROKER_URL || "http://nano-broker.sub-agent.svc.cluster.local:8091";
    console.log(`[LocalNano] Proxy starting for Actor: ${this.actorId}`);
  }

  // Registration: Forward to External Broker
  public async handleLocalRegistration() {
    console.log(`[LocalNano] Received registration from local agent. Forwarding to External Broker...`);
    try {
      const resp = await fetch(`${this.brokerUrl}/register`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ actorId: this.actorId })
      });
      const data = await resp.json();
      console.log(`[LocalNano] External registration successful:`, data);
      return { ok: true, broker: data.broker };
    } catch (e: any) {
      console.error(`[LocalNano] External registration failed: ${e.message}`);
      return { ok: false, error: e.message };
    }
  }

  // External Trigger: Synthetic Injection
  public handleExternalTrigger(task: string, sender: string, source: string) {
    console.log(`[LocalNano] External Trigger (${source}) Received from ${sender}: "${task}". Injecting...`);
    this.localQueue.push({ task, sender, source });
    
    const reasoning = `[WHATSAPP_SKILL] Event detected from ${sender}.\n` +
                      `[SYNTHETIC_INJECTION] Appending to local task queue.\n` +
                      `[REHYDRATION] State successfully thawed on Substrate.`;
    
    return { ok: true, status: "injected", result: reasoning };
  }

  // WhatsApp Reply Skill: Forward to Broker
  public async handleSendMessage(to: string, text: string) {
    console.log(`[LocalNano] Agent calling WhatsApp Skill for ${to}...`);
    try {
      await fetch(`${this.brokerUrl}/send-message`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ to, text })
      });
      return { ok: true };
    } catch (e: any) {
      console.error(`[LocalNano] WhatsApp Skill failed: ${e.message}`);
      return { ok: false, error: e.message };
    }
  }

  public fetchMessage() {
    return this.localQueue.shift() || null;
  }
}

const app = new Hono();
const proxy = new LocalNanoProxy();

// --- Local Endpoints (for AgentApp) ---

app.post("/local/register", async (c) => {
  const result = await proxy.handleLocalRegistration();
  return c.json(result);
});

app.get("/local/messages", (c) => {
  return c.json(proxy.fetchMessage());
});

app.post("/local/send-whatsapp", async (c) => {
  const { to, text } = await c.req.json();
  const result = await proxy.handleSendMessage(to, text);
  return c.json(result);
});

// --- External Endpoints (for Broker) ---

app.post("/task", async (c) => {
  const { task, sender, source } = await c.req.json();
  const result = proxy.handleExternalTrigger(task, sender, source);
  return c.json(result);
});

export default {
  port: 8080,
  fetch: app.fetch
};
