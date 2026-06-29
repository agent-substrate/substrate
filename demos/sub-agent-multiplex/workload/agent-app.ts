import { Hono } from "hono";

/**
 * Substrate-Ignorant Nano Agent App
 * 
 * This represents the "End User Agent".
 */
class AgentApp {
  private taskCounter: number = 0;
  private readonly localNanoUrl: string = "http://localhost:8080";

  constructor() {
    console.log("[AgentApp] Starting...");
    this.registerWithLocalNano();
    this.pollForTasks();
  }

  private async registerWithLocalNano() {
    try {
      await fetch(`${this.localNanoUrl}/local/register`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ name: "NanoClaw-User-Agent" })
      });
    } catch (e: any) {}
  }

  private async pollForTasks() {
    setInterval(async () => {
      try {
        const resp = await fetch(`${this.localNanoUrl}/local/messages`);
        const data = await resp.json();
        
        if (data && data.task) {
          console.log(`[AgentApp] New message received: "${data.task}" from ${data.sender}`);
          await this.executeTask(data.task, data.sender);
        }
      } catch (e) {}
    }, 2000);
  }

  private async executeTask(task: string, sender: string) {
    this.taskCounter++;
    console.log(`[AgentApp] Executing task #${this.taskCounter}: ${task}`);
    
    // Simulate real work
    await new Promise(r => setTimeout(r, 4000));
    
    // High-Fidelity Reasoning Payload (Simulated LLM)
    const report = `THOUGHT: User requested "${task}". I need to verify cluster-wide security policies.\n` +
                   `ANALYSIS: Scanning gVisor sandbox boundaries for Actor ${process.env.ATE_ACTOR_ID}...\n` +
                   `RESULT: No anomalies detected. Filesystem rehydration confirmed. Memory state preserved.\n` +
                   `ACTION: Returning audit report to WhatsApp Gateway.`;

    const replyText = `✅ NanoClaw Task Complete!\n\n${report}\n\nProcessed on: Substrate (gVisor)\nDensity Ratio: 1.5x Overcommit.`;
    
    try {
      await fetch(`${this.localNanoUrl}/local/send-whatsapp`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ to: sender, text: replyText })
      });
      console.log("[AgentApp] Reply sent via WhatsApp skill.");
    } catch (e) {
      console.error("[AgentApp] Failed to send reply.");
    }
  }

  public getStatus() {
    return { counter: this.taskCounter, status: "ready" };
  }
}

const app = new Hono();
const agent = new AgentApp();
app.get("/health", (c) => c.json(agent.getStatus()));

export default {
  port: 8081,
  fetch: app.fetch
};
