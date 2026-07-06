import { serve } from "@hono/node-server";
import proxy from "./local-nano";
import agent from "./agent-app";

/**
 * NanoClaw Unified Entrypoint
 * 
 * This starts the two-layered architecture:
 * 1. Port 8080: The "Local Nano Proxy" (Substrate-aware Infrastructure)
 * 2. Port 8081: The "End User Agent App" (Substrate-ignorant Logic)
 */

console.log("=== NanoClaw Fleet Management Master Initialized ===");

// Start Local Infrastructure Proxy
serve({
  fetch: proxy.fetch,
  port: proxy.port,
}, (info) => {
  console.log(`[Infra] Local Nano Proxy active on port ${info.port}`);
});

// Start End User Agent Application
serve({
  fetch: agent.fetch,
  port: agent.port,
}, (info) => {
  console.log(`[User] End User Agent App active on port ${info.port}`);
});
