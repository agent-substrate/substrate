import { defineConfig } from "tsdown";

const env = {
  NODE_ENV: "production",
  OPENCLAW_SANDBOX_BACKEND: "native",
};

export default defineConfig({
  entry: {
    "substrate/actor-wrapper": "substrate/workload/actor-wrapper.ts",
    "substrate/demo-ui": "substrate/ui/demo-ui.ts",
  },
  env,
  fixedExtension: false,
  platform: "node",
  // Ensure we bundle everything
  unbundle: false,
  deps: {
    // Only externalize core node modules
    external: [/node:/, "fs", "path", "os", "child_process", "crypto", "http", "https", "net", "url", "util", "zlib", "stream", "events", "tty", "readline", "dns", "buffer"],
    skipNodeModulesBundle: false,
  },
});
