# Google Claw on Agent Substrate PoC
# Portable Dockerfile for OSS Substrate Migration

# Stage 1: Build the standalone bundles
FROM node:22-slim AS builder

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy standalone package files
COPY package.json ./
# Use npm install for simplicity and portability in the standalone package
RUN npm install

# Copy source code
COPY ui/ ./ui/
COPY workload/ ./workload/

# Build zero-dependency bundles
RUN ./node_modules/.bin/esbuild workload/agent.ts \
    --bundle \
    --platform=node \
    --target=node22 \
    --outfile=dist/agent.js \
    --external:node:*

RUN ./node_modules/.bin/esbuild ui/demo-ui.ts \
    --bundle \
    --platform=node \
    --target=node22 \
    --outfile=dist/demo-ui.js \
    --external:node:*

# Stage 2: Final Production Image
FROM node:22-slim AS runner

WORKDIR /app

# Copy the entire context to check for local binaries
COPY . .

# Install runtime dependencies (tini for signal forwarding, kubectl for dashboard sync)
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    tini \
    && curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl" \
    && chmod +x kubectl \
    && mv kubectl /usr/local/bin/ \
    && rm -rf /var/lib/apt/lists/*

# Copy built assets
COPY --from=builder /app/dist/ ./dist/
# Copy kubectl-ate binary if it exists in context, otherwise download it
# This makes the Dockerfile portable across environments
RUN if [ -f "./kubectl-ate" ]; then \
        mv ./kubectl-ate /usr/local/bin/kubectl-ate; \
    else \
        curl -L -o /usr/local/bin/kubectl-ate https://github.com/agent-substrate/substrate/releases/latest/download/kubectl-ate-linux-amd64; \
    fi && chmod +x /usr/local/bin/kubectl-ate

# Create a /pause hook for Substrate rehydration
RUN echo '#!/bin/sh' > /pause && \
    echo 'echo "[pause] Starting Google Claw agent..."' >> /pause && \
    echo 'exec /usr/bin/tini -- /usr/local/bin/node /app/dist/agent.js' >> /pause && \
    chmod +x /pause

# Default entrypoint (can be overridden by deployment to run demo-ui)
ENTRYPOINT ["/usr/bin/tini", "--", "node", "dist/agent.js"]
