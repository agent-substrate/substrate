FROM node:22-slim AS builder
WORKDIR /app
COPY package.json ./
RUN npm install
COPY . .
RUN npm run build

FROM node:22-slim AS runner
WORKDIR /app
COPY --from=builder /app/dist/ ./dist/
RUN apt-get update && apt-get install -y tini curl && \
    curl -LO "https://dl.k8s.io/release/v1.30.0/bin/linux/amd64/kubectl" && \
    chmod +x kubectl && mv kubectl /usr/local/bin/ && \
    curl -L -o /usr/local/bin/kubectl-ate https://github.com/agent-substrate/substrate/releases/latest/download/kubectl-ate-linux-amd64 && \
    chmod +x /usr/local/bin/kubectl-ate
RUN echo '#!/bin/sh' > /pause && chmod +x /pause
ENTRYPOINT ["/usr/bin/tini", "--"]
