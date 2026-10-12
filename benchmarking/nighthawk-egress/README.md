# Nighthawk Egress Benchmark

Benchmarks the Agent Substrate egress dataplane (`atunnel` -> `atenet-egress`) using [Nighthawk](https://github.com/envoyproxy/nighthawk).

## Architecture

```mermaid
flowchart LR
    client["nighthawk_client<br/>(host)"]
    invoker["traffic-driver-invoker<br/>(:19980, adds ate-target-actor)"]
    router["atenet-router<br/>(:80)"]

    subgraph worker["WorkerPod (ateom-gvisor)"]
        atunnel-ingress["ingress atunnel"]
        actor["Actor: nighthawk/tester<br/>(nighthawk_service :80)"]
        atunnel-egress["egress atunnel"]
    end

    egress["atenet-egress<br/>(HTTP / HTTPS MITM / TLS passthrough)"]
    server["nighthawk-server<br/>(:80, :443, :8443)"]

    client -->|"start test"| invoker
    invoker -->|"HTTP/2 + ate-target-actor: nighthawk/tester"| router
    router --> atunnel-ingress
    atunnel-ingress --> actor
    actor ==>|"test traffic"| atunnel-egress
    atunnel-egress ==>|"mTLS"| egress
    egress ==> server
```

To measure pre-CPU performance characteristics of Envoy dataplane set ATE_ENVOY_CONCURRENCY=1 environment
variable when deploying ATE.

## Prerequisites

1. **Cluster with Agent Substrate deployed**:
   - **Existing cluster** (with `.ate-dev-env.sh` configured):
     ```bash
     hack/install-ate.sh --deploy-ate-system --credential-provider='{"name":"k8s.io"}'
     ```
   - **Local `kind` cluster**:
     ```bash
     hack/install-ate-kind.sh --deploy-ate-system --credential-provider='{"enabled":false}'
     ```
   - To set the `atenet-egress` Envoy worker thread count (`--concurrency`), set `ATE_ENVOY_CONCURRENCY` when deploying:
     ```bash
     ATE_ENVOY_CONCURRENCY=1 hack/install-ate-kind.sh --deploy-ate-system --credential-provider='{"enabled":false}'
     ```
2. **`kubectl-ate` installed in `PATH`**:
   ```bash
   go install ./cmd/kubectl-ate
   ```
3. **Docker** running locally (used to run `nighthawk_client`).

## Setup

### 1. Deploy `nighthawk-server` and `traffic-driver-invoker`

```bash
./benchmarking/nighthawk-egress/deploy.sh
```

### 2. Deploy the worker pool, actor template, and `nighthawk/tester` actor

```bash
./benchmarking/nighthawk-egress/deploy-traffic-driver.sh
```

### 3. Port-forward `traffic-driver-invoker`

In a separate terminal, forward local port `19980` to the `traffic-driver-invoker` service:

```bash
kubectl -n ate-system port-forward svc/traffic-driver-invoker 19980:19980
```

## Running the Benchmark

Run `nighthawk_client` against the port-forwarded `traffic-driver-invoker` endpoint (`grpc://127.0.0.1:19980`), which routes the gRPC request to `nighthawk_service` inside the `nighthawk/tester` actor:

```bash
docker run --rm --network=host --entrypoint /usr/local/bin/nighthawk_client envoyproxy/nighthawk-dev:latest --nighthawk-service grpc://127.0.0.1:19980 --rps 1000 --concurrency 1 --protocol http1 --duration 60  http://nighthawk-server.ate-system.svc.cluster.local:80
```

See [Nighthawk CLI help](https://github.com/envoyproxy/nighthawk#using-the-nighthawk-client-cli) for all command line options.

### Target URLs by Egress Mode

| Egress Mode | Target URL | Port |
|---|---|---|
| Plaintext HTTP | `http://nighthawk-server.ate-system.svc.cluster.local:80` | `80` |
| HTTPS (TLS MITM) | `https://nighthawk-server.ate-system.svc.cluster.local:443` | `443` |
| TLS Passthrough | `https://nighthawk-server.ate-system.svc.cluster.local:8443` | `8443` |

## Cleanup

Delete the `nighthawk/tester` actor, actor template, atespace, and worker pool:

```bash
./benchmarking/nighthawk-egress/deploy-traffic-driver.sh --delete
```

Delete `nighthawk-server` and `traffic-driver-invoker` from `ate-system`:

```bash
./benchmarking/nighthawk-egress/deploy.sh --delete
```

### Components

- **[`nighthawk-server.yaml`](nighthawk-server.yaml)**: In-cluster `nighthawk_test_server` (`Deployment` + `Service` in `ate-system`) acting as the upstream target. Listens for plaintext HTTP on `:80`/`:8080` and TLS on `:443`/`:8443` (with a serving certificate issued by `servicedns.podcert.ate.dev/identity` for `nighthawk-server.ate-system.svc.cluster.local`).
- **[`traffic-driver-invoker.yaml`](traffic-driver-invoker.yaml)**: In-cluster Envoy proxy (`Deployment` + `Service` in `ate-system`) listening on `:19980`. Injects `ate-target-actor: nighthawk/tester` onto incoming `nighthawk_client` gRPC requests and forwards them to `atenet-router.ate-system.svc.cluster.local:80`.
- **[`traffic-driver.yaml.tmpl`](traffic-driver.yaml.tmpl)**: `nighthawk` Kubernetes namespace and 1-replica `ateom-gvisor` `WorkerPool` (`traffic-driver`).
- **[`traffic-driver-template.yaml.tmpl`](traffic-driver-template.yaml.tmpl)**: Substrate `ActorTemplate` (`nighthawk/traffic-driver`) running `nighthawk_service` (`traffic-generator-service`) on port `80` inside a gVisor sandbox, configured to trust the egress gateway's MITM CA bundle (`egress-mitm.ate.dev` and `system-roots.ate.dev`).
- **[`egress-policy.yaml`](egress-policy.yaml)**: Substrate `EgressPolicy` applied to the `nighthawk/tester` actor allowing:
  - HTTP to `*` on port `80`
  - HTTPS (TLS MITM) to `*` on port `443`
  - TLS passthrough to `*` on port `8443`
