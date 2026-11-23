# Nighthawk Egress Benchmark

Benchmarks the Agent Substrate egress dataplane (`atunnel` -> `atenet-egress`) using [Nighthawk](https://github.com/envoyproxy/nighthawk).

## Architecture

```mermaid
flowchart LR
    client["nighthawk_client<br/>(host)"]
    proxy["kubectl-ate proxy actor<br/>(:19980)"]
    router["atenet-router<br/>(:8444)"]

    subgraph worker["WorkerPod (ateworker-gvisor)"]
        atunnel-ingress["ingress atunnel"]
        actor["Actor: nighthawk/tester<br/>(nighthawk_service :80)"]
        atunnel-egress["egress atunnel"]
    end

    egress["atenet-egress<br/>(HTTP / HTTPS MITM / TLS passthrough)"]
    server["nighthawk-server<br/>(:80, :443, :8443)"]

    client -->|"start test"| proxy
    proxy -->|"CONNECT to nighthawk/tester:80"| router
    router --> atunnel-ingress
    atunnel-ingress --> actor
    actor ==>|"test traffic"| atunnel-egress
    atunnel-egress ==>|"mTLS"| egress
    egress ==> server
```

## Tuning

### Configuration knobs (the `nighthawk-egress:` block)

A `type: nighthawk-egress` entry in [`benchmarking/automation/tests.yaml`](../automation/tests.yaml) sets both knobs in its `nighthawk-egress:` block:

| Knob | Default | Meaning |
|---|---|---|
| `envoyCpu` | required | CPU request and limit of `envoy` and `ext-proc` in `deployment/atenet-egress`, and the Envoy `--concurrency`. |
| `mode` | required | `http`, `mitm`, or `passthrough`. |

## Prerequisites

The actor, the worker pool, and `nighthawk-server` are sized to outscale `atenet-egress` with one Envoy worker thread, so a run measures the gateway and not the load generator or the origin server. `--cordon-system-infra-components` keeps the gateway off the nodes that run the load. The cluster needs the two tainted node pools of [System Infra Isolation](../../docs/concepts/architecture.md#system-infra-isolation) and a default pool:

| Node pool | Example size | Runs | Kept there by |
|---|---|---|---|
| `ate-system-infra` | 1 x `c3-standard-22` | `atenet-egress`, the system under test, beside the other system infra components, which carry no test traffic | the node selector and toleration that `--cordon-system-infra-components` adds |
| `ate-postgres` | 1 x `c3-standard-4` | Postgres | the same flag |
| default | 2 x `c3-standard-22` (22 vCPUs and 88 GiB each) | the worker pod with the `nighthawk/tester` actor, `nighthawk-server`, and `traffic-driver-invoker` | the `NoSchedule` taints of the other two pools, which these pods do not tolerate |

The sizes:

- `traffic-driver-template.yaml.tmpl` gives the `nighthawk/tester` sandbox 16 vCPUs and 8 GiB, so `nighthawk_service` runs the 16 worker threads that the example `nighthawk_client` command below asks for (`--concurrency 16`) without CPU throttling.
- `traffic-driver.yaml.tmpl` requests 12 vCPUs and 8 GiB for the worker pod and sets no limits, so only the actor's own limits cap the load generator. The request fits a `c3-standard-22` node beside the system DaemonSets. The worker pod and `nighthawk-server` request 18 vCPUs together, so the scheduler may place them on the same node; this benchmark accepts that.
- `nighthawk-server.yaml` runs `nighthawk_test_server` with `--concurrency 8` and requests 6 vCPUs, so upstream HTTP and TLS on `:80`, `:443`, and `:8443` do not saturate before the gateway.

1. **Multi-node benchmark cluster with Agent Substrate deployed**:
   - **Existing cluster** (with `.ate-dev-env.sh` configured):
     ```bash
     E2E_ATE_ENVOY_CONCURRENCY=1 go run ./cmd/ate-setup deploy ate-system \
       --cordon-system-infra-components \
       --credential-provider='{"name":"k8s.io"}' \
       --ingress-allowed-spiffe-ids=spiffe://cluster.local/ns/ate-system/sa/atenet-client
     ```
   - `E2E_ATE_ENVOY_CONCURRENCY` sets the `atenet-egress` Envoy worker thread count (`--concurrency`). `E2E_ATE_ENVOY_CONCURRENCY=1` measures a single Envoy core, which the sizes above are for.
   - Provision the CA bundle in `ate-system` so `atenet-egress` trusts `nighthawk-server`'s `servicedns` certificate on `:443`. The script defaults to a kind context, so name the cluster's context:
     ```bash
     KUBECTL_CONTEXT=<context> hack/setup-e2e-egress-tls-kind.sh
     ```
   - To run on a smaller cluster, lower `requests.cpu` and `requests.memory` in `traffic-driver.yaml.tmpl` and `nighthawk-server.yaml` before deploying. A worker pod without limits offers its node's allocatable CPU and memory to actors, so on nodes with fewer than 16 allocatable vCPUs also lower `cpuMillicores` in `traffic-driver-template.yaml.tmpl`. On a cluster without the two tainted pools, leave out `--cordon-system-infra-components`; the gateway then shares nodes with the load.
2. **`kubectl-ate`**: `deploy-traffic-driver.sh` builds it from `./cmd/kubectl-ate` into a temporary directory.
3. **Docker** running locally (used to run `nighthawk_client`).

## Setup

### 1. Deploy `nighthawk-server`

```bash
./benchmarking/nighthawk-egress/deploy.sh
```

### 2. Deploy the worker pool, actor template, and `nighthawk/tester` actor

```bash
./benchmarking/nighthawk-egress/deploy-traffic-driver.sh
```

### 3. Forward a port to the actor

In a separate terminal, forward local port `19980` to `nighthawk_service` in the `nighthawk/tester` actor. `kubectl-ate proxy actor` connects as `spiffe://cluster.local/ns/ate-system/sa/atenet-client`, which is the ID in the deploy command above:

```bash
go run ./cmd/kubectl-ate proxy actor tester -a nighthawk 19980:80
```

## Running the Benchmark

Run `nighthawk_client` against the forwarded port (`grpc://127.0.0.1:19980`), which reaches `nighthawk_service` inside the `nighthawk/tester` actor:

```bash
docker run --rm --network=host --entrypoint /usr/local/bin/nighthawk_client envoyproxy/nighthawk-dev:latest --nighthawk-service grpc://127.0.0.1:19980 --rps 1000 --concurrency 16 --protocol http1 --duration 60  http://nighthawk-server.nighthawk.svc:80
```

Nighthawk multiplies `--rps` by `--concurrency`, so this command targets 16,000 requests per second in total. One Envoy worker thread holds less than that, so the gateway saturates and the output shows the rate it reached.

See [Nighthawk CLI help](https://github.com/envoyproxy/nighthawk#using-the-nighthawk-client-cli) for all command line options.

### Target URLs and Default Egress Policy

| Egress Mode | Target URL | Port |
|---|---|---|
| Plaintext HTTP | `http://nighthawk-server.nighthawk.svc:80` | `80` |
| HTTPS (TLS MITM) | `https://nighthawk-server.nighthawk.svc:443` | `443` |
| TLS Passthrough | `https://nighthawk-server.nighthawk.svc:8443` | `8443` |

## Cleanup

Delete the `nighthawk/tester` actor, actor template, atespace, and worker pool:

```bash
./benchmarking/nighthawk-egress/deploy-traffic-driver.sh --delete
```

Delete `nighthawk-server` from `nighthawk`:

```bash
./benchmarking/nighthawk-egress/deploy.sh --delete
```

Neither script deletes the `nighthawk` namespace. Delete it with `kubectl delete namespace nighthawk` after `deploy-traffic-driver.sh --delete`, because the actor, template, and atespace live in the ate API, not in the namespace.
