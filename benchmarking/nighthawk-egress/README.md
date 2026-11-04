# Nighthawk Egress Benchmark

Benchmarks the Agent Substrate egress dataplane (`atunnel` -> `atenet-egress`) using [Nighthawk](https://github.com/envoyproxy/nighthawk) in [adaptive load mode](https://github.com/envoyproxy/nighthawk/blob/main/docs/root/adaptive_load_controller.md).

## Objective

For a given `atenet-egress` CPU configuration (`envoyCpu`) and egress mode (`http`, `mitm`, `passthrough`), find the maximum request rate the egress gateway sustains while keeping tail latency (`mean + 2σ`), success rate (`>= 0.998`), and open-loop send rate (`>= 0.90`) within their thresholds, and verify from `atenet-egress` Prometheus counters that every request traversed the expected Envoy filter chain and `ext_proc` policy stream.

## Architecture

The benchmark places `nighthawk_service` inside a sandboxed actor (`nighthawk/tester`) so every outbound request traverses the production egress dataplane from the actor network namespace through `egress-atunnel` and `atenet-egress` to `nighthawk-server`.

```mermaid
flowchart LR
    subgraph runner_job["nighthawk-egress-runner Job (benchmarking ns)"]
        runner["runner.py"]
        alc["nighthawk_adaptive_load_client<br/>exponential ramp + binary search"]
        runner --> alc
    end

    invoker["traffic-driver-invoker<br/>(:19980, adds ate-target-actor)"]
    router["atenet-router<br/>(:443)"]

    subgraph worker["WorkerPod (ateworker-gvisor)"]
        atunnel-ingress["ingress atunnel"]
        actor["Actor: nighthawk/tester<br/>(nighthawk_service :80)"]
        atunnel-egress["egress atunnel<br/>(HTTP/2 CONNECT pool, 100 streams/conn)"]
    end

    subgraph gateway["atenet-egress Pod (envoy + ext-proc pinned to envoyCpu)"]
        envoy["envoy (:443, :15090/stats/prometheus)<br/>listener egress + internal mitm_listener"]
        extproc["ext-proc sidecar<br/>EgressPolicy evaluation"]
        envoy <-->|"gRPC ext_proc"| extproc
    end

    server["nighthawk-server<br/>(:80 HTTP, :443 MITM, :8443 Passthrough)"]

    alc -->|"gRPC ExecutionStream"| invoker
    invoker -->|"mTLS + ate-target-actor: nighthawk/tester"| router
    router -->|"mTLS"| atunnel-ingress
    atunnel-ingress --> actor
    actor ==>|"outbound HTTP/1.1 or TLS"| atunnel-egress
    atunnel-egress ==>|"mTLS HTTP/2 CONNECT"| envoy
    envoy ==>|"upstream HTTP / TLS"| server
    runner -.->|"scrape before & after (:15090)"| envoy
```

### Egress Modes

`egress-policy.yaml` gives `nighthawk/tester` one rule per mode for `nighthawk-server.nighthawk.svc` and `nighthawk-server.nighthawk.svc.cluster.local` (actor `/etc/resolv.conf` uses `ndots:5`). `atenet-egress` picks the inner filter chain from the kind of rule that matched; see "egress legs" in `cmd/atenet/internal/router/README.md` for how each chain handles the stream.

| Egress Mode | Target URL | Port | Filter chain |
|---|---|---|---|
| Plaintext HTTP (`http`) | `http://nighthawk-server.nighthawk.svc:80/` | `80` | `egress_cleartext` |
| HTTPS TLS MITM (`mitm`) | `https://nighthawk-server.nighthawk.svc:443/` | `443` | `egress_tls_mitm` |
| TLS Passthrough (`passthrough`) | `https://nighthawk-server.nighthawk.svc:8443/` | `8443` | `egress_passthrough` |

`egress-atunnel` pools mTLS HTTP/2 connections to `atenet-egress:443` (`ClientConn.Reserve` in `internal/atunnel/egress/client.go`) up to Envoy's `http2_protocol_options.max_concurrent_streams: 100` on listener `egress` (`manifests/ate-install/atenet-egress.yaml`), opening a new upstream mTLS TCP connection for every 100 concurrent `CONNECT` streams.

For TLS modes (`:443` and `:8443`), `spec.py` configures an `envoy.extensions.transport_sockets.tls.v3.UpstreamTlsContext` `transport_socket` with `sni: "nighthawk-server.nighthawk.svc"`, `alpn_protocols: ["http/1.1"]`, and an exact DNS SAN matcher on `nighthawk-server.nighthawk.svc`. In `mitm` mode, `nighthawk_service` validates the `sdsmint` leaf certificate against the actor's projected trust bundle (`trusted_ca.filename: "/run/ate/egress-trust-bundle/trust-bundle.pem"`). In `passthrough` mode, `runner.py` reads the `servicedns.podcert.ate.dev` `ClusterTrustBundle` mounted at `/run/servicedns-ca/ca.crt` (`--servicedns-ca-cert`) and embeds it via `trusted_ca.inline_string`.

## Adaptive Runner Lifecycle and Module Dependency Graph

### 6-Step Adaptive Benchmark Lifecycle

```mermaid
sequenceDiagram
    participant Dev as run-dev.sh
    participant Job as runner.py (K8s Job)
    participant Prom as atenet-egress (:15090)
    participant ALC as nighthawk_adaptive_load_client
    participant Inv as traffic-driver-invoker (:19980)
    participant Actor as nighthawk/tester (nighthawk_service)
    participant Srv as nighthawk-server (:80/:443/:8443)
    participant GCS as GCS / Local Artifacts

    Dev->>Job: 1. Build image, render Job manifest, kubectl apply
    Job->>Job: 2. Render spec.textproto via spec.build_egress_session_spec()
    Job->>Prom: 3. Scrape gateway_before.prom & parse baseline counters
    Job->>ALC: 4. Launch nighthawk_adaptive_load_client (--spec-file)
    loop Adjusting stages (exponential ramp + binary search) & Testing stage
        ALC->>Inv: gRPC ExecutionRequest (target URI, RPS, concurrency=16)
        Inv->>Actor: Route via atenet-router (ate-target-actor: nighthawk/tester)
        Actor->>Srv: Open-loop HTTP/TLS traffic via egress-atunnel & atenet-egress
        Actor-->>ALC: ExecutionResponse (histograms, HTTP/SSL/pool counters)
    end
    ALC-->>Job: Write output.textproto & exit
    Job->>Prom: 5. Scrape gateway_after.prom & compute gateway_counter_deltas()
    Job->>GCS: 6. Write capacity.json, stats.jsonl, results.json & upload to --dest
```

### Python Module and Container Dependency Graph

`benchmarking/nighthawk-egress` reuses the protobuf descriptor pool loader, `AdaptiveLoadSessionSpec` builder (`build_session_spec`), and `AdaptiveLoadSessionOutput` parser (`parse_output_textproto`, `stage_rows`, `stats_records`, `write_jsonl`, `session_succeeded`) from `benchmarking/nighthawk_common` (`/app/nighthawk_common` in the container image).

```mermaid
flowchart TD
    subgraph common["benchmarking/nighthawk_common (packaged as nighthawk_common)"]
        cm_spec["spec.py<br/>build_session_spec()<br/>spec_dict_to_textproto()<br/>load_pool()"]
        cm_out["output.py<br/>parse_output_textproto()<br/>stage_rows()<br/>stats_records() / write_jsonl()<br/>session_succeeded()"]
    end

    subgraph egress["benchmarking/nighthawk-egress"]
        eg_spec["spec.py<br/>MODE_URIS (:80, :443, :8443)<br/>build_egress_session_spec()"]
        eg_out["output.py<br/>GATEWAY_COUNTER_KEYS<br/>parse_gateway_prometheus()<br/>gateway_counter_deltas()<br/>capacity_summary()"]
        eg_run["runner.py<br/>scrape_gateway_prometheus()<br/>main()"]
        eg_df["Dockerfile<br/>copies /out/nighthawk.desc + nighthawk_common"]
    end

    cm_spec --> eg_spec
    cm_out --> eg_out
    eg_spec --> eg_run
    eg_out --> eg_run
    eg_df -.->|"packages"| cm_spec
    eg_df -.->|"packages"| cm_out
    eg_df -.->|"packages"| eg_run
```

### Script and Manifest Dependency Graph

```mermaid
flowchart TD
    deploy_sh["deploy.sh"]
    deploy_td["deploy-traffic-driver.sh"]
    ns["namespace.yaml<br/>(Namespace: nighthawk)"]
    srv["nighthawk-server.yaml<br/>(ConfigMap + Deployment + Service)"]
    inv["traffic-driver-invoker.yaml<br/>(ConfigMap + Deployment + Service)"]
    met["atenet-egress-metrics.yaml<br/>(Service: atenet-egress-metrics :15090)"]
    pool["traffic-driver.yaml.tmpl<br/>(WorkerPool: 12-18 vCPU, 8-10 GiB)"]
    tmpl["traffic-driver-template.yaml.tmpl<br/>(ActorTemplate: 16 vCPU, 8 GiB)"]
    pol["egress-policy.yaml<br/>(EgressPolicy: :80, :443, :8443)"]
    kate["cmd/kubectl-ate<br/>(auto-built by build_kubectl_ate)"]
    ko["hack/run-tool.sh ko"]

    deploy_sh -->|"kubectl apply"| ns
    deploy_sh -->|"kubectl apply"| srv
    deploy_sh -->|"kubectl apply"| inv
    deploy_sh -->|"kubectl apply"| met

    deploy_td -->|"build_kubectl_ate"| kate
    deploy_td -->|"kubectl apply"| ns
    deploy_td -->|"ko resolve + kubectl apply"| ko
    ko --> pool
    deploy_td -->|"kubectl-ate create actor-template"| tmpl
    deploy_td -->|"kubectl-ate create egress-policy"| pol
```

## Key Design Decisions

### 1. Unified 25 ms Tail-Latency SLO (`latency-ns-mean-plus-2stdev <= 25 ms`)

`spec.build_egress_session_spec` defaults to `tail_latency_slo_ms=25.0` across all three egress modes (`http`, `mitm`, `passthrough`), matching `nighthawk-ingress`:

```python
def build_egress_session_spec(
    mode: str,
    *,
    concurrency: int = DEFAULT_CONCURRENCY,
    connections: int = DEFAULT_CONNECTIONS,
    max_pending: int = DEFAULT_MAX_PENDING,
    initial_rps: int = DEFAULT_INITIAL_RPS,
    exp_factor: float = DEFAULT_EXP_FACTOR,
    measuring_period_s: int = DEFAULT_MEASURING_PERIOD_S,
    convergence_deadline_s: int = DEFAULT_CONVERGENCE_DEADLINE_S,
    testing_stage_s: int = DEFAULT_TESTING_STAGE_S,
    cooldown_s: int = DEFAULT_COOLDOWN_S,
    success_rate_threshold: float = DEFAULT_SUCCESS_RATE_THRESHOLD,
    send_rate_threshold: float | None = DEFAULT_SEND_RATE_THRESHOLD,
    tail_latency_slo_ms: float | None = DEFAULT_TAIL_LATENCY_SLO_MS,
    servicedns_ca_pem: str | None = None,
) -> dict[str, Any]:
```

Every run records `slo_max_rps` (the highest RPS stage passing all three thresholds) and `slo_max_stage` in `capacity.json` with `binding_threshold` identifying any breached thresholds even if the final 60s verification stage experiences transient tail jitter above 25 ms.

### 2. Connection Pool and Pending Queue Sizing (`connections=16, max_pending=32`)

In Nighthawk's Envoy client, setting a large `max_pending_requests` (such as `10000`) on an HTTPS/TLS pool (`mitm` or `passthrough`) queues thousands of pending requests during overload stages and spawns TLS handshakes that are still in flight when the adjusting stage ends. Tearing down an `Http1PoolImpl` with in-flight TLS handshakes triggers an upstream Nighthawk C++ crash (`Pure virtual function called!`) inside `nighthawk_service`.

Bounding `max_pending` to `32` per worker with `connections=16` (256 parallel connections across 16 workers, verified up to `connections=128` / 2,048 connections) optimizes connection reuse while draining pending requests cleanly at stage boundaries without crashing `nighthawk_service`.

At 256 total connections, Little's Law bounds the client pool concurrency on high-RPS suites (`http_2cpu`, `passthrough`). `capacity.json` flags this saturation via `client_pool_bound: true` (tripped when `upstream_rq_pending_overflow > 0` or `queue_to_connect_p50_ms > 1.0`).

### 3. 10-Counter Gateway Dataplane Proof (`output.py`)

`runner.py` scrapes `http://atenet-egress-metrics.ate-system.svc:15090/stats/prometheus` before and after the adaptive session and computes deltas for 10 canonical gateway counters via `gateway_counter_deltas()`:

| `capacity.json` key | Prometheus series on `atenet-egress:15090` | Moves in |
|---|---|---|
| `egress_connect_2xx` | `envoy_http_downstream_rq_xx{envoy_http_conn_manager_prefix="egress_connect",envoy_response_code_class="2"}` | all modes |
| `egress_downstream_cx_total` | `envoy_http_downstream_cx_total{envoy_http_conn_manager_prefix="egress_connect"}` | all modes |
| `mitm_downstream_cx_total` | `envoy_listener_downstream_cx_total{envoy_listener_address="mitm"}` | all modes |
| `mitm_ssl_handshake` | `envoy_listener_ssl_handshake{envoy_listener_address="mitm"}` | `mitm` |
| `cleartext_ext_proc_streams` | `envoy_http_ext_proc_egress_policy_cleartext_streams_started{envoy_http_conn_manager_prefix="mitm_cleartext"}` | `http` |
| `mitm_ext_proc_streams` | `envoy_http_ext_proc_egress_policy_mitm_streams_started{envoy_http_conn_manager_prefix="mitm_http"}` | `mitm` |
| `passthrough_cx_total` | `envoy_tcp_downstream_cx_total{envoy_tcp_prefix="passthrough"}` | `passthrough` |
| `ext_proc_upstream_rq_total` | `envoy_cluster_upstream_rq_total{envoy_cluster_name="ext_proc_server"}` | all modes |
| `forward_proxy_cx_overflow` | `envoy_cluster_upstream_cx_overflow{envoy_cluster_name=~"egress_forward_proxy.*"}` | 0 (circuit breaker) |
| `forward_proxy_rq_pending_overflow` | `envoy_cluster_upstream_rq_pending_overflow{envoy_cluster_name=~"egress_forward_proxy.*"}` | 0 (circuit breaker) |

The prefixes are the `stat_prefix` values `manifests/ate-install/atenet-egress.yaml` sets on the `egress` listener, the `mitm` listener, and its three filter chains.

Across every run, the counters verify the exact dataplane invariant:
```text
ext_proc_upstream_rq_total == egress_connect_2xx + cleartext_ext_proc_streams + mitm_ext_proc_streams
```

(`capacity.json` records `ext_proc_invariant_ok`, `mode_exclusivity_ok`, and `forward_proxy_overflow_ok` checking these invariants automatically.)
- **`http` (`:80`)**: `cleartext_ext_proc_streams > 0`, `mitm_ssl_handshake == 0`, `mitm_ext_proc_streams == 0`, `passthrough_cx_total == 0`.
- **`mitm` (`:443`)**: `mitm_ssl_handshake > 0`, `mitm_ext_proc_streams > 0`, `cleartext_ext_proc_streams == 0`, `passthrough_cx_total == 0`.
- **`passthrough` (`:8443`)**: `passthrough_cx_total == egress_connect_2xx`, `mitm_ssl_handshake == 0`, `cleartext_ext_proc_streams == 0`, `mitm_ext_proc_streams == 0`, and `ext_proc_upstream_rq_total == egress_connect_2xx` (1 `ext_proc` call per TCP connection at `CONNECT` time, 0 per inner HTTP request).

## Prerequisites

The actor, the worker pool, and `nighthawk-server` are sized to outscale `atenet-egress`, so a run measures the gateway and not the load generator or the origin server. The sizes target a multi-node benchmark cluster, such as 3 x `c3-standard-22` nodes (22 vCPUs and 88 GiB each):

- `traffic-driver-template.yaml.tmpl` gives the `nighthawk/tester` sandbox 16 vCPUs and 8 GiB, so `nighthawk_service` runs the 16 worker threads that the example `nighthawk_client` command below asks for (`--concurrency 16`) without CPU throttling.
- `traffic-driver.yaml.tmpl` requests 12 vCPUs and 8 GiB for the worker pod and limits it to 18 vCPUs and 10 GiB. The request fits a `c3-standard-22` node beside the system DaemonSets, and the limit leaves burst room for the actor and the worker's atunnel. The worker pod and `nighthawk-server` request 18 vCPUs together, so the scheduler may place them on the same node; this benchmark accepts that.
- `nighthawk-server.yaml` runs `nighthawk_test_server` with `--concurrency 8` and 6-8 vCPUs, so upstream HTTP and TLS on `:80`, `:443`, and `:8443` do not saturate before the gateway.

1. **Multi-node benchmark cluster with Agent Substrate deployed**:
   - **Existing cluster** (with `.ate-dev-env.sh` configured):
     ```bash
     go run ./cmd/ate-setup deploy ate-system \
       --credential-provider='{"name":"k8s.io"}' \
       --ingress-allowed-spiffe-ids=spiffe://cluster.local/ns/nighthawk/sa/traffic-driver-invoker
     ```
   - To set the `atenet-egress` Envoy worker thread count (`--concurrency`), prefix the command above with `E2E_ATE_ENVOY_CONCURRENCY=<n>`. `E2E_ATE_ENVOY_CONCURRENCY=1` measures a single Envoy core.
   - Provision the CA bundle in `ate-system` so `atenet-egress` trusts `nighthawk-server`'s `servicedns` certificate on `:443`. The script defaults to a kind context, so name the cluster's context:
     ```bash
     KUBECTL_CONTEXT=<context> hack/setup-e2e-egress-tls-kind.sh
     ```
   - To run on a smaller cluster, lower `requests.cpu` and `requests.memory` in `traffic-driver.yaml.tmpl` and `nighthawk-server.yaml` before deploying.
2. **`kubectl-ate`**: `deploy-traffic-driver.sh` builds it from `./cmd/kubectl-ate` into a temporary directory.
3. **Docker** running locally (used to run `nighthawk_client`).

## Settled 25 ms Tail-Latency SLO GKE Results (`c3-standard-22`)

Measured on GKE cluster `nighthawk-bench-1` (`us-east1-b`, 4 x `c3-standard-22` nodes, 22 vCPUs / 88 GiB RAM per node) with `envoy` and `ext-proc` in `deployment/atenet-egress` both pinned to `envoyCpu` (`1 vCPU` and `2 vCPU` at the default `connections=16, max_pending=32`, plus earlier `4 vCPU` runs at `connections=64, max_pending=32`):

### Throughput and Latency Summary

| Mode | `envoyCpu` (`envoy` + `ext-proc`) | Conns / MaxPending | SLO (`mean+2σ`) | Converged | Binding Threshold(s) | `slo_max_rps` | Testing Stage `achieved_rps` | `p50_ms` | `p90_ms` | `p95_ms` | `p99_ms` | `error_rate` |
|---|---:|---:|---:|:---:|---|---:|---:|---:|---:|---:|---:|---:|
| `http` | `1 + 1` | `16 / 32` | `25 ms` | `true` | `latency-ns-mean-plus-2stdev` | **7,424.0** | **7,424.0** | 10.81 | 16.52 | 21.78 | 31.97 | 0.000132 |
| `mitm` | `1 + 1` | `16 / 32` | `25 ms` | `true` | `latency-ns-mean-plus-2stdev`, `send-rate` | **8,484.3** | **8,546.7** | 14.45 | 30.43 | 32.25 | 36.39 | 0.000246 |
| `passthrough` | `1 + 1` | `16 / 32` | `25 ms` | `true` | `send-rate` | **41,673.2** | **41,673.2** | 5.83 | 6.96 | 7.38 | 8.43 | 0.000102 |
| `http` | `2 + 2` | `16 / 32` | `25 ms` | `true` | `latency-ns-mean-plus-2stdev`, `send-rate` | **16,426.8** | **16,044.6** | 15.50 | 21.26 | 22.24 | 24.41 | 0.000266 |
| `mitm` | `2 + 2` | `16 / 32` | `25 ms` | `true` | `latency-ns-mean-plus-2stdev`, `send-rate` | **8,725.8** | **8,795.4** | 12.85 | 27.48 | 29.18 | 32.55 | 0.000188 |
| `passthrough` | `2 + 2` | `16 / 32` | `25 ms` | `true` | `send-rate` | **44,069.1** | **42,447.4** | 5.76 | 6.95 | 7.31 | 8.13 | 0.000101 |
| `http` | `4 + 4` | `64 / 32` | `25 ms` | `true` | `latency-ns-mean-plus-2stdev` | **10,432.0** | **10,432.0** | 1.18 | 1.81 | 3.93 | 14.62 | 0.0 |
| `mitm` | `4 + 4` | `64 / 32` | `25 ms` | `false` | `latency-ns-mean-plus-2stdev`, `success-rate` | **18,383.1** | **18,542.9** | 3.10 | 23.93 | 29.11 | 42.65 | 0.0 |
| `passthrough` | `4 + 4` | `64 / 32` | `25 ms` | `false` | `send-rate` | **65,848.5** | **65,870.5** | 11.93 | 15.72 | 17.21 | 20.32 | 0.0 |

Across all six `connections=16` runs, the 256-connection pool (`16` workers x `16` connections) held `cx_churn_ratio = 1.0` (`upstream_cx_total = 256` in the 60s `testing` stage, `0` connect failures or timeouts, and `sequencer_blocking_p99_ms = 0.0`). In `mitm` and `passthrough` at `2 + 2` vCPU, `egress-atunnel` on the single `traffic-driver` pod multiplexed 256 tunnels onto 3 outer HTTP/2 connections (`100` streams per connection), and `SO_REUSEPORT` placed the active outer connections onto one Envoy worker thread (`mitm_listener` stays on the accepting worker thread), capping `atenet-egress:envoy` near `1,000m`.

### Gateway Counter Deltas (`gateway_counters`)

| Run | `egress_connect_2xx` | `egress_downstream_cx_total` | `mitm_downstream_cx_total` | `mitm_ssl_handshake` | `cleartext_ext_proc_streams` | `mitm_ext_proc_streams` | `passthrough_cx_total` | `ext_proc_upstream_rq_total` |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| `http` (1 CPU, 25ms) | 2,125 | 3 | 2,125 | 0 | **1,455,712** | 0 | 0 | **1,457,837** |
| `mitm` (1 CPU, 25ms) | 4,069 | 5 | 4,069 | **4,069** | 0 | **2,069,056** | 0 | **2,073,125** |
| `passthrough` (1 CPU, 25ms) | 5,345 | 9 | 5,345 | 0 | 0 | 0 | **5,345** | **5,345** |
| `http` (2 CPU, 25ms) | 6,860 | 7 | 6,860 | 0 | **3,798,074** | 0 | 0 | **3,804,934** |
| `mitm` (2 CPU, 25ms) | 3,948 | 5 | 3,948 | **3,948** | 0 | **2,121,294** | 0 | **2,125,242** |
| `passthrough` (2 CPU, 25ms) | 6,904 | 7 | 6,904 | 0 | 0 | 0 | **6,904** | **6,904** |
| `http` (4 CPU, 25ms, `conn=64`) | 8,018 | 11 | 8,006 | 0 | **1,641,973** | 0 | 0 | **1,649,991** |
| `mitm` (4 CPU, 25ms, `conn=64`) | 10,187 | 10 | 10,187 | **10,187** | 0 | **3,041,258** | 0 | **3,051,445** |
| `passthrough` (4 CPU, 25ms, `conn=64`) | 12,245 | 11 | 12,245 | 0 | 0 | 0 | **12,245** | **12,245** |

### Peak Container CPU and Memory Utilization (`kubectl top pods --containers`)

| Run | `atenet-egress:envoy` | `atenet-egress:ext-proc` | `atenet-router:envoy` | `traffic-driver:worker` | `nighthawk-server` |
|---|---:|---:|---:|---:|---:|
| `http` (1 CPU, 25ms) | `1,000m` / `52Mi` | `469m` / `17Mi` | `6m` / `25Mi` | `7,178m` / `667Mi` | `409m` / `20Mi` |
| `mitm` (1 CPU, 25ms) | `1,000m` / `54Mi` | `515m` / `20Mi` | `7m` / `25Mi` | `7,490m` / `672Mi` | `232m` / `23Mi` |
| `passthrough` (1 CPU, 25ms) | `999m` / `56Mi` | `14m` / `23Mi` | `8m` / `25Mi` | `12,460m` / `679Mi` | `2,310m` / `27Mi` |
| `http` (2 CPU, 25ms) | `1,998m` / `56Mi` | `971m` / `27Mi` | `7m` / `25Mi` | `8,298m` / `698Mi` | `843m` / `29Mi` |
| `mitm` (2 CPU, 25ms) | `1,001m` / `65Mi` | `513m` / `22Mi` | `7m` / `25Mi` | `7,550m` / `677Mi` | `505m` / `29Mi` |
| `passthrough` (2 CPU, 25ms) | `999m` / `67Mi` | `123m` / `22Mi` | `8m` / `25Mi` | `12,370m` / `681Mi` | `2,383m` / `30Mi` |
| `http` (4 CPU, 25ms, `conn=64`) | `2,345m` / `115Mi` | `1,138m` / `20Mi` | `7m` / `25Mi` | `6,805m` / `762Mi` | `515m` / `35Mi` |
| `mitm` (4 CPU, 25ms, `conn=64`) | `2,957m` / `143Mi` | `1,554m` / `21Mi` | `7m` / `25Mi` | `8,103m` / `769Mi` | `715m` / `33Mi` |
| `passthrough` (4 CPU, 25ms, `conn=64`) | `1,929m` / `144Mi` | `1,539m` / `21Mi` | `8m` / `26Mi` | `11,510m` / `784Mi` | `3,385m` / `45Mi` |

## Running the Benchmark

### Automated Adaptive Run (`run-dev.sh`)

With `.ate-dev-env.sh` configured and `ate-system` deployed on the cluster, run one adaptive capacity benchmark end to end:

```bash
./benchmarking/nighthawk-egress/run-dev.sh --mode http --envoy-cpu 1 --tail-latency-slo-ms 25
./benchmarking/nighthawk-egress/run-dev.sh --skip-deploy --mode mitm --envoy-cpu 1 --tail-latency-slo-ms 25
./benchmarking/nighthawk-egress/run-dev.sh --skip-deploy --mode passthrough --envoy-cpu 1 --tail-latency-slo-ms 25
```

### Output Artifacts

Each run writes its artifacts under `<dest>/runs/<name>/run_date=YYYY-MM-DD/run_ts=<epoch>/run_tag=<tag>/`:

| File | Description |
|---|---|
| `capacity.json` | Verdict summary: `mode`, `envoy_cpu`, `converged`, `testing_stage_completed`, `testing_passed`, `slo_max_rps`, `slo_max_stage`, `binding_threshold`, `client_pool_bound`, `gateway_counters`, `tunnels_per_stage`, `requests_per_connection`, `cx_churn_ratio`, connection setup / bottleneck telemetry (`queue_to_connect_*`, `sequencer_blocking_*`), and testing-stage `p50_ms`/`p95_ms`/`p99_ms`. Runner exits `0` iff `nighthawk_adaptive_load_client` exits `0`, `testing_stage_completed` is true, `slo_max_rps` is non-null, and gateway Prometheus pod IPs/counters remained valid across the session (`testing_passed` records whether the 60s stage stayed within thresholds); it also records `ext_proc_invariant_ok`, `mode_exclusivity_ok`, and `forward_proxy_overflow_ok`. |
| `stats.jsonl` | Per-stage JSONL records with Nighthawk built-in metrics, HTTP/SSL/pool counters, connection metrics (`requests_per_connection`, `cx_churn_ratio`), connection setup telemetry (`queue_to_connect_*`, `sequencer_blocking_*`), latency percentiles, and `failed_thresholds`. |
| `results.json` | Full `AdaptiveLoadSessionOutput` converted to JSON. |
| `spec.textproto`, `output.textproto` | Raw Nighthawk input spec and output textproto. |
| `gateway_before.prom`, `gateway_after.prom` | Raw Prometheus snapshots from `atenet-egress:15090/stats/prometheus` before and after the run. |
| `status.json`, `logs.txt` | Adaptive client exit code, convergence flag, `testing_stage_completed`, `slo_max_rps`, and combined stdout/stderr logs. |

### Manual Setup and Single-Rate Check

To deploy the workload components manually:

```bash
./benchmarking/nighthawk-egress/deploy.sh
./benchmarking/nighthawk-egress/deploy-traffic-driver.sh
kubectl -n nighthawk port-forward svc/traffic-driver-invoker 19980:19980
```

`deploy-traffic-driver.sh` builds `kubectl-ate` from `./cmd/kubectl-ate` into a temporary directory.

Then run `nighthawk_client` against `grpc://127.0.0.1:19980`:

```bash
docker run --rm --network=host --entrypoint /usr/local/bin/nighthawk_client envoyproxy/nighthawk-dev:latest --nighthawk-service grpc://127.0.0.1:19980 --rps 1000 --concurrency 16 --protocol http1 --duration 60 http://nighthawk-server.nighthawk.svc:80/
```

Nighthawk multiplies `--rps` by `--concurrency`, so this command targets 16,000 requests per second in total.

## Cleanup

Delete the `nighthawk/tester` actor, actor template, atespace, and worker pool:

```bash
./benchmarking/nighthawk-egress/deploy-traffic-driver.sh --delete
```

Delete nighthawk-server, traffic-driver-invoker, and the atenet-egress-metrics Service:

```bash
./benchmarking/nighthawk-egress/deploy.sh --delete
```

Neither script deletes the `nighthawk` namespace. Delete it with `kubectl delete namespace nighthawk` after `deploy-traffic-driver.sh --delete`, because the actor, template, and atespace live in the ate API, not in the namespace.
