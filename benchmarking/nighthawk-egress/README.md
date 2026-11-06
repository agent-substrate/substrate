# Nighthawk Egress Benchmark

Benchmarks the Agent Substrate egress dataplane (`atunnel` -> `atenet-egress`) using [Nighthawk](https://github.com/envoyproxy/nighthawk) in [adaptive load mode](https://github.com/envoyproxy/nighthawk/blob/main/docs/root/adaptive_load_controller.md).

## Objective

For a given `atenet-egress` CPU configuration (`envoyCpu`) and egress mode (`http`, `mitm`, `passthrough`), find the maximum request rate the egress gateway sustains while keeping tail latency (`mean + 2stdev`), success rate (`>= 0.998`), and open-loop send rate (`>= 0.90`) within their thresholds. Before and after each session, scrape `atenet-egress` Prometheus counters to verify that requests traversed the expected Envoy filter chain and `ext_proc` policy stream.

## Architecture

The benchmark runs `nighthawk_service` inside a sandboxed actor (`nighthawk/tester`) so every outbound request traverses the egress dataplane from the actor network namespace through `egress-atunnel` and `atenet-egress` to `nighthawk-server`.

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

`egress-policy.yaml` grants `nighthawk/tester` one rule per mode for `nighthawk-server.nighthawk.svc` and `nighthawk-server.nighthawk.svc.cluster.local`. The actor's `/etc/resolv.conf` keeps the pod's search domains and options (`SandboxResolvConf` in `internal/ateworker/actornet/resolvconf.go`), so it inherits kubelet's `ndots:5`, and `internal/atunnel/dns/evaluator.go` evaluates the `cluster.local`-suffixed query while Envoy matches `Host` or SNI `nighthawk-server.nighthawk.svc`. `atenet-egress` selects the inner filter chain from the matched rule kind (see `cmd/atenet/internal/router/README.md`).

| Egress Mode | Target URL | Port | Filter chain |
|---|---|---|---|
| Plaintext HTTP (`http`) | `http://nighthawk-server.nighthawk.svc:80/` | `80` | `egress_cleartext` |
| HTTPS TLS MITM (`mitm`) | `https://nighthawk-server.nighthawk.svc:443/` | `443` | `egress_tls_mitm` |
| TLS Passthrough (`passthrough`) | `https://nighthawk-server.nighthawk.svc:8443/` | `8443` | `egress_passthrough` |

`egress-atunnel` pools mTLS HTTP/2 connections to `atenet-egress:443` (`ClientConn.Reserve` in `internal/atunnel/egress/client.go`) up to Envoy's `http2_protocol_options.max_concurrent_streams: 100` on listener `egress` (`manifests/ate-install/atenet-egress.yaml`), opening one upstream mTLS TCP connection per 100 concurrent `CONNECT` streams.

For TLS modes (`:443` and `:8443`), `spec.py` configures an `envoy.extensions.transport_sockets.tls.v3.UpstreamTlsContext` `transport_socket` with `sni: "nighthawk-server.nighthawk.svc"`, `alpn_protocols: ["http/1.1"]`, and an exact DNS SAN matcher on `nighthawk-server.nighthawk.svc`. In `mitm` mode, `nighthawk_service` validates the `sdsmint` leaf certificate against the actor's projected trust bundle (`trusted_ca.filename: "/run/ate/egress-trust-bundle/trust-bundle.pem"`). In `passthrough` mode, `runner.py` reads the `servicedns.podcert.ate.dev` `ClusterTrustBundle` mounted at `/run/servicedns-ca/ca.crt` (`--servicedns-ca-cert`) and embeds it via `trusted_ca.inline_string`.

## Adaptive Runner Lifecycle

```mermaid
sequenceDiagram
    participant Dev as run-dev.sh / orchestrator.py
    participant Job as runner.py (K8s Job)
    participant Prom as atenet-egress (:15090)
    participant ALC as nighthawk_adaptive_load_client
    participant Inv as traffic-driver-invoker (:19980)
    participant Actor as nighthawk/tester (nighthawk_service)
    participant Srv as nighthawk-server (:80/:443/:8443)
    participant GCS as GCS / Local Artifacts

    Dev->>Job: 1. Build image, shape atenet-egress, render Job manifest, kubectl apply
    Job->>Job: 2. Render spec.textproto via spec.build_egress_session_spec()
    Job->>Prom: 3. Scrape gateway_before.prom across pod IPs & parse baseline counters
    Job->>ALC: 4. Launch nighthawk_adaptive_load_client (--spec-file)
    loop Adjusting stages (exponential ramp + binary search) & Testing stage
        ALC->>Inv: gRPC ExecutionRequest (target URI, RPS, concurrency=16)
        Inv->>Actor: Route via atenet-router (ate-target-actor: nighthawk/tester)
        Actor->>Srv: Open-loop HTTP/TLS traffic via egress-atunnel & atenet-egress
        Actor-->>ALC: ExecutionResponse (histograms, HTTP/SSL/pool counters)
    end
    ALC-->>Job: Write output.textproto & exit
    Job->>Prom: 5. Scrape gateway_after.prom & compute gateway_counter_deltas()
    Job->>GCS: 6. Write capacity.json, stats.jsonl, results.json, status.json & upload to --dest
```

## Key Design Decisions

### 1. Unified 25 ms Tail-Latency SLO (`latency-ns-mean-plus-2stdev <= 25 ms`)

`spec.build_egress_session_spec` defaults to `tail_latency_slo_ms=25.0` across all three egress modes (`http`, `mitm`, `passthrough`), matching `benchmarking/nighthawk-ingress`. Every run records `slo_max_rps` (the highest RPS stage passing all thresholds) and `slo_max_stage` in `capacity.json`, with `binding_threshold` listing every threshold any stage breached.

### 2. Connection Pool and Pending Queue Sizing (`connections=16, max_pending=32`)

Setting a large `max_pending_requests` (such as `10000`) on an HTTPS or TLS pool (`mitm` or `passthrough`) queues thousands of pending requests during overload stages and starts TLS handshakes that remain in flight when an adjusting stage ends. Tearing down `Http1PoolImpl` with in-flight TLS handshakes triggers an upstream Nighthawk C++ crash (`Pure virtual function called!`) inside `nighthawk_service`.

Bounding `max_pending` to `32` per worker with `connections=16` (256 parallel connections across 16 workers) reuses connections while draining pending requests cleanly at stage boundaries. When 256 connections limit client concurrency on high-RPS suites, `capacity.json` sets `client_pool_bound: true` (`upstream_rq_pending_overflow > 0` or `queue_to_connect_p50_ms > 1.0`).

### 3. 10-Counter Gateway Dataplane Proof (`output.py`)

`runner.py` resolves all pod endpoints of `atenet-egress-metrics.ate-system.svc` (`atenet-egress-metrics.yaml`), scrapes `:15090/stats/prometheus` before and after the adaptive session, and computes deltas for 10 gateway counters via `gateway_counter_deltas()`:

| `capacity.json` key | Prometheus series on `atenet-egress:15090` | Increments in |
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

The prefixes match the `stat_prefix` values in `manifests/ate-install/atenet-egress.yaml`.

When gateway pod IPs remain stable across the run, `output.capacity_summary` evaluates four checks on the deltas:

- `ext_proc_invariant_ok`: `ext_proc_upstream_rq_total == egress_connect_2xx + cleartext_ext_proc_streams + mitm_ext_proc_streams`.
- `mode_exclusivity_ok` (`_check_mode_exclusivity` in `output.py`) checks that inactive filter chains recorded zero traffic:
  - `http`: `passthrough_cx_total == 0`, `mitm_ssl_handshake == 0`, and `mitm_ext_proc_streams == 0`.
  - `mitm`: `passthrough_cx_total == 0` and `cleartext_ext_proc_streams == 0`.
  - `passthrough`: `mitm_ssl_handshake == 0`, `cleartext_ext_proc_streams == 0`, and `mitm_ext_proc_streams == 0`.
- `forward_proxy_overflow_ok`: `forward_proxy_cx_overflow == 0` and `forward_proxy_rq_pending_overflow == 0`.
- `gateway_counter_reset`: `true` if any counter delta is negative (for example after a container restart).

`runner.py` exits non-zero when `mode_exclusivity_ok` or `ext_proc_invariant_ok` is false, after it uploads the artifacts. `forward_proxy_overflow_ok` and `client_pool_bound` are informational.

In active traffic, `http` increments `cleartext_ext_proc_streams`, `mitm` increments `mitm_ssl_handshake` and `mitm_ext_proc_streams`, and `passthrough` increments `passthrough_cx_total` with one `ext_proc` request per `CONNECT` tunnel and zero per inner request.

## Prerequisites

The actor, the worker pool, and `nighthawk-server` are sized to outscale `atenet-egress`, so a run measures the gateway rather than the load generator or origin server. The sizes target a multi-node benchmark cluster such as 3 x `c3-standard-22` nodes (22 vCPUs and 88 GiB each):

- `traffic-driver-template.yaml.tmpl` gives the `nighthawk/tester` sandbox 16 vCPUs and 8 GiB, so `nighthawk_service` runs 16 worker threads (`--client-concurrency 16`) without CPU throttling.
- `traffic-driver.yaml.tmpl` requests 12 vCPUs and 8 GiB for the worker pod and limits it to 18 vCPUs and 10 GiB. The request fits a `c3-standard-22` node beside the system DaemonSets, and the limit leaves burst room for the actor and the worker's atunnel. The worker pod and `nighthawk-server` request 18 vCPUs together, so the scheduler may place them on the same node.
- `nighthawk-server.yaml` runs `nighthawk_test_server` with `--concurrency 8` and 6-8 vCPUs, so upstream HTTP and TLS on `:80`, `:443`, and `:8443` do not saturate before the gateway.

1. **Multi-node benchmark cluster with Agent Substrate deployed** (with `.ate-dev-env.sh` configured at the repository root):
   ```bash
   go run ./cmd/ate-setup deploy ate-system \
     --credential-provider='{"name":"k8s.io"}' \
     --ingress-allowed-spiffe-ids=spiffe://cluster.local/ns/nighthawk/sa/traffic-driver-invoker
   ```
   - `traffic-driver-invoker` runs under ServiceAccount `nighthawk/traffic-driver-invoker` (`traffic-driver-invoker.yaml`), so `atenet-router` requires `spiffe://cluster.local/ns/nighthawk/sa/traffic-driver-invoker` in `--ingress-allowed-spiffe-ids`.
   - For manual runs without `run-dev.sh`, provision the CA bundle in `ate-system` so `atenet-egress` trusts `nighthawk-server`'s `servicedns` certificate on `:443` (`run-dev.sh` and `orchestrator.py` run this automatically unless `--skip-deploy` is passed):
     ```bash
     KUBECTL_CONTEXT="$(kubectl config current-context)" hack/setup-e2e-egress-tls-kind.sh
     ```
   - To run on a smaller cluster, lower `requests.cpu` and `requests.memory` in `traffic-driver.yaml.tmpl` and `nighthawk-server.yaml` before deploying.
2. **`kubectl-ate`**: `deploy-traffic-driver.sh` builds it from `./cmd/kubectl-ate` into a temporary directory.
3. **Docker** running locally (builds the runner image in `run-dev.sh`, or runs `nighthawk_client` for manual checks).

## Results

Measured on GKE with 4 x `c3-standard-22` nodes, `envoy` and `ext-proc` in `deployment/atenet-egress` each pinned to 1 vCPU, and the default client shape (`connections=16`, `max_pending=32`, 16 workers). Each band covers repeat runs on two clusters and both trigger paths, `run-dev.sh` and `orchestrator.py`.

| Suite | `slo_max_rps` | Runs | Limit |
|---|---:|---:|---|
| `egress_gatewaycap_http_1cpu` | 7.4k to 8.2k | 7 | `envoy` at 1 vCPU |
| `egress_gatewaycap_mitm_1cpu` | 8.4k to 9.7k | 7 | `envoy` at 1 vCPU |
| `egress_gatewaycap_passthrough_1cpu` | 41.7k to 44.4k | 6 | `send-rate`: the one actor sends no faster |

A 2 vCPU gateway with one actor is out of scope, because Envoy pins each of the actor's 3 to 5 outer `CONNECT` connections to one worker thread.

## Running the Benchmark

### Quick Dev Run (`run-dev.sh`)

With `.ate-dev-env.sh` configured and `ate-system` deployed on the cluster, `run-dev.sh` validates the run knobs through `benchmarking/automation/orchestrator.py`, builds and pushes `${KO_DOCKER_REPO}/nighthawk-egress-test:<tag>`, shapes `deployment/atenet-egress` and deploys the `nighthawk` rig via `testtypes.nighthawk_egress` (or shapes `deployment/atenet-egress` alone when `--skip-deploy` is passed), submits the `nighthawk-egress-runner-job.yaml.tmpl` Job, streams progress logs, and prints `capacity.json` from GCS:

```bash
./benchmarking/nighthawk-egress/run-dev.sh --mode http --envoy-cpu 1 --tail-latency-slo-ms 25
./benchmarking/nighthawk-egress/run-dev.sh --skip-deploy --mode mitm --envoy-cpu 1 --tail-latency-slo-ms 25
./benchmarking/nighthawk-egress/run-dev.sh --skip-deploy --mode passthrough --envoy-cpu 1 --tail-latency-slo-ms 25
```

See `./benchmarking/nighthawk-egress/run-dev.sh --help` for `--measuring-period`, `--testing-stage-duration`, `--convergence-deadline`, and `--dest` overrides (default `--dest` is `gs://${BUCKET_NAME}/nighthawk-egress-results`).

### CI Automation (`benchmarking/automation`)

`benchmarking/automation/tests.yaml` registers three `type: nighthawk-egress` suites (`egress_gatewaycap_http_1cpu`, `egress_gatewaycap_mitm_1cpu`, and `egress_gatewaycap_passthrough_1cpu`), dispatched by `benchmarking/automation/orchestrator.py` to `benchmarking/automation/testtypes/nighthawk_egress.py`. Because `nighthawk-egress` deploys its own gVisor rig (`USES_SHARED_WORKLOADS = False`), `sandboxClass` must be `gvisor` and shared-workload top-level fields (`workerCount`, `actorMemory`, `actorCPU`, `workerMemory`, `workerWaitTimeout`, `isolate`) are rejected.

### Configuration Knobs (`nighthawk-egress:` block in `tests.yaml`)

| Knob (`tests.yaml`) | CLI Flag (`runner.py`) | Default | Description |
|---|---|---|---|
| `envoyCpu` | `--envoy-cpu` | *(required)* | CPU request and limit pinned on `envoy` and `ext-proc` in `deployment/atenet-egress` (at 1 replica), plus Envoy `--concurrency`. |
| `mode` | `--mode` | `http` | Egress dataplane path: `http` (`:80`), `mitm` (`:443`), or `passthrough` (`:8443`). |
| `tailLatencySloMs` | `--tail-latency-slo-ms` | `25` | Latency SLO bound (`mean + 2stdev`) in milliseconds. |
| `measuringPeriod` | `--measuring-period` | `20s` | Duration of each adaptive search step. |
| `convergenceDeadline` | `--convergence-deadline` | `600s` | Total time budget for the adaptive search phase. |
| `testingStageDuration` | `--testing-stage-duration` | `60s` | Duration of the confirmation stage at the converged RPS. |

`runner.py` also accepts `--servicedns-ca-cert` (default `/run/servicedns-ca/ca.crt`, mounted from `servicedns.podcert.ate.dev/identity` in `nighthawk-egress-runner-job.yaml.tmpl` and read when `--mode passthrough`), `--invoker-addr`, `--gateway-metrics-url`, and `--desc-path`.

### Output Artifacts

Each run writes its artifacts under `<dest>/runs/<name>/run_date=YYYY-MM-DD/run_ts=<epoch>/run_tag=<tag>/`:

| File | Description |
|---|---|
| `capacity.json` | Verdict summary, including `mode`, `envoy_cpu`, `converged`, `testing_stage_completed`, `testing_passed`, `slo_max_rps`, `slo_max_stage`, `binding_threshold`, `client_pool_bound`, `gateway_pod_ips_changed`, `gateway_scrape_error`, `gateway_counters`, `tunnels_per_stage`, `cx_churn_ratio`, connection setup telemetry (`queue_to_connect_p50_ms`, `queue_to_connect_p99_ms`, `sequencer_blocking_p99_ms`), gateway checks (`ext_proc_invariant_ok`, `mode_exclusivity_ok`, `forward_proxy_overflow_ok`, `gateway_counter_reset`), and testing-stage `p50_ms`, `p95_ms`, and `p99_ms`. Connection telemetry comes from `slo_max_stage`, else the testing stage, else the last stage. If `atenet-egress` pod IPs change during the run or the post-run gateway scrape fails, `runner.py` still writes and uploads `results.json`, `stats.jsonl`, `capacity.json` (with `gateway_counters: {}` and `tunnels_per_stage: null`), `logs.txt`, and `status.json` before exiting non-zero. |
| `stats.jsonl` | Per-stage JSONL records with Nighthawk built-in metrics (`achieved_rps` and `send_rate` are recomputed from the exact stage duration, with Nighthawk's own values under the `*_cpp` keys), HTTP/SSL/pool counters, `requests_per_connection`, `cx_churn_ratio`, `queue_to_connect_*`, `sequencer_blocking_*`, latency percentiles, and `failed_thresholds`. |
| `results.json` | Full `AdaptiveLoadSessionOutput` converted to JSON. |
| `spec.textproto`, `output.textproto` | Raw Nighthawk input spec and output textproto. |
| `gateway_before.prom`, `gateway_after.prom` | Raw Prometheus snapshots from `atenet-egress:15090/stats/prometheus` before and after the run (`gateway_after.prom` is omitted if the post-run scrape fails). |
| `status.json`, `logs.txt` | Adaptive client exit code, `converged`, `testing_stage_completed`, `slo_max_rps`, `gateway_pod_ips_changed`, `gateway_scrape_error`, and combined stdout/stderr logs. |

### Manual Setup and Single-Rate Check

To deploy the workload components manually and drive a single fixed-rate `nighthawk_client` run:

```bash
./benchmarking/nighthawk-egress/deploy.sh
./benchmarking/nighthawk-egress/deploy-traffic-driver.sh
kubectl -n nighthawk port-forward svc/traffic-driver-invoker 19980:19980
```

In a second terminal, run `nighthawk_client` against `grpc://127.0.0.1:19980`:

```bash
docker run --rm --network=host --entrypoint /usr/local/bin/nighthawk_client envoyproxy/nighthawk-dev:latest --nighthawk-service grpc://127.0.0.1:19980 --rps 1000 --concurrency 16 --protocol http1 --duration 60 http://nighthawk-server.nighthawk.svc:80/
```

Nighthawk multiplies `--rps` by `--concurrency`, so this command targets 16,000 requests per second in total. See [Nighthawk CLI help](https://github.com/envoyproxy/nighthawk#using-the-nighthawk-client-cli) for all command line options.

## Cleanup

Delete the `nighthawk/tester` actor, actor template, atespace, and worker pool:

```bash
./benchmarking/nighthawk-egress/deploy-traffic-driver.sh --delete
```

Delete `nighthawk-server`, `traffic-driver-invoker`, and the `atenet-egress-metrics` Service:

```bash
./benchmarking/nighthawk-egress/deploy.sh --delete
```

Neither script deletes the `nighthawk` namespace during manual runs (`orchestrator.py` deletes it during automated suite teardown). Delete it manually with `kubectl delete namespace nighthawk` after `deploy-traffic-driver.sh --delete`, because the actor, template, and atespace live in the ate API rather than in the namespace.
