# Ingress capacity baselines

This file records comparable, reproducible capacity baselines. The capacity
number is `slo_max_rps`: the highest *clean adjusting stage* that passed every
configured Nighthawk threshold. Do not compare a final `testing_*` row with it:
the adaptive controller deliberately probes its convergence boundary and that
final stage may fail.

## 2026-09-11: 2-vCPU proxy setting

Topology: six GKE 1.37 `c3-standard-22` nodes, with dedicated router, runner,
and control-plane nodes and three actor-worker nodes. Workload: 50 warmed
glutton actors, 16 Nighthawk event loops, 1,000 connections per loop, 500
initial RPS, 10-second adjustment stages, 60-second final stage, 99.9% success
threshold, 90% send-rate threshold, and a 25 ms `mean + 2σ` latency threshold.

| Dataplane | CPU allocation | `slo_max_rps` | Binding thresholds |
| --- | --- | ---: | --- |
| Envoy | 2 vCPU Envoy + 2 vCPU `ext_proc` | 10,864 | latency, send rate, success rate |
| AgentGateway | 2 vCPU single container | 21,744 | latency, send rate, success rate |

This is a baseline for regression tracking, not an equal-total-CPU comparison:
the Envoy topology includes the separately allocated `ext_proc` sidecar.
