# Cloud Monitoring dashboards

Google Cloud Monitoring dashboard definitions for ATE. They turn the raw
`prometheus.googleapis.com/...` metrics that ATE emits into readable
per-method / per-stage latency / throughput / error views.

| File | Shows |
|------|-------|
| `ate-grpc-dashboard.json` | ateapi & atelet gRPC latency (p50/p95/p99), request rate, and error rate, by method |
| `substrate-e2e-latency-dashboard.json` | The single request-latency dashboard ("Substrate E2E Latency"). Substrate E2E P50/P95/P99, P99 by stage (Substrate E2E / ateapi ResumeActor / atelet Restore), P99 by ActorTemplate, QPS by status, plus the full round-trip P99 (Envoy, ms — includes actor compute, so it's context, not our overhead). Needs the `atenet-router-envoy` PodMonitoring for the round-trip line. |

## Applying

Dashboards are created/updated (idempotently) by setup:

```sh
go run ./tools/setup-gcp --create-monitoring-dashboards   # also part of: --all
```

Or apply any single file by hand:

```sh
gcloud monitoring dashboards create --config-from-file=monitoring/dashboards/<file>.json
```
