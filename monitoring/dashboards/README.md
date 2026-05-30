# Cloud Monitoring dashboards

Google Cloud Monitoring dashboard definitions for ATE. They turn the raw
`prometheus.googleapis.com/...` metrics that ATE emits into readable
per-method latency / throughput / error views.

| File | Shows |
|------|-------|
| `ate-grpc-dashboard.json` | ateapi & atelet gRPC latency (p50/p95/p99), request rate, and error rate, by method |

## Applying

Dashboards are created/updated (idempotently) by setup:

```sh
go run ./tools/setup-gcp --create-monitoring-dashboards   # also part of: --all
```

Or apply any single file by hand:

```sh
gcloud monitoring dashboards create --config-from-file=monitoring/dashboards/<file>.json
```
