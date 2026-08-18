<!--
Copyright 2026 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Telemetry meter

The meter measures how much telemetry substrate sends, and which component
sends it.

The managed collector sends its self-metrics to Cloud Monitoring only, and
gives no data for each source. Thus [`meter.yaml`](meter.yaml) is a second
collector that counts the spans and the datapoints by `service.name`. The meter
is a tee: it counts the data and then sends it on to the managed collector, thus
the managed collector keeps the load of the run.

For the prerequisites and the scenario ladder, read
[observability.md](../observability.md). For the volume model, read
[OTel Collector](../../docs/dev/best-practices/otel-collector.md).

## Make a measurement

Install the meter, then send the control plane to it:

```bash
kubectl apply -f benchmarking/telemetry/meter.yaml

METER=http://telemetry-meter.benchmarking.svc.cluster.local:4317
./hack/install-ate.sh --deploy-ate-system --otlp-endpoint "${METER}"
```

`--otlp-endpoint` patches the `ate-otel-config` ConfigMap and restarts the
workloads that read it. One patch is sufficient for `ateapi`, `ate-controller`,
`atelet`, and `atenet-router`, because each one reads the ConfigMap through
`envFrom`. `ate-controller` also copies the values to the ateom worker pods
that it creates.

The actor containers are different. Substrate puts no OTLP configuration in
them, thus they use the `env` of the ActorTemplate:

```bash
./benchmarking/workloads/deploy.sh --deploy
```

Without `--otlp-endpoint`, the script reads the endpoint from the
`ate-otel-config` ConfigMap. The step above already changed that ConfigMap,
thus the actors follow the control plane to the meter. Give `--otlp-endpoint`
only to send the actors to a different address than the control plane.

Keep the load generator on the usual collector. If you do not, the meter counts
the telemetry of the load generator as telemetry from substrate.

## Read the numbers

[`monitoring.yaml`](../monitoring.yaml) scrapes the meter. Read the values
through Prometheus. Do not compare raw scrapes manually.

```bash
kubectl apply -f benchmarking/monitoring.yaml
kubectl port-forward -n benchmarking svc/prometheus 9090:9090
```

| To find | Query |
|---|---|
| Spans/sec for each service | `sum by (svc) (rate(substrate_spans_total[5m]))` |
| Datapoints/min for each service | `60 * sum by (svc) (rate(substrate_datapoints_total[5m]))` |
| If the data arrived | `sum(rate(otelcol_receiver_accepted_spans[5m]))` |
| If the collector rejected the data | `sum(rate(otelcol_receiver_refused_spans[5m]))` |

Keep the range at three times the 60s push interval or more. `[5m]` is a good
default. A range less than 3m gives noise.

Always read the last two counters. A service that reports zero volume gives no
data by itself: `accepted` must be more than zero and `refused` must be zero
for the same window. If they are not, the zero shows that the data did not
arrive, not that the service sent no data.

## The meter is a tee

The meter counts the telemetry and then sends it on to the managed collector.
Thus one run gives both measurements: the volume for each service, from the
meter, and the cost of the load, from the managed collector.

```promql
max_over_time(
  container_memory_working_set_bytes{namespace="gke-managed-otel", container!=""}[5m]
)
```

Use the working set for the memory. `kubectl top` reads metrics-server, which
calculates an average across its own window and hides short peaks. The working
set is the value that the OOM killer uses. Divide the working set by
`container_spec_memory_limit_bytes` to get the fraction of the limit in use.

Record the number of replicas with the CPU and the memory. The managed
collector is an HPA-scaled Deployment. Without the number of replicas, a
collector at its maximum replica count and a collector with much margin give
the same table.

### Three conditions of the tee

**The managed collector attributes the telemetry to the meter.** It applies
`k8sattributes` with `from: connection`, and each forwarded item now comes from
the meter pod. The volume and the CPU stay correct, but a query that groups by
pod on the managed side does not. Group by `service.name` from the resource,
which stays correct after the hop.

**The shape of the load changes.** Usually many processes each keep a
connection to the managed collector. With the tee there is one sender and
larger batches. Thus the total is correct, but the distribution across the
replicas is not, and the effect of connection stickiness disappears.

**The meter can become the limit.** It is one replica. Add its own counters to
the pass criteria:

```promql
sum(rate(otelcol_receiver_refused_spans[5m]))   # must be 0
sum(rate(otelcol_exporter_sent_spans[5m]))      # must be more than 0
max_over_time(otelcol_exporter_queue_size[5m])  # must stay level
absent(otelcol_exporter_sent_spans)             # must give no result
```

Do not use `otelcol_exporter_send_failed_spans` alone. The default
`retry_on_failure.max_elapsed_time` is 5 minutes, thus the exporter retries for
that period before it counts a failure. A meter that sheds data looks correct
for the length of a short step. The depth of the queue moves immediately, thus
it is the signal to watch. `absent()` catches the different condition where the
meter reports nothing at all.

If the meter sheds data, the volume and the cost are both incorrect, and the
two errors hide each other.

To make the meter terminal, delete the two forward pipelines in
[`meter.yaml`](meter.yaml). Use a terminal meter for a long soak, to keep that
telemetry out of Cloud Monitoring. The resource values of the managed collector
are then those of an idle collector, and you must not record them.

## Counters for drops at the client

The meter shows the data that arrived. It does not show the data that the
client dropped. Turn on the instrumentation in the SDK:

```bash
kubectl set env -n ate-system deployment/ate-api-server OTEL_GO_X_OBSERVABILITY=true
```

Scrape `:9090/metrics` on the process. That endpoint is a Prometheus reader
that does not use the OTLP path. Thus it continues to operate during the
congestion that it measures.

| Metric | Shows |
|---|---|
| `otel_sdk_span_started_total{otel_span_parent_origin,otel_span_sampling_result}` | The sample decisions, for root spans and for inherited spans |
| `otel_sdk_processor_span_processed_total{error_type="queue_full"}` | The spans that the client dropped |
| `otel_sdk_processor_span_queue_size` vs `_capacity` | The margin before the drops start |

Without the counter for the drops, a loss at the client looks the same as a
stable plateau.

The flag is experimental. It is in `sdk/internal/x`, thus the names of the
metrics can change when you upgrade the SDK. Examine the names again after each
upgrade.

## Remove the meter

```bash
./hack/install-ate.sh --deploy-ate-system
./benchmarking/workloads/deploy.sh --deploy
kubectl delete -f benchmarking/telemetry/meter.yaml
```

The two scripts go back to the default endpoint when you do not give
`--otlp-endpoint`.
