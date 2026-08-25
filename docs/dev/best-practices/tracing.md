# Tracing Best Practices

This document outlines the tracing best practices for the Agent Substrate project using OpenTelemetry.

## Why Do We Need Tracing?

Tracing is important for debugging and performance optimization. It allows you to see how a request is processed and where it might be slow.

## What is Tracing?

Tracing is a way to track the flow of a request through a system. It allows you to see how long each step takes and where the bottlenecks are.

Ideally, tracing outlines the entire flow of a request from the client to the server and back and includes all services that are invoked.

Tracing consists of **spans** and **traces**. A span is a single operation, and a trace is a collection of spans that are related to each other.
Spans have a start and end time, and they can have attributes (key-value pairs) that provide additional information about the operation.
Traces have a trace ID and a span ID, which are used to identify the trace and the span.

## How Tracing Works

Tracing data is maintained in Golang's context object, which allows state to propagate through the call stack.

When HTTP requests are made, tracing data may be included in the HTTP headers. For gRPC, tracing data is included in the metadata object.
Otel middleware handles both the extraction and injection of tracing data automatically.

Servers have an exporter service that batches spans and pushes them to a remote collector for analysis.

## Implementing Tracing

### For servers

All servers need to initialize an OpenTelemetry exporter and tracer provider.  See `internal/serverboot.InitTracing()` (used by `cmd/ateapi/main.go`) for an example:

```go
tp, err := serverboot.InitTracing(ctx, serverboot.TracingOptions{
	ServiceName: "ateapi",
	Sampling:    serverboot.ResolveTraceSampling(ctx, serverboot.ParentRatioSampling(serverboot.ControlPlaneTraceRatio)),
})
if err != nil {
	serverboot.Fatal(ctx, "Failed to initialize tracing", err)
}
defer serverboot.ShutdownProvider("TracerProvider", tp.Shutdown)
```

`InitTracing` registers the OTLP exporter, resource, sampler, and TraceContext propagator. Be sure to defer the shutdown (as above) to ensure that the tracer provider is properly shut down when the server exits.

Note the following important features:

* We are not validating the TLS certs of the collector
* We provide a service name to exporter to identify which process is emitting the spans
* Every component samples by default at a per-component ratio (see below); a client that arrives with a sampled trace context is always traced end to end
  * For production, we will want to gate who/how client-forced tracing can be enabled for security purposes

### Sampling defaults and overrides

Samplers are resolved with `serverboot.ResolveTraceSampling`, which applies the standard `OTEL_TRACES_SAMPLER` / `OTEL_TRACES_SAMPLER_ARG` environment variables on top of a per-component default. Pass the component default; never pass a raw sampler to the provider, because an explicit sampler silences the env vars.

| Component | Default |
|---|---|
| ateapi, atelet, ateom-gvisor, ateom-microvm | `parentbased_traceidratio` 0.1 |
| atenet router (data plane root) | `parentbased_traceidratio` 0.01, mirrored into Envoy's `RandomSampling` |
| glutton (benchmarking) | `parentbased_always_off` |
| boomer (benchmarking) | runtime-controlled via dynconfig, ignores the env vars |

All defaults are `ParentBased`, so a request that arrives already sampled stays sampled on every hop, and one that arrives explicitly unsampled stays unsampled. Only parentless requests are subject to the ratio, at whichever component roots the trace.

An invalid sampler name, or a missing or unparsable ratio arg, keeps the component default and logs a warning. This deliberately diverges from the OTel SDK's own env handling, which falls back to 100% sampling on invalid input and reads a missing arg as ratio 1.0.

In agentgateway mode the data plane root fraction lives in the agentgateway ConfigMap (`randomSampling`, same 0.01 default). Unlike Envoy's `RandomSampling`, it is static config that env overrides on the router do not reach, so adjust both together.

These are head sampling ratios that bound what leaves the process. Keep decisions based on request outcome (errors, latency) belong in a collector pipeline, not in substrate binaries.

### Disabling tracing (perf/load tests)

Set `OTEL_TRACES_SAMPLER=always_off` on the components under test (for ateom workers, via the controller's `--otel-traces-sampler` flag). `parentbased_always_off` is not enough under a load generator: boomer and locust send ratio-sampled trace context, and parent based samplers honor it. Alternatively set the generator's `trace_probability` to 0 and leave the servers alone. On kind, also override ateapi's `parentbased_always_on` pin.

The YAML manifest for your server needs `OTEL_EXPORTER_OTLP_ENDPOINT` set so the
exporter knows where to push spans. Do not hardcode it — consume the shared
`ate-otel-config` ConfigMap via `envFrom`, so your server follows the collector
address for whichever environment it is deployed to:

```yaml
      containers:
        - name: ateapi
          image: ko://github.com/agent-substrate/substrate/cmd/ateapi
          ports:
            - containerPort: 443
          # Supplies OTEL_EXPORTER_OTLP_ENDPOINT (and, on kind, the metric
          # export tunables) for every control plane component.
          envFrom:
            - configMapRef:
                name: ate-otel-config
```

The ConfigMap is defined in
[`manifests/ate-install/ate-otel-config.yaml`](../../../manifests/ate-install/ate-otel-config.yaml)
for GKE, with a kind replacement of the same name in
[`manifests/ate-install/kind/ate-otel-config.yaml`](../../../manifests/ate-install/kind/ate-otel-config.yaml)
that points at the in-cluster collector. Editing either one does not restart the
pods that consume it; follow a change with `kubectl rollout restart`.

For how to deploy that collector — the GKE managed option, a self-managed DaemonSet, and the constraints on what endpoints Substrate can talk to — see [OpenTelemetry Collector Best Practices](otel-collector.md).

#### gRPC Servers
When implementing a gRPC server, you should include the following middleware to handle tracing:

```go
server := grpc.NewServer(
    grpc.StatsHandler(otelgrpc.NewServerHandler())
)
```

#### HTTP Servers
When implementing an HTTP server, you should wrap the root multiplexer with `otelhttp.NewHandler`:

```go
tracedMux := otelhttp.NewHandler(
    mux,
    "/",
)
```

While this model ensures all requests are eligible for tracing, it does not add the nature of the request to the span. As such, you should create a span in your handler to capture the nature of the request:

```go
tracer := otel.Tracer("my-server-name")

func someHandler(w http.ResponseWriter, r *http.Request) {
  ctx, span := tracer.Start(r.Context(), "operationIdentifier")
  defer span.End()
  // ... rest of your handler
}
```

#### Sub-Spans
If you want to provide visibility into the internal workings of the server, you can create sub-spans at any point:

```go
tracer := otel.Tracer("my-package-name")

func someFunc(ctx context.Context) {
  ctx, span := tracer.Start(ctx, "operationIdentifier")
  defer span.End()
}
```

### For Clients

Clients are not expected to instantiate an exporter, but they should give the option to include
tracing metadata in their requests to give users the ability to initiate a trace.

#### Golang

Like for servers, the tracer provider must be initialized and shutdown, but no exporter is required. When tracing is not requested, install nothing: a provider with a `NeverSample` sampler would inject an explicitly unsampled trace context, which pins every `ParentBased` server sampler downstream to not sampled and defeats the server side ratios. With the OTel globals left as noop, no context is injected and the server roots the trace itself.

```go
func initTracing(ctx context.Context, enabled bool) (*sdktrace.TracerProvider, error) {
	if !enabled {
		return nil, nil
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.UserAgentOriginal("my-client-name"),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})

	return tp, nil
}
```

If your server is also a client, this step is redundant and can be omitted.

Note that we are setting the UserAgentOriginal attribute here because we are assuming this is a user-facing client.
If this is a system service, we must set the ServiceName attribute instead.

##### gRPC Clients

When using a gRPC client, include the stats handler:

```go
clientConn, err := grpc.NewClient(
    serverAddr,
    grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
)
```

##### HTTP Clients

For HTTP clients, add Otel's transport wrapper to your transport:

```go
client := &http.Client{
  Transport: otelhttp.NewTransport(http.DefaultTransport),
}
```

#### Python

Just like with Go, the provider must be initialized (note that because Python is only
used for load testing, we are using probability-based tracing):

```python
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.sampling import TraceIdRatioBased
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.propagate import set_global_textmap, inject
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

def init_tracing(probability: float = 1.0):
  sampler = TraceIdRatioBased(probability)
  resource = Resource(attributes={
      SERVICE_NAME: "my-locust-service"
  })
  provider = TracerProvider(sampler=sampler, resource=resource)

  trace.set_tracer_provider(provider)
  set_global_textmap(TraceContextTextMapPropagator())
```

##### gRPC Clients

When using a gRPC client, simply instantiate a span and inject the headers, sending them as metadata:

```python
from opentelemetry import trace
from opentelemetry.propagate import inject

tracer = trace.get_tracer("my-service")

def call_with_trace(stub, method, request):
  with tracer.start_as_current_span("operationIdentifier") as span:
    headers = {}
    inject(headers)
    metadata = list(headers.items())
    response = stub.GetActor(
        ateapi_pb2.GetActorRequest(actor_ref=ateapi_pb2.ActorRef(atespace="default", name="my-actor")),
        metadata=metadata
    )
```

##### HTTP Clients

For HTTP clients, instantiate a span and inject the headers into the HTTP request:

```python
from opentelemetry import trace
from opentelemetry.propagate import inject

tracer = trace.get_tracer("my-service")

def call_with_trace(stub, method, request):
  with tracer.start_as_current_span("operationIdentifier") as span:
    headers = {}
    inject(headers)
    response = requests.get(
        "http://example.com",
        headers=headers
    )
```
