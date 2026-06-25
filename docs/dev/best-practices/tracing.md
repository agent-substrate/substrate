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

All servers need to initialize an OpenTelemetry exporter and tracer provider.  See `cmd/ateapi/ateapi.go:initTracing()` for an example:

```go
func initTracing(ctx context.Context) (*sdktrace.TracerProvider, error) {
	exporter, err := otlptracegrpc.New(ctx,
		// GKE managed traces doesn't support validating the TLS certs of the collector
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create OTLP exporter: %w", err)
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName("ateapi"),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
		// Only trace on-demand when signaled by the client (e.g. via --trace flag)
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.NeverSample())),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})

	return tp, nil
}
```

When calling the `initTracing()` function, be sure to `defer tp.Shutdown(ctx)` after the call to ensure that the tracer provider is properly shut down when the server exits:

```go
defer func() {
  if err := tp.Shutdown(ctx); err != nil {
    slog.Error("Failed to shutdown TracerProvider", slog.Any("err", err))
  }
}()
```

Note the following important features:

* We are not validating the TLS certs of the collector
* We provide a service name to exporter to identify which process is emitting the spans
* We only trace on-demand when desired by the client, as determined by the presence of tracing metadata/headers in the request
  * For production, we will want to gate who/how tracing can be enabled for security purposes

The YAML manifest for your server should include the `OTEL_EXPORTER_OTLP_ENDPOINT` environment variable to point the exporter to GKE's managed traces collector, e.g.:

```yaml
      containers:
        - name: ateapi
          image: ko://github.com/agent-substrate/substrate/cmd/ateapi
          ports:
            - "443:443"
          env:
            # Tracing related environment variables
            - name: OTEL_EXPORTER_OTLP_ENDPOINT
              value: "http://opentelemetry-collector.gke-managed-otel.svc.cluster.local:4317"
```

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

Like for servers, the tracer provider must be initialized and shutdown, but no exporter is required (note the sampler toggle):

```go
func initTracing(ctx context.Context, enabled bool) (*sdktrace.TracerProvider, error) {
	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.UserAgentOriginal("my-client-name"),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource: %w", err)
	}

	sampler := sdktrace.NeverSample()
	if enabled {
		sampler = sdktrace.AlwaysSample()
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sampler),
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
        ateapi_pb2.GetActorRequest(actor_key="my-actor"),
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
