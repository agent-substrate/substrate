# Logging Best Practices

This document covers how to write logs in a Substrate component. It is the
logging counterpart of [Metrics Best Practices](metrics.md) and
[Tracing Best Practices](tracing.md).

For how to read actor and component logs, see
[Actor Observability](../../observability.md#1-logging).

## Logging protos that hold secrets

Fields labeled `debug_redact` (see
[API style guide section 11](../../api-style-guide.md#11-sensitive-fields-debug_redact))
are masked when a proto is logged. The label applies to every proto in this
repository, not only the public API: atelet, ateom, and plugin protos are
logged the same way.

Masking is done by the shared slog handler in `internal/contextlogging`, which
`serverboot.InitLogger` and `InitLoggerWithWriter` install as the default
logger. A server must set up logging through one of them; a process that
builds its own handler gets no masking.

The handler masks labeled fields in a proto it is handed as an attribute value
(`slog.Any("resp", resp)`), inside a `slog.Group`, as the result of a
`slog.LogValuer`, or bound with `logger.With`. This covers the request and
response bodies the gRPC interceptors log. A singular string field becomes
`[REDACTED]`. A field of any other kind (bytes, repeated, map, message) is
dropped, because no placeholder fits it. The caller's message is not modified.

- Log a proto as its own attribute. The handler cannot see a proto that is:
  - inside a slice, map, or plain Go struct: `slog.Any("env", []*EnvVar{...})`;
  - turned into a string first. protobuf-go's `String()`, `%v`, `prototext`,
    and `protojson` all print labeled fields in clear;
  - packed in a `google.protobuf.Any`.
- The label only affects logs written by that handler. Span attributes, metric
  labels, error messages, and the OTLP copy of actor events (built by
  `internal/actorevent`) are not masked; keep secrets out of them.
- Adding or removing a label fails the tests in `internal/protoredact` until
  the list of labeled fields there is updated, so the change is reviewed as a
  decision about what logs may show.

The exact list of what the handler covers is on `ContextHandler.Handle` in
`internal/contextlogging`.
