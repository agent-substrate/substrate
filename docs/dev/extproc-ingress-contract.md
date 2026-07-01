# ExtProc Ingress Integration Contract

This document describes the contract that any ExtProc-capable proxy must satisfy to integrate with Substrate's actor-aware routing core.

## Overview

The request flow is:

```
Client
  → external proxy (Envoy / gateway)
  → ExtProc gRPC  →  atenet-router:50051
  → RouteResolver  (ResumeActor → worker pod IP)
  ← ExtProc response (header mutation or denial)
  → external proxy forwards to worker-ip:80
  → actor pod
```

atenet-router exposes a standard Envoy `ExternalProcessor` gRPC service. Any proxy that supports the `ext_proc` filter can call it.

---

## Required Request Metadata

The proxy **must** forward the following HTTP headers verbatim to ExtProc:

| Header | Requirement | Notes |
|--------|-------------|-------|
| `:authority` / `Host` | **Required** | Must end with `.actors.resources.substrate.ate.dev`. The actor ID is the subdomain preceding that suffix. |
| `traceparent` | Recommended | W3C Trace Context. Substrate extracts this to link its internal span to the proxy's ingress span. |
| `tracestate` | Recommended | Forwarded alongside `traceparent`. |

No other headers are required. Additional headers are passed through unchanged.

### Actor ID format

Given `:authority: <actor-id>.<atespace>.actors.resources.substrate.ate.dev`:

- Valid actor IDs satisfy the `resources.ActorIDRegexPattern` regex (UUID-like identifiers).
- Port numbers in the authority header (e.g. `actor.actors...ate.dev:80`) are stripped before parsing.

---

## Success Behavior

When the actor is found and a worker pod is ready, Substrate returns an ExtProc `HeadersResponse` with a **header mutation**:

```
SetHeaders:
  :authority = <worker-pod-ip>:80
```

The proxy should then forward the request to `<worker-pod-ip>:80` using the rewritten authority. The recommended mechanism is Envoy's **dynamic_forward_proxy** cluster, which resolves the authority as a host:port at request time.

---

## Denial Behavior

When routing cannot complete, Substrate returns an ExtProc **ImmediateResponse**:

| Condition | HTTP Status | Example body |
|-----------|-------------|--------------|
| Host doesn't match actor DNS suffix | 404 | `invalid host "foo.example.com": …` |
| Actor not found in control plane | 404 | `actor "abc-123" not found` |
| No free workers (FailedPrecondition) | 503 | `actor "abc-123" unavailable: no free workers available` |
| Control plane unreachable | 503 | `actor "abc-123" unavailable` |
| Resume timed out | 504 | `actor "abc-123" request timed out` |
| Invalid worker IP returned | 500 | `actor "abc-123" routing failed` |
| Unknown error | 500 | `error resuming actor "abc-123"` |

All denial responses carry `Content-Type: text/plain`.

The body is **client-safe**: internal error detail (stack traces, internal IDs) is never included.

---

## ExtProc Filter Configuration

The following ExtProc filter configuration is required on the proxy side:

```yaml
name: envoy.filters.http.ext_proc
typed_config:
  "@type": type.googleapis.com/envoy.extensions.filters.http.ext_proc.v3.ExternalProcessor
  grpc_service:
    envoy_grpc:
      cluster_name: extproc_cluster   # must point to atenet-router:50051
    timeout: 5s
  mutation_rules:
    allow_all_routing: true           # required: lets ExtProc rewrite :authority
  message_timeout: 5s
  processing_mode:
    request_header_mode: SEND         # only request headers are processed
    response_header_mode: SKIP
    request_body_mode: NONE
    response_body_mode: NONE
    request_trailer_mode: SKIP
    response_trailer_mode: SKIP
```

`allow_all_routing: true` is mandatory. Without it Envoy silently drops the `:authority` mutation and the proxy routes to the wrong upstream.

### ExtProc cluster

```yaml
name: extproc_cluster
connect_timeout: 0.25s
type: STRICT_DNS
typed_extension_protocol_options:
  envoy.extensions.upstreams.http.v3.HttpProtocolOptions:
    "@type": type.googleapis.com/envoy.extensions.upstreams.http.v3.HttpProtocolOptions
    explicit_http_config:
      http2_protocol_options: {}     # gRPC requires HTTP/2
load_assignment:
  cluster_name: extproc_cluster
  endpoints:
  - lb_endpoints:
    - endpoint:
        address:
          socket_address:
            address: atenet-router.ate-system.svc
            port_value: 50051
```

> **TLS:** ExtProc gRPC is plaintext for the PoC. atenet-router and the proxy are cluster-internal; mTLS hardening is deferred.

---

## DNS Integration

The DNS controller must resolve actor hostnames to your **gateway's** ClusterIP, not atenet-router's (atenet-router only serves ExtProc gRPC, not HTTP):

```
atenet dns --ingress-service-name=atenet-gateway ...
```

This writes the `atenet-gateway` Service ClusterIP into the CoreDNS Corefile so that `*.actors.resources.substrate.ate.dev` resolves to the external proxy.

---

## Security Notes

- The ExtProc endpoint (port 50051) should only be reachable by the proxy within the cluster. Use NetworkPolicy to restrict access if needed.
- Substrate never forwards client secrets (Authorization headers, cookies, query parameters) to logs. The `traceparent` header is the only header value logged.
- Denial message bodies are client-safe: they do not include internal stack traces, IP addresses of failed backends, or gRPC internal error strings (except for `FailedPrecondition`, whose description is considered actionable and non-sensitive).
