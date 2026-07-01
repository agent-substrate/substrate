# Ingress integration: gateway reference configs

Substrate does not ship or manage a proxy. `atenet-router` exposes an ext_proc
gRPC endpoint (port 50051) that any dynamic-forwarding proxy can call to
resolve actor routes — see [the contract](./extproc-ingress-contract.md). You
bring and configure your own gateway in front of it.

This doc covers the reference configs in `manifests/ate-install/examples/`:
a basic path (raw Envoy, scripted) and power-user paths (agentgateway, plus
Gateway API variants of both). Substrate does not generate any of these
resources — apply the one you want yourself.

---

## Proxy Configurations

### Standalone Envoy Deployment

`manifests/ate-install/examples/atenet-gateway-envoy.yaml` is a standalone
Envoy deployment configured directly (no Gateway API / gateway controller).
`hack/poc-pluggable-ingress.sh deploy` applies it on a kind cluster end to
end — this is the fastest way to see the pipeline working.

```
Client
  → Envoy (static config)
      ext_proc filter → atenet-router:50051
          resolves actor → rewrites :authority to <worker-ip>:80  (or denies)
      dynamic_forward_proxy cluster
          re-resolves target from :authority, connects to <worker-ip>:80
  → worker pod (ateom)
```

Envoy's `dynamic_forward_proxy` cluster re-resolves the request authority
**after** the ext_proc filter has run and dials that address directly — so the
actor-aware decision made by `atenet-router` becomes the upstream target with
no per-actor config. Denials work unchanged: `atenet-router` returns an
ext_proc `ImmediateResponse` (404/503/etc.), which Envoy honors directly.

DNS: point actor hostnames at the gateway's Service. `atenet-gateway` is
already the DNS controller's default ingress target
(`atenet dns --ingress-service-name`, see
[extproc-ingress-contract.md](./extproc-ingress-contract.md#dns-integration)),
so no flag is needed if you keep that Service name.

For anything past the basic path — agentgateway, or a Gateway API–driven
control plane — see the power-user section below.

#### Standalone AgentGateway Deployment

`manifests/ate-install/examples/atenet-gateway-agw.yaml` is the agentgateway
(https://agentgateway.dev) equivalent of the basic Envoy path: a hand-written
config file, not Gateway API. Deploy it **instead of**
`atenet-gateway-envoy.yaml` (they share the `atenet-gateway` Service name).
Same mapping as the basic path: ext_proc rewrites `:authority`, agentgateway's
`dynamicForwardProxy` backend reads it after ext_proc runs.

### AgentGateway (Gateway API)

`manifests/ate-install/examples/atenet-gateway-agw-gatewayapi.yaml` drives the
same pipeline through the Kubernetes Gateway API instead of a hand-written
config file:

```
Client
  → agentgateway (Gateway API)
      AgentgatewayPolicy.extProc → atenet-router:50051
          resolves actor → rewrites :authority to <worker-ip>:80  (or denies)
      HTTPRoute → AgentgatewayBackend (dynamicForwardProxy)
          re-resolves target from :authority, connects to <worker-ip>:80
  → worker pod (ateom)
```

Prerequisites:
- agentgateway installed, providing a `GatewayClass` (match the class name to
  your install; the manifest uses `agentgateway`). You can follow [this official guide](https://agentgateway.dev/docs/kubernetes/main/quickstart/install/) to install agentgateway.
- All resources live in `ate-system` so the ext_proc `backendRef` and the
  route/backend stay in one namespace (no `ReferenceGrant` needed).

> **Why a gateway-specific config and not one portable Gateway API config?**
> There is no portable Gateway API way to express this pipeline. It needs two
> things the standard CRDs don't expose: permission for ext_proc to rewrite
> `:authority`, and a dynamic-forward-proxy backend. Each proxy that supports
> them does so through its own extension CRDs.

Validate on kind:
```
curl -v -H "Host: <actor-id>.<atespace>.actors.resources.substrate.ate.dev" \
     http://<gateway-address>/
```
