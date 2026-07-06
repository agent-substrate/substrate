# Egress Capture

This documents the demo install path and the ateom capture setup using
the dedicated egress demo. The demo is a small HTTP actor that resumes through
the router and opens an outbound HTTPS request to `https://httpbin.org/get`
by default.

## Architecture

When egress capture is enabled, each worker pod gets `ATE_EGRESS_*`
configuration from the controller. The reusable capture core lives in
`internal/egress`: it owns environment parsing, capture listeners,
authority derivation, CONNECT tunnel transports, and byte proxying. The
runtime-specific `ateom` egress proxy setup supplies the original-destination
lookup and packet-capture rules.

The current gVisor implementation starts a local capture listener and installs
actor-network redirects for TCP/80 and TCP/443. From the actor's point of view
it still opens a normal HTTP or HTTPS connection to the original destination.
MicroVM or future hypervisor implementations should reuse
`internal/egress` for the local listener, authority derivation, tunnel
transport, and byte proxying. Each runtime still provides its own egress proxy
setup for redirecting actor traffic and recovering the original destination.

The redirected connection lands on `ateom`, which records the original
destination and derives a stable CONNECT authority from the first bytes of the
actor connection:

| Actor traffic | Authority source | Example CONNECT authority |
| --- | --- | --- |
| HTTPS / TCP 443 | TLS ClientHello SNI | `httpbin.org:443` |
| Plaintext HTTP / TCP 80 | HTTP `Host` header | `example.com:80` |

The shared capture core then opens a plaintext HTTP/2 CONNECT stream to the
agentgateway data plane at
`ate-egress.agentgateway-system.svc.cluster.local:15008`. Agentgateway maps the
CONNECT authority to its configured TCP listener and routes the tunnel to a
Kubernetes Service backed by an EndpointSlice.

The demo setup configures only `httpbin.org:443` for egress.
Other hosts or plaintext HTTP destinations need their own agentgateway
Service, EndpointSlice, listener, and route. For HTTPS, TLS is still end-to-end
between the actor and the external service; agentgateway only routes the
encrypted bytes after CONNECT succeeds.

Enabling egress on an already-running ATE system creates or updates the
`ate-egress-capture` ConfigMap, but that ConfigMap does not currently force an
`ate-controller` restart. If egress variables are missing from worker pods after
enabling capture, restart `ate-controller` so it rereads the config and
reconciles WorkerPool deployments.

## Prerequisites

- A working Kubernetes cluster and kubeconfig.
- `kubectl`, `helm`, `jq`, and `curl`.
- `kubectl ate` available from this repo, for example:

```bash
go install ./cmd/kubectl-ate
export PATH="$(go env GOPATH)/bin:${PATH}"
```

## Install with capture enabled

For a normal cluster:

```bash
./hack/install-ate.sh --egress --deploy-ate-system
```

For kind:

```bash
./hack/install-ate-kind.sh --egress --deploy-ate-system
```

This deploys agentgateway with a static `httpbin.org:443` egress route, creates
the `ate-system/ate-egress-capture` config map, and deploys the ATE system.
When `ATE_EGRESS_CAPTURE_ENABLED=true`, `ATE_EGRESS_PEP_ADDRESS` is required;
the install script sets it to the in-cluster `ate-egress` Service by default.

The install script resolves `httpbin.org` during install and creates the
`httpbin-egress` Service and EndpointSlice for those IPs. `ateom` derives the
CONNECT authority from SNI for this HTTPS demo.

Verify the egress config:

```bash
kubectl get configmap -n ate-system ate-egress-capture -o yaml
```

Expected values:

```yaml
ATE_EGRESS_CAPTURE_ENABLED: "true"
ATE_EGRESS_PEP_ADDRESS: ate-egress.agentgateway-system.svc.cluster.local:15008
```

Verify the static agentgateway resources:

```bash
kubectl get gateway -n agentgateway-system ate-egress
kubectl get tcproute -n agentgateway-system httpbin-egress
kubectl get agentgatewaypolicy -n agentgateway-system ate-egress-connect
kubectl get service -n agentgateway-system httpbin-egress
kubectl get endpointslice -n agentgateway-system httpbin-egress
```

Expected resources include:

```text
gateway.gateway.networking.k8s.io/ate-egress
tcproute.gateway.networking.k8s.io/httpbin-egress
agentgatewaypolicy.agentgateway.dev/ate-egress-connect
service/httpbin-egress
endpointslice.discovery.k8s.io/httpbin-egress
```

## Deploy and call the egress actor

Deploy the egress demo:

```bash
./hack/install-ate.sh --deploy-demo-egress
```

For kind, keep using the kind wrapper so the demo uses the same local image
registry and snapshot bucket settings:

```bash
./hack/install-ate-kind.sh --deploy-demo-egress
```

Create an actor:

```bash
kubectl ate create atespace demo
kubectl ate create actor my-egress-1 --template ate-demo-egress/egress -a demo
```

Forward the router locally:

```bash
kubectl port-forward -n ate-system svc/atenet-router 8000:80
```

From another terminal, send an external request through the router. The actor
will make an outbound HTTPS request to `https://httpbin.org/get` by default:

```bash
curl -i -X POST \
  -H "Host: my-egress-1.demo.actors.resources.substrate.ate.dev" \
  http://localhost:8000
```

Expected response:

```text
HTTP/1.1 200 OK
egress target: https://httpbin.org/get
upstream status: 200 OK
body bytes read: ...
```

The response must name `https://httpbin.org/get`. That proves the actor opened a
TCP connection to `httpbin.org:443` from inside the sandbox.

To test a different `httpbin.org` path, pass it as the `url` query parameter:

```bash
curl -i -X POST --get \
  -H "Host: my-egress-1.demo.actors.resources.substrate.ate.dev" \
  --data-urlencode "url=https://httpbin.org/headers" \
  "http://localhost:8000"
```

Do not use this query parameter for a different host unless you also update the
agentgateway route. `ateom` will derive the new host from SNI, but the demo
agentgateway config only routes `httpbin.org:443`.

## Verify capture was installed

Find the worker pod hosting the actor:

```bash
actor_json=$(kubectl ate get actor my-egress-1 -a demo -o json)
ateom_ns=$(jq -r '.actors[0].ateomPodNamespace' <<<"${actor_json}")
ateom_pod=$(jq -r '.actors[0].ateomPodName' <<<"${actor_json}")

echo "${ateom_ns}/${ateom_pod}"
```

Check that the ateom pod received capture configuration:

```bash
kubectl get pod -n "${ateom_ns}" "${ateom_pod}" \
  -o jsonpath='{range .spec.containers[?(@.name=="ateom")].env[*]}{.name}={.value}{"\n"}{end}' \
  | grep ATE_EGRESS
```

Expected output includes:

```text
ATE_EGRESS_CAPTURE_ENABLED=true
ATE_EGRESS_PEP_ADDRESS=ate-egress.agentgateway-system.svc.cluster.local:15008
```

Check the ateom logs:

```bash
kubectl logs -n "${ateom_ns}" "${ateom_pod}" -c ateom | grep "Started actor egress capture listener"
```

Expected output includes one log line for the local capture listener:

```text
Started actor egress capture listener ... "port":15001
```

After the egress request, the logs should also show the captured stream:

```bash
kubectl logs -n "${ateom_ns}" "${ateom_pod}" -c ateom | grep "Proxying captured actor egress"
```

Expected output includes:

```text
Proxying captured actor egress ... "originalDestination":"...:443" ... "connectAuthority":"httpbin.org:443"
```

## Check agentgateway logs

The `ate-egress` Gateway creates an agentgateway dataplane pod in the
`agentgateway-system` namespace. Check dataplane logs with:

```bash
kubectl logs -n agentgateway-system \
  -l gateway.networking.k8s.io/gateway-name=ate-egress \
  --all-containers --tail=200
```

After a successful egress request, dataplane logs should include a TCP route
entry similar to:

```text
request gateway=agentgateway-system/ate-egress listener=https route=agentgateway-system/httpbin-egress ... protocol=tcp
```

If the Gateway, TCPRoute, or policy is not being programmed, check the
agentgateway controller logs:

```bash
kubectl logs -n agentgateway-system deploy/agentgateway --tail=200
```

## Clean up

```bash
kubectl ate suspend actor my-egress-1 -a demo
kubectl ate delete actor my-egress-1 -a demo
./hack/install-ate.sh --delete-demo-egress
```

## Troubleshooting

If redeploying fails with `The ActorTemplate "egress" is invalid: spec:
Invalid value: Spec is immutable`, recreate the demo resources:

```bash
./hack/install-ate-kind.sh --delete-demo-egress
./hack/install-ate-kind.sh --deploy-demo-egress
```

If the `ATE_EGRESS_*` variables are missing from the worker pod, restart the
controller and recreate the egress WorkerPool pods after creating the config
map:

```bash
kubectl rollout restart deployment/ate-controller -n ate-system
kubectl rollout status deployment/ate-controller -n ate-system
kubectl rollout restart deployment/egress-deployment -n ate-demo-egress
kubectl rollout status deployment/egress-deployment -n ate-demo-egress
```

If the capture listener logs are missing, confirm that the actor is running on a
fresh worker pod created after egress was enabled:

```bash
kubectl ate get actor my-egress-1 -a demo
kubectl get pods -n ate-demo-egress -l ate.dev/worker-pool=egress
```

If the egress request fails after changing the `url` host, remember that this demo only
configures agentgateway for `httpbin.org:443`. Add matching static agentgateway
backend resources for the new destination:

- HTTPS: Service, EndpointSlice, TCP listener on `443`, and TCPRoute.
- Plaintext HTTP: Service, EndpointSlice, TCP listener on `80`, and TCPRoute.

Traffic without SNI or a plaintext HTTP `Host` header falls back to the captured
original destination IP and port, which requires matching agentgateway routing
for that address.
