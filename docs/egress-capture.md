# Egress Capture

This documents the demo install path and the ateom capture setup using
the existing counter demo. The counter demo is useful because an external curl to
the router resumes a real gVisor actor, and its `/egress` path makes that actor
open an outbound HTTPS request to `https://httpbin.org/get` by default.

## Architecture

When egress capture is enabled, each worker pod gets `ATE_EGRESS_*`
configuration from the controller. The reusable capture core lives in
`internal/egresscapture`: it owns environment parsing, capture listeners,
authority derivation, CONNECT tunnel transports, and byte proxying. The
runtime-specific `ateom` egress proxy setup supplies the original-destination
lookup and packet-capture rules.

The current gVisor implementation starts a local capture listener and installs
actor-network redirects for TCP/80 and TCP/443. From the actor's point of view
it still opens a normal HTTP or HTTPS connection to the original destination.
MicroVM or future hypervisor implementations should reuse
`internal/egresscapture` for the local listener, authority derivation, tunnel
transport, and byte proxying. Each runtime still provides its own egress proxy
setup for redirecting actor traffic and recovering the original destination.

The redirected connection lands on `ateom`, which records the original
destination and derives a stable CONNECT authority from the first bytes of the
actor connection:

| Actor traffic | Authority source | Example CONNECT authority |
| --- | --- | --- |
| HTTPS / TCP 443 | TLS ClientHello SNI | `httpbin.org:443` |
| Plaintext HTTP / TCP 80 | HTTP `Host` header | `example.com:80` |

The shared capture core then opens a tunnel selected by
`ATE_EGRESS_TUNNEL_PROTOCOL`. The default `connect` transport opens a plaintext
HTTP/2 CONNECT stream to the agentgateway data plane at
`ate-egress.agentgateway-system.svc.cluster.local:15008`. The transport registry
also supports TLS CONNECT variants and is intended to allow future transports
such as HBONE without changing the runtime-specific capture setup. Agentgateway
maps the CONNECT authority to its configured TCP listener and routes the tunnel
to a Kubernetes Service backed by an EndpointSlice.

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
ATE_EGRESS_TUNNEL_PROTOCOL: connect
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

## Deploy and call the counter actor

Deploy the existing counter demo:

```bash
./hack/install-ate.sh --deploy-demo-counter
```

Create an actor:

```bash
kubectl ate create actor my-counter-1 --template ate-demo-counter/counter
```

Forward the router locally:

```bash
kubectl port-forward -n ate-system svc/atenet-router 8000:80
```

From another terminal, send an external request through the router to prove
normal ingress still works:

```bash
curl -i -X POST \
  -H "Host: my-counter-1.actors.resources.substrate.ate.dev" \
  http://localhost:8000
```

Expected response:

```text
HTTP/1.1 200 OK
hello from: 169.254.17.2 | preserved memory count: 1
```

The exact count can differ. The important part is that the actor reaches
`STATUS_RUNNING` and is assigned to an ateom pod.

Now send an external request to the demo's egress path:

```bash
curl -i -X POST \
  -H "Host: my-counter-1.actors.resources.substrate.ate.dev" \
  http://localhost:8000/egress
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
  -H "Host: my-counter-1.actors.resources.substrate.ate.dev" \
  --data-urlencode "url=https://httpbin.org/headers" \
  "http://localhost:8000/egress"
```

Do not use this query parameter for a different host unless you also update the
agentgateway route. `ateom` will derive the new host from SNI, but the demo
agentgateway config only routes `httpbin.org:443`.

## Run the microVM counter demo with egress enabled

The microVM demo uses the same counter container and `/egress` handler, but runs
it with `sandboxClass: microvm` on `ateom-microvm`. This is useful for checking
that egress configuration reaches microVM worker pods and for exercising the
demo request path.

When egress capture is enabled, `ateom-microvm` starts the same reusable
`internal/egresscapture` listener as the gVisor runtime and installs
microVM-specific redirect rules in the worker pod network namespace. HTTP and
HTTPS egress from the guest is redirected to the local ateom egress proxy, which
recovers `SO_ORIGINAL_DST`; the shared capture core derives CONNECT authority
from HTTP `Host` or TLS SNI and opens the configured tunnel to the egress PEP.

For kind, deploy the ATE system first so the in-cluster rustfs service exists
before the microVM demo stages runtime assets. Then run the microVM bring-up,
enable the egress resources, and restart microVM workers so the controller's
egress environment is copied into fresh worker pods:

```bash
KIND_CLUSTER_NAME="${KIND_CLUSTER_NAME:-kind}" ./hack/install-ate-kind.sh --deploy-ate-system
KIND_CLUSTER_NAME="${KIND_CLUSTER_NAME:-kind}" ./hack/run-microvm-demo-kind.sh
KIND_CLUSTER_NAME="${KIND_CLUSTER_NAME:-kind}" ./hack/install-ate-kind.sh --egress --deploy-ate-controller
```

For kind, the node must expose `/dev/kvm` to the kind node and carry the
`ate.dev/sandboxClass=microvm` label. `hack/create-kind-cluster.sh` does both
when `/dev/kvm` is available in the Docker environment. Without that label, the
microVM worker pods remain `Pending`, and router requests fail with:

```text
actor "my-counter-microvm-1" unavailable: no free workers available
```

For a normal cluster, run:

```bash
./hack/run-microvm-demo.sh
./hack/install-ate.sh --egress --deploy-ate-controller
```

Before creating the actor, verify that the microVM worker pods are scheduled and
available:

```bash
kubectl get nodes -L ate.dev/sandboxClass
kubectl get pods -n ate-demo-counter-microvm -o wide

kubectl wait --for=condition=Available \
  deployment/counter-microvm-deployment -n ate-demo-counter-microvm --timeout=300s

kubectl wait --for=condition=Ready \
  actortemplate/counter-microvm -n ate-demo-counter-microvm --timeout=600s
```

Expected node output includes `microvm` in the `ATE.DEV/SANDBOXCLASS` column,
and the `counter-microvm` pods should be `Running`. If the pods are `Pending`,
inspect the scheduling reason:

```bash
kubectl describe pod -n ate-demo-counter-microvm \
  -l ate.dev/worker-pool=counter-microvm
```

If `/dev/kvm` is mounted but the label is missing, add the label and recreate the
worker pods:

```bash
kubectl label node kind-control-plane ate.dev/sandboxClass=microvm --overwrite
kubectl rollout restart deployment/counter-microvm-deployment -n ate-demo-counter-microvm
kubectl rollout status deployment/counter-microvm-deployment -n ate-demo-counter-microvm
```

If `/dev/kvm` is not mounted into the node, recreate the kind cluster with
`hack/create-kind-cluster.sh` on a host or Docker VM that has KVM available.

After the worker deployment and golden snapshot are ready, create a microVM actor
and call it through the router:

```bash
kubectl ate create actor my-counter-microvm-1 \
  --template ate-demo-counter-microvm/counter-microvm

kubectl port-forward -n ate-system svc/atenet-router 8000:80
```

From another terminal, verify the normal counter path:

```bash
curl -i -X POST \
  -H "Host: my-counter-microvm-1.actors.resources.substrate.ate.dev" \
  http://localhost:8000
```

Then exercise the demo egress path:

```bash
curl -i -X POST \
  -H "Host: my-counter-microvm-1.actors.resources.substrate.ate.dev" \
  http://localhost:8000/egress
```

Verify that the microVM worker pod received the egress configuration:

```bash
actor_json=$(kubectl ate get actor my-counter-microvm-1 -o json)
ateom_ns=$(jq -r '.actors[0].ateomPodNamespace' <<<"${actor_json}")
ateom_pod=$(jq -r '.actors[0].ateomPodName' <<<"${actor_json}")

kubectl get pod -n "${ateom_ns}" "${ateom_pod}" \
  -o jsonpath='{range .spec.containers[?(@.name=="ateom")].env[*]}{.name}={.value}{"\n"}{end}' \
  | grep ATE_EGRESS
```

Expected output includes:

```text
ATE_EGRESS_CAPTURE_ENABLED=true
ATE_EGRESS_PEP_ADDRESS=ate-egress.agentgateway-system.svc.cluster.local:15008
ATE_EGRESS_TUNNEL_PROTOCOL=connect
```

Check that the microVM runtime installed the capture listener:

```bash
kubectl logs -n "${ateom_ns}" "${ateom_pod}" -c ateom | grep "Started actor egress capture listener"
```

Expected output includes one log line for the local capture listener:

```text
Started actor egress capture listener ... "port":15001 ... "protocol":"connect"
```

After the `/egress` request, the logs should also show the captured stream:

```bash
kubectl logs -n "${ateom_ns}" "${ateom_pod}" -c ateom | grep "Proxying captured actor egress"
```

Expected output includes:

```text
Proxying captured actor egress ... "originalDestination":"...:443" ... "connectAuthority":"httpbin.org:443"
```

## Verify capture was installed

Find the worker pod hosting the actor:

```bash
actor_json=$(kubectl ate get actor my-counter-1 -o json)
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
ATE_EGRESS_TUNNEL_PROTOCOL=connect
```

Check the ateom logs:

```bash
kubectl logs -n "${ateom_ns}" "${ateom_pod}" -c ateom | grep "Started actor egress capture listener"
```

Expected output includes one log line for the local capture listener:

```text
Started actor egress capture listener ... "port":15001 ... "protocol":"connect"
```

After the `/egress` request, the logs should also show the captured stream:

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

After a successful `/egress` request, dataplane logs should include a TCP route
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
kubectl ate suspend actor my-counter-1
kubectl ate delete actor my-counter-1
./hack/install-ate.sh --delete-demo-counter
```

## Troubleshooting

If the `ATE_EGRESS_*` variables are missing from the worker pod, restart the
controller and recreate the counter WorkerPool pods after creating the config
map:

```bash
kubectl rollout restart deployment/ate-controller -n ate-system
kubectl rollout status deployment/ate-controller -n ate-system
kubectl rollout restart deployment/counter-deployment -n ate-demo-counter
kubectl rollout status deployment/counter-deployment -n ate-demo-counter
```

If the capture listener logs are missing, confirm that the actor is running on a
fresh worker pod created after egress was enabled:

```bash
kubectl ate get actor my-counter-1
kubectl get pods -n ate-demo-counter -l ate.dev/worker-pool=counter
```

If the microVM curl returns `503 Service Unavailable` with `no free workers
available`, the request reached the router and ate-api, but no eligible microVM
worker was available for assignment. Check the worker pods and node label:

```bash
kubectl get nodes -L ate.dev/sandboxClass
kubectl get pods -n ate-demo-counter-microvm -o wide
kubectl describe pod -n ate-demo-counter-microvm \
  -l ate.dev/worker-pool=counter-microvm
```

The microVM WorkerPool pods require `nodeSelector:
ate.dev/sandboxClass=microvm` and `/dev/kvm`. For kind, recreate the cluster with
`hack/create-kind-cluster.sh`, or label the node if KVM is already mounted:

```bash
kubectl label node kind-control-plane ate.dev/sandboxClass=microvm --overwrite
kubectl rollout restart deployment/counter-microvm-deployment -n ate-demo-counter-microvm
kubectl rollout status deployment/counter-microvm-deployment -n ate-demo-counter-microvm
```

If `/egress` fails after changing the `url` host, remember that this demo only
configures agentgateway for `httpbin.org:443`. Add matching static agentgateway
backend resources for the new destination:

- HTTPS: Service, EndpointSlice, TCP listener on `443`, and TCPRoute.
- Plaintext HTTP: Service, EndpointSlice, TCP listener on `80`, and TCPRoute.

Traffic without SNI or a plaintext HTTP `Host` header falls back to the captured
original destination IP and port, which requires matching agentgateway routing
for that address.
