# Egress Capture

This documents the demo install path and the ateom capture setup using
the dedicated egress demo. The demo is a small HTTP actor that resumes through
the router and opens an outbound HTTPS request to `https://httpbin.org/get`
by default.

## Architecture

Egress capture has no global on/off switch. ate-api watches Gateways labeled
`ate.dev/egress-pep`. On actor resume, ate-api picks the best matching PEP
Gateway for that actor and sends one optional PEP address to ateom. An empty PEP
address means no redirect, so capture is enabled per actor purely by whether a
matching labeled Gateway exists. The reusable capture core lives in
`internal/egress`:
it owns capture listeners, authority derivation, CONNECT tunnel transports, and
byte proxying. The runtime-specific `ateom` egress proxy setup supplies the
original-destination lookup and packet-capture rules.

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

The shared capture core then opens a plaintext HTTP/2 CONNECT stream to the PEP
address selected by ate-api. Only Gateways with condition `Programmed=True` are
candidates; an unprovisioned Gateway is skipped so resolution falls back to the
next-best PEP. The address host comes from the Gateway's `status.addresses`
when the implementation publishes one, and otherwise falls back to
`<gateway>.<namespace>.svc.cluster.local`, matching the agentgateway service
convention (agentgateway on kind publishes no address because the LoadBalancer
stays pending). The port is the Gateway's HTTP listener port. Agentgateway maps
the CONNECT authority to its configured TCP listener and routes the tunnel to a
Kubernetes Service backed by an EndpointSlice.

The demo setup configures only `httpbin.org:443` for egress.
Other hosts or plaintext HTTP destinations need their own agentgateway
Service, EndpointSlice, listener, and route. For HTTPS, TLS is still end-to-end
between the actor and the external service; agentgateway only routes the
encrypted bytes after CONNECT succeeds.

### Selecting a PEP for an actor

`ate.dev/egress-pep` is a marker: its value is ignored, any Gateway carrying
the label key is a candidate PEP. The atespace/actor labels scope which actors
a candidate serves:

| Gateway labels | Scope | Match precedence |
| --- | --- | --- |
| `ate.dev/egress-pep` only | Global (any actor) | lowest |
| `ate.dev/egress-pep` + `ate.dev/atespace=<atespace>` | All actors in an atespace | medium |
| `ate.dev/egress-pep` + `ate.dev/atespace=<atespace>` + `ate.dev/actor=<actor-id>` | One actor | highest |

Actor scoping requires **both** scoping labels; `ate.dev/actor` alone matches
no actor and ate-api logs a warning. On resume, ate-api picks the
highest-precedence match (`resolveEgressPEPAddress` in
`cmd/ateapi/internal/controlapi/egress_pep.go`).

#### Tie-breaking

| Situation | Result |
| --- | --- |
| Multiple candidates tied at the top score | Lowest `(namespace, name)` wins; the others are **silently ignored** |
| No labeled candidate | Empty PEP address → no redirect, capture off |

Don't deploy multiple PEPs at the same tier for the same actor — use the
scoping labels to raise the intended one's tier instead of relying on name
order.

#### Trust model

The `ate.dev/egress-pep` label **is** the PEP control surface: ate-api trusts
every labeled Gateway in every namespace, so Gateway RBAC is the security
boundary — restrict Gateway create/update to the platform team. Anyone who can
label a Gateway can:

- **Intercept actor egress**: copy an actor's `ate.dev/atespace` +
  `ate.dev/actor` labels onto their own Gateway to out-score its real PEP.
- **Block all resumes**: label a Gateway with no HTTP listener (broken PEP
  config fails resolution cluster-wide by design).

Substrate deliberately does not second-guess labeled Gateways. If Gateway RBAC
can't be that strict, scope the watch to an allowlist of PEP namespaces in
ate-api first.

#### When the binding is (re)computed

The PEP binding is a **snapshot taken at resume**, recorded on the actor as
`egress_pep_address`. Gateway relabels have no effect on RUNNING actors.

```
   create
      │
      ▼
  SUSPENDED ──────────────────────────────────┐
      │  resume / boot                         │
      ▼                                        │
  RESUMING   ── resolve PEP now:               │
      │         AssignWorkerStep scores every  │
      │         labeled Gateway and writes     │
      │         actor.egress_pep_address       │
      ▼                                        │
  RUNNING    ── uses the PEP captured at        │
      │         resume for its whole lifetime;  │
      │         Gateway relabels are IGNORED    │
      │  suspend / pause                        │
      ▼                                        │
  SUSPENDED / PAUSED ── egress_pep_address ─────┘
                        cleared; re-resolved
                        on the next resume
```

To move a running actor to a different PEP: relabel the Gateways **and** cycle
the actor (see "Point an actor at a different PEP").

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

This deploys agentgateway with a static `httpbin.org:443` egress route, labels
the `agentgateway-system/ate-egress` Gateway as a PEP, and deploys the ATE
system. No fixed PEP address is configured; ate-api derives the address from the
labeled Gateway's HTTP listener.

The install script resolves `httpbin.org` during install and creates the
`httpbin-egress` Service and EndpointSlice for those IPs. `ateom` derives the
CONNECT authority from SNI for this HTTPS demo.

Verify the static agentgateway resources and PEP label:

```bash
kubectl get gateway -n agentgateway-system ate-egress --show-labels
kubectl get tcproute -n agentgateway-system httpbin-egress
kubectl get agentgatewaypolicy -n agentgateway-system ate-egress-connect
kubectl get service -n agentgateway-system httpbin-egress
kubectl get endpointslice -n agentgateway-system httpbin-egress
```

Expected resources include:

```text
gateway.gateway.networking.k8s.io/ate-egress ... ate.dev/egress-pep=true
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

Check the ateom logs:

```bash
kubectl logs -n "${ateom_ns}" "${ateom_pod}" -c ateom | grep "Started actor egress capture listener"
```

Expected output includes one log line for the local capture listener:

```text
Started actor egress capture listener ... "port":15001 ... "pepAddress":"ate-egress.agentgateway-system.svc.cluster.local:15008"
```

After the egress request, the logs should also show the captured stream:

```bash
kubectl logs -n "${ateom_ns}" "${ateom_pod}" -c ateom | grep "Proxying captured actor egress"
```

Expected output includes:

```text
Proxying captured actor egress ... "originalDestination":"...:443" ... "connectAuthority":"httpbin.org:443"
```

## Check which PEP an actor uses

ate-api records the PEP it resolved for the actor on its most recent resume, so
you can read the binding directly instead of inferring it from Gateway labels.

From the actor status (`null` means no PEP matched — the field is omitted from
JSON when empty — so capture is off):

```bash
kubectl ate get actor my-egress-1 -a demo -o json | jq -r '.actors[0].egressPepAddress'
```

Expected value for the demo:

```text
ate-egress.agentgateway-system.svc.cluster.local:15008
```

ate-api also logs the resolution on each resume. This is the easiest way to
answer "which actors fall through to a global PEP" — grep the PEP address across
ate-api logs:

```bash
kubectl logs -n ate-system deploy/ate-api-server-deployment | grep "Resolved egress PEP for actor"
```

Expected output includes:

```text
Resolved egress PEP for actor ... "actorId":"my-egress-1" "atespace":"demo" "pepAddress":"ate-egress.agentgateway-system.svc.cluster.local:15008"
```

Actors that matched no PEP log `Resolved no egress PEP for actor; egress capture
disabled` instead, and their `egressPepAddress` status field is absent.

## Point an actor at a different PEP

Suppose you want `my-egress-1` to egress through a second Gateway,
`ate-egress-alt`, instead of the shared `ate-egress` PEP. Because an actor's PEP
is a snapshot taken at resume (see "When the binding is (re)computed"), you both
relabel the Gateways and cycle the actor.

1. Create the alternate Gateway and label it so it out-scores the global PEP for
   this actor. Scoping it to the actor (atespace + actor) gives it the highest
   tier, so it wins regardless of the global PEP:

```bash
kubectl apply -f - <<'EOF'
apiVersion: gateway.networking.k8s.io/v1
kind: Gateway
metadata:
  name: ate-egress-alt
  namespace: agentgateway-system
  labels:
    ate.dev/egress-pep: "true"
    ate.dev/atespace: "demo"
    ate.dev/actor: "my-egress-1"
spec:
  gatewayClassName: agentgateway
  listeners:
  - name: connect
    port: 15008
    protocol: HTTP
    allowedRoutes:
      namespaces:
        from: Same
  - name: https
    port: 443
    protocol: TCP
    allowedRoutes:
      kinds:
      - group: gateway.networking.k8s.io
        kind: TCPRoute
      namespaces:
        from: Same
EOF
```

   The `connect` listener must be `protocol: HTTP` — ate-api derives the PEP
   address from the Gateway's HTTP listener, and a labeled Gateway without one
   is treated as a configuration error that fails PEP resolution.

2. Give the alternate Gateway the CONNECT policy and a route to the backend.
   Labeling alone is not enough: without these the tunnel opens but agentgateway
   has nothing routing `httpbin.org:443`, and egress fails with a 502 / connection
   reset. The `httpbin-egress` Service and EndpointSlice created by the installer
   are backend resources and can be reused as-is; you only need a CONNECT policy
   and a TCPRoute parent for the new Gateway:

   ```bash
kubectl apply -f - <<'EOF'
apiVersion: agentgateway.dev/v1alpha1
kind: AgentgatewayPolicy
metadata:
  name: ate-egress-alt-connect
  namespace: agentgateway-system
spec:
  targetRefs:
  - group: gateway.networking.k8s.io
    kind: Gateway
    name: ate-egress-alt
  frontend:
    connect:
      mode: Tunnel
EOF
   ```

   Attach the existing `httpbin-egress` TCPRoute to the alternate Gateway by
   adding it as a second parent (this leaves `ate-egress` routing intact):

   ```bash
   kubectl patch tcproute -n agentgateway-system httpbin-egress --type=json \
     -p '[{"op":"add","path":"/spec/parentRefs/-","value":{"group":"gateway.networking.k8s.io","kind":"Gateway","name":"ate-egress-alt","sectionName":"https"}}]'
   ```

   Confirm both the policy and route attached before continuing:

   ```bash
   kubectl get agentgatewaypolicy -n agentgateway-system ate-egress-alt-connect
   kubectl get tcproute -n agentgateway-system httpbin-egress \
     -o jsonpath='{range .status.parents[*]}{.parentRef.name}{" Accepted="}{.conditions[?(@.type=="Accepted")].status}{"\n"}{end}'
   ```

3. Cycle the actor so ate-api re-resolves the PEP. A running actor keeps its old
   PEP until it is suspended (or paused) and resumed:

   ```bash
   kubectl ate suspend actor my-egress-1 -a demo
   kubectl ate resume actor my-egress-1 -a demo
   ```

4. Confirm the actor now points at the alternate PEP:

   ```bash
   kubectl ate get actor my-egress-1 -a demo -o json | jq -r '.actors[0].egressPepAddress'
   ```

   Expected value:

   ```text
   ate-egress-alt.agentgateway-system.svc.cluster.local:15008
   ```

5. Drive traffic again and verify it goes through the new PEP:

   ```bash
   curl -i -X POST \
     -H "Host: my-egress-1.demo.actors.resources.substrate.ate.dev" \
     http://localhost:8000
   ```

   The authoritative signal is the ateom capture log, which names the PEP the
   actor actually tunnelled through (see "Verify capture was installed" for how
   to find the worker pod):

   ```bash
   kubectl logs -n "${ateom_ns}" "${ateom_pod}" -c ateom | grep "Started actor egress capture listener"
   # ... "pepAddress":"ate-egress-alt.agentgateway-system.svc.cluster.local:15008"
   ```

   On the agentgateway side, match any request the alt Gateway handled rather
   than a specific protocol — the CONNECT frontend logs `protocol=http`, and a
   `protocol=tcp` line only appears once bytes tunnel end to end (a 5xx from the
   upstream can prevent it):

   ```bash
   kubectl logs -n agentgateway-system \
     -l gateway.networking.k8s.io/gateway-name=ate-egress-alt \
     --all-containers --tail=200 | grep "gateway=agentgateway-system/ate-egress-alt"
   ```

To revert, remove the alternate Gateway's route parent and delete the Gateway
and its CONNECT policy, then cycle the actor again; ate-api falls back to the
next-best PEP, the global `ate-egress`:

```bash
kubectl patch tcproute -n agentgateway-system httpbin-egress --type=json \
  -p '[{"op":"remove","path":"/spec/parentRefs/1"}]'
kubectl delete gateway -n agentgateway-system ate-egress-alt
kubectl delete agentgatewaypolicy -n agentgateway-system ate-egress-alt-connect
kubectl ate suspend actor my-egress-1 -a demo
kubectl ate resume actor my-egress-1 -a demo
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

If capture listener logs are missing after labeling a Gateway on an
already-running ATE system, no ate-api restart is needed for the label itself —
the Gateway watcher is a live watch and sees label changes immediately. The
usual cause is that the actor was already running: the PEP binding is a
snapshot taken at resume, so cycle the actor (suspend, then resume) to
re-resolve.

The one case that does require an ate-api restart is installing the Gateway API
CRDs *after* ate-api started: ate-api checks for the Gateway resource once at
boot and disables PEP resolution if it is absent (it logs
`Gateway API resource not served; egress PEP resolution disabled`):

```bash
kubectl rollout restart deployment/ate-api-server-deployment -n ate-system
kubectl rollout status deployment/ate-api-server-deployment -n ate-system
```

Capture is decided per resume from the PEP address ate-api sends to ateom, so
worker pods do not need to carry any egress config or be restarted. An actor
already running before its PEP Gateway existed picks up capture on its next
resume.

Worker images must include egress support: an older ateom silently ignores the
PEP address, so `egressPepAddress` on the actor can report a binding the
sandbox does not enforce. If capture logs are missing despite a resolved PEP,
check the WorkerPool's ateom image version.

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
