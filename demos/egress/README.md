# Egress Demo — Pluggable Egress Networking

This demo shows an Actor's outbound traffic being **transparently tunneled through an
egress gateway** and **authenticated by actor identity**, end to end. The same demo runs
with either Envoy or [agentgateway](https://agentgateway.dev/).

The Actor is a tiny service that accepts `{"url":"..."}`, performs an HTTP `GET`, and returns
the upstream response. The Actor believes it is dialing plain HTTP directly — but its egress is
intercepted and carried over mTLS to a gateway that verifies who is making the request.

## What it demonstrates

```text
  ┌──────────────── ateom worker pod ─────────────────┐
  │  Actor (gVisor)                                     │
  │  GET http://<dst-ip>:80/   (plain HTTP)             │
  │        │                                            │
  │        ▼  nftables REDIRECT                         │
  │  atunnel egress  ──(mTLS with the actor's own       │
  │        │            certificate + bare CONNECT)     │
  └────────┼────────────────────────────────────────────┘
           ▼
  ┌──────────── atenet-egress pod ───────────────────┐
  │  Egress Gateway                            │
  │    • downstream mTLS, trusted by actor-id CA      │
  │    • terminates HTTP CONNECT                      │
  │    • verifies Actor identity with the ATE API     │
  │    • forwards to the CONNECT authority            │
  │           │                                       │
  └───────────┼───────────────────────────────────────┘
              ▼
     real destination (the CONNECT authority, an IP:port)
```

1. **Guide 1 - gateway accepts CONNECT + mTLS.** `atenet-egress` terminates the actor's mTLS `CONNECT` and
  tunnels to the requested destination.
2. **Guide 2 — transparent interception.** `nftables` REDIRECTs actor TCP egress into `atunnel`,
   which wraps it in mTLS + `CONNECT`.
3. **Guide 3 — HTTP-only actors, identity carried by the certificate.** The Actor only dials plain
   HTTP. atunnel presents the actor's own certificate — minted per actor by ateapi off the
   actor-identity CA, carrying an `ActorIdentity` X.509 extension — and sends a bare `CONNECT`
   with no identity headers at all.
4. **Identity authentication.** The gateway requires a client certificate signed by the
  actor-identity CA, so a non-actor client is refused at the handshake. It authorizes the
  certificate against the ATE API and rejects it unless the certified **UID** matches a real,
  `RUNNING` actor.

## Choose a dataplane

The dataplane is selected when installing `ate-system`, not when installing this demo. The Actor,
ActorTemplate, worker pool, test, and manual walkthrough are otherwise the same.

```bash
# Envoy (default)
./hack/install-ate-kind.sh --deploy-ate-system

# agentgateway
./hack/install-ate-kind.sh --deploy-ate-system --atenet-router=agentgateway
```

| | Envoy | agentgateway |
| --- | --- | --- |
| Select with | `--atenet-router=envoy` (default) | `--atenet-router=agentgateway` |
| Egress routing | Dynamic forward proxy | Dynamic backend from CONNECT authority |
| Actor authentication | Co-located atenet `ext_proc` | Built-in `substrateEgress` policy |
| Configuration | Envoy bootstrap in `atenet-egress.yaml` | Static agentgateway ConfigMap overlay |
| Access log | Text beginning with `[egress]`, including actor SAN | Structured log including `substrate.connect.authority` |
| MITM mode | Supported with `--experimental-use-sdsmint` | Supported with `--experimental-use-sdsmint` |

The experimental additional egress `ext_proc` service currently requires Envoy; the installer
rejects that option with agentgateway rather than silently omitting it.

## Components

- **Egress app (`main.go`)** — the Actor: `POST /` with `{"url":"..."}` → fetches it → returns
  status + body. It also serves `POST /grpc`, described below.
- **Egress gateway** — the `atenet-egress` Deployment. Envoy uses a co-located atenet `ext_proc`
  container started with `--mode=egress`; agentgateway uses its built-in `substrateEgress` policy
  and does not need that sidecar. The installer renders the matching configuration and container.
- **Egress opt-in** — `ate-api-server --egress-gateway-address=atenet-egress.ate-system.svc:443`
  (set in `manifests/ate-install/ate-api-server.yaml`). ateapi stamps the address onto every
  atelet `Run`/`Restore`, which turns on tunneled egress cluster-wide.
- **Actor-identity trust** — the gateway mounts the `actor-id-ca-certs` Secret, a cert-only copy of
  the actor-identity CA root that `hack/install-ate.sh` derives from `actor-id-ca-pool` (which also
  holds the CA signing key and is deliberately *not* mounted here).

## Prerequisites

- A kind cluster with Agent Substrate installed (`hack/create-kind-cluster.sh`, followed by one of
  the [dataplane installation commands above](#choose-a-dataplane)). Egress is enabled by the
  ateapi flag above.
- `ko`, `kubectl`, and `kubectl-ate` (`go install ./cmd/kubectl-ate`).

## Deploy the demo fixture

```bash
./hack/install-ate.sh --deploy-demo-egress
```

The install applies the worker pool, creates the `ate-demo-egress` atespace and
the `egress` ActorTemplate (a substrate resource, not a CRD) through the ate
API, and blocks until the template's golden snapshot is built:

```bash
kubectl ate get actor-template egress -a ate-demo-egress
```

## Run the automated test (easiest)

```bash
./demos/egress/test-egress.sh
```

It detects the deployed dataplane, deploys an in-cluster HTTP target, creates and resumes an Actor,
then asserts:

- **positive** — a real Actor's egress reaches the target (`HTTP 200`) *through the gateway*
  (the target sees the gateway's IP as its client), and the gateway logs the CONNECT. Envoy's log
  includes the actor certificate SAN; agentgateway's structured log includes the authority;
- **negative** — a pod holding a valid *pod* identity but no actor certificate cannot open a
  tunnel at all: the gateway trusts the actor-identity CA, so the mTLS handshake is
  refused before any CONNECT is answered.

Add `--cleanup` to remove everything the script created.

## Manual walkthrough

```bash
# 1. An in-cluster target the Actor will fetch (any HTTP server works).
kubectl create namespace egress-target
kubectl -n egress-target create deployment whoami --image=traefik/whoami
kubectl -n egress-target expose deployment whoami --port=80
TARGET_IP=$(kubectl -n egress-target get svc whoami -o jsonpath='{.spec.clusterIP}')

# 2. Create and resume an Actor in the demo's atespace: --template-ref
#    resolves the template by name within the actor's own atespace.
kubectl ate create actor egress-demo -a ate-demo-egress --template-ref egress
kubectl ate resume actor egress-demo -a ate-demo-egress   # wait for ACTOR_STATE_RUNNING

# 3. Drive the Actor's egress through the ingress gateway.
kubectl -n ate-system port-forward service/atenet-router 8000:80 &
curl -s -X POST http://localhost:8000/ \
  -H 'Host: egress-demo.ate-demo-egress.actors.resources.substrate.ate.dev' \
  -H 'Content-Type: application/json' \
  -d "{\"url\":\"http://${TARGET_IP}:80/\"}"
```

### What to observe

With Envoy:

```bash
# The egress gateway logs each tunneled CONNECT against the verified peer certificate:
kubectl -n ate-system logs deploy/atenet-egress -c envoy | grep '\[egress\]'
#   [egress] authority=<TARGET_IP>:80 peer_san=spiffe://substrate-actor.local/atespace/ate-demo-egress/actor/egress-demo … code=200 …

# The co-located ext_proc sidecar logs the identity decision, including the UID it authorized on:
kubectl -n ate-system logs deploy/atenet-egress -c ext-proc | grep -i 'egress identity\|egress denied'
#   egress identity authenticated  atespace=ate-demo-egress actor=egress-demo actorUid=… destination=<TARGET_IP>:80
```

With agentgateway:

```bash
# The structured access log includes the CONNECT authority:
kubectl -n ate-system logs deploy/atenet-egress -c agentgateway \
  | grep 'substrate.connect.authority'
```

The `whoami` body shows `RemoteAddr: <atenet-egress pod IP>` — proof the request egressed
*through* the gateway rather than directly.

## gRPC over the same tunnel

`POST /grpc` with `{"target":"<ip>:<port>","message":"hello","streamCount":2,"bidiCount":2}` makes
the Actor dial that address as a cleartext-HTTP/2 gRPC server and return what came back:

```json
{
  "message": "hello",
  "stream": [{"message":"hello","index":0},{"message":"hello","index":1}],
  "bidi":   [{"message":"hello-0","index":0},{"message":"hello-1","index":1}],
  "code":   "OK"
}
```

One RPC per streaming shape, because each fails differently over a network path:

- **unary `Echo`** — always run. Its status arrives in trailers, *after* the response body, which
  is what `code` reports and what a path that dropped trailers or downgraded to HTTP/1.1 could not
  produce.
- **server-streaming `EchoStream`** — when `streamCount` is positive. Many frames over one
  held-open connection.
- **bidirectional `EchoBidi`** — when `bidiCount` is positive. The Actor sends each message only
  after reading the response to the previous one, then half-closes the request direction while the
  response direction is still open. A path that serialized the two directions hangs here rather
  than answering short.

The gateway terminates the `CONNECT` and relays opaque TCP, so all of this crosses it untouched. A
failed RPC answers `502` and still carries its gRPC code in the same field.

`internal/e2e/suites/networking` drives this endpoint against the `testserver` fixture's `grpc`
subcommand (`internal/e2e/fixtures/testserver`, deployed by `e2e.DeployServerPod` from the shared
server-pod manifest) in `TestActorEgressGRPC`; the same fixture, or any h2c gRPC server reachable
from the cluster, works for a manual run.

## Notes / limitations

- This milestone **authenticates** identity (is this a real, running actor?). **Authorizing**
  egress by destination and injecting upstream credentials/tokens is a follow-up, implemented in
  the same `ext_proc` (policy API TBD).
- Identity comes entirely from the actor certificate: the atespace, actor name, and UID are read
  out of the `ActorIdentity` extension and the UID is matched against the live actor, so a
  certificate cannot survive its actor being deleted and recreated under the same name. Nothing
  the actor can write into the CONNECT contributes to the decision.

## Cleanup

```bash
./demos/egress/test-egress.sh --cleanup
./hack/install-ate.sh --delete-demo-egress
```
