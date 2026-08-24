# Egress Demo — Pluggable Egress Networking

This demo shows an Actor's outbound traffic being **transparently tunneled through an
egress gateway** and **authenticated by actor identity**, end to end.

The Actor is a tiny service that accepts `{"url":"..."}`, performs an HTTP `GET`, and returns
the upstream response. The Actor believes it is dialing plain HTTP directly — but its egress is
intercepted and carried over mTLS to a gateway that verifies who is making the request.

## What it demonstrates

```
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
  │  Envoy egress gateway                              │
  │    • downstream mTLS, trusted_ca = actor-id CA     │
  │    • terminates HTTP CONNECT                       │
  │    • ext_proc ──(localhost)──►  atenet router (ext_proc sidecar)
  │      (forwards the peer chain    │  verify chain + ActorIdentity extension
  │       as x-forwarded-client-cert)│  GetActor → UID must match, must be RUNNING
  │    • dynamic_forward_proxy       │  allow / deny 403
  │           │                              
  └───────────┼───────────────────────────────────────┘
              ▼
     real destination (the CONNECT authority, an IP:port)
```

1. **Guide 1 — gateway accepts CONNECT + mTLS.** `atenet-egress` is an Envoy dynamic-forward-proxy
   that terminates the actor's mTLS `CONNECT` and tunnels to the requested destination.
2. **Guide 2 — transparent interception.** `nftables` REDIRECTs actor TCP egress into `atunnel`,
   which wraps it in mTLS + `CONNECT`.
3. **Guide 3 — HTTP-only actors, identity carried by the certificate.** The Actor only dials plain
   HTTP. atunnel presents the actor's own certificate — minted per actor by ateapi off the
   actor-identity CA, carrying an `ActorIdentity` X.509 extension — and sends a bare `CONNECT`
   with no identity headers at all.
4. **Identity authentication.** Envoy requires a client certificate signed by the actor-identity
   CA, so a non-actor client is refused at the handshake. It then forwards the verified chain to
   `ext_proc` as `x-forwarded-client-cert`, and the **atenet router** (co-located in the gateway
   pod as an ext_proc sidecar, the same binary that serves ingress, started with `--mode=egress`)
   re-verifies the chain, requires exactly one `ActorIdentity` extension with `purpose: atunnel`,
   and calls the ate API (`GetActor`). It returns **403** unless the certified **UID** matches a
   real, `RUNNING` actor. This mirrors the ingress gateway's dataplane + ext_proc co-location; a
   standalone/shared ext_proc is a future step.

## Components

- **Egress app (`main.go`)** — the Actor: `POST /` with `{"url":"..."}` → fetches it → returns
  status + body. It also serves `POST /grpc`, described below.
- **Egress gateway** — `manifests/ate-install/atenet-egress.yaml`. One pod, two containers:
  an Envoy (`envoy`) and the atenet router ext_proc (`ext-proc`, `--mode=egress`), called over
  localhost. In egress mode the router serves the egress ext_proc handler only — no xDS server,
  no ActorTemplate controller, and no Kubernetes access at all.
- **Egress opt-in** — `ate-api-server --egress-gateway-address=atenet-egress.ate-system.svc:443`
  (set in `manifests/ate-install/ate-api-server.yaml`). ateapi stamps the address onto every
  atelet `Run`/`Restore`, which turns on tunneled egress cluster-wide.
- **Actor-identity trust** — the gateway mounts the `actor-id-ca-certs` Secret, a cert-only copy of
  the actor-identity CA root that `hack/install-ate.sh` derives from `actor-id-ca-pool` (which also
  holds the CA signing key and is deliberately *not* mounted here).

## Prerequisites

- A kind cluster with Agent Substrate installed (`hack/create-kind-cluster.sh` then
  `hack/install-ate-kind.sh --deploy-ate-system`). Egress is enabled by the ateapi flag above.
- `ko`, `kubectl`, and `kubectl-ate` (`go install ./cmd/kubectl-ate`).

## Deploy the demo fixture

```bash
./hack/install-ate.sh --deploy-demo-egress
kubectl wait --for=condition=Ready actortemplate/egress -n ate-demo-egress --timeout=5m
```

## Run the automated test (easiest)

```bash
./demos/egress/test-egress.sh
```

It deploys an in-cluster HTTP target, creates & resumes an Actor, then asserts:

- **positive** — a real Actor's egress reaches the target (`HTTP 200`) *through the gateway*
  (the target sees the gateway's IP as its client), and the gateway logs the CONNECT against the
  actor's certificate SAN;
- **negative** — a pod holding a valid *pod* identity but no actor certificate cannot open a
  tunnel at all: the gateway's `trusted_ca` is the actor-identity CA, so the mTLS handshake is
  refused before any CONNECT is answered.

Add `--cleanup` to remove everything the script created.

## Manual walkthrough

```bash
# 1. An in-cluster target the Actor will fetch (any HTTP server works).
kubectl create namespace egress-target
kubectl -n egress-target create deployment whoami --image=traefik/whoami
kubectl -n egress-target expose deployment whoami --port=80
TARGET_IP=$(kubectl -n egress-target get svc whoami -o jsonpath='{.spec.clusterIP}')

# 2. Create and resume an Actor.
kubectl ate create atespace demo
kubectl ate create actor egress-demo -a demo --template ate-demo-egress/egress
kubectl ate resume actor egress-demo -a demo   # wait for ACTOR_STATE_RUNNING

# 3. Drive the Actor's egress through the ingress gateway.
kubectl -n ate-system port-forward service/atenet-router 8000:80 &
curl -s -X POST http://localhost:8000/ \
  -H 'Host: egress-demo.demo.actors.resources.substrate.ate.dev' \
  -H 'Content-Type: application/json' \
  -d "{\"url\":\"http://${TARGET_IP}:80/\"}"
```

### What to observe

```bash
# The egress gateway logs each tunneled CONNECT against the verified peer certificate:
kubectl -n ate-system logs deploy/atenet-egress | grep '\[egress\]'
#   [egress] authority=<TARGET_IP>:80 peer_san=spiffe://substrate-actor.local/atespace/demo/actor/egress-demo … code=200 …

# The co-located ext_proc sidecar logs the identity decision, including the UID it authorized on:
kubectl -n ate-system logs deploy/atenet-egress -c ext-proc | grep -i 'egress identity\|egress denied'
#   egress identity authenticated  atespace=demo actor=egress-demo actorUid=… destination=<TARGET_IP>:80
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

`internal/e2e/suites/networking` drives this endpoint against the `grpcecho` fixture
(`internal/e2e/fixtures/grpcecho`, deployed by `e2e.DeployServerPod` from the shared server-pod
manifest) in `TestActorEgressGRPC`; the same fixture, or any h2c gRPC server reachable from the
cluster, works for a manual run.

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
