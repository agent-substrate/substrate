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
  │  atunnel egress  ──(mTLS + HTTP CONNECT,            │
  │        │            X-Ate-Atespace/Actor/Version)   │
  └────────┼────────────────────────────────────────────┘
           ▼
  ┌──────────── ateway-egress pod ───────────────────┐
  │  Envoy egress gateway                              │
  │    • terminates downstream mTLS                    │
  │    • terminates HTTP CONNECT                       │
  │    • ext_proc ──(localhost)──►  atenet router (ext_proc sidecar)
  │    • dynamic_forward_proxy               │  GetActor(atespace, actor) → ate API
  │           │                              │  allow RUNNING actor / deny 403
  └───────────┼───────────────────────────────────────┘
              ▼
     real destination (the CONNECT authority, an IP:port)
```

1. **Guide 1 — gateway accepts CONNECT + mTLS.** `ateway-egress` is an Envoy dynamic-forward-proxy
   that terminates the actor's mTLS `CONNECT` and tunnels to the requested destination.
2. **Guide 2 — transparent interception.** `nftables` REDIRECTs actor TCP egress into `atunnel`,
   which wraps it in mTLS + `CONNECT`.
3. **Guide 3 — HTTP-only actors, identity injected.** The Actor only dials plain HTTP; atunnel
   still injects `X-Ate-Atespace`/`X-Ate-Actor`/`X-Ate-Actor-Version` identity headers.
4. **Identity authentication.** The gateway calls `ext_proc` on every CONNECT; the **atenet router**
   (co-located in the gateway pod as an ext_proc sidecar, the same ext_proc code used for ingress)
   validates the asserted identity against the ate API (`GetActor`) and returns **403** unless it is
   a real, `RUNNING` actor. This mirrors the ingress gateway's Envoy + ext_proc co-location; a
   standalone/shared ext_proc is a future step.

## Components

- **Egress app (`main.go`)** — the Actor: `POST /` with `{"url":"..."}` → fetches it → returns
  status + body.
- **Egress gateway** — `manifests/ate-install/ateway-egress.yaml`. One pod, two containers:
  an Envoy (`envoy`) and the atenet router ext_proc (`ext-proc`), called over localhost.
- **Egress opt-in** — `atelet --egress-gateway-address=ateway-egress.ate-system.svc:443`
  (set in the kind overlay), which turns on tunneled egress cluster-wide.

## Prerequisites

- A kind cluster with Agent Substrate installed (`hack/create-kind-cluster.sh` then
  `hack/install-ate-kind.sh --deploy-ate-system`). Egress is enabled by the atelet flag above.
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
  (the target sees the gateway's IP as its client), and the gateway logs the CONNECT with the
  actor identity;
- **negative** — a spoofed/unknown actor identity is rejected by ext_proc with **`HTTP 403`**.

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
kubectl ate resume actor egress-demo -a demo   # wait for STATUS_RUNNING

# 3. Drive the Actor's egress through the ingress gateway.
kubectl -n ate-system port-forward service/atenet-router 8000:80 &
curl -s -X POST http://localhost:8000/ \
  -H 'Host: egress-demo.demo.actors.resources.substrate.ate.dev' \
  -H 'Content-Type: application/json' \
  -d "{\"url\":\"http://${TARGET_IP}:80/\"}"
```

### What to observe

```bash
# The egress gateway logs each tunneled CONNECT with the actor identity + result:
kubectl -n ate-system logs deploy/ateway-egress | grep '\[egress\]'
#   [egress] authority=<TARGET_IP>:80 atespace=demo actor=egress-demo ver=… code=200 …

# The co-located ext_proc sidecar logs the identity decision:
kubectl -n ate-system logs deploy/ateway-egress -c ext-proc | grep -i 'egress identity\|egress denied'
#   egress identity authenticated  atespace=demo actor=egress-demo status=STATUS_RUNNING
```

The `whoami` body shows `RemoteAddr: <ateway-egress pod IP>` — proof the request egressed
*through* the gateway rather than directly.

## Notes / limitations

- This milestone **authenticates** identity (is this a real, running actor?). **Authorizing**
  egress by destination and injecting upstream credentials/tokens is a follow-up, implemented in
  the same `ext_proc` (policy API TBD).
- The gateway currently trusts any pod-identity holder's *asserted* `X-Ate-*` headers; binding the
  mTLS worker identity to the actor's assigned worker is the next hardening step.

## Cleanup

```bash
./demos/egress/test-egress.sh --cleanup
./hack/install-ate.sh --delete-demo-egress
```
