# router

Router has several responsibilities:

* Serves Envoy xDS configuration when `--atenet-router=envoy` (the default).
  Unless `--standalone` is set, it also manages the Envoy Deployment and
  Services in Kubernetes.
  With `--atenet-router=agentgateway`, the sidecar uses a static ConfigMap and
  atenet does not start an xDS server.
* ext_proc server for the proxy. To make the deployment and debugging easier, we will run this component together
  with the router, but this will be split later into its own component.
  * ext_proc will call into the ATE gRPC API to get the set of relevant backends (specific the worker IP) and
    route the traffic accordingly
  * Make sure the interface with ATE API is pluggable so that we can test with a mock ATE API.
* Runs an xDS server for the Envoy deployment that defines the Cluster information for the ATEs.
  * the xDS configuration will configure Envoy to send traffic to ext_proc
* Watches the ActorTemplates to get out the definitions for how to route the actor IDs.
* Parks requests whose actor cannot be served immediately due to transient
  worker-pool saturation, retrying the resume until the actor is routable or a
  bounded wait elapses, instead of failing fast. See
  [docs/request-parking.md](../../../../../docs/request-parking.md).
* Drains gracefully on SIGTERM: flips `/readyz` so the Service stops sending
  new connections, waits out endpoint propagation (`--drain-delay`), drives
  Envoy's admin API to drain established connections, gracefully stops the
  ext_proc server so parked requests finish normally (`--drain-timeout`,
  derived from the parking budget), then writes a drain-complete marker that
  releases the Envoy container's `preStop` hook. See `drain.go` and
  `envoydrain.go`.
* Authenticates actor identity on egress: on every CONNECT, the egress Envoy's
  ext_proc handler re-verifies the actor's client certificate against the
  actor-identity CA, reads the `ActorIdentity` X.509 extension out of it, and
  checks the certified UID against the ATE API.

## packages

The ext_proc server handles both traffic directions, and they apply opposite
trust models — egress derives the actor identity from a client certificate
Envoy verified against the actor-identity CA, ingress treats every request
header as unauthenticated client input — so the two are kept in separate
packages that cannot reach into each other:

* `extproc` — the mux, and nothing else. It terminates the ext_proc stream,
  decides which direction a request arrived on, dispatches to the `Handler`
  registered for that direction, and records latency and outcome. It also owns
  the vocabulary both handlers share (`RequestMetadata`, `Result`, `ReqError`).
  It imports neither handler package.
* `ingress` — resume, park, and route to the actor's worker.
* `egress` — certificate-based actor-identity authentication for outbound
  CONNECTs.

Direction is decided by the Envoy filter chain that accepted the request
(`xds.filter_chain_name`), never by anything in the request itself, so a client
cannot pick the egress path by crafting one. `router` itself does the wiring.

## modes

One binary serves both directions. `--mode` selects which:

| `--mode` | ext_proc handlers | xDS server + ActorTemplate controller | Kubernetes access |
| --- | --- | --- | --- |
| `ingress` | ingress | yes | yes |
| `egress` | egress | no | none |
| `all` (default) | both | yes | yes |

The mux refuses a direction this instance was not started to serve (404) rather
than falling back to the other handler, which would run the request through the
wrong trust model.

Ingress and egress are deployed separately today — `atenet-router` fronts the
ingress Envoy, `atenet-egress` the egress one — because the two scale
independently, not because they need separate binaries.

## status page

Serve a `/statusz` page on port 8080.

Contents:

* Global flags values
* Command line args
* Last 100 queries served
* Build tag
