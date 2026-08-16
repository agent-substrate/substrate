# router

Router has several responsibilities:

* Serves Envoy xDS configuration when `--atenet-router=envoy` (the default).
  With `--atenet-router=agentgateway`, the sidecar uses a static ConfigMap and
  atenet does not start an xDS server.
* ext_proc server for the dataplane. To make the deployment and debugging easier, we will run this component together
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
  new connections, waits out endpoint propagation (`--drain-delay`), drains the
  dataplane's established connections (Envoy only — driven over its admin API;
  agentgateway manages its own termination), gracefully stops the ext_proc
  server so parked requests finish normally (`--drain-timeout`, derived from
  the parking budget), then writes a drain-complete marker that releases the
  dataplane container's `preStop` hook. See `drain.go` and `envoydrain.go`.
* Authenticates actor identity on egress: on every CONNECT, the egress
  gateway's ext_proc handler re-verifies the actor's client certificate against
  the actor-identity CA, reads the `ActorIdentity` X.509 extension out of it,
  and checks the certified UID against the ATE API.
* Serves arbitrary-port ingress: a client reaches a port on the actor other
  than its default (80) by sending an HTTP CONNECT to
  `<actor-dns>:<port>` on `--port-connect`/`--port-connect-tls`, rather than
  naming the port some other way. Envoy terminates the CONNECT and reinjects
  the tunneled bytes into an internal listener that runs the same ext_proc
  path as ordinary traffic, so each request inside a long-lived tunnel still
  resumes the actor and re-routes independently if it moves workers. Only
  HTTP(S) traffic over the tunnel is supported today -- see `xds.go`'s
  `connect_terminate`/`main_internal` listeners and
  `ingress.Handler.HandleRequestHeaders`.

## packages

The ext_proc server handles both traffic directions, and they apply opposite
trust models — egress derives the actor identity from a client certificate the
gateway verified against the actor-identity CA, ingress treats every request
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

Direction is decided by the filter chain the dataplane says accepted the
request (`xds.filter_chain_name`, an Envoy attribute the egress gateway is
configured to send), never by anything in the request itself, so a client
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
ingress dataplane, `atenet-egress` the egress gateway — because the two scale
independently, not because they need separate binaries.

`--atenet-router` selects the dataplane for both Deployments. Each gateway has
its own static configuration because ingress and egress scale independently.

## status page

Serve a `/statusz` page on port 8080.

Contents:

* Global flags values
* Command line args
* Last 100 queries served
* Build tag
