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
* Authorizes egress against the actor's `EgressPolicy`: the address rules once
  per connection, at the CONNECT; the hostname rules on every request on the
  legs the gateway can read. An actor with no policy gets no tunnel. See
  [egress legs](#egress-legs).
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
  CONNECTs, and `EgressPolicy` enforcement on the legs inside the tunnel.

Direction is decided by the filter chain the dataplane says accepted the
request (`xds.filter_chain_name`, an Envoy attribute the egress gateway is
configured to send), never by anything in the request itself, so a client
cannot pick the egress path by crafting one. `router` itself does the wiring.

## egress legs

The egress gateway calls the same ext_proc sidecar from two Envoy filter
chains on the plain gateway and three on the sdsmint gateway, and the chain name (`xds.filter_chain_name`) tells the handler which
leg it is on. Each leg decides the rules whose input it can see:

| Leg (filter chain) | Where | Sees | Decides | Rules that can match |
| --- | --- | --- | --- | --- |
| `egress` | outer CONNECT, both gateways | actor certificate, original `IP:port` | per TCP connection | identity; `ip_blocks` and `all` against the original destination |
| `egress_cleartext` | HTTP the actor sent in the clear, both gateways | `Host`, method, path, headers | **every request** | `hostnames` (DNS `Host`), `ip_blocks` (IP-literal `Host`), `all` |
| `egress_tls_mitm` | TLS the sdsmint gateway terminated | same as cleartext | **every request** | same as cleartext |

Every leg polices what is dialed. The CONNECT leg answers with dynamic
metadata (`dev.ate.egress:passthrough_destination`, see
`extproc/attributes.go`): the original destination when an address rule
allowed it, absent otherwise. The outer chain copies it into the ORIGINAL_DST
filter state it shares with the inner listener, and the inner listener's
passthrough chains -- TLS the plain gateway does not terminate, and anything
neither inspector could classify -- dial that address and nothing else; with
no address to dial the connection is closed before a byte is relayed. A
CONNECT whose destination no address rule allows still opens when the policy
has `hostnames` rules, because a request inside may be allowed by name; it is
refused outright when the policy has none, and on a dataplane that calls out
for the CONNECT alone (no chain name), which has no request leg to defer to.

The request legs authorize the request's own `Host`, because that is the name
`dynamic_forward_proxy` resolves and dials: an `ip_blocks` rule for the
tunnel's original destination would otherwise let an actor dial an allowed
address and send `Host: attacker.example`. This is why HTTPS needs an address
rule on the plain gateway and a name rule on the sdsmint gateway.

Identity on the request legs is `dev.ate.actor.identity`, the actor's SPIFFE
ID that the outer chain set from the verified peer certificate and shares with
the inner listener. Because Envoy keys its connection pools without string
filter-state objects, the outer chain also sets
`envoy.network.upstream_server_name` from the same certificate — not shared
upstream — so the inner hop's pool is per actor and two actors dialing the same
address never inherit each other's identity. Nothing inside the tunnel can
write any of this; a callout without an identity is refused.

Policies are read through a per-actor cache (`--egress-policy-cache-ttl`, 10s
by default; 0 disables it). The TTL is exactly how stale a decision can be: a
create, update or delete is visible to new requests within one TTL, and a
deleted policy becomes a deny. Every policy denial answers a fixed
`egress denied` body; the reason is in the sidecar's log.

Credential injection (`inject_static_headers`) is not implemented yet: a
matched rule that declares one is denied with 501 rather than forwarded
without the credential the policy promised.

## adding a dataplane attribute

The filter-state objects and request attributes a proxy carries alongside
a request are declared once, in `extproc/attributes.go`.

| key | direction | purpose |
| --- | --- | --- |
| `dev.ate.actor.name` | ingress | carries the actor name across CONNECT re-entry |
| `dev.ate.actor.atespace` | ingress | carries the atespace across CONNECT re-entry |
| `dev.ate.connect.authority` | ingress | carries the outer CONNECT authority across re-entry for target-port selection |
| `dev.ate.actor.identity` | egress | carries the authenticated actor identity for logs and additional ext_proc services |
| `dev.ate.extproc.direction` | egress | selects the egress handler for dataplanes without Envoy filter chains |

### name it

A new key substrate owns is rooted at `dev.ate.`, then a dotted path naming the
thing it carries: `dev.ate.<area>.<thing>`, or `dev.ate.<thing>` when there is
no area to disambiguate. These keys live in a namespace owned by the proxy that
carries them — Envoy filter state, agentgateway CEL — and shared with whatever
else a deployment configures alongside substrate, so the reverse-DNS root is
what keeps them from colliding.

A key someone else owns keeps *their* reverse-DNS root; do not re-home it under
`dev.ate.` to make the set look uniform. `xds.filter_chain_name` is Envoy's own
attribute and stays in Envoy's namespace. A key defined by a vendor's extension
or by an additional ext_proc service a deployment splices in belongs under that
vendor's own root, on the same reasoning that gives substrate `dev.ate.`. The
prefix is a claim about who may rename the key, so getting it wrong means
substrate is renaming something it does not own, or holding a name it never
reserved.

### wire it up

1. **Declare the constant** in `extproc/attributes.go`.
2. **Set it in the dataplane.**
3. **Ask for it.** ext_proc only receives the attributes named in its filter's
   `request_attributes`. A key that is set but not requested arrives as nothing
   at all, which reads the same as a key that was never set.
4. **Read it** through `RequestMetadata.Attribute`.

### keep it trustworthy

An attribute is only as trustworthy as its source. The actor target carries
the client-selected actor reference, while the CONNECT authority carries
only the requested target port across tunnel re-entry. Security-sensitive
attributes, such as actor identity, must come from dataplane-authenticated
state rather than a client header.

## modes

One binary serves both directions. `--mode` selects which:

| `--mode` | ext_proc handlers | xDS server | Kubernetes access |
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
