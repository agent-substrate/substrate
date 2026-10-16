# atenet

atenet is a combined daemon for all networking functionality.

* Envoy control plane for programming ATE resolution. `atenet router`
* SDS server that mints the egress gateway's leaf certificates. `atenet sdsmint`

This is built as a single binary for convenience in the prototyping.

## Cluster deployment

### router

(Note: this deployment model combines Envoy dataplane with the router. This will
likely be split in the future for better scalability.)

* `atenet router` will be deployed as Deployment and Service
* Deployment will contain:
  * Envoy
  * atenet router
* Service will expose:
  * Envoy port 80 and 443 for ordinary ingress traffic, plus a dedicated
    CONNECT port (8081, or 8444 for TLS) for arbitrary-port ingress: a client
    reaches a port on the actor other than its default (80) with an HTTP
    `CONNECT` request naming the port in the authority. Only HTTP(S) traffic
    over the tunnel is supported today, not raw TCP.
* Upstream: Envoy reaches the actor's in-worker `atunnel` at the address
  `atenet router`'s ext_proc resolves into dynamic metadata rather than a
  header. By default (`--upstream-connect`) it opens an mTLS CONNECT tunnel to
  the worker pod's port 8443, and the port to reach on the actor rides in the
  CONNECT authority. With `--upstream-connect=false` it dials `atunnel`'s
  ingress server on port 443 over mTLS instead, and the port travels as the
  `TargetPortHeader` header (`internal/atunnel/ingress`).
  `ate-target-actor: <atespace>/<actor>` identifies the Actor independently of
  `:authority`/`Host`.
* Termination: the router drains gracefully on SIGTERM (readiness flip →
  endpoint propagation → Envoy admin-API drain → ext_proc drain), and the
  Envoy container's `preStop` hook waits for the router's drain-complete
  marker on a pod-shared emptyDir — so established connections and parked
  requests finish instead of resetting. The whole sequence must fit within
  `terminationGracePeriodSeconds` (see the manifest comments). An upgrade
  rolls the router as an ordinary Deployment update (see
  [docs/upgrade.md](../../docs/upgrade.md)). The drain lets each old pod's
  requests finish, except one still open about 15 seconds after the drain
  starts, which is cut.

RBAC permissions:
* get, list, watch on ate-system EndpointSlices

## testing

Run the package tests with `go test ./cmd/atenet/...`. Cluster e2e
tests use the shared `hack/run-e2e.sh` runner.
