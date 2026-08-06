# atenet

atenet is a combined daemon for all networking functionality.

* DNS server for ATE Actor resolution: `atenet dns`
* Envoy control plane for programming ATE resolution. `atenet router`

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
  * Envoy port 80 and 443.
* Upstream: Envoy's `ORIGINAL_DST` actor cluster dials the actor's in-worker
  `atunnel` ingress server on the worker pod's port 443 over mTLS, using the
  address `atenet router`'s ext_proc resolved into `x-ate-original-dst`.
* Termination: the router drains gracefully on SIGTERM (readiness flip →
  endpoint propagation → Envoy admin-API drain → ext_proc drain), and the
  Envoy container's `preStop` hook waits for the router's drain-complete
  marker on a pod-shared emptyDir — so established connections and parked
  requests finish instead of resetting. The whole sequence must fit within
  `terminationGracePeriodSeconds` (see the manifest comments). Upgrades are
  whole-system swaps (#473) rather than per-Deployment rolling updates; the
  drain is what makes the old system's termination lossless.

RBAC permissions:
* read, list on ActorTemplate

### dns

* `atenet dns` will be deployed as:
  * Deployment
  * Service exposing tcp and udp 53

* read, list on kube-system services
* read, list on ate-system services

## testing

Run the package tests with `go test ./cmd/atenet/...`. Cluster e2e
tests use the shared `hack/run-e2e.sh` runner.
