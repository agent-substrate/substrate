# atenet

atenet is a combined daemon for all networking functionality.

* DNS server for ATE Actor resolution: `atenet dns`
* Envoy control plane for programming ATE resolution. `atenet router`

This is built as a single binary for convenience in the prototyping.

## Cluster deployment

![atenet diagram](atenet-diagram.png)

### router

(Note: this deployment model combines Envoy dataplane with the router. This will
likely be split in the future for better scalability.)

* `atenet router` will be deployed as Deployment and Service
* Deployment will contain:
  * Envoy
  * atenet router
* Service will expose:
  * Envoy port 80 and 443.

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
