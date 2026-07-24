# atenet

atenet currently runs the DNS server for ATE Actor resolution: `atenet dns`.

### dns

* `atenet dns` will be deployed as:
  * Deployment
  * Service exposing tcp and udp 53

* read, list on kube-system services
* read, list on ate-system services

## testing

Run the package tests with `go test ./cmd/atenet/...`.
