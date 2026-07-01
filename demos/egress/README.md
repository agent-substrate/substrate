# Egress Demo

This demo deploys a small HTTP actor that opens outbound HTTP or HTTPS requests
from inside the actor sandbox. It is intended for validating actor egress
capture and agentgateway routing.

## Run

Install the CLI as a kubectl plugin if needed:

```bash
go install ./cmd/kubectl-ate
export PATH="$(go env GOPATH)/bin:${PATH}"
```

Deploy the ATE system with egress capture enabled, then deploy the demo:

```bash
./hack/install-ate.sh --egress --deploy-ate-system
./hack/install-ate.sh --deploy-demo-egress
```

For kind, use the kind wrapper for both commands:

```bash
./hack/install-ate-kind.sh --egress --deploy-ate-system
./hack/install-ate-kind.sh --deploy-demo-egress
```

Create an actor and port-forward the router:

```bash
kubectl ate create actor my-egress-1 --template ate-demo-egress/egress
kubectl port-forward -n ate-system svc/atenet-router 8000:80
```

Call the actor:

```bash
curl -X POST \
  -H "Host: my-egress-1.actors.resources.substrate.ate.dev" \
  http://localhost:8000
```

The default target is `https://httpbin.org/get`. To call another httpbin path:

```bash
curl -X POST \
  -H "Host: my-egress-1.actors.resources.substrate.ate.dev" \
  --data-urlencode "url=https://httpbin.org/headers" \
  "http://localhost:8000"
```

The demo agentgateway setup only routes `httpbin.org:443`.

## Uninstall

```bash
kubectl ate suspend actor my-egress-1
kubectl ate delete actor my-egress-1
./hack/install-ate.sh --delete-demo-egress
```

For kind:

```bash
./hack/install-ate-kind.sh --delete-demo-egress
```
