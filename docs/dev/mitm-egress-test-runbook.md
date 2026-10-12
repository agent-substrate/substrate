# Egress protocol tests on Kind

The unified egress gateway intercepts TLS for HTTPS policy rules. It presents
an egress CA certificate to the Actor and verifies the origin certificate on
its upstream connection. The E2E networking tests use local origins signed by
the cluster service-DNS CA, so the gateway must trust that CA as well as its
normal public roots.

The CI workflow installs both egress ActorTemplates, adds the service-DNS CA
to the gateway's upstream roots with `hack/setup-e2e-egress-tls-kind.sh`, and
runs the networking suite on gVisor and micro-VM. The commands below reproduce
that setup locally.

## Prerequisites

Follow the [development quickstart](../../README.md#quickstart-development)
for Go, Docker, `kubectl`, and Kind. The micro-VM lane also needs KVM and the
setup in [Running the microVM runtime locally](microvm-local.md).

## Install the cluster and fixtures

```bash
./hack/create-kind-cluster.sh
kubectl config use-context kind-kind
ATE_CREDENTIAL_PROVIDER='{"name":"k8s.io"}' ./hack/install-ate-kind.sh --deploy-ate-system
```

For micro-VM, install its assets before deploying the fixture:

```bash
NO_DEV_ENV=true ATE_INSTALL_KIND=true KUBECTL_CONTEXT=kind-kind \
  ./hack/install-microvm-deps.sh --install
```

Deploy both egress ActorTemplates and add local-origin trust to the current
gateway:

```bash
ATE_CREDENTIAL_PROVIDER='{"name":"k8s.io"}' ./hack/install-ate-kind.sh --deploy-demo-egress
ATE_CREDENTIAL_PROVIDER='{"name":"k8s.io"}' ./hack/install-ate-kind.sh --deploy-demo-egress-microvm
./hack/setup-e2e-egress-tls-kind.sh
```

The trust setup retains public roots, adds the service-DNS CA, and waits for
the egress deployment to roll out. The test origins use their Service DNS name
for SNI. The Actor verifies the gateway's minted certificate through its
projected egress trust bundle.

## Run the networking tests

Run the gVisor lane:

```bash
hack/run-e2e-kind.sh ./internal/e2e/suites/networking -run '^TestActorEgress' -v -args --no-color
```

Run the micro-VM lane sequentially on the same cluster:

```bash
E2E_SANDBOX_CLASS=microvm hack/run-e2e-kind.sh ./internal/e2e/suites/networking -run '^TestActorEgress|^TestMicroVMClockAfterDelayedRestore' -v -args --no-color
```

The HTTPS protocol test checks a verified HTTP/1.1 request to a local TLS
origin on port 8443. The WebSocket tests check that both `ws` and `wss`
upgrades are denied with HTTP 403 before the origin receives any messages.
The delayed-restore test issues a fresh origin certificate after the Actor is
suspended, then checks that the restored micro-VM can complete the TLS request.

## Troubleshooting

```bash
kubectl --context kind-kind get pods -n ate-system -o wide
kubectl --context kind-kind get workerpools -A
kubectl --context kind-kind get pods -A -l ate.dev/worker-pool -o wide
kubectl --context kind-kind -n ate-system logs deploy/atenet-egress -c envoy --tail=300
```

For AgentGateway, use `-c agentgateway` for the last command. For a local TLS
failure, check that `setup-e2e-egress-tls-kind.sh` completed and that the test
URL uses the origin's Service DNS name.
