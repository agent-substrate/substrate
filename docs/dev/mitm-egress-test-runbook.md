# MITM egress tests on Kind

Under sdsmint, the egress gateway terminates the TLS connection inside an
actor's CONNECT tunnel and opens another to the origin. The actor sees a leaf
minted by the gateway, so it must trust the gateway CA. The gateway must also
trust the origin's CA; the local test origins use the cluster's service-DNS CA.

The commands below install that setup on Kind and run the trust and protocol
tests. Use a dedicated cluster: selecting sdsmint replaces the gateway for every
actor, including actors whose tests expect passthrough. Run the gVisor and
micro-VM lanes sequentially.

## Prerequisites

Install the tools in the [development quickstart](../../README.md#quickstart-development):
Go, Docker, `kubectl`, and the repository-managed Kind tooling. The cluster
script starts both the Kind node and a `kind-registry` container on port 5001.

gVisor does not need KVM. The optional micro-VM lane does: Docker must have
access to `/dev/kvm`, and the host needs the preparation in
[Running the microVM runtime locally](microvm-local.md). On macOS, use the
Lima nested-virtualization setup described there or a KVM-capable Linux host.
Without KVM, run only the gVisor commands.

Commands below assume the repository root and the default cluster name. Set
`KIND_CLUSTER_NAME` and use the corresponding `kind-${KIND_CLUSTER_NAME}`
context if you choose another name.

## Create and install the cluster

Create the cluster and local registry. The script enables the
ClusterTrustBundle feature gates; without them, the actor cannot receive the
gateway trust bundle.

```bash
./hack/create-kind-cluster.sh
kubectl config use-context kind-kind
```

Install Substrate with the Kind registry and object-store settings:

```bash
./hack/install-ate-kind.sh --deploy-ate-system
```

For micro-VM, install the assets and `microvm` `SandboxConfig` before its
fixture:

```bash
NO_DEV_ENV=true ATE_INSTALL_KIND=true KUBECTL_CONTEXT=kind-kind \
  ./hack/install-microvm-deps.sh --install
```

The script assembles or reuses the assets for the host architecture and stages
them in the Kind-local RustFS bucket.

## Enable sdsmint and deploy fixtures

Replace the passthrough gateway with the Envoy MITM gateway:

```bash
./hack/install-ate-kind.sh --deploy-atenet --experimental-use-sdsmint
```

The flag selects the sdsmint manifest and creates the gateway CA. The demo
flags below deploy actors that trust that CA; they do not select a gateway.
Deploy the gVisor fixture, and the micro-VM fixture if that runtime is available:

```bash
./hack/install-ate-kind.sh --deploy-demo-egress-mitm
./hack/install-ate-kind.sh --deploy-demo-egress-microvm-mitm
```

The templates project `egress-mitm.ate.dev` and set both `SSL_CERT_FILE` and
`SSL_CERT_DIR`. Together those variables make the gateway CA the actor's only
trust anchor. See [the trust-bundle guide](../egress-trust-bundle.md).

The gateway's upstream TLS client trusts public roots by default. The test
origins have service-DNS certificates, which do not chain to those roots. Add
the service-DNS CA before running the networking tests:

```bash
./hack/setup-e2e-egress-tls-kind.sh
```

The script retains public roots, adds the service-DNS CA, and waits for the
gateway rollout. The tests dial Service DNS names so the ClientHello carries
SNI. Neither TLS connection disables certificate verification.

## Run the tests

Run each mode sequentially from the repository root. `hack/run-e2e-kind.sh`
sets the Kind context, local image repository, and snapshot bucket expected by
the installation.

First run the trust suite on gVisor:

```bash
E2E_EGRESS_MITM=1 hack/run-e2e-kind.sh \
  ./internal/e2e/suites/egressmitm -v -args --no-color
```

Then run it on the micro-VM runtime:

```bash
E2E_EGRESS_MITM=1 E2E_SANDBOX_CLASS=microvm hack/run-e2e-kind.sh \
  ./internal/e2e/suites/egressmitm -v -args --no-color
```

`TestActorEgressMITMTrust` sends two requests to `https://example.com/`.
The `bundle` request must succeed with only the projected gateway CA; the
`system` request must fail certificate verification. A successful request alone
would not prove interception, because a passthrough connection could validate
the origin against public roots. The negative control rules that out.

Run the networking protocol tests on gVisor:

```bash
E2E_EGRESS_MITM=1 hack/run-e2e-kind.sh \
  ./internal/e2e/suites/networking -run '^TestActorEgress' -v -args --no-color
```

Run the same tests on micro-VM:

```bash
E2E_EGRESS_MITM=1 E2E_SANDBOX_CLASS=microvm hack/run-e2e-kind.sh \
  ./internal/e2e/suites/networking -run '^TestActorEgress' -v -args --no-color
```

`TestActorEgressHTTPSNonStandardPort` checks verified HTTPS on port 8443.
`TestActorEgressWebSocket` and `TestActorEgressSecureWebSocket` check that the
HTTP/1.1 WebSocket upgrade is denied with an inner 403 and handshake error,
without echoed messages.
The `^TestActorEgress` selection includes the existing HTTP, HTTPS,
non-standard-port, and gRPC tests as well.

## Expected results and troubleshooting

If setup does not become ready, inspect the control plane and workers:

```bash
kubectl --context kind-kind get pods -n ate-system -o wide
kubectl --context kind-kind get workerpools -A
kubectl --context kind-kind get pods -A -l ate.dev/worker-pool -o wide
```

For gateway details, inspect the Envoy and init-container logs:

```bash
kubectl --context kind-kind -n ate-system logs deploy/atenet-egress -c envoy --tail=300
kubectl --context kind-kind -n ate-system logs deploy/atenet-egress -c e2e-egress-upstream-roots --tail=100
```

For certificate errors, first identify which TLS connection failed. On the
actor side, check the projected bundle and that the gateway is running sdsmint.
On the upstream side, check that the TLS setup completed and the request uses
the origin's Service DNS name.

A certificate that is "not yet valid" needs a clock check before a trust
change. The test issues a fresh origin certificate; a restored micro-VM whose
clock is behind its `NotBefore` time rejects it even with the right CA.

The MITM trust suite and all three new protocol tests have passed on gVisor
with this setup. The micro-VM lane has not been validated locally.
