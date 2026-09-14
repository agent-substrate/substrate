# MITM egress tests on Kind

This runbook sets up a dedicated Kind cluster with the Envoy `sdsmint` egress
gateway, deploys the MITM egress fixtures, and runs the trust and networking
E2E tests. The gateway swap is cluster-wide, so do not share this cluster with
passthrough egress tests. Run the gVisor and micro-VM lanes one after the other.

## Prerequisites

Complete the [development quickstart](../../README.md#quickstart-development)
for Go, Docker, `kubectl`, and the repository-managed Kind tooling. The Kind
cluster script also creates the local `kind-registry` on port 5001; Docker must
be able to run that registry and the Kind node.

The default lane uses gVisor and does not require KVM. The optional micro-VM
lane needs `/dev/kvm` available to Docker and the host preparation described in
[Running the microVM runtime locally](microvm-local.md). Native macOS does not
provide the required micro-VM execution by itself; use a KVM-capable Linux host
or the documented Lima nested-virtualization setup. If Kind reports that KVM is
unavailable, gVisor still works but the micro-VM lane cannot run.

Commands below assume the repository root and the default cluster name. Set
`KIND_CLUSTER_NAME` and use the corresponding `kind-${KIND_CLUSTER_NAME}`
context if you choose another name.

## Create and install the cluster

Create a fresh Kind cluster and its local registry. This also enables the
ClusterTrustBundle feature gates required by the projected egress trust bundle.

```bash
./hack/create-kind-cluster.sh
kubectl config use-context kind-kind
```

Install the control plane and its Kind-local registry/object-store settings:

```bash
./hack/install-ate-kind.sh --deploy-ate-system
```

For the optional micro-VM lane, install the cluster-wide assets and
`microvm` `SandboxConfig` before deploying the micro-VM fixture:

```bash
NO_DEV_ENV=true ATE_INSTALL_KIND=true KUBECTL_CONTEXT=kind-kind \
  ./hack/install-microvm-deps.sh --install
```

This step assembles or reuses the architecture-specific assets and stages them
in the Kind-local RustFS bucket. It requires the KVM-capable environment above.

## Enable sdsmint and deploy fixtures

Replace the passthrough gateway with the Envoy MITM gateway:

```bash
./hack/install-ate-kind.sh --deploy-atenet --experimental-use-sdsmint
```

This creates the gateway CA and changes the egress gateway cluster-wide.
`--deploy-demo-egress-mitm` deploys the actor fixture that consumes that CA; it
does not enable the gateway. Deploy both fixture variants when running both
lanes:

```bash
./hack/install-ate-kind.sh --deploy-demo-egress-mitm
./hack/install-ate-kind.sh --deploy-demo-egress-microvm-mitm
```

The second command requires the micro-VM dependencies from the previous
section. The fixture templates project the `egress-mitm.ate.dev` bundle and set
both `SSL_CERT_FILE` and `SSL_CERT_DIR`; see [the trust-bundle guide](../egress-trust-bundle.md)
for the actor-side trust model.

Before the networking tests, add the Kind service-DNS CA to the gateway's
upstream public roots and wait for the gateway rollout:

```bash
./hack/setup-e2e-egress-tls-kind.sh
```

The setup script keeps public roots and adds the service-DNS CA. The test
origin uses that CA and Service DNS names for SNI, so TLS verification remains
enabled on both the actor-to-gateway and gateway-to-origin connections.

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

`TestActorEgressMITMTrust` has two assertions: the `bundle` request to
`https://example.com/` succeeds using only the projected gateway CA, while the
`system` request fails with a certificate or x509 error. Together they prove
that the actor trusts the sdsmint per-SNI leaf through the projected bundle and
that the traffic is actually intercepted rather than passed through to a
publicly trusted origin.

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

The three protocol-focused additions are `TestActorEgressHTTPSNonStandardPort`,
`TestActorEgressWebSocket`, and `TestActorEgressSecureWebSocket`. The
`^TestActorEgress` selection also includes the existing HTTP, HTTPS,
non-standard-port, and gRPC cases. The
protocol cases create private origins and, in MITM mode, use the service-DNS
CA installed by `hack/setup-e2e-egress-tls-kind.sh`; the actor uses the
projected gateway CA. The broad selection is the repository's existing check
for all of these egress paths.

## Expected results and troubleshooting

The install and fixture commands wait for their resources, and the TLS setup
waits for the `atenet-egress` rollout. Before testing, check the relevant
objects if a command appears stuck:

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

An actor-side `certificate` or `x509` error usually means the projected
`egress-mitm.ate.dev` bundle is missing or the fixture is running without the
sdsmint gateway. A gateway-to-origin certificate error usually means the TLS
setup script did not complete or the origin is not using its Service DNS name.
If a micro-VM HTTPS or WSS case reports a certificate that is not yet valid,
check host and guest clocks; a guest clock behind the freshly issued
certificate's `NotBefore` time can produce that symptom.

The MITM trust suite and all three new protocol tests have passed on gVisor
with this setup. The micro-VM lane has not been validated locally.
