# E2E testing

```shell
$ source .ate-dev-env.sh
$ go test -v ./internal/e2e/suites/... -args --e2e
```

## Principles

* Keep it simple -- use go test for the harness.
* e2e tests live under `internal/e2e/suites/<suite>`
* Each suite should implement TestMain using e2e.RunTestMain()
  * e2e tests will be skipped for ordinary unit tests unless the `--e2e` flag
    is set e.g. `go test ./internal/e2e/suites/... -args --e2e`
* Helper libraries live under `internal/e2e`
* Setup and Teardown are on a per-component basis and the component's
  author's responsibility.

## Preconditions

The e2e tests assume you have a cluster set up with Agent Substrate installed,
for example via `hack/install-ate.sh --deploy-ate-system` or
`hack/install-ate-kind.sh --deploy-ate-system`.

## Sandbox classes

The suites are runtime-agnostic: the same tests run against gVisor and against
the micro-VM (kata + cloud-hypervisor) sandbox class. `E2E_SANDBOX_CLASS`
selects which, by repointing every fixture at its variant --- see
`e2e.CounterFixture`, `e2e.EgressFixture` and `e2e.RenderFixtureManifest` in
[sandbox.go](sandbox.go). Unset means gVisor.

```shell
# gVisor (the default), against the demos install-ate-kind.sh deploys
$ hack/run-e2e-kind.sh -v -args --no-color

# micro-VM, against the counter-microvm and egress-microvm demos
$ E2E_SANDBOX_CLASS=microvm hack/run-e2e-kind.sh -v -args --no-color
```

The micro-VM lane needs its fixtures installed first, which also needs a node
with `/dev/kvm` (`hack/create-kind-cluster.sh` detects one and labels the node):

```shell
$ hack/run-microvm-demo-kind.sh                        # counter-microvm + assets
$ hack/install-ate-kind.sh --deploy-demo-egress-microvm # egress-microvm
```

A handful of knobs override the class defaults, mostly for a cluster that
installs the fixtures elsewhere: `E2E_TEMPLATE_NAMESPACE` / `E2E_TEMPLATE_NAME`
point the counter fixture somewhere else, and `E2E_TEMPLATE_READY_TIMEOUT`
replaces the golden-snapshot budget (90s on gVisor, 10m on micro-VM, where the
golden is a cloud-hypervisor cold boot plus a checkpoint).

## After a failure

A suite deletes the namespaces it created only when it passed. A failed run
keeps them, because the failure is usually explained inside a worker pod (the
ateom logs, and for a micro-VM worker the guest's console tail), and deleting
the namespace takes those pods with it:

```shell
$ kubectl logs -n <kept-namespace> <worker-pod>
```

Nothing reclaims them afterwards, and each namespace holds a WorkerPool's worth
of running pods, so clean up once you are done reading:

```shell
$ hack/cleanup-e2e.sh   # deletes every namespace labeled ate.dev/e2e
```

## Creating a new test suite

Copy `testmain_test.go` from `internal/e2e/suites/example` into your new suite. It will
look like this:

```go
func run(m *testing.M) int {
	Setup()
	defer Teardown()
	// return allows the deferred Teardown to run.
	return e2e.RunTestMain(m)
}

func TestMain(m *testing.M) { os.Exit(run(m)) }
```

This will handle the standard flags and checks for running an e2e test suite.
