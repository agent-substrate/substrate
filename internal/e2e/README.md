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
for example via `hack/install-ate.sh --deploy-ate-system --credential-provider='{"name":"k8s.io"}'`
or `hack/install-ate-kind.sh --deploy-ate-system --credential-provider='{"name":"k8s.io"}'`.
The egress credential injection suite needs that bundled provider, which is
what CI installs on the envoy lane; see `internal/e2e/suites/egresscredinject`.

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
installs the fixtures elsewhere: `E2E_SUBSTRATE_TEMPLATE_ATESPACE` /
`E2E_SUBSTRATE_TEMPLATE_NAME` / `E2E_SUBSTRATE_POOL_NAMESPACE` /
`E2E_SUBSTRATE_POOL_NAME` point the counter fixture somewhere else, and
`E2E_TEMPLATE_READY_TIMEOUT` replaces the golden-snapshot budget (90s on
gVisor, 10m on micro-VM, where the golden is a cloud-hypervisor cold boot plus
a checkpoint).

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

## Golden snapshot application liveness

The `golden` suite uses real gVisor workers to check that an application exiting
with status 0 or 1 cannot produce a golden snapshot, including when another
container has a healthy wakeup probe. It checks the template's error and absence
of a golden tag through the control API. A healthy two-container template must
restore two actors with the same per-container boot IDs, proving that their
process state came from the shared golden rather than a cold boot.

The suite also checks an ordinary actor without a wakeup probe. It saves a MEMORY
snapshot, resumes it, increments an in-memory counter, and exits the application.
Suspend must fail, leave the actor CRASHED, and retain the previous snapshot URI.
After revert and resume, both the boot ID and counter must match the saved
snapshot. The newer, unsaved increment must not survive recovery.

After installing the gVisor counter demo (the source of the worker image and
sandbox configuration), run:

```shell
$ hack/run-e2e-kind.sh ./internal/e2e/suites/golden -count=1
```

The normal gVisor E2E lane includes this suite. It is skipped in the micro-VM
lane because the checkpoint liveness check is specific to gVisor.

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
