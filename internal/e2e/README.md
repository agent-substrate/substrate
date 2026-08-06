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
