# E2E testing

```shell
$ source .ate-dev-env.sh
$ go test -v ./internal/e2e/suites/... -args -e2e
```

## Principles

* Keep it simple -- use go test for the harness.
* e2e tests live under `internal/e2e/suites/<suite>`
* Each suite should implement TestMain using e2e.RunTestMain()
  * e2e tests will be skipped for ordinary unit tests unless the `-e2e` flag
    is set e.g. `go test ./internal/e2e/suites/... -args -e2e`
* Helper libraries live under `internal/e2e`
* Setup and Teardown are on a per-component basis and the component's
  author's responsibility.

## Preconditions

The e2e tests assume you have a cluster setup with `hack/install.sh`.

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
