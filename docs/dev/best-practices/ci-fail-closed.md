# CI Fail-Closed Preconditions

How to write a test that skips locally when a dependency is absent, but fails
in CI. One code path, strictness resolved from the environment.

Counterpart to [Code Style Guide](../../code-style-guide.md) § Testing.

## Why

A test needing Docker, a cluster, or an artifact directory has three options:

| | Locally | In CI |
|---|---|---|
| Hard-fail everywhere | Unrunnable without setup | Correct |
| Skip everywhere | Runnable | **Green having run nothing** |
| Skip locally, fail in CI | Runnable | Correct |

A skip and a pass are the same exit code. ~280 PostgreSQL-backed tests once
skipped when their testcontainer failed to start, including a 2,800-line store
contract, and the job stayed green.

## Convention

Resolve strictness in one named function. GitHub Actions sets `CI=true`.

### Go

Reference implementation, `cmd/ateapi/internal/store/dockerenv.Required()`:

```go
func Required() bool {
    return os.Getenv("CI") == "true" || os.Getenv("REQUIRE_DOCKER") == "true"
}
```

Callers branch on it (`storetest.go:187-192`, `atepg/main_test.go:105-110`):

```go
if containerErr != nil {
    if dockerenv.Required() {
        t.Fatalf("PostgreSQL testcontainer unavailable and required "+
            "(CI or REQUIRE_DOCKER is set): %v", containerErr)
    }
    t.Skipf("PostgreSQL testcontainer unavailable (requires Docker): %v", containerErr)
}
```

Both messages name the cause; the fatal one also names what made it fatal.

### Bash

```bash
if [[ -z "${E2E_JUNIT_FILE:-}" && "${CI:-}" == "true" ]]; then
    echo "run-e2e.sh: E2E_JUNIT_FILE must be set when CI=true." >&2
    echo "  Set it to a path unique to this run, e.g." >&2
    echo "    E2E_JUNIT_FILE=\"\${ARTIFACTS}/e2e-gvisor.xml\"" >&2
    exit 1
fi
```

## Rules

1. **One named predicate.** `Required()`, `inCI()`. An inline `os.Getenv("CI")`
   cannot be audited or changed in one place.
2. **Make the strict branch reachable locally.** Pair `CI` with an override:
   `REQUIRE_DOCKER=true`, or a flag such as junittool's `-reject-duplicate`.
3. **Failures state the fix.** Name the variable to set and a valid value.
   Assume the reader has not seen this check before.
4. **Never warn instead of failing.** Warnings in a green run are not read.

## Verifying both branches

Only the permissive branch runs by default. Force the strict branch before
merging:

```bash
# Docker precondition, without Docker
sudo systemctl stop docker.socket docker.service
CI=true go test ./cmd/ateapi/internal/store/...          # expect FAIL, not SKIP

# JUnit precondition, unset
CI=true hack/run-e2e.sh ./internal/e2e/suites/example    # expect exit 1

# Duplicate registration
CI=true go -C tools/junittool run . register /tmp/a.xml
CI=true go -C tools/junittool run . register /tmp/a.xml  # expect exit 1
```

Two traps that have produced false passes:

- `DOCKER_HOST=/nonexistent` does not disable Docker. testcontainers walks a
  six-source resolution chain and reaches the real daemon.
- `systemctl stop docker` leaves `docker.socket` active, which restarts the
  daemon on the next connection. Stop both units.

## When not to use this

- **The precondition should always hold.** Absence is a bug: fail everywhere.
- **The test can assert it instead.** A skip on cluster state usually means a
  missing precondition check in the harness.
- **The test can create what it needs.** Do that rather than branching.

## Current users

`dockerenv.Required()` is the reference implementation. The same shape is in
`hack/run-e2e.sh` and `hack/run-root-tests.sh` (a JUnit path is mandatory in
CI) and `tools/junittool` (re-registering a path is fatal in CI).

`git grep -n 'CI.*==.*true'` finds the current set. Not maintained as an
inventory.
