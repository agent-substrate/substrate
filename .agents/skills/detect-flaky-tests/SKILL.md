---
name: detect-flaky-tests
description: >
  Detects flaky Go tests by analyzing GitHub Actions workflow runs across the last 7 days
  and all PRs — covering both the run-tests job (unit/integration) and the e2e-test job
  (gVisor and microVM lanes). For each newly-detected flaky test or infra issue, opens a
  GitHub issue with full evidence and a draft fix PR. Does not touch BigQuery, dashboards,
  or any external storage — those are cron-job concerns layered on top.
---

# Detect Flaky Tests

A test is **flaky** when it produces both PASS and FAIL outcomes across multiple independent
CI runs in the last 7 days, with no code change to that test's package explaining the
inconsistency. Cross-PR analysis provides the strongest signal: if the same test fails on
PR-A but passes on PR-B, that inconsistency is almost certainly non-determinism, not a
legitimate regression.

## Flakiness threshold (keeps false-positive rate low)

A test is flagged only when **all three** conditions hold in the 7-day window:

| Condition | Rationale |
|---|---|
| `fail_count >= 2` | One failure could be infra noise |
| `pass_count >= 2` | One pass could be a pre-fix lucky run |
| `0.05 < fail_rate < 0.95` | Outside this band it is either reliably broken or reliably passing |

---

## Step 1 — Collect workflow run IDs (last 7 days)

```bash
SINCE=$(date -u -v-7d +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || date -u -d '7 days ago' +%Y-%m-%dT%H:%M:%SZ)
gh api --paginate \
  "repos/agent-substrate/substrate/actions/workflows/pr-workflow.yaml/runs?status=completed&per_page=100&created=>=$SINCE" \
  --jq '.workflow_runs[] | {id: .id, conclusion: .conclusion, head_sha: .head_sha, created_at: .created_at}'
```

`--paginate` is required: a typical week has several hundred completed runs (600+ as of
August 2026), far more than one page of 100.

Collect all run IDs. Process both successful and failed runs — both contain test output.

---

## Step 2 — Download and parse logs: two jobs, three lanes per run

For each run, you need logs from **two jobs**:

| Job name | Coverage |
|---|---|
| `run-tests` | Unit + integration tests (`go test -race -v ./...`) |
| `e2e-test` | E2E suite, both sandbox classes — two sequential steps in the one job |

```bash
# List all jobs for a run
gh api "repos/agent-substrate/substrate/actions/runs/<RUN_ID>/jobs" \
  --jq '.jobs[] | {id: .id, name: .name, conclusion: .conclusion}'

# Download log for a specific job
gh api "repos/agent-substrate/substrate/actions/jobs/<JOB_ID>/logs" > /tmp/job_<JOB_ID>.log
```

The two e2e lanes are NOT a matrix — `e2e-test` is a single job that runs the step
"Run E2E tests (gVisor)" followed by "Run E2E tests (micro-VM)" (same
`hack/run-e2e-kind.sh` command, the second with `E2E_SANDBOX_CLASS: microvm`). Split the
one `e2e-test` job log at the "Run E2E tests (micro-VM)" step boundary: PASS/FAIL lines
before it belong to the gVisor lane, lines after it to the microVM lane. Because the
steps are sequential, a gVisor-lane failure means the micro-VM step never ran — record
no microVM results for that run rather than counting them as failures.

Parse `go test -v` output from each log:
```bash
grep -E '^--- (PASS|FAIL): ' /tmp/job_<JOB_ID>.log \
  | awk '{print $2, $3}' | sed 's/://'
```

Track results per (test_name, job_type) where job_type is one of:
`unit`, `e2e-gvisor`, `e2e-microvm`.

---

## Step 3 — Infra issue triage (do this BEFORE aggregating flakiness)

An infra failure is when the job itself breaks before or during setup — not when a test
produces FAIL output. Infra failures must be identified and reported separately; they do
NOT count toward a test's fail_count.

### How to detect an infra failure

A job log is an **infra failure** (not a test failure) when it contains ANY of:

| Signal | Example log pattern |
|---|---|
| Go module proxy error | `INTERNAL_ERROR`, `proxy.golang.org: dial`, `go mod download: ...500` |
| kind cluster creation failure | `ERROR: failed to create cluster`, `node(s) not ready`, `timed out waiting for the condition` |
| Image pull failure | `failed to pull image`, `ErrImagePull`, `ImagePullBackOff` |
| Docker/containerd failure | `failed to start containerd`, `Error response from daemon` |
| OOM / out of disk | `OOMKilled`, `No space left on device` |
| Network/DNS failure in setup | `dial tcp: lookup`, `connection refused` during setup steps (not inside a test) |
| No test output at all | Log ends before any `--- PASS` or `--- FAIL` line appears |
| Setup step non-zero exit | A step before the `go test` or `hack/run-e2e-kind.sh` command fails |

**Critical rule:** If a job log contains `--- FAIL: TestFoo` AND infra error patterns, you
must determine which came first chronologically. If the infra error appears before the first
test ran, treat the whole job as an infra failure (zero test results). If tests started
running and then an infra error interrupted them mid-run, count only the completed test
results and note the truncation.

### Triple-check protocol

For any run where you suspect an infra issue, verify it three ways:

1. **Pattern match**: does the log contain one of the signals above?
2. **Timeline check**: does the error appear before `=== RUN   Test` lines, or between test completions?
3. **Cross-run consistency**: did the same infra error appear in ≥2 other runs around the same time? If yes, it is definitely infrastructure. If it appeared only once, it may be a transient fluke — still classify as infra but flag lower confidence.

### Infra issue aggregation

Collect infra failures separately:

| infra_pattern | job_type | fail_count | example_run_ids |
|---|---|---|---|
| `proxy.golang.org INTERNAL_ERROR` | unit | 4 | [run_1, run_2, ...] |
| `kind cluster: node(s) not ready` | e2e-gvisor | 2 | [run_5, ...] |

An infra pattern that appears in ≥2 runs warrants a GitHub issue (Step 5b).

---

## Step 4 — Aggregate test flakiness across runs

Using only the runs NOT classified as infra failures, build a per-test table:

| test_name | job_type | fail_count | pass_count | total_runs |
|---|---|---|---|---|

**Treat each lane independently.** A test that is flaky only in `e2e-gvisor` is still
flagged — it does not need to be flaky in `e2e-microvm` too.

Apply the threshold: `fail_count >= 2 AND pass_count >= 2 AND 0.05 < fail_rate < 0.95`.

---

## Step 5a — Create issues for flaky tests

Before creating, check for an existing open issue:

```bash
gh issue list \
  --repo agent-substrate/substrate \
  --state open \
  --label "kind/bug,area/tests" \
  --search "flaky: <TEST_NAME>" \
  --json number,title
```

If none exists, create:

```bash
gh issue create \
  --repo agent-substrate/substrate \
  --title "flaky: <TEST_NAME>" \
  --label "kind/bug,area/tests" \
  --body "$(cat <<'BODY'
## Flaky test detected

**Test:** `<TEST_NAME>`
**Job:** `<e2e-gvisor | e2e-microvm | unit>`

### Evidence (last 7 days)

| Metric | Value |
|---|---|
| Runs analysed | <TOTAL_RUNS> |
| Failures | <FAIL_COUNT> |
| Passes | <PASS_COUNT> |
| Flake rate | <FLAKE_RATE>% |
| Infra-failure runs excluded | <INFRA_EXCLUDED> |

### Failing run examples
<links to 2-3 failing runs>

### Passing run examples
<links to 1-2 passing runs>

### Infra triage
Infra failures were excluded before computing this flake rate. The remaining
failures cannot be explained by cluster setup, image pull, or proxy errors.

A draft fix PR will be opened by the detect-flaky-tests agent.
BODY
)"
```

---

## Step 5b — Create issues for recurring infra failures

For each infra pattern appearing in ≥2 runs:

```bash
gh issue list \
  --repo agent-substrate/substrate \
  --state open \
  --search "infra: <PATTERN_SUMMARY>" \
  --json number,title
```

If none exists, create:

```bash
gh issue create \
  --repo agent-substrate/substrate \
  --title "infra: <PATTERN_SUMMARY>" \
  --label "kind/bug,area/dev-infra" \
  --body "$(cat <<'BODY'
## Recurring infrastructure failure in CI

**Pattern:** `<infra error pattern>`
**Job type:** `<unit | e2e-gvisor | e2e-microvm>`

### Evidence

| Metric | Value |
|---|---|
| Occurrences in last 7 days | <COUNT> |
| Example runs | <links> |

### Impact

This failure causes entire CI jobs to abort before tests run. It inflates
apparent failure rates and masks real test flakiness. Fixing it will improve
flakiness signal quality.

### Log excerpt
\`\`\`
<paste 3-5 lines of the actual error from the log>
\`\`\`
BODY
)"
```

---

## Step 6 — Open a draft fix PR for each new flaky test

Read the test source file. Diagnose the likely cause using the patterns below (unit tests
and e2e tests share most root causes, but e2e has additional patterns):

### Common Go flakiness patterns and fixes

| Pattern | Symptoms | Fix |
|---|---|---|
| **Timing / sleep** | `time.Sleep` before an assertion | Replace with `require.Eventually` or `testutil.WaitFor` |
| **Shared global state** | Package-level var mutated without cleanup | Move to test-local; `t.Cleanup` to restore |
| **Port conflicts** | Hardcoded port or race on ephemeral port | Use `ln.Addr()` from the actual listener |
| **Goroutine leak** | Goroutines from one test race the next | `t.Cleanup(cancel)` + wait for goroutines to exit |
| **File system races** | Shared temp path across parallel tests | Use `t.TempDir()` |
| **Context not cancelled** | Long operation outlives test | `t.Context()` (Go 1.21+) or `t.Cleanup(cancel)` |
| **Order dependency** | Test relies on prior test's side effects | Make each test self-contained |
| **E2E: actor/pod not ready** | Test proceeds before actor reaches Running state | Poll with `require.Eventually` on status, increase timeout with justification |
| **E2E: resource cleanup race** | Prior test's namespace/actor not fully deleted before next test | Add explicit `WaitForDeletion` in `t.Cleanup` |
| **E2E: network policy timing** | Policy applied but not yet enforced at assertion time | Retry the connectivity check, not just the policy application |

Steps for the fix PR:

1. Create a branch: `fix/flaky-<test-name-kebab>` from `main`
2. Apply the fix
3. Commit: `fix(tests): resolve flakiness in <TestName>\n\nFixes #<issue_number>`
4. Open a **draft** PR:

```bash
gh pr create \
  --repo agent-substrate/substrate \
  --title "fix(tests): resolve flakiness in <TestName>" \
  --draft \
  --body "$(cat <<'BODY'
## Summary

Fixes the flaky test `<TestName>` in `<package>` (`<job_type>` lane).

**Root cause:** <one sentence>
**Fix:** <one sentence>

Closes #<issue_number>

## Evidence

Flake rate over last 7 days: <FLAKE_RATE>% (<FAIL_COUNT> fail / <PASS_COUNT> pass)
Infra-failure runs excluded from count: <INFRA_EXCLUDED>

Failing runs: <links>
Passing runs: <links>

## Test plan

- [ ] Run `go test -race -count=10 ./path/to/package/...` locally for unit tests
- [ ] For e2e: re-run the affected suite 3+ times against a kind cluster
BODY
)"
```

Do not open a fix PR for infra issues — those require infra investigation, not test code changes.

---

## Step 7 — Report

Output two tables:

### Flaky tests

| Test | Job | Flake rate | Fail/Pass | Infra excluded | Issue | Fix PR | Action |
|---|---|---|---|---|---|---|---|
| TestFoo | e2e-gvisor | 40% | 4/6 | 2 runs | #NNN | #MMM | created |

### Infra issues

| Pattern | Job | Occurrences | Issue | Action |
|---|---|---|---|---|
| `proxy.golang.org INTERNAL_ERROR` | unit | 4 | #OOO | created |

If nothing found in either category: `No new flaky tests or infra issues detected.`

---

## Notes

- This skill does NOT write to BigQuery, update a dashboard, or perform any storage
  operations. Those are handled by the cron job that invokes this skill.
- If log download fails for a run (e.g. logs expired after 90 days), skip that run and
  note it in the report.
- The `--add-label` flag may fail if a label does not exist on the repo; fall back to
  omitting labels and add a comment instead.
- E2E flakiness in one lane (gVisor or microVM) is flagged independently — a test does
  not need to be flaky in both lanes to warrant an issue.
