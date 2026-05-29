# codeexec lifecycle benchmark

Standalone scripts used to produce [`docs/codeexec-benchmark.md`](../../docs/codeexec-benchmark.md).
They exercise the `codeexec` actor template's lifecycle on a running Substrate
cluster and report latency percentiles.

## Prerequisites

- `kubectl-ate` on `PATH` (`go install ./cmd/kubectl-ate`)
- The `codeexec` WorkerPool + ActorTemplate deployed in `ate-demo-codeexec`
- A port-forward to the router (for `bench.py`):
  ```bash
  kubectl port-forward -n ate-system svc/atenet-router 8000:80 &
  ```

## Usage

```bash
# create / cold-activation / warm-serving latency (HTTP via the router)
python3 benchmarking/codeexec/bench.py 5

# control-plane resume/suspend latency (times the synchronous kubectl-ate calls)
python3 benchmarking/codeexec/cycle.py 8
```

`cycle.py` is the reliable source for suspend/resume latency; `bench.py`'s
suspend column can read `n/a` due to a status-polling artifact (kubectl-ate
opens a fresh port-forward per call). See the report for details and results.
