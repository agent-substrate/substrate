# goodput

Task goodput for benchmark runs: how many **good** tasks the platform completes
per hour, per node, per vCPU and per GB of RAM, and why the bad ones were bad.
The task logs it reads are currently the sweperf benchmark's.

The benchmark only records facts; this script decides good or bad from them. A
rule change is a config edit (`config.json`, or a per-test file passed with
`--config`) and a rerun, with no new benchmark run.
Follow-up work is tracked in #2837.

Python 3.10+, standard library only. GCS access goes through `gcloud storage`.

## Commands

```sh
# One run, local directory or a run path in GCS. The run to score goes first.
tools/goodput/goodput.py compute <run dir | gs://.../run_date=.../run_ts=.../run_tag=.../> [--out DIR] \
    [--baseline FILE | --baseline-run <1-user run>...]

# Every run under a prefix since a date. Writes locally only, mirroring the input layout.
tools/goodput/goodput.py backfill 'gs://<bucket>/<prefix>/runs/sweperf_*' --since 2026-10-17 --out /tmp/goodput \
    [--baseline FILE]

# A baseline file from 1-user runs.
tools/goodput/goodput.py baseline <1-user run>... > baseline.json
```

`--config` selects a different config file (default `config.json` next to the script).

## Running it

- **Automated runs** will run goodput from `runner.py` after each test (#2838).
- **Runs on your own cluster:** `tools/goodput/goodput.py compute <dest>/runs/<name>/run_date=<YYYY-MM-DD>/run_ts=<ts>/run_tag=<tag>/`,
  the directory `runner.py` writes `logs.txt`, `stats.jsonl` and `server_summary.json` to.

## Inputs

From the run directory the runner uploads:

| file | used for |
|---|---|
| `logs.txt` | task records, failure lines, the shutdown marker, startup failures |
| `stats.jsonl` | `trial_summary` (sandbox class, machine type, node count, allocatable cores and RAM) |
| `server_summary.json` | `metadata`: steady window, run start/end, test name, tag |
| `stats_history.csv` | steady window fallback, same rule as the runner |

The benchmark logs one `sweperf task record` line per finished task
(`benchmarking/README.md`, "Sweperf Log Lines"). Runs from before it (#2537)
aren't supported: `compute` fails on a `logs.txt` without task records.

## Rules

**Tasks.** Each task runs on a fresh actor.
- A finished task is one task record. Its wall time, in-sandbox time, slowest
  resume and TTFE, retries and first failure come from the record.
- A task that never finished is an actor with a failure line ("ResumeActor
  failed in step", "execute steps failed" or "SuspendActor failed") before the
  cut-off and no task record.
- Task records of another version, or missing a required field, are skipped and
  counted in `skipped_records`. Optional fields a record doesn't log are `null`.

**Cut-off.** This is when the load stopped. Failures after it are teardown, not
part of a user's task. The first available of:
1. the benchmark's shutdown marker ("sweperf shutdown started");
2. boomer's "Recv quit message from master" line;
3. `trial_summary.run_end`;
4. `server_summary` `end_ts`.

Interrupted tasks log no record, so a record logged just after the cut-off still
counts; its end is taken as the cut-off.

**Good or bad.**
- **Failures (G1).** A record with `failed: true` is bad (`resume_failed`,
  `execute_failed`, `suspend_failed` from its `fail_step`, else `failed`). A
  resume that failed and was retried successfully still makes the task bad, the
  same rule as the benchmark's `TaskCEL`/`TaskWallClock` failures. A task that
  never finished is always bad, named by its first failure line. A failed task
  gets only its failure reason.
- **Checks.** A task that didn't fail is then held to every check of the mode
  (see Config). It is bad with the reason of each check it breaks, in config
  order.
- **Startup failures** ("sweperf on_start failed") are reported as attempts and
  as episodes: a user's consecutive startup failures until its next task record.
  They stay out of the ratio.

**Windows.**
- A finished task counts in the window it **ended** in. An unfinished task counts
  in the window of its first failure.
- `steady` is the steady window from `server_summary.json`, falling back to
  `stats_history.csv`, then the run's start/end, then the log's first/last line.
  It ends at the cut-off at the latest.
- `whole` runs from the run start to the cut-off.

## Config (`config.json`)

```json
{
  "version": 2,
  "modes": {
    "strict": {
      "checks": [
        {"field": "resume_max_ms", "op": "<=", "limit": 1500, "reason": "slow_resume"},
        {"field": "cel_ms", "op": "<=", "baseline_ratio": 1.5, "reason": "slow_cel"}
      ]
    }
  }
}
```

- `modes`: each mode has a list of `checks`. An empty list judges failures only.
- A check passes when `<field> <op> <threshold>` holds for the task's record.
  - `field`: `wall_ms`, `cel_ms`, `resume_max_ms`, `ttfe_max_ms`, `startup_ms`,
    `think_ms`, `retries`, `cycles` or `steps`.
  - `op`: `<=`, `<`, `>=` or `>`.
  - Exactly one of `limit` (any number) and `baseline_ratio` (a number above 0;
    the threshold is the ratio times the baseline's value for the field).
  - `reason`: the name a bad task gets, unique within the mode and different
    from the failure reasons.
- A task without a value for the field isn't judged by that check. A
  `baseline_ratio` check is skipped when there is no baseline value above 0 for
  its field; the mode lists it in `checks_skipped`.
- Unknown keys and a `version` other than 2 are errors.

The default modes:
- strict: `resume_max_ms` ≤ 1500, `ttfe_max_ms` ≤ 1000, and `cel_ms` and
  `wall_ms` ≤ 1.5× the baseline;
- relaxed: `cel_ms` and `wall_ms` ≤ 2× the baseline.

The resume and TTFE limits are placeholders until #2843, and the ratios until
the 1-user baselines (#2841). `ttfe_max_ms` currently includes the resume (#2843).

## Baselines

A baseline holds reference values for `baseline_ratio` checks, and the runs'
template, runtime and machine type:

```json
{"version": 1, "template": "sweperf-astropy-7336", "runtime": "microvm", "machine_type": "c3-highcpu-88",
 "refs": {"cel_ms": 6000.0, "wall_ms": 8000.0}}
```

`goodput.py baseline` prints one from 1-user runs. It pools the tasks that
finished without failures from all the runs, and takes the median of each
check field over the tasks that logged it. A field no task logged is left out.
The file also records `runs`, `tasks` and `sources`, to show where it came from.

- `template`, `runtime` and `machine_type` come from `trial_summary`, the test
  name and the task records, the same way as for the scored run. `""` means
  unknown; a file without one of the three keys is an error.
- `baseline` refuses runs that name different values for a key. A run that
  doesn't know a value takes the others'.
- `compute --baseline-run <run>...` builds the baseline from the given runs
  directly. The best baseline is a 1-user run paired with the test, on the same
  cluster.
- `compute --baseline FILE` and `backfill --baseline FILE` read a saved one.

Before using a baseline, goodput compares its `template`, `runtime` and
`machine_type` with the run's, as exact strings, for the keys both know (a
mixed `gvisor,microvm` run doesn't match `microvm`). On a mismatch, `compute`
exits with an error, and `backfill` scores that run without the baseline, so its
`baseline_ratio` checks are skipped.

Without a baseline, `baseline_ratio` checks are skipped.

## Outputs

`goodput.json`:
- `version` (2), `run`, `cutoff`, `steady_window`;
- `baseline`: `source` (`file:<path>`, `run:<paths>` or `null`), `identity` (the
  baseline's template, runtime and machine type), `checked` (the keys compared),
  `mismatch` (`{key: [run value, baseline value]}`) and `refs` (the values used;
  empty on a mismatch);
- per mode (`strict`, `relaxed`): `checks_applied` and `checks_skipped` (check
  reasons), and per window (`steady`, `whole`):
  - `tasks_done`, `tasks_good`, `tasks_bad`, `tasks_unfinished_bad`, `bad_reasons`,
    `startup_failure_attempts`, `startup_failure_episodes`;
  - `good_tasks_per_hr`, and per node/vCPU/GB-RAM hour;
  - `goodput_ratio_pct` = good ÷ (done + unfinished bad);
  - wall-time percentiles of good tasks;
  - `<field>_ratio_p50/p90/p99` (value ÷ baseline) for each applied
    `baseline_ratio` check;
  - `eta_infra` (the baseline's `cel_ms` ÷ median wall time of good tasks) when
    the baseline's `cel_ms` is above 0;
- `totals` over the whole log;
- `tasks`: one row per task (actor, user, end, wall and CEL time, slowest resume
  and TTFE, retries, first failure, status and reasons per mode).

`goodput.jsonl`: one row per mode, shaped like `stats.jsonl` (`metric` =
`goodput_strict` / `goodput_relaxed`, string `measurements`). Whole-run values
carry a `whole_` prefix. `checks_applied` and `checks_skipped` are
comma-separated, `baseline_source` is empty without a baseline, and
`baseline_mismatch` is empty unless the baseline didn't match (e.g.
`runtime:gvisor!=microvm`).

The summary line printed per run names a mode's skipped checks and why, e.g.
`relaxed (no baseline; slow_cel, slow_wall skipped)` or
`strict (baseline skipped: runtime:gvisor!=microvm; slow_cel, slow_wall skipped)`.

## Tests

```sh
python3 tools/goodput/test_goodput.py
```

The tests build stand-in run directories and log lines inline (including copies
of real task log lines). They never contact GCS or a cluster.
