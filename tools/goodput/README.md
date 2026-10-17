# goodput

Task goodput for benchmark runs: how many **good** tasks the platform completes
per hour, per node, per vCPU and per GB of RAM, and why the bad ones were bad
([go/agentic-task-goodput](http://go/agentic-task-goodput)). The task logs it reads
are currently the sweperf benchmark's.

The benchmark only records facts; this script decides good or bad from them. A
rule change is an edit to `config.json` and a rerun, with no new benchmark run.

Python 3.10+, standard library only. GCS access goes through `gcloud storage`.

## Commands

```sh
# One run, local directory or a run path in GCS.
tools/goodput/goodput.py compute <run dir | gs://.../run_date=.../run_ts=.../run_tag=.../> [--out DIR]

# Every run under a prefix since a date. Writes locally only, mirroring the from_prow layout.
tools/goodput/goodput.py backfill 'gs://dev-measurements/from_prow/runs/sweperf_*' --since 2026-10-01 --out /tmp/goodput

# A baselines[] entry from 1-user runs (median wall time of tasks without failures, task CEL).
tools/goodput/goodput.py baseline <1-user run>...
```

`--config` selects a different rules file (default `config.json` next to the script).

## Running it

- **Prow runs** will get goodput from a post-test step (not from the runner); that step is a follow-up.
- **Runs on your own cluster:** `tools/goodput/goodput.py compute <runner --dest dir>`. That's the directory
  the runner wrote `logs.txt`, `stats.jsonl` and `server_summary.json` to.

## Inputs

From the run directory the runner uploads:

| file | used for |
|---|---|
| `logs.txt` | the task log lines: cycle start/end, failures, task end, quit |
| `stats.jsonl` | `trial_summary` (machine type, node count, allocatable cores and RAM) |
| `server_summary.json` | `metadata`: steady window, run start/end, test name, tag |
| `stats_history.csv` | steady window fallback, same rule as the runner |

## Rules

**Tasks.** A task is one pass over the trajectory (all of its cycles).
- An actor may serve task after task, so each actor's lines are split into task
  segments at every "Actor finished all cycles" line.
- A cycle's latest "Starting sweperf cycle" is paired with its "Completed sweperf
  cycle" only within the current segment.
- A task's wall time is the sum of its cycles' Starting-to-Completed times, which
  leaves out the think time between cycles.
- A retried cycle counts only its last attempt in wall time.

**Task log wording.** Both task-end wordings are read:
- `reused_actor`: "... and reset session back to cycle 1", where one actor serves
  task after task. Historical runs use this one.
- `fresh_actor`: "... and was deleted; a new actor will run the trajectory next",
  where every task gets a new actor.

**Cut-off.** This is when the load stopped. Failures after it are teardown, not
part of a user's task. The first available of:
1. the benchmark's shutdown marker ("sweperf shutdown started");
2. boomer's "Recv quit message from master" line;
3. `trial_summary.run_end`;
4. `server_summary` `end_ts`.

**Good or bad.**
- **G1, no failure.** Any failure before the cut-off makes the task bad: a failed
  resume, execute or suspend. A resume that failed and was then retried
  successfully still makes the task bad. This is the same rule as the benchmark's
  `loopFailed`, which reports the task's TaskCEL/TaskWallClock as a failure.
  - G1 counts **tasks**, not failure lines. An actor that crashed and fails every
    retry until the run stops is one bad task, however many failures it logs.
- **G2, latency (strict mode).** Max TTFE ≤ `ttfe_max_s` and max resume ≤
  `resume_max_s`. This needs a per-task record line the logs don't have yet, so
  it is reported as `g2: n/a`.
- **G3, slowness.** Wall time ÷ the baseline's `wall_ref_ms` ≤ `s_max`.
  - It only applies when `config.json` has a baseline for the run's runtime,
    machine type and task log wording (`g3_basis: wall`); otherwise it's `skipped`.
  - `slow` is only added when the task has no other failure.
- **Unfinished tasks.** A task that hadn't finished by the cut-off is bad if it
  failed before the cut-off. Otherwise it is unfinished and not counted.
- **Startup failures** ("sweperf on_start failed") are reported as attempts. They
  stay out of the ratio (`startup_basis: none`), because the line can't tie an
  attempt to a user.

**Windows.**
- A finished task counts in the window it **ended** in. An unfinished bad task
  counts in the window of its first failure.
- `steady` is the steady window from `server_summary.json`, falling back to
  `stats_history.csv`, then the run's start/end, then the log's first/last line.
- `whole` runs from the run start to the cut-off.

## Outputs

`goodput.json`:
- `run`, `cutoff`, `steady_window`, `baseline`;
- per mode (`strict`, `relaxed`) and window (`steady`, `whole`):
  - `tasks_done`, `tasks_good`, `tasks_bad`, `tasks_unfinished_bad`, `bad_reasons`,
    `startup_failure_attempts`;
  - `good_tasks_per_hr`, and per node/vCPU/GB-RAM hour;
  - `goodput_ratio_pct` = good ÷ (done + unfinished bad);
  - wall-time percentiles of good tasks;
  - with a baseline: wall ratio percentiles and `eta_infra` (reference CEL ÷ median wall);
- `totals` over the whole log;
- `tasks`: one row per task (actor, start, end, wall, cycles, retries, first
  failure, status per mode).

`goodput.jsonl`: one row per mode, shaped like `stats.jsonl` (`metric` =
`goodput_strict` / `goodput_relaxed`, string `measurements`). Whole-run values
carry a `whole_` prefix.

## Config (`config.json`)

- `modes`: thresholds per mode.
  - strict: G2 on, `ttfe_max_s` 1.0, `resume_max_s` 1.5, `s_max` 1.5;
  - relaxed: G2 off, `s_max` 2.0.
- `baselines`: `{runtime, machine_type, template, format, wall_ref_ms, task_cel_ref_ms}`
  entries, printed by `goodput.py baseline`.
  - `format` is the task log wording (`reused_actor` or `fresh_actor`); a run only
    uses a baseline of the same wording.
  - `template` is matched only when both the baseline and the run's
    `trial_summary` name one.

## Tests

```sh
python3 tools/goodput/test_goodput.py
```

The tests build stand-in run directories and log lines inline (including copies
of real task log lines). They never contact GCS or a cluster.
