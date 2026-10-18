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

# A baselines[] entry from 1-user runs (see Config).
tools/goodput/goodput.py baseline <1-user run>... [--template T] [--name N]

# Many runs' goodput.json files (or directories of them): one row per test, as CSV and Markdown.
tools/goodput/goodput.py summary /tmp/goodput [--out DIR]
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
| `logs.txt` | the task log lines: task records, failures, startup failures, shutdown; cycle lines in older runs |
| `stats.jsonl` | `trial_summary` (machine type, node count, allocatable cores and RAM) |
| `server_summary.json` | `metadata`: steady window, run start/end, test name, tag |
| `stats_history.csv` | steady window fallback, same rule as the runner |

## Rules

**Tasks.** A task is one pass over the trajectory (all of its cycles). Newer
runs log a "sweperf task record" line per finished task (see "Sweperf Log Lines"
in `benchmarking/README.md`); older runs don't.

- **With task records:**
  - Each record logged before the cut-off is a finished task; the record's
    `cel_ms`, `wall_ms`, `resume_max_ms`, `ttfe_max_ms`, `failed` and `fail_step`
    decide the gates.
  - An actor whose failure lines before the cut-off have no record after them has
    an unfinished task. Cycle lines are not used.
- **Without task records** (historical runs):
  - An actor may serve task after task, so each actor's lines are split into task
    segments at every "Actor finished all cycles" line.
  - A cycle's latest "Starting sweperf cycle" is paired with its "Completed sweperf
    cycle" only within the current segment.
  - A task's wall time is the sum of its cycles' Starting-to-Completed times, which
    leaves out the think time between cycles.
  - A retried cycle counts only its last attempt in wall time.

**Task log format.**
- `task_record`: the run logged task records.
- Otherwise the task-end wording: `reused_actor` ("... and reset session back to
  cycle 1", where one actor serves task after task; historical runs use this one)
  or `fresh_actor` ("... and was deleted; a new actor will run the trajectory
  next", where every task gets a new actor).

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
  - With a task record, that's its `failed` flag, and the reason is its first
    failed step (`fail_step`). Without one, the reasons are every kind of failure
    line the task logged.
  - G1 counts **tasks**, not failure lines. An actor that crashed and fails every
    retry until the run stops is one bad task, however many failures it logs.
- **G2, latency (strict mode).** The record's `resume_max_ms` ≤ the resume bound
  (`slow_resume`) and `ttfe_max_ms` ≤ `ttfe_max_s` (`slow_ttfe`). A record without
  a TTFE isn't checked for it.
  - The resume bound is `resume_max_lssd_s` (0.5 s) on a local SSD machine type
    (`machine_type` ends in `-lssd`), else `resume_max_s` (1.5 s).
  - G2 needs task records: it's `g2: on` for runs with them and `g2: n/a` for runs
    without (`off` in modes that turn it off).
- **G3, slowness.** A task's slowdown ÷ its baseline ≤ `s_max`, else `slow`.
  - With a task record, the slowdown is `cel_ms` ÷ `task_cel_ref_ms`
    (`g3_basis: cel`). The wall ratio is reported but doesn't gate, unless the
    mode sets `g3_wall`: then a task with a record whose `wall_ms` ÷ `wall_ref_ms`
    is above `s_max` is also `slow_wall` (`g3_wall: on`). That catches tasks whose
    lifecycle calls (such as a slow suspend) stretch them while CEL stays fine.
  - Without records it's the cycle-line wall time ÷ `wall_ref_ms` (`g3_basis: wall`).
  - It only applies when `config.json` has a baseline for the task (see Config);
    otherwise it's `skipped`.
- G2 and G3 apply only to a finished task that passed G1. A task can fail both.
- **Unfinished tasks.** A task that hadn't finished by the cut-off is bad if it
  failed before the cut-off. Otherwise it is unfinished and not counted.
- **Startup failures** ("sweperf on_start failed") are reported as attempts. They
  stay out of the ratio (`startup_basis: none`).

**Broken runs.** A run is `valid: false`, with an `invalid_reason`, when it has
no finished task, or more than `max_startup_failures_per_task` (10) startup
failures per finished task. Such a run measured a broken setup, not the platform,
and `summary` leaves it out. A log with no sweperf line at all is an error.

**Windows.**
- A finished task counts in the window it **ended** in. An unfinished bad task
  counts in the window of its first failure.
- `steady` is the steady window from `server_summary.json`, falling back to
  `stats_history.csv`, then the run's start/end, then the log's first/last line.
- `whole` runs from the run start to the cut-off.

## Outputs

`goodput.json`:
- `run`, `valid`, `invalid_reason`, `cutoff`, `steady_window`, `baseline`;
- `abs_speed_ratio`: the run baseline's `task_cel_ref_ms` ÷ that of the
  `reference_baseline` named in `config.json` (above 1 is slower than the reference);
- per mode (`strict`, `relaxed`) and window (`steady`, `whole`):
  - `tasks_done`, `tasks_good`, `tasks_bad`, `tasks_unfinished_bad`, `bad_reasons`,
    `startup_failure_attempts`;
  - `good_tasks_per_hr`, and per node/vCPU/GB-RAM hour; `done_tasks_per_hr`;
  - `goodput_ratio_pct` = good ÷ (done + unfinished bad);
  - wall-time percentiles of good tasks;
  - from task records: percentiles of `resume_max_ms` and `ttfe_max_ms`;
  - with a baseline: percentiles of the wall ratio (every finished task) and of
    the CEL slowdown (finished tasks that passed G1), and `eta_infra` (reference
    CEL ÷ median wall of every finished task);
- `totals` over the whole log;
- `tasks`: one row per task (actor, start, end, wall, cycles, retries, first
  failure, status per mode, and a task record's facts).

`goodput.jsonl`: one row per mode, shaped like `stats.jsonl` (`metric` =
`goodput_strict` / `goodput_relaxed`, string `measurements`). Whole-run values
carry a `whole_` prefix.

`summary` reads `goodput.json` files and writes, per `test_name`:
- `goodput_summary.csv`: one row per test, with plain numeric columns
  `<mode>_<metric>_<median|min|max>` of the steady-window metrics over the
  test's valid runs (counts, ratio, rates, bad reasons, latency and slowdown
  percentiles, `eta_infra`), `abs_speed_ratio_*`, and valid/invalid run counts;
- `goodput_summary.md`: the same as tables, plus the invalid runs and why;
- `goodput_runs.csv`: one row per run, valid or not, with the same metrics
  unaggregated (`<mode>_<metric>`).

## Config (`config.json`)

- `modes`: thresholds per mode.
  - strict: G2 on, `ttfe_max_s` 1.0, `resume_max_s` 1.5 (`resume_max_lssd_s` 0.5
    on local SSD), `s_max` 1.5;
  - relaxed: G2 off, `s_max` 2.0.
  - Any mode can add `"g3_wall": true` to also gate on the wall ratio (see G3).
    Each mode gets its own outputs, so a mode such as strict plus `g3_wall` can be
    reported next to strict.
- `max_startup_failures_per_task`: the broken-run rule.
- `baselines`: `{name, runtime, machine_type, template, format, task_cel_ref_ms, wall_ref_ms, ...}`
  entries, printed by `goodput.py baseline`.
  - From runs with task records, it takes the clean finished tasks' medians:
    `task_cel_ref_ms` (`cel_ms`), `wall_ref_ms` (`wall_ms`), `resume_max_ref_ms`
    and `ttfe_max_ref_ms`, for the records' template.
  - From runs without, it takes the median cycle-line wall time and the
    request-weighted `actor_TaskCEL` median.
  - A task uses a baseline with the same runtime, machine type and `format` (the
    task log format), and the task's template: a task record's, else
    `trial_summary`'s. A baseline for its exact template comes first, then one
    that names no template.
- `reference_baseline`: the `name` of the baseline that `abs_speed_ratio` compares
  with, if any.

## Tests

```sh
cd tools/goodput && python3 -m unittest test_goodput
```

The tests build stand-in run directories and log lines inline (including copies
of real task log lines). They never contact GCS or a cluster.
