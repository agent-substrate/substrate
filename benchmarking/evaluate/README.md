# Run evaluations

`evaluate` judges a finished benchmark run against conditions declared in its
test entry, and writes the verdicts next to the run's other results.
Standard library only, plus
[`cel-expr-python`](https://github.com/cel-expr/cel-python) for the `cel`
source (`requirements.txt`, Python 3.11 or later).

## Model

- A **source** reads a run's files and returns several named **values**, or
  says it can't judge the run (`invalid`, with a reason). Sources:
  `cel`, `run_results`.
- A **variable** is one source query: `{name, source, source_cfg}`. Its values
  are referenced as `<variable>.<value>`.
- A **condition** is a check over those values. The only check type is
  `threshold`.
- An **evaluation** is a named set of variables and conditions, AND-ed: it
  `fail`s if any condition failed, else is `invalid` if any was invalid, else
  `pass`es. Evaluations are judged independently, with no hierarchy between
  them.
- The run's **summary** combines its evaluations the same way.

## Config

Evaluations live in the test entry, next to the test they judge. This one
checks two envelope health gates:

```yaml
- name: sweperf_150u_c3hc192metal_microvm
  # ...
  evaluations:
  - name: envelope_health
    description: "Failure ratio and worker memory within the scalability envelope"
    variables:
    - name: trial
      source: run_results
      source_cfg: {metric: trial_summary, measurements: [aggregate_failure_ratio, allocatable_ram_gb]}
    - name: server
      source: run_results
      source_cfg: {metric: server_summary, measurements: [working_set_node_max_gb]}
    conditions:
    - name: failure_rate
      type: threshold
      check: {value: trial.aggregate_failure_ratio, op: "<=", limit: 0.001}
    - name: worker_memory
      type: threshold
      check: {value: server.working_set_node_max_gb, op: "<", limit: {ref: trial.allocatable_ram_gb, times: 0.85}}
```

- Names match `^[a-z][a-z0-9_]*$`, without `__`, at most 64 characters, and
  are unique where they live. A value's name may also have capitals, so a
  measurement keeps the name its row gives it (`locust_ResumeActor`).
  `summary` is reserved. Unknown keys are errors. There is no version field
  while the schema is experimental.
- A threshold `check` is `{value, op, limit}`. `op` is one of `<= < >= > ==
  !=`; `< <= > >=` compare numbers only, and `==` and `!=` also compare bools
  and strings. `limit` is a literal, or relative: `{ref, times (default 1),
  at_least, at_most}`. A numeric string limit is a number, as `run_results`
  reads measurements: `"1.10"` equals a measurement `"1.1"`.
- A value that is missing or null (not measured), or whose variable is
  invalid, makes the condition `invalid`, never 0.

## Sources

A source is a class with three hooks (`sources/base.py`):

| hook | runs | does |
|---|---|---|
| `validate(cfg)`, a classmethod | when the evaluations are loaded | raises `ValueError` for a bad `source_cfg` |
| `init(run)` | once, when the run starts (its files may not exist yet) | takes the `BenchmarkRunContext`: the run's files by name, and `paired_run(test)`, which returns the run of another test from the same invocation or raises `LookupError` |
| `query(cfg)` | after the run, once per distinct `source_cfg` | returns named values (numbers, bools, strings or null), or raises `SourceInvalid(reason, values)` when it can't judge the run |

The evaluator creates one instance of each source the evaluations name. The
values of a `SourceInvalid` are kept in the outputs, but its variable's
conditions are `invalid`. Any other exception from a hook makes the source's
variables `invalid` with the error. Sources never write files. A new source is
a class in `sources/` and a line in `SOURCES`.

A source can also give, from the classmethod `value_names(cfg)`, the names of
the values `query` returns for `cfg` (`run_results`: its `measurements`). A
condition that reads another value then fails to load. The default, `None`,
means the source can't tell before the run.

### `cel`

Judges each record of a run with CEL rules, and counts the good ones.
`kind` names the kind of record, and each kind has its own keys:
`sweperf_tasks` (SWE-Perf tasks) and `json_lines` (the lines of a JSON-lines
file). A record is good when its kind gives it no failure of its own and it
breaks no rule.

- A rule is `{name, cel}`: a [CEL](https://cel.dev/) expression that is true
  for a good record. A rule's name is the reason a record that breaks it is
  bad. `rules: []` applies no rule.
- `values` (optional) maps more value names to CEL expressions over the
  counts (`records`, `good`, `bad`) and the kind's facts, all doubles: for
  example `good_ratio_pct: "round(100.0 * good / records, 2)"`, where
  `round(x, digits)` rounds as Python does. An expression gives a double or a
  bool and can't read another value. A value is null when it reads a fact the
  run lacks, or when it isn't finite (x / 0).
- Loading the evaluations compiles the rules and values: an unknown name, an
  int mixed with a double, a result of the wrong type or a value name the
  source gives is an error. A rule or value that fails to evaluate makes the
  variable `invalid`.
- The source gives `records`, `good`, `bad`, `bad_<reason>` for every rule and
  reason of the kind (0 when none, null for a skipped rule), `rules_applied`,
  `rules_skipped`, the kind's own values, and those of `values`. The keys are
  the same on every run: null where there is nothing to measure.
- Invalid: a kind with no records to judge, and the kind's own cases below.

#### `sweperf_tasks`

Which SWE-Perf tasks were good. A task is one task record line in `logs.txt`
(see "Sweperf Log Lines" in
[docs/guides/benchmarking.md](../../docs/guides/benchmarking.md)): a finished
task is good when it did not fail (G1, built in) and passes every rule. A task
that logged a failure but no record never finished, and is bad.

- **G1:** no unrecovered error.
- **G2:** wake latency: the slowest resume (`resume_max_ms`) and TTFE, resume to
  first execute (`ttfe_max_ms`), within limits (1.5 s and 1 s below).
- **G3:** slowdown against the 1-user run: `cel_ms` (command execution
  latency, the time the task's commands ran in the sandbox) and `wall_ms`
  within a multiple of the 1-user run's medians (1.5x below).

```yaml
- name: sweperf_astropy_150_users_think
  # ...
  evaluations:
  - name: goodput_strict
    description: "No failure (G1), resume <= 1.5 s and TTFE <= 1 s (G2), CEL and wall <= 1.5x the 1-user run (G3)"
    variables:
    - name: goodput
      source: cel
      source_cfg:
        kind: sweperf_tasks
        rules:
        - {name: slow_resume, cel: "task.resume_max_ms <= 1500.0"}
        - {name: slow_ttfe, cel: "task.ttfe_max_ms <= 1000.0"}
        - {name: slow_cel, cel: "task.cel_ms <= 1.5 * baseline.cel_ms"}
        - {name: slow_wall, cel: "task.wall_ms <= 1.5 * baseline.wall_ms"}
        baseline_test: sweperf_astropy_1_user
        require_baseline: true   # without the 1-user run the G3 rules can't apply: invalid
        values:
          good_ratio_pct: "round(100.0 * good / records, 2)"
          good_per_hour: "round(good / window_hours, 3)"
          good_per_node_hour: "round(good / window_hours / nodes, 4)"
          good_per_vcpu_hour: "round(good / window_hours / vcpus, 4)"
          good_per_gb_ram_hour: "round(good / window_hours / gb_ram, 4)"
    conditions:
    - name: good_ratio
      type: threshold
      check: {value: goodput.good_ratio_pct, op: ">=", limit: 80}
```

- `window`: `steady` (the default, the run's steady window) or `whole` (the
  run's start to the load's end). One window per variable. The records are
  the tasks that ended in it, and the unfinished ones that first failed in it.
- Rules read `task.<field>` (the task's record) and `baseline.<field>`.
  Fields: `wall_ms`, `cel_ms`, `resume_max_ms`, `ttfe_max_ms`, `startup_ms`,
  `think_ms`, `retries`, `cycles`, `steps`. They are doubles, so numbers need a
  decimal point (`1500.0`). A task without a field a rule reads isn't judged
  by that rule, and rules can't test for a field with `has()`. The built-in
  reasons are `resume_failed`, `execute_failed`, `suspend_failed` and `failed`.
- `baseline.<field>` is the median over the clean finished tasks of the run
  of `baseline_test` that `paired_run` returns: the latest run of that test
  from the same invocation. Without one (no `baseline_test`, no such run, a
  run in the other lifecycle mode), the rules that read it are skipped and
  listed in `rules_skipped`, and `baseline_note` says why; `require_baseline:
  true` makes that `invalid`, keeping the values.
- Facts for `values`: `unfinished`, `window_hours`, and the cluster's `nodes`,
  `vcpus` and `gb_ram` (missing when the run didn't record them).
- Values: `unfinished` (tasks that never finished, all bad),
  `good_wall_ms_p50/p90/p99`, `startup_failure_attempts` and `_episodes`,
  `window_start_ts`, `window_end_ts`, `window_s`, `window_source`,
  `cutoff_source`, `<field>_ratio_p50/p90/p99` (task over baseline, for each
  field a rule reads from both), `baseline_run`, `baseline_note`,
  `baseline_tasks`, `baseline_<field>` for each field the rules read from the
  baseline, `lifecycle_mode` (`async`, `sync`, `mixed`, or empty when the run
  doesn't say; `wall_ms` covers the snapshot upload only in the sync mode),
  `skipped_records`.
- Invalid: no `logs.txt` or no task records, an empty window or one without
  tasks, and `require_baseline` with skipped rules.

#### `json_lines`

```yaml
source_cfg:
  kind: json_lines
  file: stats.jsonl                          # one of the run's files, by name
  select: 'line.metric.startsWith("grpc_")'  # optional: the lines to judge; all by default
  rules:
  - {name: failed_requests, cel: "line.measurements.failure_count == 0.0"}
  values: {good_ratio_pct: "round(100.0 * good / records, 2)"}
```

- `select` and rules read `line`, the line's JSON object. Its values are
  coerced as in `run_results`: numbers and numeric strings become doubles,
  `"true"`/`"false"` become bools.
- A blank line is skipped. A line that isn't a JSON object makes the variable
  `invalid`, and so does a `select` or rule that fails to evaluate on a line,
  for example on a key the line lacks: test optional keys with `has()`.

### `run_results`

The measurements of one `stats.jsonl` row:

```yaml
source_cfg:
  metric: trial_summary                    # if repeated, the last row wins
  measurements: [aggregate_failure_ratio]  # returned under the same names
```

Numeric strings become numbers and `"true"`/`"false"` bools; a measurement the
row lacks, or a number that isn't finite (NaN, an infinity, `1e999`), is null.

## Outputs

- `evaluation.json`: the run, the `summary` (`status`, `passed`, `failed`,
  `invalid`), and per evaluation its status and reason, each condition with
  the value and limit it compared, each variable with its values (and why it
  is invalid), and the evaluation as loaded (`config`). `error` is set, and
  nothing is judged, when the evaluations failed to load.
- `stats.jsonl` rows in the file's usual five keys: one `evaluation_<name>` row
  per evaluation, with every value of every variable as `<variable>__<value>`,
  and one `evaluation_summary` row. Measurements are strings, or null.

## Layout

| file | |
|---|---|
| `config.py` | loads and validates an `evaluations` block |
| `checks.py` | the threshold check |
| `evaluator.py` | runs the source hooks and judges a run: the content of `evaluation.json` |
| `outputs.py` | `evaluation.json` and the `stats.jsonl` rows |
| `sources/` | the hooks (`base.py`) and the sources |

## Tests

```sh
hack/verify/benchmarking-python.sh   # as CI runs them; or, with requirements.txt installed:
cd benchmarking/evaluate && python3 -m unittest discover -s unit_tests
```
