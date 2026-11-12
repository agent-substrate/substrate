# Run evaluations

`evaluate` judges a finished benchmark run against CEL conditions declared in
its test entry, and returns the verdicts as a `Report`.
Depends on [`cel-expr-python`](https://github.com/cel-expr/cel-python) and
[pydantic](https://docs.pydantic.dev/) (`requirements.txt`, Python 3.11 or later).

## Model

- A **source** reads the run and returns several named **values**, or
  says it can't judge the run (`invalid`, with a reason). Sources:
  `cel`, `run_results`.
- A **variable** is one source query: `{name, source, source_cfg}`. Its values
  are exposed to CEL as a map under `<variable>` (`<variable>.<value>`, or
  `<variable>["<value>"]` when `<value>` contains dots or CEL keywords).
- A **condition** is `{name, cel}`: a [CEL](https://cel.dev/) boolean
  expression over the variables.
- An **evaluation** is a named set of variables and conditions, AND-ed: it
  `fail`s if any condition failed, else is `invalid` if any condition or
  variable is invalid, else `pass`es. Evaluations are judged independently, with no hierarchy between
  them.
- The run's **summary** combines its evaluations the same way.
- `passed` is true for `pass` only, and false for both `fail` and `invalid`,
  so an alert should key on `passed`.

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
    - {name: failure_rate, cel: "trial.aggregate_failure_ratio <= 0.001"}
    - {name: worker_memory, cel: "server.working_set_node_max_gb < 0.85 * trial.allocatable_ram_gb"}
```

- Evaluation, variable and condition names match `^[a-z][a-z0-9_]*$`, at most
  64 characters, and are unique where they live. A variable name can't contain
  `__` or end in `_` (see [Outputs](#outputs)). CEL itself rejects a
  variable named after a CEL type (`int`, `map`) or a keyword (`in`, `true`)
  when a condition compiles. Value names come from the source
  and may be any non-empty string
  (`locust_ResumeActor`, `metric_nighthawk.builtin_attempted_rps`, `p99.9`).
  Unknown or non-string mapping keys are errors, and `source_cfg` holds JSON
  values only, so a YAML date in it is an error too. There is no version field
  while the schema is experimental.
- Conditions are standard CEL, compiled when the evaluations load, with
  `round(x, digits)` added. Each must evaluate to `bool`: a bare value such as
  `trial.ok` compiles, and a result that isn't a bool makes the condition `invalid`.
- Values enter CEL as doubles, whatever number type the source returns,
  since they come from JSON, which has one number type, and as ints `a / b`
  would be integer division. The results keep the source's own value types.
  CEL has no arithmetic between int and double, so write `2.0 * t.x`, not
  `2 * t.x`; the same goes for comparing a double expression
  (`round(t.x, 1) >= 90.0`). A value compared directly may use an int
  literal (`t.x <= 1500`). Mixing int and double fails when the evaluations
  load if CEL can see both types; otherwise (`t.x * 100 >= 99`, where
  `t.x` is only known at run time) the condition is `invalid` on every run,
  so it never passes.
- Missing or null values are omitted from `<variable>`'s map: reading one
  directly fails lookup and makes the condition `invalid`, while
  `has(<variable>.<key>)` returns `false` without error. A value CEL can't
  hold makes the conditions `invalid`. An `invalid` variable is left out of
  the CEL data, so a condition that reads it is `invalid` with CEL's error,
  and the evaluation is `invalid` even if no condition reads it: a source
  failure never lets an evaluation pass.
- CEL `==` and `!=` across distinct dynamic types (`1.1 == "1.10"`,
  `true == 1.0`) evaluate to `false` and `true` without raising an error. Float
  division inside a condition follows IEEE-754 (`1.0 / 0.0` produces `+Inf`
  and `0.0 / 0.0` produces `NaN`).

## Sources

A source is a class with three hooks (`sources/base.py`):

| hook | runs | does |
|---|---|---|
| `validate(cfg)`, a classmethod | when the evaluations are loaded | raises `ValueError` for a bad `source_cfg` |
| `init(run)` | once, when the run starts (its files may not exist yet) | takes the `BenchmarkRunContext`, the run being evaluated: which run it is (`identity`: `uri`, and `test_name`, `tag`, `run_date`, `run_ts` when known), where it is stored, and its stored artifacts (`artifact(name)`, a local path or None); the source chooses what it reads. `paired_run(test)` returns the run of another test from the same invocation or raises `LookupError` |
| `query(cfg)` | after the run, once per distinct `source_cfg` | returns named values (numbers, bools, strings or null), or raises `SourceInvalid(reason, values)` when it can't judge the run |

Value names come from `source_cfg` or are fixed by the source, never from the
run's data. The evaluator creates one instance of each source the evaluations
name. The values of a `SourceInvalid` are kept in the results, while the
variable and the conditions reading it become `invalid`. Any other exception
from a hook makes the source's variables `invalid` with the error, and so does
a value of another type, such as a list or a mapping, or a value name that
isn't a non-empty string. Sources never write files. A new source is a class
in `sources/` and a line in `SOURCES`.

### `cel`

Binds data read from runs to CEL, and evaluates named expressions into
values. The source has no notion of what the data means: what counts as
good, which run to compare against, and every formula live in the config.

```yaml
source: cel
source_cfg:
  inputs:                    # the data, each a CEL variable
    stats: {reader: json_lines, file: stats.jsonl}
  locals:                    # optional: ordered list of named in-between results
  - name: trials
    cel: "stats.lines.filter(l, l.metric == 'trial_summary')"
  checks:                    # optional: what must hold for the values to mean anything
    has_trial: "size(trials) > 0"
  values:                    # the outputs
    cores: "size(trials) > 0 ? dyn(trials[size(trials) - 1].measurements.allocatable_cores) : null"
```

- `inputs: {<name>: {reader: <reader>, test: <test>?, ...options}}`: each input
  names a reader and its options, and the reader's data becomes the CEL
  variable `<name>`. By default the reader reads this run. With `test:` it
  reads that test's run from the same invocation (`paired_run`), which lets a
  config compare two runs.
- `locals: [{name: <name>, cel: <cel>}, ...]`: ordered list of named in-between
  results, such as a filtered list or a median. They are evaluated top to
  bottom, so each local may read inputs and earlier locals by bare name.
  A local that fails to evaluate is left out, so only what reads it fails,
  and that error includes the local's own. Entries can share a list of locals
  through a YAML anchor and use only some of them. A local holds whatever CEL
  produces, including `timestamp` and `duration`.
- `checks: {<name>: <cel bool>}`: conditions the data must meet before the
  values mean anything (for example, both runs used the same mode). A check
  that is false or fails to evaluate makes the variable `invalid`, and the
  reason names the check. Unlike an evaluation's conditions, a check judges
  the data, not the run.
- `values: {<name>: <cel>}`: the outputs. Each must give a double, bool,
  string or `null`; a whole number such as `size(x)` must be converted with
  `double()`. Use CEL's native `null` with `dyn()` for missing or empty cases
  (`size(xs) > 0 ? dyn(...) : null`). A non-finite float (`0.0 / 0.0`) also
  becomes `null`.
- Names match `^[a-z][a-z0-9_]*$`. An input and a local can't share a name.
  Whole numbers stay integers here (`size(x) - 1` indexes a list), so write
  doubles as `1500.0`.
- Functions beyond standard CEL: the CEL math extension (`ext_math`:
  `math.least`, `math.greatest`, `math.round`, ...), plus two explicitly
  defined numeric helpers: `round(x, digits)` (Python-style rounding, shared
  with evaluation conditions) and `percentile(xs, p)` over `list(double)`
  (linear interpolation between the closest ranks, `p` from 0 to 100, raising
  an error on an empty list like `math.least`/`math.greatest`).

What is checked, when, and what happens on a mismatch:

| what | when | on a mismatch |
|---|---|---|
| unknown `source_cfg` key or reader, bad reader options, bad names | config load | the evaluations fail to load |
| `select` / `until` expressions on `json_lines` | config load | load error |
| every expression compiles with inferred local types, and value return types can be double, bool, string or null | config load | load error |
| reader output matches its declared `fields` and types | run time | `TypeError` makes the variable `invalid` |
| missing JSON keys inside `lines` or `data` | run time | reading a missing key makes the variable `invalid`, unless guarded with `has()` |
| a reader can't read its run (absent file, unhashable `unique_by`, `select` eval error, no run of `test`) | run time | `invalid`, and the reason names the input |
| a check or value fails to evaluate (including reading a local that failed), or a value gives another type | run time | `invalid`, and the reason names it and any failed local |

A reader is a class in `sources/cel/` and a line in `READERS`
(`sources/cel/reader.py`): its `fields` (the top-level fields of its data,
with their CEL types), `validate(options)` and `read(run, options)`, which
raises `ReadError` when the run can't be read. A reader exposes data and
passes no judgment on it. All numbers are doubles, and a field a record lacks
is absent rather than null.

#### `json`

One JSON object file from the run: `{reader: json, file: server_summary.json}`.

- `data`: the parsed JSON object, with leaf coercion as in `run_results`.
- `uri`: where the run is stored.

#### `json_lines`

Filtered lines of one of the run's JSON-lines or prefixed-JSON files:

```yaml
records:
  reader: json_lines
  file: logs.txt
  prefix: "[boomer] "
  until: "has(line.msg) && line.msg == 'sweperf shutdown started'"
  select: "has(line.msg) && line.msg == 'sweperf task record' && has(line.v) && line.v == 1.0"
```

- `lines`: selected JSON objects, in file order. Values are coerced as in
  `run_results`: numbers and numeric strings without `_` become doubles,
  `"true"`/`"false"` become bools, and a null or non-finite number is left out.
- `unparsed_lines`: `[{line_no, text, error}]` for each prefixed (or, when
  `prefix` is unset, non-blank) line that isn't a JSON object.
- `uri`: where the run is stored.
- Optional options:
  - `prefix`: skip lines not starting with `prefix` and strip it before
    parsing JSON (without it, blank lines are skipped).
  - `until`: CEL boolean expression over `line`; stop reading when a parsed
    object matches (that line is not emitted).
  - `select`: CEL boolean expression over `line`; keep only matching objects.
  - `unique_by`: field name; keep only the first selected object per distinct
    value of that field (useful to bound list size before CEL evaluation).

Goodput, as config only: a task is good when it ended in the steady phase,
didn't fail, woke within limits (slowest resume 1.5 s, slowest time to first
execute 1 s), and its command time (`cel_ms`) and wall time stayed within 1.5x
of the 1-user run's medians.

```yaml
- name: sweperf_astropy_150_users_think
  # ...
  evaluations:
  - name: goodput_strict
    variables:
    - name: goodput
      source: cel
      source_cfg:
        inputs:
          records:
            reader: json_lines
            file: logs.txt
            prefix: "[boomer] "
            until: "has(line.msg) && line.msg == 'sweperf shutdown started'"
            select: "has(line.msg) && line.msg == 'sweperf task record' && has(line.v) && line.v == 1.0"
          failures:
            reader: json_lines
            file: logs.txt
            prefix: "[boomer] "
            until: "has(line.msg) && line.msg == 'sweperf shutdown started'"
            select: "has(line.msg) && line.msg in ['ResumeActor failed in step', 'execute steps failed', 'SuspendActor failed', 'SuspendActorAsync failed'] && has(line.actor) && line.actor != ''"
            unique_by: actor
          shutdown:
            reader: json_lines
            file: logs.txt
            prefix: "[boomer] "
            select: "has(line.msg) && line.msg == 'sweperf shutdown started'"
          summary: {reader: json, file: server_summary.json}
          stats: {reader: json_lines, file: stats.jsonl}
          one_user:
            reader: json_lines
            file: logs.txt
            prefix: "[boomer] "
            until: "has(line.msg) && line.msg == 'sweperf shutdown started'"
            select: "has(line.msg) && line.msg == 'sweperf task record' && has(line.v) && line.v == 1.0"
            test: sweperf_astropy_1_user
          one_user_stats: {reader: json_lines, file: stats.jsonl, test: sweperf_astropy_1_user}
        locals:
        - {name: lo, cel: "timestamp(int(summary.data.metadata.steady_start_ts))"}
        - {name: steady_end, cel: "timestamp(int(summary.data.metadata.steady_end_ts))"}
        - {name: stop, cel: "size(shutdown.lines) > 0 ? timestamp(shutdown.lines[0].time) : steady_end"}
        - {name: hi, cel: "stop < steady_end ? stop : steady_end"}
        - {name: window_s_raw, cel: "double((hi - lo).getSeconds()) + double((hi - lo).getMilliseconds()) / 1000.0"}
        - {name: hours, cel: "math.greatest(window_s_raw, 0.0) / 3600.0"}
        - {name: done, cel: "records.lines.filter(t, timestamp(t.end) >= lo && timestamp(t.end) <= hi)"}
        - {name: recorded, cel: "records.lines.map(t, t.actor)"}
        - {name: lost, cel: "failures.lines.filter(f, !(f.actor in recorded) && timestamp(f.time) >= lo && timestamp(f.time) <= hi)"}
        - {name: ref_cel_ms, cel: "round(percentile(one_user.lines.filter(t, !t.failed).map(t, t.cel_ms), 50.0), 3)"}
        - {name: ref_wall_ms, cel: "round(percentile(one_user.lines.filter(t, !t.failed).map(t, t.wall_ms), 50.0), 3)"}
        - {name: trials, cel: "stats.lines.filter(l, l.metric == 'trial_summary')"}
        - {name: cores, cel: "trials[size(trials) - 1].measurements.allocatable_cores"}
        - {name: run_async, cel: "stats.lines.exists(l, has(l.metric) && l.metric == 'grpc_SuspendActorAsync')"}
        - {name: run_sync, cel: "stats.lines.exists(l, has(l.metric) && l.metric == 'grpc_SuspendActor')"}
        - {name: one_async, cel: "one_user_stats.lines.exists(l, has(l.metric) && l.metric == 'grpc_SuspendActorAsync')"}
        - {name: one_sync, cel: "one_user_stats.lines.exists(l, has(l.metric) && l.metric == 'grpc_SuspendActor')"}
        - {name: run_mode, cel: "run_async && run_sync ? 'mixed' : (run_async ? 'async' : (run_sync ? 'sync' : ''))"}
        - {name: one_mode, cel: "one_async && one_sync ? 'mixed' : (one_async ? 'async' : (one_sync ? 'sync' : ''))"}
        - {name: good, cel: "done.filter(t, !t.failed && t.resume_max_ms <= 1500.0 && t.ttfe_max_ms <= 1000.0 && t.cel_ms <= 1.5 * ref_cel_ms && t.wall_ms <= 1.5 * ref_wall_ms)"}
        checks:
          has_shutdown: "size(shutdown.lines) > 0"
          same_lifecycle_mode: "'' in [run_mode, one_mode] || run_mode == one_mode"
          one_user_has_clean_tasks: "one_user.lines.exists(t, !t.failed)"
          tasks_in_steady_window: "size(done) + size(lost) > 0"
        values:
          good: "double(size(good))"
          good_ratio_pct: "round(100.0 * double(size(good)) / double(size(done) + size(lost)), 2)"
          good_per_vcpu_hour: "round(double(size(good)) / hours / cores, 4)"
          cel_ms_ratio_p90: "size(done) > 0 ? dyn(round(percentile(done.map(t, t.cel_ms / ref_cel_ms), 90.0), 3)) : null"
    conditions:
    - {name: good_ratio, cel: "goodput.good_ratio_pct >= 80.0"}
```

### `run_results`

The measurements of one `stats.jsonl` row:

```yaml
source_cfg:
  metric: trial_summary                    # if repeated, the last row wins
  measurements: [aggregate_failure_ratio]  # returned under the same names
```

Numeric strings without `_` become floats and `"true"`/`"false"` bools. Strings
with `_` (`"2026_11"`) stay strings. A measurement the row lacks, or a number
that isn't finite (NaN, an infinity, `1e999`), is null. Blank lines are skipped;
any other line that isn't a JSON object with a `metric` makes the variable
`invalid`, naming the line, since the runner writes one such object per line.

## Results

`Evaluator.evaluate` returns a `Report` (`evaluator.py`): the run, the
`summary` (`status`, `passed`, `failed`, `invalid`), and per evaluation its
status and reason, each condition with its CEL expression and verdict, each
variable with its values (and why it is invalid), and the evaluation as loaded
(`config`). An evaluation's reason names the conditions that failed, else the
invalid conditions and variables.

## Outputs

- `evaluation.json`: the `Report` as JSON. When the evaluations failed to
  load, `error` says why and nothing is judged.
- `stats.jsonl` rows in the file's usual five keys: one `evaluation_<name>` row
  per evaluation and one `evaluations` row for the summary. Measurements are
  strings, or null. An evaluation's row has its `status`, `passed`, `reason`,
  `failed_conditions` and `invalid_conditions`, and each value as
  `<variable>__<value name>`. Value names follow the rule in
  [Sources](#sources), so the keys stay bounded. A variable name can't contain
  `__` or end in `_`, so no two values share a key.

## In the runner

The orchestrator passes the whole `locust` test entry to `runner.py` as
`--test-entry` (JSON), and the runner reads its `evaluations`. Before the
load test, the runner loads them and runs the
sources' `init`, so a config error is in `logs.txt` from the start. As its
last step after the run, it runs the queries, writes `evaluation.json` (a
config error goes there too), uploads it with the run's other files, appends
the rows to `stats.jsonl`, and logs one line per evaluation. `status.json` and
the exit code don't change: an evaluation never fails a run.

The runner's `paired_run(test)` returns the run of `test` under the same
`--dest` with the same `--tag` and the latest `run_ts`, at most 6 h before
this run (`rundir.py`). When both runs record a cluster (`trial_summary`'s
`metadata_cluster_name`), it must be the same. On a `gs://` `--dest`, this
needs read access to the bucket (see the
[automation README](../automation/README.md#iam-prerequisites)).

## Evaluating stored runs

```sh
cd benchmarking
python3 -m evaluate RUN... (--evaluations JSON|@FILE | --tests-file FILE --test-name NAME | --from-run) [--out DIR]
python3 -m evaluate --check-config --tests-file automation/tests.yaml   # check the blocks only
```

`RUN` is a run directory or `gs://.../runs/<test>/run_date=.../run_ts=.../run_tag=.../`,
whose artifacts are each downloaded to a temporary directory when a source
first reads one (through `gcloud storage`, or the google-cloud-storage library
when installed). A `RUN` stored in the runner's
layout, local or `gs://`, has the paired runs the runner would give it, under
the same `<dest>`, and any other `RUN` has none. `--from-run` re-reads the
evaluations the run was judged with, from its `evaluation.json`. The outputs
go to `--out`, by default `./evaluate-out/<test>/run_ts=<ts>/`, never into the
stored run: `evaluation.json` and `evaluation_rows.jsonl`. Exit status: 0
evaluated (whatever the verdict), 2 config error, 1 I/O error. `--tests-file`
needs PyYAML.

## Layout

| file | |
|---|---|
| `config.py` | loads and validates an `evaluations` block (pydantic models) |
| `cel_env.py` | shared CEL environment, helpers (`round`, `percentile`), and evaluation |
| `evaluator.py` | runs the source hooks and judges a run into a `Report` |
| `outputs.py` | `evaluation.json` and the `stats.jsonl` rows |
| `sources/` | the hooks (`base.py`) and the sources |
| `rundir.py` | the runner's storage layout: opening local or `gs://` runs (a `gs://` artifact is downloaded on first use), finding a paired run |
| `__init__.py` | `prepare` and `write`, which `runner.py` calls |
| `__main__.py` | the CLI |

## Tests

```sh
hack/verify/benchmarking-python.sh   # as CI runs them; or, with requirements.txt installed:
cd benchmarking/evaluate && python3 -m unittest discover -s unit_tests
```
