# Run evaluations

`evaluate` judges a finished benchmark run against conditions declared in its
test entry, and writes the verdicts next to the run's other results.
Standard library only.

## Model

- A **source** reads a run's files and returns several named **values**, or
  says it can't judge the run (`invalid`, with a reason). Sources:
  `run_results`.
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
  are unique where they live. `summary` is reserved. Unknown keys are errors.
  There is no version field while the schema is experimental.
- A threshold `check` is `{value, op, limit}`. `op` is one of `<= < >= > ==
  !=`; `< <= > >=` compare numbers only, and `==` and `!=` also compare bools
  and strings. `limit` is a literal, or relative: `{ref, times (default 1),
  at_least, at_most}`.
- A value that is missing or null (not measured), or whose variable is
  invalid, makes the condition `invalid`, never 0.

## Sources

A source is a class with three hooks (`sources/base.py`):

| hook | runs | does |
|---|---|---|
| `validate(cfg)`, a classmethod | when the evaluations are loaded | raises `ValueError` for a bad `source_cfg` |
| `init(run)` | once, when the run starts (its files may not exist yet) | takes the `BenchmarkRunContext`: the run's files by name |
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

### `run_results`

The measurements of one `stats.jsonl` row:

```yaml
source_cfg:
  metric: trial_summary                    # if repeated, the last row wins
  measurements: [aggregate_failure_ratio]  # returned under the same names
```

Numeric strings become numbers and `"true"`/`"false"` bools; a measurement the
row lacks is null.

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
hack/verify/benchmarking-python.sh   # as CI runs them; or:
cd benchmarking/evaluate && python3 -m unittest discover -s unit_tests
```
