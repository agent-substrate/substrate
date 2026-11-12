# Run evaluations

`evaluate` judges a finished benchmark run against CEL conditions declared in
its test entry, and returns the verdicts as a `Report`.
Depends on [`cel-expr-python`](https://github.com/cel-expr/cel-python) and
[pydantic](https://docs.pydantic.dev/) (`requirements.txt`, Python 3.11 or later).

## Model

- A **source** reads the run and returns several named **values**, or
  says it can't judge the run (`invalid`, with a reason). Sources:
  `run_results`.
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
  64 characters, and are unique where they live. CEL itself rejects a
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
| `init(run)` | once, when the run starts (its files may not exist yet) | takes the `BenchmarkRunContext`, the run being evaluated: which run it is (`identity`: `uri`, and `test_name`, `tag`, `run_date`, `run_ts` when known), where it is stored, and its stored artifacts (`artifact(name)`, a local path or None); the source chooses what it reads |
| `query(cfg)` | after the run, once per distinct `source_cfg` | returns named values (numbers, bools, strings or null), or raises `SourceInvalid(reason, values)` when it can't judge the run |

Value names come from `source_cfg` or are fixed by the source, never from the
run's data. The evaluator creates one instance of each source the evaluations
name. The values of a `SourceInvalid` are kept in the results, while the
variable and the conditions reading it become `invalid`. Any other exception
from a hook makes the source's variables `invalid` with the error, and so does
a value of another type, such as a list or a mapping, or a value name that
isn't a non-empty string. Sources never write files. A new source is a class
in `sources/` and a line in `SOURCES`.

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

## Layout

| file | |
|---|---|
| `config.py` | loads and validates an `evaluations` block (pydantic models) |
| `cel_env.py` | shared CEL environment, `round()`, and evaluation |
| `evaluator.py` | runs the source hooks and judges a run into a `Report` |
| `sources/` | the hooks (`base.py`) and the sources |

## Tests

```sh
hack/verify/benchmarking-python.sh   # as CI runs them; or, with requirements.txt installed:
cd benchmarking/evaluate && python3 -m unittest discover -s unit_tests
```
