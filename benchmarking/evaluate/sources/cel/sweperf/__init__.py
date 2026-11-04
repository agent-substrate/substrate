# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""sweperf_tasks: the SWE-Perf tasks of a run, as records of the cel source.

A task is one task record line in logs.txt, one JSON line per finished unit of
work (see "Sweperf Log Lines" in docs/guides/benchmarking.md). The benchmark
only records facts and the rules judge them, so a rule change is a config edit
and a re-evaluation, with no new benchmark run.

  kind: sweperf_tasks
  window: steady                # steady (default) | whole; one window per query
  baseline_test: sweperf_astropy_1_user  # the test whose run is the baseline
  rules:
  - {name: slow_resume, cel: "task.resume_max_ms <= 1500.0"}
  - {name: slow_wall, cel: "task.wall_ms <= 1.5 * baseline.wall_ms"}
  values:
    good_per_vcpu_hour: "round(good / window_hours / vcpus, 4)"

A task that failed is bad whatever the rules say, with its failure as the
reason (tasks.py). Rules read task.<field>, from the task's record, and
baseline.<field>, the median over the finished tasks without failures of the
run of baseline_test that BenchmarkRunContext.paired_run returns. Without one,
a rule that reads it makes the variable invalid, and the reason says why.
Each run is read once, however many queries there are.

`values:` expressions also read unfinished (tasks that never finished, all
bad), window_hours, and the cluster's nodes, vcpus and gb_ram, which are
missing when the run didn't record them.
"""

import statistics
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping, Optional

from cel_expr_python import cel

from ...base import BenchmarkRunContext, SourceInvalid
from ...run_results import read_stats
from .. import rules
from ..kind import Kind, Record, Records, Summary, values_env
from . import metrics, parse, tasks

WINDOWS = ("steady", "whole")
# The run's files load_run reads, by name; only logs.txt is required.
INPUTS = ("logs.txt", "stats.jsonl", "server_summary.json", "stats_history.csv")
UNAVAILABLE = "baseline unavailable: "
TASK, BASELINE = "task", "baseline"
# Every field is a double, and so is every whole number in a rule (rules.Env).
ENV = rules.Env({f"{ns}.{f}": cel.Type.DOUBLE for ns in (TASK, BASELINE) for f in tasks.FIELDS})
FACTS = ("unfinished", "window_hours", "nodes", "vcpus", "gb_ram")
CAPACITY = {"nodes": "node_count", "vcpus": "allocatable_cores", "gb_ram": "allocatable_ram_gb"}
# The values summary gives besides those of metrics.window_values.
OWN_VALUES = {"window_source", "cutoff_source", "lifecycle_mode", "skipped_records", "baseline_run", "baseline_tasks"}


@dataclass
class Run:
    """One run's files, read and split into tasks: what every query of the run shares."""
    log: parse.RunLog
    tasks: list[tasks.Task]
    startup_ts: list[float]  # startup failures before the cut-off
    episode_ts: list[float]  # the starts of their episodes
    cut_source: str
    windows: dict[str, tuple[tuple[float, float], str]]  # steady and whole: (start_ts, end_ts), and what says so
    capacity: dict[str, Optional[float]]
    lifecycle_mode: str


@dataclass(frozen=True)
class Baseline:
    """The refs from a baseline run (none when unavailable), where they came from, and why there are none."""
    refs: Mapping[str, float] = field(default_factory=dict)
    run: str = ""
    tasks: int = 0
    note: str = ""


@dataclass(kw_only=True)
class TaskRecords(Records):
    """The tasks of one query's window, and what its values report about them."""
    run: Run
    window: str
    span: tuple[float, float]
    window_source: str
    baseline: Baseline


def record(t: tasks.Task) -> Record:
    """A task as a record: its failure, or task.<field> for each field its record logged."""
    data = {f"{TASK}.{f}": float(v) for f in tasks.FIELDS if t.record and (v := getattr(t.record, f)) is not None}
    return Record(where=t.actor, data=data, failure=tasks.failure(t), item=t)


def run_data(refs: Mapping[str, float]) -> dict[str, float]:
    """baseline.<field> for each field of the baseline, 0 included."""
    return {f"{BASELINE}.{f}": float(v) for f, v in refs.items()}


def fields(rule: rules.Rule, ns: str) -> list[str]:
    """The fields rule reads from ns (TASK or BASELINE)."""
    return [v.partition(".")[2] for v in rule.reads if v.startswith(ns + ".")]


def ratio_fields(compiled: list[rules.Rule]) -> list[str]:
    """The fields a rule reads from both the task and the baseline, in order."""
    return list(dict.fromkeys(f for r in compiled for f in fields(r, BASELINE) if f in fields(r, TASK)))


class SweperfTasks(Kind):
    ENV = ENV
    VALUES_ENV = values_env(FACTS)
    RUN_VARIABLES = frozenset(v for v in ENV.variables if v.startswith(BASELINE + "."))
    FAILURE_REASONS = tasks.FAILURE_REASON_ORDER
    CFG_KEYS = frozenset({"window", "baseline_test"})

    @classmethod
    def validate(cls, cfg: Mapping[str, Any]) -> None:
        if cfg.get("window", "steady") not in WINDOWS:
            raise ValueError(f"window: {cfg['window']!r} is not one of {list(WINDOWS)}")
        if "baseline_test" in cfg and (not isinstance(cfg["baseline_test"], str) or not cfg["baseline_test"]):
            raise ValueError("baseline_test: must be the name of a test")

    @classmethod
    def value_names(cls, compiled: list[rules.Rule]) -> set[str]:
        ratios = {f"{f}_ratio_p{p}" for f in ratio_fields(compiled) for p in (50, 90, 99)}
        return metrics.VALUES | OWN_VALUES | ratios | {f"baseline_{f}" for r in compiled for f in fields(r, BASELINE)}

    def __init__(self, run: BenchmarkRunContext) -> None:
        super().__init__(run)
        self._run: Optional[Run] = None
        self._run_invalid = ""
        self._baselines: dict[str, Baseline] = {}

    def records(self, cfg: Mapping[str, Any]) -> TaskRecords:
        """The tasks of the window: finished tasks that ended in it, and unfinished ones that first failed in it."""
        run = self._read()
        window = cfg.get("window", "steady")
        span, source = run.windows[window]
        test = cfg.get("baseline_test")
        base = (self._baseline(test, run.lifecycle_mode) if test
                else Baseline(note=f"{UNAVAILABLE}no baseline_test in source_cfg"))
        found = [record(t) for t in run.tasks
                 if metrics.in_window(t.end if t.finished else t.first_failure_t, span)]
        return TaskRecords(found, run_data(base.refs), run_note=base.note, run=run, window=window, span=span,
                           window_source=source, baseline=base)

    def summary(self, cfg: Mapping[str, Any], found: TaskRecords, compiled: list[rules.Rule],
                reasons: list[list[str]]) -> Summary:
        run, base, refs = found.run, found.baseline, dict(found.baseline.refs)
        values = metrics.window_values([r.item for r in found.records], [not r for r in reasons], run.startup_ts,
                                       run.episode_ts, found.span, refs, ratio_fields(compiled))
        values.update({
            "window_source": found.window_source, "cutoff_source": run.cut_source,
            "lifecycle_mode": run.lifecycle_mode, "skipped_records": run.log.skipped_records,
            "baseline_run": base.run, "baseline_tasks": base.tasks,
        })
        for name in dict.fromkeys(f for r in compiled for f in fields(r, BASELINE)):
            values[f"baseline_{name}"] = refs[name]
        facts = {"unfinished": values["unfinished"], **{fact: run.capacity[key] for fact, key in CAPACITY.items()},
                 "window_hours": max(0.0, found.span[1] - found.span[0]) / 3600.0}
        invalid = ""
        if not values["window_s"]:
            invalid = f"empty {found.window} window"
        elif not found.records:
            invalid = f"no tasks in the {found.window} window"
        return Summary(values, facts, invalid)

    def _read(self) -> Run:
        """This run, read on the first query; SourceInvalid when it has nothing to judge."""
        if self._run is None and not self._run_invalid:
            try:
                self._run = load_run(self.run)
            except ValueError as e:
                self._run_invalid = str(e)
        if self._run is None:
            raise SourceInvalid(self._run_invalid)
        return self._run

    def _baseline(self, test: str, mode: str) -> Baseline:
        """The baseline from the run of test paired with this one (mode: this run's lifecycle mode), once per test."""
        if test not in self._baselines:
            self._baselines[test] = _paired_baseline(self.run, test, mode)
        return self._baselines[test]


def _paired_baseline(run: BenchmarkRunContext, test: str, mode: str) -> Baseline:
    try:
        paired = run.paired_run(test)
    except LookupError as e:
        return Baseline(note=f"{UNAVAILABLE}{e}")
    try:
        base = load_run(paired)
        refs, n = baseline_refs(base)
    except ValueError as e:
        return Baseline(note=f"{UNAVAILABLE}{test} run {paired.label}".rstrip() + f": {e}")
    if mode and base.lifecycle_mode and base.lifecycle_mode != mode:
        # A task's wall_ms covers the snapshot upload only in the sync mode.
        return Baseline(note=f"{UNAVAILABLE}lifecycle mode differs ({mode} vs {base.lifecycle_mode})")
    return Baseline(refs=refs, run=paired.label, tasks=n)


# --- Computation -----------------------------------------------------------------

def steady_window(meta: dict[str, Any], history: Optional[Path], log_ts: list[float],
                  cut: Optional[float]) -> tuple[tuple[float, float], str]:
    """server_summary's steady window, else the runner's rule on stats_history.csv,
    else the run's start..end, else the log's first..last line before the cut-off.
    The window ends at the cut-off at the latest."""
    lo, hi = parse.num(meta.get("steady_start_ts")), parse.num(meta.get("steady_end_ts"))
    start, end = parse.num(meta.get("start_ts")) or 0.0, parse.num(meta.get("end_ts")) or 0.0
    if lo and hi and hi > lo:
        window, source = (lo, hi), "server_summary"
    elif history and (hist := parse.steady_from_history(history, start, end)):
        window, source = hist, "stats_history"
    elif end > start > 0:
        window, source = (start, end), "run"
    else:
        ts = [t for t in log_ts if cut is None or t <= cut]
        window, source = ((min(ts), max(ts)) if ts else (0.0, 0.0)), "log"
    if cut is not None and cut > window[0]:
        window = (window[0], min(window[1], cut))
    return window, source


def load_run(run: BenchmarkRunContext) -> Run:
    """Reads a run's files (see INPUTS) and builds its tasks up to the cut-off.
    Raises ValueError for a run without logs.txt or without task records."""
    logs = run.file("logs.txt")
    if logs is None:
        raise ValueError("logs.txt absent")
    log = parse.read_run(logs)
    if not log.records:
        raise ValueError("no task records in logs.txt: no task finished, or the workload logs none")
    stats = read_stats(run.file("stats.jsonl"))
    meta = parse.read_server_metadata(run.file("server_summary.json"))
    trial = (stats.get("trial_summary") or {}).get("measurements") or {}
    cut, cut_source = tasks.cutoff(log.events, parse.epoch(trial.get("run_end")), parse.num(meta.get("end_ts")))
    found, startup = tasks.build_tasks(log, cut)
    log_ts = [r.t for r in log.records] + [ev.t for ev in log.events]
    start = parse.num(meta.get("start_ts"))
    whole = (start or min(log_ts), cut if cut is not None else max(log_ts))
    return Run(
        log=log, tasks=found, startup_ts=[s.t for s in startup],
        episode_ts=tasks.startup_episodes(startup, log.records), cut_source=cut_source,
        windows={"steady": steady_window(meta, run.file("stats_history.csv"), log_ts, cut),
                 "whole": (whole, "run" if start else "log")},
        capacity={k: parse.num(trial.get(k)) for k in ("node_count", "allocatable_cores", "allocatable_ram_gb")},
        lifecycle_mode=parse.lifecycle_mode(stats, log.lifecycle_flag))


def refs_from_records(records: list[parse.TaskRecord]) -> dict[str, float]:
    """For each rule field, the median over the non-failed records that logged
    it. A field no such record logged is left out."""
    clean = [r for r in records if not r.failed]
    refs = {}
    for f in tasks.FIELDS:
        values = [getattr(r, f) for r in clean if getattr(r, f) is not None]
        if values:
            refs[f] = round(float(statistics.median(values)), 3)
    return refs


def baseline_refs(run: Run) -> tuple[dict[str, float], int]:
    """The baseline values from a baseline run, and how many tasks they come from:
    refs_from_records over its finished tasks without failures, built like any
    run's, up to the cut-off. Raises ValueError when it has none."""
    clean = [t.record for t in run.tasks if t.finished and not t.record.failed]
    if not clean:
        raise ValueError("no finished task without failures")
    return refs_from_records(clean), len(clean)
