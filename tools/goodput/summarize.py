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

"""Summarizes many runs' goodput.json files: one row per test as CSV and Markdown, and one row per run as CSV.

Runs are grouped by test_name. Each metric is a run's steady-window value,
summarized as the median, min and max over the test's valid runs.
"""

import csv
import json
import statistics
from pathlib import Path
from typing import Any, Optional

import classify

# Steady-window metrics, per mode.
METRICS = ("tasks_done", "tasks_good", "tasks_unfinished_bad", "startup_failure_attempts", "goodput_ratio_pct",
           "done_tasks_per_hr", "good_tasks_per_hr", "good_tasks_per_vcpu_hr", "resume_max_ms_p50",
           "resume_max_ms_p90", "ttfe_max_ms_p50", "ttfe_max_ms_p90", "cel_slowdown_p50", "cel_slowdown_p90",
           "wall_ratio_p50", "wall_ratio_p90", "eta_infra")
AGGREGATES = ("median", "min", "max")
IDENTITY = ("runtime", "machine_type", "template", "log_format")
CSV_NAME = "goodput_summary.csv"
MD_NAME = "goodput_summary.md"
RUNS_CSV_NAME = "goodput_runs.csv"


def find_results(paths: list[str]) -> list[Path]:
    """The goodput.json files among paths, and every goodput.json under the directories among them."""
    found: list[Path] = []
    for p in map(Path, paths):
        found += sorted(p.rglob("goodput.json")) if p.is_dir() else [p]
    return found


def load(path: Path) -> dict[str, Any]:
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def validity(result: dict[str, Any], max_startup_failures_per_task: float) -> tuple[bool, str]:
    """The run's valid flag; a goodput.json written before runs had one gets the same rule from its totals."""
    if "valid" in result:
        return bool(result["valid"]), result.get("invalid_reason") or ""
    t = result.get("totals") or {}
    return classify.run_validity(t.get("tasks_finished", 0), t.get("startup_failure_attempts", 0),
                                 max_startup_failures_per_task)


def aggregate(values: list[Optional[float]]) -> dict[str, Optional[float]]:
    """The median, min and max of the values that are set."""
    xs = [float(v) for v in values if v is not None]
    if not xs:
        return dict.fromkeys(AGGREGATES)
    return {"median": round(statistics.median(xs), 4), "min": min(xs), "max": max(xs)}


def mode_names(results: list[dict[str, Any]]) -> list[str]:
    return sorted({name for r in results for name in r.get("modes") or {}})


def bad_reason_names(results: list[dict[str, Any]]) -> list[str]:
    return sorted({reason for r in results for m in (r.get("modes") or {}).values()
                   for reason in m["steady"].get("bad_reasons") or {}})


def _metrics(steady: dict[str, Any], reasons: list[str]) -> dict[str, Optional[float]]:
    """One run's steady-window metrics by column name: METRICS, then a bad_<reason> count per reason."""
    out = {name: steady.get(name) for name in METRICS}
    out.update({f"bad_{reason}": (steady.get("bad_reasons") or {}).get(reason, 0) for reason in reasons})
    return out


def summarize(results: list[dict[str, Any]], max_startup_failures_per_task: float
              = classify.MAX_STARTUP_FAILURES_PER_TASK) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """One row per test_name (columns <mode>_<metric>_<median|min|max>), and one row per invalid run."""
    groups: dict[str, list[dict[str, Any]]] = {}
    for r in results:
        groups.setdefault(r["run"].get("test_name") or "?", []).append(r)
    modes, reasons = mode_names(results), bad_reason_names(results)
    rows, invalid = [], []
    for test, runs in sorted(groups.items()):
        valid = []
        for r in runs:
            ok, why = validity(r, max_startup_failures_per_task)
            if ok:
                valid.append(r)
            else:
                invalid.append({"test_name": test, "run_date": r["run"].get("run_date", ""),
                                "run_ts": r["run"].get("run_ts", ""), "reason": why})
        dates = sorted(r["run"].get("run_date") or "" for r in runs)
        row: dict[str, Any] = {"test_name": test, "runs": len(runs), "runs_valid": len(valid),
                               "runs_invalid": len(runs) - len(valid), "first_run_date": dates[0],
                               "last_run_date": dates[-1]}
        for key in IDENTITY:  # from the valid runs: a run with no tasks has no log format
            row[key] = "|".join(sorted({str(r["run"].get(key) or "") for r in valid or runs}))
        for mode in modes:
            per_run = [_metrics(r["modes"][mode]["steady"], reasons) for r in valid if mode in r["modes"]]
            for name in _metrics({}, reasons):
                for agg, v in aggregate([m[name] for m in per_run]).items():
                    row[f"{mode}_{name}_{agg}"] = v
        for agg, v in aggregate([r.get("abs_speed_ratio") for r in valid]).items():
            row[f"abs_speed_ratio_{agg}"] = v
        rows.append(row)
    return rows, invalid


def run_rows(results: list[dict[str, Any]], max_startup_failures_per_task: float
             = classify.MAX_STARTUP_FAILURES_PER_TASK) -> list[dict[str, Any]]:
    """One row per run, valid or not, with summarize's metrics unaggregated (columns <mode>_<metric>)."""
    modes, reasons = mode_names(results), bad_reason_names(results)
    rows = []
    def order(r: dict[str, Any]) -> tuple[str, str, str]:
        return r["run"].get("test_name") or "", r["run"].get("run_date") or "", str(r["run"].get("run_ts") or "")

    for r in sorted(results, key=order):
        run = r["run"]
        ok, why = validity(r, max_startup_failures_per_task)
        row: dict[str, Any] = {"test_name": run.get("test_name") or "?", "run_date": run.get("run_date") or "",
                               "run_ts": run.get("run_ts") or "", "tag": run.get("tag") or "", "valid": int(ok),
                               "invalid_reason": why}
        row.update({key: run.get(key) or "" for key in IDENTITY})
        for mode in modes:
            steady = (r["modes"].get(mode) or {}).get("steady")
            for name, v in _metrics(steady or {}, reasons).items():
                row[f"{mode}_{name}"] = v if steady else None
        row["abs_speed_ratio"] = r.get("abs_speed_ratio")
        rows.append(row)
    return rows


def write_csv(rows: list[dict[str, Any]], path: Path) -> None:
    """Rows with plain numeric columns (empty when unset)."""
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]) if rows else ["test_name"])
        w.writeheader()
        w.writerows(rows)


def _num(v: Optional[float], ratio: bool = False) -> str:
    """A table cell: "–" when unset, two decimals for a ratio, else as precise as the size needs."""
    if v is None:
        return "–"
    if ratio:
        return f"{v:.2f}"
    if float(v).is_integer():
        return f"{int(v):,}"
    return f"{v:,.0f}" if abs(v) >= 100 else f"{v:.1f}" if abs(v) >= 10 else f"{v:.2f}"


def _spread(row: dict[str, Any], key: str, ratio: bool = False) -> str:
    """median [min–max] of a summarized metric, or just the median when they agree."""
    med, lo, hi = (row[f"{key}_{agg}"] for agg in AGGREGATES)
    return _num(med, ratio) if lo == hi else f"{_num(med, ratio)} [{_num(lo, ratio)}–{_num(hi, ratio)}]"


def _pair(row: dict[str, Any], key50: str, key90: str, ratio: bool = False) -> str:
    return f"{_num(row[key50 + '_median'], ratio)} / {_num(row[key90 + '_median'], ratio)}"


def markdown(rows: list[dict[str, Any]], invalid: list[dict[str, Any]], modes: list[str]) -> str:
    runs = sum(r["runs"] for r in rows)
    out = ["# Task goodput summary", "",
           f"{runs} runs of {len(rows)} tests, {len(invalid)} of them invalid and left out. Steady window. "
           "Each value is the median over a test's valid runs, with [min–max] where they differ.", ""]

    def table(header: list[str], body: list[list[str]]) -> None:
        out.extend(["| " + " | ".join(header) + " |", "|" + "---|" * len(header)])
        out.extend("| " + " | ".join(cells) + " |" for cells in body)
        out.append("")

    for mode in modes:
        out += [f"## {mode}", ""]
        body = []
        for r in rows:
            reasons = sorted((k[len(mode) + 5:-7], r[k]) for k in r
                             if k.startswith(f"{mode}_bad_") and k.endswith("_median") and r[k])
            body.append([r["test_name"], f"{r['runs_valid']} / {r['runs_invalid']}",
                         _spread(r, f"{mode}_tasks_done"), _spread(r, f"{mode}_tasks_good"),
                         _spread(r, f"{mode}_tasks_unfinished_bad"), _spread(r, f"{mode}_goodput_ratio_pct"),
                         _spread(r, f"{mode}_good_tasks_per_hr"), _spread(r, f"{mode}_good_tasks_per_vcpu_hr"),
                         ", ".join(f"{k} {_num(v)}" for k, v in reasons) or "–"])
        table(["test", "runs valid / invalid", "tasks done", "good", "unfinished bad", "goodput %", "good / h",
               "good / vCPU-h", "bad reasons (median count)"], body)
    if modes:
        mode = modes[0]  # these don't depend on the mode
        out += ["## Latency and slowdown", "",
                "p50 / p90 of each run's per-task values. Slowdown and wall ratio are to the run's baseline; "
                "abs speed is the baseline's CEL over the reference baseline's.", ""]
        body = [[r["test_name"], _pair(r, f"{mode}_resume_max_ms_p50", f"{mode}_resume_max_ms_p90"),
                 _pair(r, f"{mode}_ttfe_max_ms_p50", f"{mode}_ttfe_max_ms_p90"),
                 _pair(r, f"{mode}_cel_slowdown_p50", f"{mode}_cel_slowdown_p90", ratio=True),
                 _pair(r, f"{mode}_wall_ratio_p50", f"{mode}_wall_ratio_p90", ratio=True),
                 _spread(r, f"{mode}_eta_infra", ratio=True), _spread(r, "abs_speed_ratio", ratio=True)]
                for r in rows]
        table(["test", "resume max ms", "TTFE max ms", "CEL slowdown", "wall ratio", "eta_infra", "abs speed"], body)
    if invalid:
        out += ["## Invalid runs", ""]
        table(["test", "run date", "run ts", "reason"],
              [[x["test_name"], x["run_date"], str(x["run_ts"]), x["reason"]] for x in invalid])
    return "\n".join(out)


def write(results: list[dict[str, Any]], out: Path,
          max_startup_failures_per_task: float = classify.MAX_STARTUP_FAILURES_PER_TASK) -> tuple[Path, Path, Path]:
    """Writes goodput_summary.csv, goodput_summary.md and goodput_runs.csv under out; returns their paths."""
    rows, invalid = summarize(results, max_startup_failures_per_task)
    out.mkdir(parents=True, exist_ok=True)
    csv_path, md_path, runs_path = out / CSV_NAME, out / MD_NAME, out / RUNS_CSV_NAME
    write_csv(rows, csv_path)
    md_path.write_text(markdown(rows, invalid, mode_names(results)), encoding="utf-8")
    write_csv(run_rows(results, max_startup_failures_per_task), runs_path)
    return csv_path, md_path, runs_path
