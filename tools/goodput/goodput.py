#!/usr/bin/env python3
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

"""Task goodput for benchmark runs: good tasks per hour, per node/vCPU/GB, and why tasks were bad.

The benchmark only records facts; this script decides good or bad from them, so
a rule change is a config edit and a rerun. See README.md.

  goodput.py compute <run dir | gs://.../run_ts=.../run_tag=.../> [--out DIR]
  goodput.py backfill <gs://.../runs/sweperf_*> --since YYYY-MM-DD --out DIR [--jobs N]
  goodput.py baseline <1-user run dir | gs path>... [--runtime R] [--machine-type T] [--template T]
"""

import argparse
import concurrent.futures
import json
import re
import statistics
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent))
import classify  # noqa: E402
import metrics  # noqa: E402
import parse  # noqa: E402

DEFAULT_CONFIG = Path(__file__).resolve().parent / "config.json"
RUN_FILES = ("logs.txt", "stats.jsonl", "server_summary.json", "stats_history.csv")
RUN_PATH = re.compile(r"/runs/(?P<test>[^/]+)/run_date=(?P<date>[^/]+)/run_ts=(?P<ts>\d+)/run_tag=(?P<tag>[^/]+)/?$")


def load_config(path: Path) -> dict[str, Any]:
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def runtime_of(test_name: str, trial: dict[str, Any]) -> str:
    """trial_summary's sandbox_class, else from the test name (microVM tests end in _microvm or _uvm)."""
    sc = str(trial.get("sandbox_class") or "").lower()
    if sc:
        return sc
    t = test_name.lower()
    return "microvm" if t.endswith(("_microvm", "_uvm")) else "gvisor"


def steady_window(meta: dict[str, Any], run_dir: Path, events: list[parse.Event]) -> tuple[tuple[float, float], str]:
    """server_summary's steady window, else the runner's rule on stats_history.csv,
    else the run's start..end, else the log's first..last event."""
    lo, hi = parse.num(meta.get("steady_start_ts")), parse.num(meta.get("steady_end_ts"))
    if lo and hi and hi > lo:
        return (lo, hi), "server_summary"
    start, end = parse.num(meta.get("start_ts")) or 0.0, parse.num(meta.get("end_ts")) or 0.0
    hist = parse.steady_from_history(run_dir / "stats_history.csv", start, end)
    if hist:
        return hist, "stats_history"
    if end > start > 0:
        return (start, end), "run"
    ts = [ev.t for ev in events]
    return ((min(ts), max(ts)) if ts else (0.0, 0.0)), "log"


def compute(run_dir: Path, config: dict[str, Any], identity: Optional[dict[str, str]] = None) -> dict[str, Any]:
    """goodput.json content for one run directory. identity (from a gs:// run path) wins over the files."""
    events = parse.read_events(run_dir / "logs.txt")
    stats = parse.read_stats(run_dir / "stats.jsonl")
    meta = parse.read_server_metadata(run_dir / "server_summary.json")
    trial_row = stats.get("trial_summary") or {}
    trial = trial_row.get("measurements") or {}
    ident = identity or {}
    test_name = ident.get("test_name") or meta.get("test_name") or trial_row.get("test_name", "")
    runtime = runtime_of(test_name, trial)
    machine_type = str(trial.get("machine_type") or "")
    template = str(trial.get("template") or "")
    fmt = parse.log_format(events)

    cut, cut_source = classify.cutoff(events, parse.epoch(trial.get("run_end")), parse.num(meta.get("end_ts")))
    tasks, startup = classify.build_tasks(events, cut)
    steady, steady_source = steady_window(meta, run_dir, events)
    first_t = min((ev.t for ev in events), default=0.0)
    last_t = max((ev.t for ev in events), default=0.0)
    whole = (parse.num(meta.get("start_ts")) or first_t, cut if cut is not None else last_t)
    capacity = {k: parse.num(trial.get(k)) for k in ("node_count", "allocatable_cores", "allocatable_ram_gb")}
    baseline = classify.match_baseline(config.get("baselines") or [], runtime, machine_type, template, fmt)

    task_rows = []
    for t in tasks:
        ff = t.first_failure
        task_rows.append({"actor": t.actor, "index": t.index, "start": t.start, "end": t.end,
                          "wall_ms": t.wall_ms, "cycles": len(t.cycle_ms), "retries": t.retries,
                          "first_failure_t": ff.t if ff else None, "fail_step": ff.reason if ff else "",
                          "fail_cycle": ff.cycle if ff else None, "status_by_mode": {}, "reasons_by_mode": {}})
    startup_ts = [s.t for s in startup]
    modes: dict[str, Any] = {}
    for mode_name, mode in sorted((config.get("modes") or {}).items()):
        rows = []
        for t, row in zip(tasks, task_rows):
            status, reasons = classify.classify(t, mode, baseline)
            row["status_by_mode"][mode_name] = status
            row["reasons_by_mode"][mode_name] = reasons
            rows.append({**row, "status": status, "reasons": reasons})
        modes[mode_name] = {
            "g2": "n/a",  # needs a per-task record line
            "g3_basis": "wall" if baseline and baseline.get("wall_ref_ms") else "skipped",
            "startup_basis": "none",  # attempts can't be tied to a user yet
            "steady": metrics.window_metrics(rows, startup_ts, steady, capacity, baseline),
            "whole": metrics.window_metrics(rows, startup_ts, whole, capacity, baseline),
        }

    finished = sum(1 for t in tasks if t.finished)
    return {
        "version": 1,
        "run": {"test_name": test_name, "tag": ident.get("tag") or meta.get("tag") or trial_row.get("tag", ""),
                "run_ts": str(ident.get("run_ts") or meta.get("start_ts") or ""),
                "run_date": ident.get("run_date", ""), "data_timestamp": meta.get("data_timestamp", ""),
                "runtime": runtime, "machine_type": machine_type, "template": template, "log_format": fmt,
                **capacity},
        "cutoff": {"ts": cut, "source": cut_source},
        "steady_window": {"start_ts": steady[0], "end_ts": steady[1], "source": steady_source},
        "baseline": baseline,
        "modes": modes,
        "totals": {"tasks_finished": finished, "tasks_unfinished": len(tasks) - finished,
                   "startup_failure_attempts": len(startup), "actors": len({t.actor for t in tasks})},
        "tasks": task_rows,
    }


def jsonl_rows(result: dict[str, Any]) -> list[dict[str, Any]]:
    """stats.jsonl-shaped rows (string measurements), one per mode; whole-run values get a whole_ prefix."""
    run = result["run"]
    rows = []
    for mode_name, m in sorted(result["modes"].items()):
        meas = {"g2": m["g2"], "g3_basis": m["g3_basis"], "startup_basis": m["startup_basis"],
                "log_format": run["log_format"], "runtime": run["runtime"], "machine_type": run["machine_type"],
                "template": run["template"]}
        for win, prefix in (("steady", ""), ("whole", "whole_")):
            for k, v in m[win].items():
                if k == "bad_reasons":
                    meas.update({f"{prefix}bad_{reason}": str(n) for reason, n in v.items()})
                elif v is not None:
                    meas[prefix + k] = str(v)
        rows.append({"timestamp": run["data_timestamp"], "tag": run["tag"], "test_name": run["test_name"],
                     "metric": f"goodput_{mode_name}", "measurements": meas})
    return rows


def write_outputs(result: dict[str, Any], out: Path) -> None:
    out.mkdir(parents=True, exist_ok=True)
    with open(out / "goodput.json", "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, sort_keys=True)
        f.write("\n")
    with open(out / "goodput.jsonl", "w", encoding="utf-8") as f:
        f.writelines(json.dumps(row, sort_keys=True) + "\n" for row in jsonl_rows(result))


def summary_line(result: dict[str, Any]) -> str:
    run = result["run"]
    parts = [f"{run['test_name'] or '?'} {run['run_date']} {run['runtime']} {run['machine_type']}"]
    for mode_name, m in sorted(result["modes"].items()):
        s = m["steady"]
        parts.append(f"{mode_name}: good {s['tasks_good']}/{s['tasks_done']} done (+{s['tasks_unfinished_bad']} "
                     f"unfinished bad), {s['good_tasks_per_hr']} good/h, ratio {s['goodput_ratio_pct']}%")
    return " | ".join(parts)


# --- GCS (through gcloud) -------------------------------------------------------

def gcloud(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(["gcloud", "storage", *args], capture_output=True, text=True, check=False)


def fetch_run(gs_dir: str, dest: Path) -> Path:
    """Copies a run's input files from GCS; only logs.txt is required."""
    dest.mkdir(parents=True, exist_ok=True)
    base = gs_dir.rstrip("/") + "/"
    for name in RUN_FILES:
        gcloud("cp", "-q", base + name, str(dest / name))
    if not (dest / "logs.txt").exists():
        raise FileNotFoundError(f"no logs.txt under {base}")
    return dest


def identity_from_path(path: str) -> dict[str, str]:
    """test_name, run_date, run_ts and tag from a .../runs/<test>/run_date=/run_ts=/run_tag= path."""
    m = RUN_PATH.search(path.rstrip("/"))
    if not m:
        return {}
    return {"test_name": m["test"], "run_date": m["date"], "run_ts": m["ts"], "tag": m["tag"]}


def resolve_run(arg: str, tmp: Path) -> tuple[Path, dict[str, str]]:
    if arg.startswith("gs://"):
        return fetch_run(arg, tmp / "run"), identity_from_path(arg)
    return Path(arg), identity_from_path(str(Path(arg).resolve()))


def list_runs(prefix: str, since: str) -> list[str]:
    """Run directories under prefix (may contain *) with run_date >= since."""
    res = gcloud("ls", prefix.rstrip("/") + "/**/logs.txt")
    if res.returncode != 0:
        raise RuntimeError(f"gcloud storage ls failed: {res.stderr.strip()[:300]}")
    runs = set()
    for line in res.stdout.split():
        d = line[: -len("logs.txt")]
        ident = identity_from_path(d)
        if ident and ident["run_date"] >= since:
            runs.add(d)
    return sorted(runs)


# --- Commands -------------------------------------------------------------------

def cmd_compute(a: argparse.Namespace) -> int:
    config = load_config(Path(a.config))
    with tempfile.TemporaryDirectory() as tmp:
        run_dir, ident = resolve_run(a.run, Path(tmp))
        result = compute(run_dir, config, ident)
        out = Path(a.out) if a.out else (Path("goodput-out") if a.run.startswith("gs://") else run_dir)
        write_outputs(result, out)
    print(summary_line(result))
    print(f"wrote {out / 'goodput.json'} and {out / 'goodput.jsonl'}")
    return 0


def backfill_one(gs_dir: str, config: dict[str, Any], out_root: Path) -> dict[str, Any]:
    """Computes one run under out_root (mirroring the from_prow layout); returns a summary row."""
    ident = identity_from_path(gs_dir)
    out = (out_root / ident["test_name"] / f"run_date={ident['run_date']}" / f"run_ts={ident['run_ts']}"
           / f"run_tag={ident['tag']}")
    try:
        with tempfile.TemporaryDirectory() as tmp:
            result = compute(fetch_run(gs_dir, Path(tmp)), config, ident)
        write_outputs(result, out)
    except Exception as e:  # pylint: disable=broad-except
        return {"run": gs_dir, "ok": False, "error": f"{type(e).__name__}: {e}"}
    return {"run": gs_dir, "ok": True, "line": summary_line(result),
            **{f"{k}_steady": v["steady"] for k, v in result["modes"].items()}}


def cmd_backfill(a: argparse.Namespace) -> int:
    config = load_config(Path(a.config))
    out_root = Path(a.out)
    runs = list_runs(a.prefix, a.since)
    print(f"{len(runs)} runs since {a.since}", flush=True)
    out_root.mkdir(parents=True, exist_ok=True)
    failed = 0
    with open(out_root / "backfill_summary.jsonl", "w", encoding="utf-8") as summary, \
            concurrent.futures.ThreadPoolExecutor(max_workers=max(1, a.jobs)) as pool:
        for row in pool.map(lambda d: backfill_one(d, config, out_root), runs):
            if row["ok"]:
                print(row.pop("line"), flush=True)
            else:
                failed += 1
                print(f"FAILED {row['run']}: {row['error']}", file=sys.stderr, flush=True)
            summary.write(json.dumps(row, sort_keys=True) + "\n")
    print(f"done: {len(runs) - failed} ok, {failed} failed; outputs under {out_root}")
    return 1 if failed else 0


def cmd_baseline(a: argparse.Namespace) -> int:
    """A baselines[] entry from 1-user runs: the median wall time of finished tasks
    without failures, and the request-weighted mean of actor_TaskCEL medians."""
    config = dict(load_config(Path(a.config)), baselines=[])
    walls: list[float] = []
    cels: list[tuple[float, float]] = []  # (TaskCEL median ms, request count)
    seen: dict[str, set[str]] = {"runtime": set(), "machine_type": set(), "template": set(), "format": set()}
    with tempfile.TemporaryDirectory() as tmp:
        for i, arg in enumerate(a.runs):
            run_dir, ident = resolve_run(arg, Path(tmp) / str(i))
            run = compute(run_dir, config, ident)
            for key, field in (("runtime", "runtime"), ("machine_type", "machine_type"),
                               ("template", "template"), ("format", "log_format")):
                seen[key].add(run["run"][field])
            walls += [t["wall_ms"] for t in run["tasks"] if t["end"] is not None and not t["fail_step"]]
            cel = (parse.read_stats(run_dir / "stats.jsonl").get("actor_TaskCEL") or {}).get("measurements") or {}
            med, n = parse.num(cel.get("median_response_time")), parse.num(cel.get("request_count"))
            if med and n:
                cels.append((med, n))
    entry: dict[str, Any] = {"runs": len(a.runs), "tasks": len(walls)}
    for key, override in (("runtime", a.runtime), ("machine_type", a.machine_type), ("template", a.template),
                          ("format", None)):
        if override:
            entry[key] = override
        elif len(seen[key]) == 1:
            entry[key] = seen[key].pop()
        else:
            print(f"the runs disagree on {key}: {sorted(seen[key])}", file=sys.stderr)
            return 1
    if not walls:
        print("no finished task without failures in these runs", file=sys.stderr)
        return 1
    entry["wall_ref_ms"] = round(statistics.median(walls), 1)
    entry["task_cel_ref_ms"] = round(sum(m * n for m, n in cels) / sum(n for _, n in cels), 1) if cels else None
    print(json.dumps(entry, indent=2, sort_keys=True))
    return 0


def main(argv: Optional[list[str]] = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--config", default=str(DEFAULT_CONFIG), help="rules and baselines (default: %(default)s)")
    sub = p.add_subparsers(dest="cmd", required=True)
    c = sub.add_parser("compute", help="one run: write goodput.json and goodput.jsonl")
    c.add_argument("run", help="run directory or gs:// run path")
    c.add_argument("--out", help="output directory (default: the run directory; ./goodput-out for gs://)")
    b = sub.add_parser("backfill", help="every run under a gs:// prefix since a date (local output only)")
    b.add_argument("prefix", help="e.g. gs://dev-measurements/from_prow/runs/sweperf_*")
    b.add_argument("--since", required=True, help="first run_date to include, YYYY-MM-DD")
    b.add_argument("--out", required=True, help="local output root (mirrors the from_prow layout)")
    b.add_argument("--jobs", type=int, default=8, help="runs fetched and computed in parallel")
    s = sub.add_parser("baseline", help="print a baselines[] entry from 1-user runs")
    s.add_argument("runs", nargs="+", help="run directories or gs:// run paths")
    s.add_argument("--runtime", default="", help="default: the runs' runtime")
    s.add_argument("--machine-type", default="", help="default: the runs' trial_summary machine_type")
    s.add_argument("--template", default="", help="default: the runs' trial_summary template, if any")
    a = p.parse_args(argv)
    return {"compute": cmd_compute, "backfill": cmd_backfill, "baseline": cmd_baseline}[a.cmd](a)


if __name__ == "__main__":
    sys.exit(main())
