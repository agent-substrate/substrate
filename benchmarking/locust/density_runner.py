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

"""Actor density search driven by runner.py --density-step-duration.

Doubles concurrent users from 1 up to -u and binary-searches once a step fails
any check in evaluate_step (see "Density Limit Benchmark" in
docs/guides/benchmarking.md). Publishes stats.jsonl, logs.txt, and status.json
under <dest>/runs/<name>/run_date=<date>/run_ts=<ts>/run_tag=<tag>/.
"""

import argparse
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone
import json
import math
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import time
from typing import Any, TextIO

RUNNER_SCRIPT = Path(__file__).with_name("runner.py")
SEARCH_RESOLUTION = 8
KNEE_MULTIPLIER = 2.0
KNEE_FLOOR_MS = 1000.0
MAX_FAILURE_RATIO = 0.01
MIN_LIVE_ACTOR_RATIO = 0.9
_UNIT_SECONDS = {"": 1, "s": 1, "m": 60, "h": 3600}

SERVER_PASSTHROUGH_KEYS = (
    "active_actors_p50",
    "working_set_node_p90_gb",
    "snapshot_size_p50_mb",
    "checkpoint_mb_s",
    "psi_cpu_stall_p90",
    "psi_mem_stall_p90",
    "psi_io_stall_p90",
)


@dataclass
class StepResult:
    users: int
    exit_code: int = 0
    passed: bool = False
    failure_reason: str = ""
    live_actors: int | None = None
    wake_p50_ms: float | None = None
    wake_p90_ms: float | None = None
    suspend_p50_ms: float | None = None
    suspend_p90_ms: float | None = None
    wake_rps: float | None = None
    failure_ratio: float | None = None
    crash_count: int = 0
    oom_counts: dict[str, int] = field(default_factory=dict)
    server_fields: dict[str, float | None] = field(default_factory=dict)
    trial_summary: dict[str, Any] | None = None
    server_summary: dict[str, Any] | None = None


def _to_float(val: Any) -> float | None:
    if val is None or val in ("", "N/A"):
        return None
    try:
        out = float(val)
    except (TypeError, ValueError):
        return None
    return None if math.isnan(out) or math.isinf(out) else out


def _to_int(val: Any) -> int | None:
    num = _to_float(val)
    return None if num is None else int(round(num))


def _parse_duration_s(spec: str) -> int:
    m = re.fullmatch(r"(\d+)([smh]?)", spec.strip())
    if not m:
        raise ValueError(f"invalid duration: {spec!r}")
    return int(m[1]) * _UNIT_SECONDS[m[2]]


def tee(logs: TextIO, msg: str) -> None:
    print(msg, flush=True)
    logs.write(msg + "\n")
    logs.flush()


def next_step_users(steps: list[StepResult], max_users: int) -> int | None:
    """Returns the next user count to run, or None when the search is done."""
    if max_users < 1:
        return None
    if not steps:
        return 1
    passed = [s.users for s in steps if s.passed]
    if not passed:
        return None
    low = max(passed)
    high = min((s.users for s in steps if not s.passed and s.users > low), default=None)
    if high is None:
        return min(2 * low, max_users) if low < max_users else None
    # Stop once the bracket is within 8 users or 12.5% of the failing step.
    if high - low <= max(SEARCH_RESOLUTION, high // 8):
        return None
    return (low + high) // 2


def parse_step_stats(users: int, exit_code: int, jsonl_path: Path | None) -> StepResult:
    """Reads wake, suspend, actor survival, and server fields from stats.jsonl."""
    res = StepResult(users=users, exit_code=exit_code)
    if jsonl_path is None or not jsonl_path.exists():
        return res

    rows: dict[str, dict[str, Any]] = {}
    for line in jsonl_path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        metric, measurements = row.get("metric"), row.get("measurements")
        if isinstance(metric, str) and isinstance(measurements, dict):
            rows[metric] = measurements

    if not rows:
        return res
    res.trial_summary = rows.get("trial_summary")
    res.server_summary = rows.get("server_summary")
    trial = res.trial_summary or {}
    srv = res.server_summary or {}

    if ttw := rows.get("summary_TimeToWake"):
        res.wake_p50_ms = _to_float(ttw.get("p50"))
        res.wake_p90_ms = _to_float(ttw.get("p90"))
        res.wake_rps = _to_float(ttw.get("requests_per_s"))

    if susp := rows.get("grpc_SuspendActor") or rows.get("grpc_SuspendActorAsync"):
        res.suspend_p50_ms = _to_float(susp.get("p50"))
        res.suspend_p90_ms = _to_float(susp.get("p90"))

    if create := rows.get("grpc_CreateActor"):
        created = (_to_int(create.get("request_count")) or 0) - (
            _to_int(create.get("failure_count")) or 0
        )
        deletes = _to_int((rows.get("grpc_DeleteActor") or {}).get("request_count")) or 0
        retired = _to_int((rows.get("actor_TaskWallClock") or {}).get("request_count")) or 0
        res.live_actors = min(users, max(created - max(deletes - retired, 0), 0))

    if crash_row := rows.get("actor_CrashCount"):
        res.crash_count = _to_int(crash_row.get("failure_count")) or 0

    res.failure_ratio = _to_float(
        trial.get("time_to_wake_failure_ratio", trial.get("aggregate_failure_ratio"))
    )
    for k in SERVER_PASSTHROUGH_KEYS:
        res.server_fields[k] = _to_float(srv.get(k))
    for k, v in srv.items():
        if k.startswith("oom_events_") and (val := _to_int(v)) is not None:
            res.oom_counts[k] = val

    return res


def knee_ceiling_ms(baseline_p90_ms: float | None) -> float | None:
    if baseline_p90_ms is None:
        return None
    return max(KNEE_MULTIPLIER * baseline_p90_ms, KNEE_FLOOR_MS)


def evaluate_step(step: StepResult, baseline: StepResult | None) -> str:
    """Returns an empty string when `step` passes, else the failure reason."""
    if step.wake_p90_ms is None:
        return "no wake measurements recorded"

    bad_counters = [f"{k}={v}" for k, v in step.oom_counts.items() if v > 0]
    if step.crash_count > 0:
        bad_counters.append(f"actor_crashes={step.crash_count}")
    if bad_counters:
        return ", ".join(bad_counters)

    min_live = math.ceil(MIN_LIVE_ACTOR_RATIO * step.users)
    if step.live_actors is None or step.live_actors < min_live:
        return f"live actors {step.live_actors} < {min_live}"

    if step.failure_ratio is not None and step.failure_ratio > MAX_FAILURE_RATIO:
        return f"failure ratio {step.failure_ratio:.4f} > {MAX_FAILURE_RATIO:.4f}"

    if baseline is not None:
        wake_limit = knee_ceiling_ms(baseline.wake_p90_ms)
        if wake_limit is not None and step.wake_p90_ms > wake_limit:
            return f"wake P90 {step.wake_p90_ms:.0f} ms > {wake_limit:.0f} ms"

        susp_limit = knee_ceiling_ms(baseline.suspend_p90_ms)
        if (
            susp_limit is not None
            and step.suspend_p90_ms is not None
            and step.suspend_p90_ms > susp_limit
        ):
            return f"suspend P90 {step.suspend_p90_ms:.0f} ms > {susp_limit:.0f} ms"

    return ""


def _stringify(raw: dict[str, Any]) -> dict[str, str | None]:
    return {
        k: (None if v is None else str(v).lower() if isinstance(v, bool) else str(v))
        for k, v in raw.items()
    }


def summarize_sweep(steps: list[StepResult]) -> dict[str, Any]:
    ordered = sorted(steps, key=lambda s: s.users)
    baseline = next((s for s in ordered if s.users == 1), None)
    passed = [s for s in ordered if s.passed]
    best = passed[-1] if passed else None
    max_actors = best.users if best else 0
    first_failed = next(
        (s for s in ordered if not s.passed and s.users > max_actors), None
    )
    return {
        "max_actors": max_actors,
        "ceiling_reached": first_failed is not None,
        "first_failed_users": first_failed.users if first_failed else None,
        "failure_reason": first_failed.failure_reason if first_failed else None,
        "wake_knee_ms": knee_ceiling_ms(baseline.wake_p90_ms) if baseline else None,
        "suspend_knee_ms": knee_ceiling_ms(baseline.suspend_p90_ms) if baseline else None,
    }


def build_jsonl_records(
    steps: list[StepResult],
    summary: dict[str, Any],
    *,
    timestamp: str,
    tag: str,
    test_name: str,
    run_start: int,
    run_end: int,
) -> list[dict[str, Any]]:
    def row(metric: str, measurements: dict[str, Any]) -> dict[str, Any]:
        return {
            "timestamp": timestamp,
            "tag": tag,
            "test_name": test_name,
            "metric": metric,
            "measurements": _stringify(measurements),
        }

    ordered = sorted(steps, key=lambda s: s.users)
    best = next((s for s in reversed(ordered) if s.passed), None)
    records: list[dict[str, Any]] = []
    if best and best.trial_summary:
        records.append(
            row(
                "trial_summary",
                {**best.trial_summary, "run_start": run_start, "run_end": run_end},
            )
        )
    if best and best.server_summary:
        records.append(row("server_summary", best.server_summary))
    records.append(row("density_limit", summary))
    for s in ordered:
        records.append(
            row(
                f"density_step_{s.users:04d}",
                {
                    "users": s.users,
                    "passed": s.passed,
                    "failure_reason": s.failure_reason or None,
                    "live_actors": s.live_actors,
                    "wake_p50_ms": s.wake_p50_ms,
                    "wake_p90_ms": s.wake_p90_ms,
                    "suspend_p50_ms": s.suspend_p50_ms,
                    "suspend_p90_ms": s.suspend_p90_ms,
                    "wake_rps": s.wake_rps,
                    "failure_ratio": s.failure_ratio,
                    **s.server_fields,
                },
            )
        )
    return records


def _strip_flag_with_value(flags: list[str], name: str) -> list[str]:
    out: list[str] = []
    i = 0
    while i < len(flags):
        cur = flags[i]
        if cur == name:
            i += 2
            continue
        if cur.startswith(f"{name}="):
            i += 1
            continue
        out.append(cur)
        i += 1
    return out


def build_step_cmd(
    args: argparse.Namespace, users: int, step_dest: Path
) -> list[str]:
    if args.actors_per_user not in (None, 1):
        raise ValueError("--density-step-duration does not support --actors-per-user")
    step_meta = {**(args.metadata or {}), "density_step_users": users}
    cmd = [
        sys.executable,
        str(RUNNER_SCRIPT),
        "-f",
        args.file,
        "-t",
        args.density_step_duration,
        "-u",
        str(users),
        "--tag",
        args.tag,
        "--name",
        f"{args.name}_u{users}",
        "--dest",
        str(step_dest),
        "--prometheus-url",
        args.prometheus_url,
        "--atelet-lag-s",
        str(args.atelet_lag_s),
        # Skip per-step pod log scraping; only the step metrics are needed.
        "--no-phase-breakdown",
        "--metadata",
        json.dumps(step_meta),
    ]
    if not args.cluster_facts:
        cmd.append("--no-cluster-facts")
    extra = list(args.locust_extra)
    if users == 1:
        # Skip the random startup delay for the single-user baseline so it uses
        # the full step window.
        extra = _strip_flag_with_value(extra, "--agentsession-phase-in")
    return [*cmd, *extra]


def run_step(args: argparse.Namespace, users: int, step_dest: Path, logs: TextIO) -> StepResult:
    cmd = build_step_cmd(args, users, step_dest)
    tee(logs, f"\n==== Density step users={users} ====")
    tee(logs, f"Running: {' '.join(cmd)}")
    proc = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, bufsize=1, text=True
    )
    assert proc.stdout is not None
    for line in proc.stdout:
        tee(logs, f"[u={users}] {line.rstrip()}")
    exit_code = proc.wait()
    jsonl_path = next(step_dest.rglob("stats.jsonl"), None)
    return parse_step_stats(users, exit_code, jsonl_path)


def run_density_sweep(
    args: argparse.Namespace, upload: Callable[[Path, str], None]
) -> None:
    now = datetime.now(timezone.utc)
    path_ts = now.strftime("%Y%m%dT%H%M%SZ")
    data_ts = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    run_date = now.strftime("%Y-%m-%d")
    run_ts = int(now.timestamp())
    step_budget_s = _parse_duration_s(args.density_step_duration) + args.atelet_lag_s
    deadline_ts = run_ts + _parse_duration_s(args.duration)

    work_dir = Path(tempfile.mkdtemp(prefix=f"{path_ts}-density-runner-"))
    steps_root = work_dir / "steps"
    jsonl_path = work_dir / "stats.jsonl"
    logs_path = work_dir / "logs.txt"
    status_path = work_dir / "status.json"
    prefix = (
        f"{args.dest.rstrip('/')}/runs/{args.name}"
        f"/run_date={run_date}/run_ts={run_ts}/run_tag={args.tag}"
    )

    steps: list[StepResult] = []
    baseline: StepResult | None = None
    sweep_ok = False
    upload_ok = True
    try:
        with open(logs_path, "w", encoding="utf-8") as logs:
            while (users := next_step_users(steps, args.users)) is not None:
                if steps and time.time() + step_budget_s > deadline_ts:
                    tee(logs, "Stopping search: remaining -t budget cannot fit another step")
                    break
                step = run_step(args, users, steps_root / f"u{users:04d}", logs)
                step.failure_reason = evaluate_step(step, baseline)
                step.passed = step.failure_reason == ""
                if baseline is None and step.users == 1:
                    baseline = step
                steps.append(step)
                verdict = "PASS" if step.passed else f"FAIL ({step.failure_reason})"
                tee(logs, f"Step users={users} verdict: {verdict}")
                if step.exit_code != 0:
                    tee(logs, f"Stopping search: step exited with code {step.exit_code}")
                    break

            summary = summarize_sweep(steps)
            tee(
                logs,
                f"Result: max_actors={summary['max_actors']} "
                f"first_failed={summary['first_failed_users']} "
                f"reason={summary['failure_reason']}",
            )
    finally:
        run_end_ts = int(datetime.now(timezone.utc).timestamp())
        summary = summarize_sweep(steps)
        records = build_jsonl_records(
            steps,
            summary,
            timestamp=data_ts,
            tag=args.tag,
            test_name=args.name,
            run_start=run_ts,
            run_end=run_end_ts,
        )
        with open(jsonl_path, "w", encoding="utf-8") as f:
            for rec in records:
                f.write(json.dumps(rec) + "\n")

        sweep_ok = summary["max_actors"] >= 1 and all(s.exit_code == 0 for s in steps)
        status_path.write_text(
            json.dumps({"locust_exit_code": 0 if sweep_ok else 1, "stats_generated": sweep_ok}),
            encoding="utf-8",
        )
        for src, basename in (
            (status_path, "status.json"),
            (logs_path, "logs.txt"),
            (jsonl_path, "stats.jsonl"),
        ):
            if not src.exists():
                continue
            dest = f"{prefix}/{basename}"
            try:
                upload(src, dest)
                print(f"Uploaded {src} -> {dest}", flush=True)
            except Exception as e:
                print(f"Upload of {src} failed: {e}", flush=True)
                upload_ok = False

    if not sweep_ok or not upload_ok:
        sys.exit(1)
