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

"""Harvests server-side Prometheus ground-truth timeseries during benchmark trials.

Queries Prometheus over [T_start, T_end] and the steady-state window [T_steady, T_end]
to capture dynamic cluster packing, node PSI stalls, and snapshot throughput.
"""

from __future__ import annotations

import csv
import json
import math
from pathlib import Path
import sys
from typing import Any, TextIO
import urllib.parse
import urllib.request


def query_prometheus_instant(
    base_url: str,
    query: str,
    time_ts: float | int | None = None,
    timeout_s: float = 5.0,
) -> list[dict[str, Any]]:
    """Executes an instant query against Prometheus /api/v1/query."""
    params = {"query": query}
    if time_ts is not None:
        params["time"] = str(time_ts)
    url = f"{base_url.rstrip('/')}/api/v1/query?{urllib.parse.urlencode(params)}"
    try:
        req = urllib.request.Request(
            url, headers={"User-Agent": "Substrate-Locust-Runner"}
        )
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            if data.get("status") == "success":
                return data.get("data", {}).get("result", [])
    except Exception as e:
        print(f"Warning: Instant query failed '{query}': {e}", file=sys.stderr)
    return []


def query_prometheus_range(
    base_url: str,
    query: str,
    start_ts: int,
    end_ts: int,
    step: str = "5s",
    timeout_s: float = 8.0,
) -> list[dict[str, Any]]:
    """Executes a range query against Prometheus /api/v1/query_range."""
    # Guard against Prometheus 400 Bad Request: end must be greater than start
    if end_ts <= start_ts:
        end_ts = start_ts + 1

    params = {
        "query": query,
        "start": str(start_ts),
        "end": str(end_ts),
        "step": step,
    }
    url = f"{base_url.rstrip('/')}/api/v1/query_range?{urllib.parse.urlencode(params)}"
    try:
        req = urllib.request.Request(
            url, headers={"User-Agent": "Substrate-Locust-Runner"}
        )
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            if data.get("status") == "success":
                return data.get("data", {}).get("result", [])
    except Exception as e:
        print(f"Warning: Range query failed '{query}': {e}", file=sys.stderr)
    return []


def compute_percentiles(values: list[float]) -> dict[str, float | None]:
    """Computes min, p50, p90, p99, max, avg after filtering out NaN and Inf values."""
    clean = sorted([v for v in values if not math.isnan(v) and not math.isinf(v)])
    if not clean:
        return {
            "min": None,
            "p50": None,
            "p90": None,
            "p99": None,
            "max": None,
            "avg": None,
        }
    n = len(clean)
    return {
        "min": round(clean[0], 4),
        "p50": round(clean[int(n * 0.50)], 4),
        "p90": round(clean[min(int(n * 0.90), n - 1)], 4),
        "p99": round(clean[min(int(n * 0.99), n - 1)], 4),
        "max": round(clean[-1], 4),
        "avg": round(sum(clean) / n, 4),
    }


def get_steady_state_window(
    stats_history_csv: Path,
    active_users: int,
    start_ts: int,
    end_ts: int,
) -> tuple[int, int]:
    """Derives steady-state [T_steady, T_end] where User Count >= 0.9 * active_users."""
    if not stats_history_csv.exists() or active_users <= 0:
        return start_ts, end_ts

    steady_ts: int | None = None
    try:
        with open(stats_history_csv, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                name = row.get("Name", "")
                if (
                    name in ("", "Aggregated", "Total")
                    and "User Count" in row
                    and "Timestamp" in row
                ):
                    try:
                        users = float(row["User Count"])
                        ts = int(row["Timestamp"])
                        if users >= active_users * 0.9:
                            steady_ts = ts
                            break
                    except (ValueError, TypeError):
                        continue
    except Exception:
        pass

    if steady_ts is not None and start_ts <= steady_ts <= end_ts:
        return steady_ts, end_ts
    return start_ts, end_ts


def _parse_instant_float(res: list[dict[str, Any]]) -> float | None:
    if res and "value" in res[0]:
        try:
            v = float(res[0]["value"][1])
            return None if math.isnan(v) or math.isinf(v) else round(v, 4)
        except (KeyError, IndexError, TypeError, ValueError):
            # Every caller treats None as "no reading". A bad response shape
            # must cost this one field, not the whole server_summary.json.
            pass
    return None


def _parse_instant_int(res: list[dict[str, Any]]) -> int | None:
    """Returns None, not 0, when the query yielded nothing.

    Prometheus is optional, so an unreachable server is an expected outcome.
    Zero is a legitimate reading here, so returning it on failure would leave
    a consumer unable to tell "no checkpoints happened" from "we never found
    out". Mirrors _parse_instant_float.
    """
    val = _parse_instant_float(res)
    return int(val) if val is not None else None


def _query_quantile_with_fallback(
    prom_url: str,
    quantile: float,
    rate_metric_expr: str,
    raw_metric_expr: str,
    end_ts: int,
    unit_scale: float = 1.0,
) -> float | None:
    """Queries histogram quantile using rate(5m), falling back to cumulative buckets if NaN/empty."""
    # _parse_instant_float maps NaN and Inf to None, so None is the only
    # "no reading" value either query can produce.
    q_rate = (
        f"histogram_quantile({quantile}, sum({rate_metric_expr}) by (le)) / {unit_scale}"
    )
    val = _parse_instant_float(
        query_prometheus_instant(prom_url, q_rate, time_ts=end_ts)
    )
    if val is not None:
        return val

    # Fallback to cumulative bucket distribution
    q_cum = (
        f"histogram_quantile({quantile}, sum by (le) ({raw_metric_expr})) / {unit_scale}"
    )
    return _parse_instant_float(
        query_prometheus_instant(prom_url, q_cum, time_ts=end_ts)
    )


def harvest_server_telemetry(
    prom_url: str,
    start_ts: int,
    end_ts: int,
    steady_start_ts: int,
    worker_pod_count: int | None,
) -> dict[str, Any]:
    """Harvests all 4 ground truth metric streams from Prometheus."""
    summary: dict[str, Any] = {
        "cluster_packing": {},
        "node_psi": {},
        "snapshots": {},
    }

    # 1. Cluster Packing Timeseries (deduping ateapi replicas via max by state)
    packing_query = (
        'max by (ate_worker_state) '
        '(ate_workerpool_workers{ate_workerpool_name="benchmark-ateom"})'
    )
    packing_series = query_prometheus_range(
        prom_url, packing_query, start_ts, end_ts, step="5s"
    )

    ts_packing_map: dict[int, dict[str, float]] = {}
    for series in packing_series:
        state = series.get("metric", {}).get("ate_worker_state", "unknown")
        for pt in series.get("values", []):
            try:
                t = int(pt[0])
                val = float(pt[1])
                # Inf as well as NaN: these reach server_summary.json, and
                # json.dumps would emit a bare Infinity that strict parsers reject.
                if not math.isnan(val) and not math.isinf(val):
                    ts_packing_map.setdefault(t, {})[state] = val
            except (ValueError, IndexError):
                continue

    packing_points = []
    steady_packing_ratios = []
    for t in sorted(ts_packing_map.keys()):
        states = ts_packing_map[t]
        assigned = states.get("assigned", 0.0)
        # Real worker pod count is the physical denominator; when unknown, use
        # the worker states Prometheus reports rather than assuming a number.
        total = (
            float(worker_pod_count)
            if worker_pod_count
            else (sum(states.values()) or 1.0)
        )
        ratio = round(assigned / total, 4)
        packing_points.append({
            "timestamp": t,
            "assigned_workers": assigned,
            "total_workers": total,
            "packing_ratio": ratio,
        })
        if t >= steady_start_ts:
            steady_packing_ratios.append(ratio)

    summary["cluster_packing"] = {
        "summary": compute_percentiles(steady_packing_ratios),
        "timeseries": packing_points,
    }

    # 2. Host Linux Kernel PSI Stalls & CFS Throttling
    #
    # _waiting_ is PSI "some" (at least one task stalled); cAdvisor also
    # exports _stalled_, PSI "full" (all tasks stalled). "some" is the earlier
    # warning signal and the CPU full-stall series carries none, so one series
    # keeps the three comparable. Both are scraped, so "full" stays available.
    def psi_query(resource: str) -> str:
        return (
            f'sum by (instance) (rate(container_pressure_{resource}_waiting_seconds_total'
            '{container="node"}[1m])) * 100'
        )

    # container!="" drops the per-pod rollups cAdvisor reports alongside each
    # container, which would double count. Unlike the memory and CPU series
    # these are not dropped at scrape time (see monitoring.yaml).
    cfs_throttled_query = (
        'sum(rate(container_cpu_cfs_throttled_seconds_total'
        '{container!=""}[1m]))'
    )

    psi_cpu_res = query_prometheus_range(
        prom_url, psi_query("cpu"), start_ts, end_ts, step="5s"
    )
    psi_mem_res = query_prometheus_range(
        prom_url, psi_query("memory"), start_ts, end_ts, step="5s"
    )
    psi_io_res = query_prometheus_range(
        prom_url, psi_query("io"), start_ts, end_ts, step="5s"
    )
    cfs_res = query_prometheus_range(
        prom_url, cfs_throttled_query, start_ts, end_ts, step="5s"
    )

    def extract_steady_values(results: list[dict[str, Any]]) -> list[float]:
        vals = []
        for s in results:
            for pt in s.get("values", []):
                try:
                    if int(pt[0]) >= steady_start_ts:
                        v = float(pt[1])
                        # Inf needs no filter here: compute_percentiles is the
                        # only consumer and it drops both NaN and Inf.
                        if not math.isnan(v):
                            vals.append(v)
                except (ValueError, IndexError):
                    pass
        return vals

    summary["node_psi"] = {
        "cpu_stall_pct": compute_percentiles(extract_steady_values(psi_cpu_res)),
        "mem_stall_pct": compute_percentiles(extract_steady_values(psi_mem_res)),
        "io_stall_pct": compute_percentiles(extract_steady_values(psi_io_res)),
        "cfs_throttled_rate": compute_percentiles(extract_steady_values(cfs_res)),
    }

    # 3. Snapshot Sizes, Checkpoint Count & Latencies (with histogram fallback)
    snap_bucket = "atelet_snapshot_size_bytes_bucket"
    snap_rate = f"rate({snap_bucket}[5m])"
    snap_p50 = _query_quantile_with_fallback(
        prom_url, 0.50, snap_rate, snap_bucket, end_ts, unit_scale=1024 * 1024
    )
    snap_p90 = _query_quantile_with_fallback(
        prom_url, 0.90, snap_rate, snap_bucket, end_ts, unit_scale=1024 * 1024
    )
    snap_p95 = _query_quantile_with_fallback(
        prom_url, 0.95, snap_rate, snap_bucket, end_ts, unit_scale=1024 * 1024
    )

    snap_count_query = "sum(atelet_snapshot_size_bytes_count)"
    snap_count_start = query_prometheus_instant(
        prom_url, snap_count_query, time_ts=steady_start_ts
    )
    snap_count_end = query_prometheus_instant(
        prom_url, snap_count_query, time_ts=end_ts
    )
    c_start = _parse_instant_int(snap_count_start)
    c_end = _parse_instant_int(snap_count_end)

    # A missing endpoint, or a counter that went backwards because an atelet
    # restarted, makes the delta unknown. 0 would read as "nothing happened".
    if c_start is None or c_end is None or c_end < c_start:
        window_checkpoints = None
    else:
        window_checkpoints = c_end - c_start

    # Mean over the same window as the percentiles above, from the histogram's
    # own _sum/_count so it is exact rather than bucket interpolated. _sum
    # counts up from atelet start, so the end value alone would average the
    # whole lifetime, not the test.
    snap_sum_query = "sum(atelet_snapshot_size_bytes_sum)"
    s_start = _parse_instant_float(
        query_prometheus_instant(prom_url, snap_sum_query, time_ts=steady_start_ts)
    )
    s_end = _parse_instant_float(
        query_prometheus_instant(prom_url, snap_sum_query, time_ts=end_ts)
    )
    snap_avg = None
    if (
        s_start is not None
        and s_end is not None
        and s_end >= s_start
        and window_checkpoints
    ):
        snap_avg = round((s_end - s_start) / window_checkpoints / (1024 * 1024), 4)

    def rpc_bucket(method: str) -> str:
        return (
            'rpc_server_call_duration_seconds_bucket'
            f'{{rpc_method="atelet.AteomHerder/{method}"}}'
        )

    restore_bucket = rpc_bucket("Restore")
    restore_rate = f"rate({restore_bucket}[5m])"
    ckpt_bucket = rpc_bucket("Checkpoint")
    ckpt_rate = f"rate({ckpt_bucket}[5m])"

    restore_p50 = _query_quantile_with_fallback(
        prom_url, 0.50, restore_rate, restore_bucket, end_ts
    )
    restore_p95 = _query_quantile_with_fallback(
        prom_url, 0.95, restore_rate, restore_bucket, end_ts
    )
    ckpt_p50 = _query_quantile_with_fallback(
        prom_url, 0.50, ckpt_rate, ckpt_bucket, end_ts
    )
    ckpt_p95 = _query_quantile_with_fallback(
        prom_url, 0.95, ckpt_rate, ckpt_bucket, end_ts
    )

    steady_duration_s = max(1, end_ts - steady_start_ts)
    throughput_mb_s = None
    # `is not None`, not truthiness: a genuine 0.0 median or 0 checkpoints is a
    # reading, not a missing value, and must not be mistaken for an absent one.
    if (
        snap_p50 is not None
        and window_checkpoints is not None
        and window_checkpoints > 0
    ):
        throughput_mb_s = round(
            (snap_p50 * window_checkpoints) / steady_duration_s, 2
        )

    summary["snapshots"] = {
        "size_p50_mb": snap_p50,
        "size_p90_mb": snap_p90,
        "size_p95_mb": snap_p95,
        "size_avg_mb": snap_avg,
        "checkpoints_in_window": window_checkpoints,
        "checkpoints_cumulative": c_end,
        "restore_p50_s": restore_p50,
        "restore_p95_s": restore_p95,
        "checkpoint_p50_s": ckpt_p50,
        "checkpoint_p95_s": ckpt_p95,
        "throughput_mb_s": throughput_mb_s,
    }

    return summary


def extract_and_record_server_telemetry(
    prom_url: str,
    start_ts: int,
    end_ts: int,
    stats_history_csv: Path,
    active_users: int,
    worker_pod_count: int | None,
    output_json_path: Path,
    jsonl_path: Path,
    data_ts: str,
    tag: str,
    test_name: str,
    logs: TextIO | None = None,
) -> None:
    """Entry point called by runner.py to query Prometheus and persist artifacts."""
    def log(msg: str) -> None:
        if logs:
            print(f"[ServerTelemetry] {msg}", file=logs, flush=True)
        print(f"[ServerTelemetry] {msg}", flush=True)

    log(f"Harvesting Prometheus metrics from {prom_url} over [{start_ts}, {end_ts}]...")
    steady_start, steady_end = get_steady_state_window(
        stats_history_csv, active_users, start_ts, end_ts
    )
    log(
        f"Detected steady-state window: [{steady_start}, {steady_end}] "
        f"({steady_end - steady_start}s)"
    )

    telemetry = harvest_server_telemetry(
        prom_url, start_ts, end_ts, steady_start, worker_pod_count
    )

    full_artifact = {
        "metadata": {
            "test_name": test_name,
            "tag": tag,
            "data_timestamp": data_ts,
            "prom_url": prom_url,
            "start_ts": start_ts,
            "end_ts": end_ts,
            "steady_start_ts": steady_start,
            "steady_end_ts": steady_end,
            "worker_pod_count": worker_pod_count,
        },
        **telemetry,
    }

    output_json_path.write_text(json.dumps(full_artifact, indent=2) + "\n")
    log(f"Wrote server summary artifact to {output_json_path}")

    # Append normalized single-row summary into stats.jsonl
    packing_s = telemetry.get("cluster_packing", {}).get("summary", {})
    psi = telemetry.get("node_psi", {})
    snaps = telemetry.get("snapshots", {})

    jsonl_row = {
        "timestamp": data_ts,
        "tag": tag,
        "test_name": test_name,
        "metric": "server_summary",
        "cluster_packing_p50": packing_s.get("p50"),
        "cluster_packing_p90": packing_s.get("p90"),
        "cluster_packing_p99": packing_s.get("p99"),
        "psi_cpu_stall_p90": psi.get("cpu_stall_pct", {}).get("p90"),
        "psi_mem_stall_p90": psi.get("mem_stall_pct", {}).get("p90"),
        "psi_io_stall_p90": psi.get("io_stall_pct", {}).get("p90"),
        "cfs_throttled_rate_avg": psi.get("cfs_throttled_rate", {}).get("avg"),
        "snapshot_size_p50_mb": snaps.get("size_p50_mb"),
        "checkpoints_in_window": snaps.get("checkpoints_in_window"),
        "restore_p50_s": snaps.get("restore_p50_s"),
        "checkpoint_p50_s": snaps.get("checkpoint_p50_s"),
        "checkpoint_throughput_mb_s": snaps.get("throughput_mb_s"),
    }

    with open(jsonl_path, "a", encoding="utf-8") as f:
        f.write(json.dumps(jsonl_row) + "\n")
    log(f"Appended server_summary row to {jsonl_path}")
