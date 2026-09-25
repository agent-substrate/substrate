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
to capture cluster packing, node and pod PSI stalls, and snapshot sizes, latencies
and throughput.
"""

import csv
import json
import math
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, TextIO


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
    """Computes min, p50, p90, p95, p99, max, avg, ignoring NaN and Inf values."""
    clean = sorted([v for v in values if not math.isnan(v) and not math.isinf(v)])
    if not clean:
        return {
            "min": None,
            "p50": None,
            "p90": None,
            "p95": None,
            "p99": None,
            "max": None,
            "avg": None,
        }
    n = len(clean)
    return {
        "min": round(clean[0], 4),
        "p50": round(clean[int(n * 0.50)], 4),
        "p90": round(clean[min(int(n * 0.90), n - 1)], 4),
        "p95": round(clean[min(int(n * 0.95), n - 1)], 4),
        "p99": round(clean[min(int(n * 0.99), n - 1)], 4),
        "max": round(clean[-1], 4),
        "avg": round(sum(clean) / n, 4),
    }


def get_steady_state_window(
    stats_history_csv: Path,
    start_ts: int,
    end_ts: int,
) -> tuple[int, int]:
    """Derives the steady-state window [T_steady, T_end] from Locust's samples.

    T_steady is the first sample reaching 90% of the highest user count the run
    reached and T_end the last, so teardown after the load stops is left out.
    The target is read from the samples rather than the `-u` flag,
    because a custom load shape ignores the flag. Unlike the frontier
    percentiles this has to stay a single threshold: the result is one
    contiguous span, which is what a Prometheus range query takes.
    """
    if not stats_history_csv.exists():
        return start_ts, end_ts

    samples: list[tuple[int, float]] = []
    try:
        with open(stats_history_csv, encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                name = row.get("Name", "")
                if (
                    name in ("", "Aggregated", "Total")
                    and "User Count" in row
                    and "Timestamp" in row
                ):
                    try:
                        samples.append((int(row["Timestamp"]),
                                        float(row["User Count"])))
                    except (ValueError, TypeError):
                        continue
    except Exception:
        # A read that threw partway leaves only the rows before the fault,
        # which drags the peak down and opens the window during ramp-up.
        samples = []

    peak = max((users for _, users in samples), default=0.0)
    if peak <= 0:
        return start_ts, end_ts

    steady = [ts for ts, users in samples if users >= peak * 0.9]
    steady_ts, last_ts = steady[0], steady[-1]

    if start_ts <= steady_ts <= end_ts:
        return steady_ts, min(max(last_ts, steady_ts), end_ts)
    return start_ts, end_ts


def _parse_instant_float(res: list[dict[str, Any]]) -> float | None:
    if res and "value" in res[0]:
        try:
            v = float(res[0]["value"][1])
            return None if math.isnan(v) or math.isinf(v) else round(v, 4)
        except (IndexError, TypeError, ValueError):
            # Malformed response yields None for this field without failing harvest.
            pass
    return None


def _window_delta(
    prom_url: str, query: str, start_ts: int, end_ts: int
) -> tuple[float | None, float | None]:
    """The growth of a cumulative sum between two instants, and its end value.

    Exact where increase() is not: increase() drops everything before the first
    sample of a series born inside the window. A series absent at the start was
    born inside the window, so its start counts as zero. A missing end leaves
    the delta unknown, and so does an end below the start, which means a
    process restarted and reset the counter.
    """
    start = _parse_instant_float(
        query_prometheus_instant(prom_url, query, time_ts=start_ts)
    )
    end = _parse_instant_float(
        query_prometheus_instant(prom_url, query, time_ts=end_ts)
    )
    if end is None:
        return None, None
    if start is None:
        start = 0.0
    if end < start:
        return None, end
    return round(end - start, 4), end


def _query_window_quantile(
    prom_url: str,
    quantile: float,
    buckets: str,
    start_ts: int,
    end_ts: int,
    unit_scale: float = 1.0,
) -> float | None:
    """A histogram quantile over the bucket growth between two instants.

    Uses the same two reads as `_window_delta`, so percentiles cover the same
    events as the counts; rate() would skip growth before its first sample.
    """
    now = f"sum by (le) ({buckets})"
    then = f"sum by (le) ({buckets} @ {start_ts})"
    query = (
        f"histogram_quantile({quantile}, {now} - ({then} or {now} * 0))"
        f" / {unit_scale}"
    )
    return _parse_instant_float(
        query_prometheus_instant(prom_url, query, time_ts=end_ts)
    )


# Each ateom pod's whole cgroup, systemd or cgroupfs; drops the pause container.
POD_PSI_SELECTOR = (
    'pod=~"benchmark-ateom.*", container="", '
    'id=~".*[/-]pod[^/]+(\\\\.slice)?"'
)


def _psi_query(
    resource: str, selector: str = 'container="node"', by: str = "instance"
) -> str:
    """The stall percentage query for one PSI resource, summed by `by`.

    `_waiting_` is PSI "some" (at least one task stalled); cAdvisor also
    exports `_stalled_`, PSI "full" (all tasks stalled). "some" is the earlier
    warning signal and the CPU full-stall series carries none, so one series
    keeps the three comparable. Both are scraped, so "full" stays available.
    """
    return (
        f'sum by ({by}) (rate(container_pressure_{resource}_waiting_seconds_total'
        f'{{{selector}}}[1m])) * 100'
    )


def _steady_values(
    results: list[dict[str, Any]], steady_start_ts: int
) -> list[float]:
    """Every range series value at or after `steady_start_ts`."""
    vals = []
    for s in results:
        for pt in s.get("values", []):
            try:
                if int(pt[0]) >= steady_start_ts:
                    # NaN and Inf are filtered downstream by compute_percentiles.
                    vals.append(float(pt[1]))
            except (ValueError, IndexError):
                pass
    return vals


def _rpc_buckets(method: str) -> str:
    """The duration bucket selector for one AteomHerder RPC."""
    return (
        'rpc_server_call_duration_seconds_bucket'
        f'{{rpc_method="atelet.AteomHerder/{method}"}}'
    )


def _harvest_cluster_packing(
    prom_url: str,
    start_ts: int,
    end_ts: int,
    steady_start_ts: int,
    worker_pod_count: int | None,
) -> dict[str, Any]:
    """Assigned workers over total workers, per sample and as percentiles."""
    # Dedupes ateapi replicas via max by state.
    packing_query = (
        'max by (ate_worker_state) '
        '(ate_workerpool_workers{ate_workerpool_name="benchmark-ateom"})'
    )
    packing_series = query_prometheus_range(
        prom_url, packing_query, start_ts, end_ts, step="10s"
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
            if worker_pod_count is not None
            else sum(states.values())
        )
        # The total is recorded either way, but a ratio over zero workers is
        # not a reading, so it stays null rather than taking a stand-in.
        ratio = round(assigned / total, 4) if total > 0 else None
        packing_points.append({
            "timestamp": t,
            "assigned_workers": assigned,
            "total_workers": total,
            "packing_ratio": ratio,
        })
        if t >= steady_start_ts and ratio is not None:
            steady_packing_ratios.append(ratio)

    return {
        "summary": compute_percentiles(steady_packing_ratios),
        "timeseries": packing_points,
    }


def _harvest_node_psi(
    prom_url: str, start_ts: int, end_ts: int, steady_start_ts: int
) -> dict[str, Any]:
    """Host kernel pressure stall percentages over the steady-state window."""
    psi_cpu_res = query_prometheus_range(
        prom_url, _psi_query("cpu"), start_ts, end_ts, step="10s"
    )
    psi_mem_res = query_prometheus_range(
        prom_url, _psi_query("memory"), start_ts, end_ts, step="10s"
    )
    psi_io_res = query_prometheus_range(
        prom_url, _psi_query("io"), start_ts, end_ts, step="10s"
    )

    return {
        "cpu_stall_pct": compute_percentiles(
            _steady_values(psi_cpu_res, steady_start_ts)
        ),
        "mem_stall_pct": compute_percentiles(
            _steady_values(psi_mem_res, steady_start_ts)
        ),
        "io_stall_pct": compute_percentiles(
            _steady_values(psi_io_res, steady_start_ts)
        ),
    }


def _harvest_pod_psi(
    prom_url: str, start_ts: int, end_ts: int, steady_start_ts: int
) -> dict[str, Any]:
    """Worker pod stall percentages, every pod's steady samples pooled."""
    out: dict[str, Any] = {}
    pods: set[str] = set()
    for key, resource in (("cpu", "cpu"), ("mem", "memory"), ("io", "io")):
        res = query_prometheus_range(
            prom_url, _psi_query(resource, POD_PSI_SELECTOR, "pod"),
            start_ts, end_ts, step="10s",
        )
        pods.update(s.get("metric", {}).get("pod", "") for s in res)
        out[f"{key}_stall_pct"] = compute_percentiles(
            _steady_values(res, steady_start_ts)
        )
    out["pods"] = len(pods - {""})
    return out


def _ratio(
    num: float | None, den: float | None, scale: float = 1.0
) -> float | None:
    """num / den / scale, or None when either is unknown or den is zero."""
    if num is None or not den:
        return None
    return round(num / den / scale, 4)


def _harvest_snapshots(
    prom_url: str, steady_start_ts: int, steady_end_ts: int, lag_s: int = 0
) -> dict[str, Any]:
    """Snapshot sizes, checkpoint volume, latencies and write throughput.

    The atelet exports on an interval, so observations land up to `lag_s` late.
    Both edges are read `lag_s // 2` late, the middle of that delay.
    """
    start_at, end_at = steady_start_ts + lag_s // 2, steady_end_ts + lag_s // 2
    mb = 1024 * 1024

    def delta(query: str) -> float | None:
        return _window_delta(prom_url, query, start_at, end_at)[0]

    def quantiles(buckets: str, scale: float = 1.0) -> list[float | None]:
        return [
            _query_window_quantile(
                prom_url, q, buckets, start_at, end_at, unit_scale=scale
            )
            for q in (0.50, 0.90, 0.95, 0.99)
        ]

    # pages.img is the memory image and dominates the bytes, so sizes scope to it.
    snap_selector = 'atelet_snapshot_size_bytes%s{file_name="pages.img"}'
    size_q = quantiles(snap_selector % "_bucket", scale=mb)

    count, count_end = _window_delta(
        prom_url, f"sum({snap_selector % '_count'})", start_at, end_at
    )
    window_checkpoints = round(count) if count is not None else None
    c_end = round(count_end) if count_end is not None else None
    snap_avg = _ratio(delta(f"sum({snap_selector % '_sum'})"), count, scale=mb)

    def rpc_mean(method: str) -> float | None:
        sel = f'{{rpc_method="atelet.AteomHerder/{method}"}}'
        return _ratio(
            delta(f"sum(rpc_server_call_duration_seconds_sum{sel})"),
            delta(f"sum(rpc_server_call_duration_seconds_count{sel})"),
        )

    restore_q = quantiles(_rpc_buckets("Restore"))
    ckpt_q = quantiles(_rpc_buckets("Checkpoint"))

    # All files' bytes over `total`-phase seconds, failed saves excluded.
    written = delta("sum(atelet_snapshot_size_bytes_sum)")
    spent = delta(
        "sum(ate_actor_checkpoint_duration_seconds_sum"
        '{ate_snapshot_phase="total", '
        'ate_failure_reason!="FAILED_SAVE_SNAPSHOT"})'
    )
    checkpoint_mb_s = _ratio(written, spent, scale=mb)

    return {
        "size_p50_mb": size_q[0],
        "size_p90_mb": size_q[1],
        "size_p95_mb": size_q[2],
        "size_p99_mb": size_q[3],
        "size_avg_mb": snap_avg,
        "checkpoints_in_window": window_checkpoints,
        "checkpoints_cumulative": c_end,
        "restore_p50_s": restore_q[0],
        "restore_p90_s": restore_q[1],
        "restore_p95_s": restore_q[2],
        "restore_p99_s": restore_q[3],
        "restore_mean_s": rpc_mean("Restore"),
        "checkpoint_p50_s": ckpt_q[0],
        "checkpoint_p90_s": ckpt_q[1],
        "checkpoint_p95_s": ckpt_q[2],
        "checkpoint_p99_s": ckpt_q[3],
        "checkpoint_mean_s": rpc_mean("Checkpoint"),
        "checkpoint_mb_s": checkpoint_mb_s,
    }


def harvest_server_telemetry(
    prom_url: str,
    start_ts: int,
    end_ts: int,
    steady_start_ts: int,
    worker_pod_count: int | None,
    lag_s: int = 0,
    steady_end_ts: int | None = None,
) -> dict[str, Any]:
    """Harvests the ground truth metric streams from Prometheus.

    Only the atelet-exported snapshot block is read late, by up to `lag_s`;
    packing and PSI are scraped directly and read over the run window as is.
    """
    steady_end = end_ts if steady_end_ts is None else steady_end_ts
    return {
        "cluster_packing": _harvest_cluster_packing(
            prom_url, start_ts, end_ts, steady_start_ts, worker_pod_count
        ),
        "node_psi": _harvest_node_psi(prom_url, start_ts, end_ts, steady_start_ts),
        "pod_psi": _harvest_pod_psi(prom_url, start_ts, end_ts, steady_start_ts),
        "snapshots": _harvest_snapshots(
            prom_url, steady_start_ts, steady_end, lag_s
        ),
    }


def extract_and_record_server_telemetry(
    prom_url: str,
    start_ts: int,
    end_ts: int,
    stats_history_csv: Path,
    worker_pod_count: int | None,
    output_json_path: Path,
    jsonl_path: Path,
    data_ts: str,
    tag: str,
    test_name: str,
    logs: TextIO | None = None,
    lag_s: int = 15,
) -> None:
    """Entry point called by runner.py to query Prometheus and persist artifacts.

    Waits until `end_ts + lag_s` first, so the atelet's last export of the run
    has been scraped; lag_s must cover its export interval plus one scrape.
    """
    def log(msg: str) -> None:
        if logs:
            print(f"[ServerTelemetry] {msg}", file=logs, flush=True)
        print(f"[ServerTelemetry] {msg}", flush=True)

    wait_s = max(0.0, end_ts + lag_s - time.time())
    if wait_s:
        log(f"Waiting {wait_s:.0f}s for the atelet's last export to be scraped...")
        time.sleep(wait_s)

    log(f"Harvesting Prometheus metrics from {prom_url} over [{start_ts}, {end_ts}]...")
    steady_start, steady_end = get_steady_state_window(
        stats_history_csv, start_ts, end_ts
    )
    log(
        f"Detected steady-state window: [{steady_start}, {steady_end}] "
        f"({steady_end - steady_start}s), snapshot reads at "
        f"[{steady_start + lag_s // 2}, {steady_end + lag_s // 2}]"
    )

    telemetry = harvest_server_telemetry(
        prom_url, start_ts, end_ts, steady_start, worker_pod_count, lag_s,
        steady_end_ts=steady_end,
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
            "atelet_lag_s": lag_s,
        },
        **telemetry,
    }

    output_json_path.write_text(
        json.dumps(full_artifact, indent=2) + "\n", encoding="utf-8"
    )
    log(f"Wrote server summary artifact to {output_json_path}")

    # Append normalized single-row summary into stats.jsonl
    packing_s = telemetry.get("cluster_packing", {}).get("summary", {})
    psi = telemetry.get("node_psi", {})
    pod_psi = telemetry.get("pod_psi", {})
    snaps = telemetry.get("snapshots", {})
    ps = ("p50", "p90", "p95", "p99")

    measurements: dict[str, Any] = {
        f"cluster_packing_{p}": packing_s.get(p) for p in ps
    }
    for res in ("cpu", "mem", "io"):
        for p in ps:
            measurements[f"psi_{res}_stall_{p}"] = (
                psi.get(f"{res}_stall_pct", {}).get(p)
            )
            measurements[f"pod_psi_{res}_stall_{p}"] = (
                pod_psi.get(f"{res}_stall_pct", {}).get(p)
            )
    for p in ps:
        measurements[f"snapshot_size_{p}_mb"] = snaps.get(f"size_{p}_mb")
    measurements["snapshot_size_avg_mb"] = snaps.get("size_avg_mb")
    measurements["checkpoints_in_window"] = snaps.get("checkpoints_in_window")
    for rpc in ("restore", "checkpoint"):
        for p in ps:
            measurements[f"{rpc}_{p}_s"] = snaps.get(f"{rpc}_{p}_s")
        measurements[f"{rpc}_mean_s"] = snaps.get(f"{rpc}_mean_s")
    measurements["checkpoint_mb_s"] = snaps.get("checkpoint_mb_s")

    jsonl_row = {
        "timestamp": data_ts,
        "tag": tag,
        "test_name": test_name,
        "metric": "server_summary",
        # Flat here, nested in server_summary.json: the jsonl is the graph
        # feed and every row in it carries the same five keys.
        "measurements": {
            k: (str(v) if v is not None else None)
            for k, v in measurements.items()
        },
    }

    with open(jsonl_path, "a", encoding="utf-8") as f:
        f.write(json.dumps(jsonl_row) + "\n")
    log(f"Appended server_summary row to {jsonl_path}")
