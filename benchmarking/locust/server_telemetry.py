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

Queries Prometheus over [T_start, T_end] and the steady-state window inside it
to capture cluster packing, node and pod PSI stalls, snapshot sizes, latencies
and throughput, running actors and memory working set.
"""

import csv
import json
import math
import sys
import time
import urllib.parse
import urllib.request
from collections.abc import Callable
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
    prom_url: str, selector: str, start_ts: int, end_ts: int
) -> tuple[float | None, float | None]:
    """Returns (growth, total) for the counter sum(selector): its value at
    end_ts minus its value at start_ts, and its value at end_ts.

    Exact where increase() is not: increase() drops everything before the first
    sample of a series born inside the window. Both edges are read in one query
    at `end_ts`, so a series absent at the start counts from zero while a failed
    read leaves the delta unknown. A negative delta means a process restarted
    and reset the counter, so it is unknown too.
    """
    now = f"sum({selector})"
    delta = _parse_instant_float(query_prometheus_instant(
        prom_url, f"{now} - (sum({selector} @ {start_ts}) or vector(0))",
        time_ts=end_ts,
    ))
    end = _parse_instant_float(
        query_prometheus_instant(prom_url, now, time_ts=end_ts)
    )
    if delta is not None and delta < 0:
        delta = None
    return delta, end


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


def _node_psi_query(resource: str) -> str:
    """Node stall percentage, kept to nodes hosting a worker pod at that time.

    System nodes run no actors, and averaging them in would dilute the stall.
    A worker pod counts only while its own PSI series has a rate: cAdvisor
    stamps its samples, so a deleted pod's series lingers for the lookback.
    """
    return (
        f"{_psi_query(resource)} and on (instance) "
        f"{_psi_query(resource, POD_PSI_SELECTOR)}"
    )


def _steady_values(
    results: list[dict[str, Any]], steady_start_ts: int, steady_end_ts: int
) -> list[float]:
    """Every range series value inside [steady_start_ts, steady_end_ts]."""
    vals = []
    for s in results:
        for pt in s.get("values", []):
            try:
                if steady_start_ts <= int(pt[0]) <= steady_end_ts:
                    # NaN and Inf are filtered downstream by compute_percentiles.
                    vals.append(float(pt[1]))
            except (ValueError, IndexError):
                pass
    return vals


def _snapshot_op(op: str, suffix: str) -> str:
    """atelet's histogram for a whole suspend (checkpoint) or resume (restore).

    Buckets reach 60s. Every snapshot kind counts, like the size metric, which
    has no kind label.
    """
    return f'ate_actor_{op}_duration_seconds_{suffix}{{ate_snapshot_phase="total"}}'


def _series_samples(series: dict[str, Any]) -> dict[int, float]:
    """A range series' finite values by timestamp; malformed points are skipped.

    Inf is dropped as well as NaN: these reach server_summary.json, and
    json.dumps would emit a bare Infinity that strict parsers reject.
    """
    samples: dict[int, float] = {}
    for pt in series.get("values", []):
        try:
            t, val = int(pt[0]), float(pt[1])
        except (ValueError, IndexError, TypeError):
            continue
        if not math.isnan(val) and not math.isinf(val):
            samples[t] = val
    return samples


def _process_key(metric: dict[str, str]) -> str:
    """`exported_instance` when a collector re-exports, else the scraped one."""
    return metric.get("exported_instance") or metric.get("instance", "")


def _harvest_cluster_packing(
    prom_url: str,
    start_ts: int,
    end_ts: int,
    steady_start_ts: int,
    steady_end_ts: int,
) -> dict[str, Any]:
    """Workers with an actor over all workers, per sample and as percentiles."""
    packing_query = (
        'ate_workerpool_workers{ate_workerpool_name="benchmark-ateom"}'
    )
    packing_series = query_prometheus_range(
        prom_url, packing_query, start_ts, end_ts, step="10s"
    )

    # State maps per ateapi process, per sample.
    by_instance: dict[str, dict[int, dict[str, float]]] = {}
    for series in packing_series:
        metric = series.get("metric", {})
        state = metric.get("ate_worker_state", "unknown")
        samples = by_instance.setdefault(_process_key(metric), {})
        for t, val in _series_samples(series).items():
            samples.setdefault(t, {})[state] = val

    # The collector re-exports exited ateapis until they expire, so read
    # the process whose series runs latest, which is a live one.
    last_seen = {i: max(s) for i, s in by_instance.items() if s}
    ts_packing_map: dict[int, dict[str, float]] = {}
    for t in sorted({t for s in by_instance.values() for t in s}):
        live = max(
            (i for i in last_seen if t in by_instance[i]),
            key=lambda i: (last_seen[i], i),
        )
        ts_packing_map[t] = by_instance[live][t]

    packing_points = []
    steady_packing_ratios = []
    for t in sorted(ts_packing_map.keys()):
        states = ts_packing_map[t]
        busy = states.get("partial", 0.0) + states.get("at_capacity", 0.0)
        # The states partition the pool, so this is its size at this sample.
        total = sum(states.values())
        # The total is recorded either way, but a ratio over zero workers is
        # not a reading, so it stays null rather than taking a stand-in.
        ratio = round(busy / total, 4) if total > 0 else None
        packing_points.append({
            "timestamp": t,
            "busy_workers": busy,
            "total_workers": total,
            "packing_ratio": ratio,
        })
        if steady_start_ts <= t <= steady_end_ts and ratio is not None:
            steady_packing_ratios.append(ratio)

    return {
        "summary": compute_percentiles(steady_packing_ratios),
        "timeseries": packing_points,
    }


def _harvest_node_psi(
    prom_url: str, start_ts: int, end_ts: int, steady_start_ts: int,
    steady_end_ts: int,
) -> dict[str, Any]:
    """Worker node stall percentages, every node's steady samples pooled."""
    out: dict[str, Any] = {}
    nodes: set[str] = set()
    for key, resource in (("cpu", "cpu"), ("mem", "memory"), ("io", "io")):
        res = query_prometheus_range(
            prom_url, _node_psi_query(resource), start_ts, end_ts, step="10s"
        )
        nodes.update(s.get("metric", {}).get("instance", "") for s in res)
        out[f"{key}_stall_pct"] = compute_percentiles(
            _steady_values(res, steady_start_ts, steady_end_ts)
        )
    out["nodes"] = len(nodes - {""})
    return out


def _harvest_pod_psi(
    prom_url: str, start_ts: int, end_ts: int, steady_start_ts: int,
    steady_end_ts: int,
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
            _steady_values(res, steady_start_ts, steady_end_ts)
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

    # Sizes and the checkpoint count scope to the memory image, which each
    # checkpoint writes once: pages.img (gVisor) or memory-ranges (microVM).
    # A run mixing both runtimes blends their sizes into one distribution.
    snap_selector = (
        'atelet_snapshot_size_bytes%s{file_name=~"pages.img|memory-ranges"}'
    )
    size_q = quantiles(snap_selector % "_bucket", scale=mb)

    count, count_end = _window_delta(
        prom_url, snap_selector % "_count", start_at, end_at
    )
    window_checkpoints = round(count) if count is not None else None
    c_end = round(count_end) if count_end is not None else None
    snap_avg = _ratio(delta(snap_selector % "_sum"), count, scale=mb)

    def op_mean(op: str) -> float | None:
        return _ratio(
            delta(_snapshot_op(op, "sum")), delta(_snapshot_op(op, "count"))
        )

    restore_q = quantiles(_snapshot_op("restore", "bucket"))
    ckpt_q = quantiles(_snapshot_op("checkpoint", "bucket"))

    # All files' bytes over `total`-phase seconds.
    written = delta("atelet_snapshot_size_bytes_sum")
    spent = delta(
        'ate_actor_checkpoint_duration_seconds_sum{ate_snapshot_phase="total"}'
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
        "restore_mean_s": op_mean("restore"),
        "checkpoint_p50_s": ckpt_q[0],
        "checkpoint_p90_s": ckpt_q[1],
        "checkpoint_p95_s": ckpt_q[2],
        "checkpoint_p99_s": ckpt_q[3],
        "checkpoint_mean_s": op_mean("checkpoint"),
        "checkpoint_mb_s": checkpoint_mb_s,
    }


def _count_active_actors(
    prom_url: str,
    start_ts: int,
    end_ts: int,
    steady_start_ts: int,
    steady_end_ts: int,
    lag_s: int = 0,
) -> dict[str, Any]:
    """Running actors over the run, cluster-wide.

    `summary` is percentiles of the cluster-wide total over the steady window.
    `timeseries` covers the whole run, ramp-up included: every 10s, the total
    and `active_atelets` hosting an actor. `atelets` counts the atelet
    processes seen in the run, not nodes.

    The atelet drops a template's series when its last actor on the node
    leaves, so at a step where at least one atelet reported, a missing series
    is zero actors. A step where none reported is skipped, since a failed
    scrape looks the same as no actors anywhere. If the metric was never seen,
    every field stays null.

    The collector re-exports an exited atelet's last value for about 5 minutes,
    so a restarted atelet can briefly count twice. An atelet scraped directly
    and through a collector shows up under 2 keys and is not merged.

    The atelet exports it like the snapshot block, so it is read `lag_s // 2`
    late and each point is stamped back by the same amount.
    """
    shift = lag_s // 2
    first, last = start_ts + shift, max(end_ts, start_ts + 1) + shift
    steady_first, steady_last = steady_start_ts + shift, steady_end_ts + shift
    res = query_prometheus_range(
        prom_url,
        "sum by (instance, exported_instance) (ate_actor_stats_sampled_actors)",
        first, last, step="10s",
    )
    # Same key as packing; 2 series under one key (e.g. 2 collector replicas)
    # describe the same atelet, so they merge by max, not sum.
    by_atelet: dict[str, dict[int, float]] = {}
    for series in res:
        new = _series_samples(series)
        if not new:
            continue
        samples = by_atelet.setdefault(_process_key(series.get("metric", {})), {})
        for t, v in new.items():
            samples[t] = max(samples.get(t, v), v)

    if not by_atelet:
        return {
            "summary": compute_percentiles([]),
            "atelets": None,
            "timeseries": [],
        }

    # Range query samples land on first + k * step.
    points, steady_totals = [], []
    for t in range(first, last + 1, 10):
        if not any(t in s for s in by_atelet.values()):
            continue
        counts = [s.get(t, 0.0) for s in by_atelet.values()]
        total = sum(counts)
        points.append({
            "timestamp": t - shift,
            "active_actors": total,
            "active_atelets": sum(1 for v in counts if v > 0),
        })
        if steady_first <= t <= steady_last:
            steady_totals.append(total)
    return {
        "summary": compute_percentiles(steady_totals),
        "atelets": len(by_atelet),
        "timeseries": points,
    }


# Live worker pods: the rate drops a deleted pod's stale series in ~1 min, not 5.
WORKER_PODS = (
    "rate(container_cpu_usage_seconds_total"
    '{namespace="benchmark-workloads", container="ateom"}[1m]) > 0'
)

# Only nodes/pods with a live worker; max, not sum: a restart briefly has 2 series.
WORKING_SET_QUERIES = {
    "node": (
        'max by (instance) (container_memory_working_set_bytes{container="node"})'
        f" and on (instance) count by (instance) ({WORKER_PODS})"
    ),
    "ateom": (
        "max by (pod) (container_memory_working_set_bytes"
        '{namespace="benchmark-workloads", container="ateom"})'
        f" and on (pod) ({WORKER_PODS})"
    ),
    "atelet": (
        "max by (pod, instance) (container_memory_working_set_bytes"
        '{namespace="ate-system", container="atelet"})'
        f" and on (instance) count by (instance) ({WORKER_PODS})"
    ),
}

# Actors' working set and running count per atelet, all templates summed; keyed
# like packing and active_actors. Not node names, so no worker filter: the
# atelet stops exporting on a node with no running actor.
ACTOR_WORKING_SET = (
    "sum by (instance, exported_instance) (ate_actor_stats_memory_working_set_bytes)"
)
ACTOR_COUNT = "sum by (instance, exported_instance) (ate_actor_stats_sampled_actors)"
WORKING_SET_KEYS = (*WORKING_SET_QUERIES, "actor", "per_actor")

BYTES_PER_GIB = 2**30


def _label_key(metric: dict[str, str]) -> tuple:
    """Every label, so each series keeps its own key."""
    return tuple(sorted(metric.items()))


def _read_series(
    prom_url: str, query: str, start_ts: int, end_ts: int, shift: int = 0,
    key: Callable[[dict[str, str]], Any] = _label_key,
) -> dict[Any, dict[int, float]]:
    """Samples per `key` every 10s, read `shift` late and moved back by it.

    Series sharing a key describe the same thing, so they merge by max.
    """
    res = query_prometheus_range(
        prom_url, query, start_ts + shift, end_ts + shift, step="10s"
    )
    out: dict[Any, dict[int, float]] = {}
    for series in res:
        new = _series_samples(series)
        if not new:
            continue
        samples = out.setdefault(key(series.get("metric", {})), {})
        for t, val in new.items():
            samples[t - shift] = max(samples.get(t - shift, val), val)
    return out


def _summarize_gb(
    series: dict[Any, dict[int, float]],
    steady_start_ts: int,
    steady_end_ts: int,
) -> dict[str, Any]:
    """Steady percentiles, series seen in steady, and a 10s sum/max timeseries."""
    by_ts: dict[int, list[float]] = {}
    steady_gb: list[float] = []
    steady_series = 0
    for samples in series.values():
        in_steady = False
        for t, val in samples.items():
            gb = val / BYTES_PER_GIB
            by_ts.setdefault(t, []).append(gb)
            if steady_start_ts <= t <= steady_end_ts:
                steady_gb.append(gb)
                in_steady = True
        steady_series += in_steady
    return {
        "summary": compute_percentiles(steady_gb),
        "count": steady_series if by_ts else None,
        "timeseries": [
            {
                "timestamp": t,
                "total_gb": round(sum(v), 4),
                "max_gb": round(max(v), 4),
            }
            for t, v in sorted(by_ts.items())
        ],
    }


def _measure_working_set(
    prom_url: str,
    start_ts: int,
    end_ts: int,
    steady_start_ts: int,
    steady_end_ts: int,
    lag_s: int = 0,
) -> dict[str, Any]:
    """Working set GiB per node, ateom, atelet and actor; null, not 0, with no series.

    `ateom` is the whole worker pod, every actor on it included. `actor` is the
    atelet's sum over the actors it measured, per atelet; `per_actor` divides it
    by their count. The atelet samples about once a minute and exports late, so
    both are read `lag_s // 2` late and moved back onto the run's 10s grid.
    """
    def summarize(series: dict[Any, dict[int, float]]) -> dict[str, Any]:
        return _summarize_gb(series, steady_start_ts, steady_end_ts)

    out: dict[str, Any] = {
        key: summarize(_read_series(prom_url, query, start_ts, end_ts))
        for key, query in WORKING_SET_QUERIES.items()
    }
    shift = lag_s // 2
    actor = _read_series(
        prom_url, ACTOR_WORKING_SET, start_ts, end_ts, shift, key=_process_key
    )
    count = _read_series(
        prom_url, ACTOR_COUNT, start_ts, end_ts, shift, key=_process_key
    )
    out["actor"] = summarize(actor)
    # Same atelet and timestamp; a missing or zero count is skipped.
    out["per_actor"] = summarize({
        key: {t: v / count[key][t] for t, v in samples.items()
              if count.get(key, {}).get(t)}
        for key, samples in actor.items()
    })
    return out


def harvest_server_telemetry(
    prom_url: str,
    start_ts: int,
    end_ts: int,
    steady_start_ts: int,
    lag_s: int = 0,
    steady_end_ts: int | None = None,
) -> dict[str, Any]:
    """Harvests the ground truth metric streams from Prometheus.

    Only the atelet-exported snapshot, active actor and actor working set
    blocks are read late, by `lag_s // 2`; packing, PSI and the cAdvisor
    working set aren't shifted and are read over the run window as is.
    """
    steady_end = end_ts if steady_end_ts is None else steady_end_ts
    return {
        "cluster_packing": _harvest_cluster_packing(
            prom_url, start_ts, end_ts, steady_start_ts, steady_end
        ),
        "node_psi": _harvest_node_psi(
            prom_url, start_ts, end_ts, steady_start_ts, steady_end
        ),
        "pod_psi": _harvest_pod_psi(
            prom_url, start_ts, end_ts, steady_start_ts, steady_end
        ),
        "snapshots": _harvest_snapshots(
            prom_url, steady_start_ts, steady_end, lag_s
        ),
        "active_actors": _count_active_actors(
            prom_url, start_ts, end_ts, steady_start_ts, steady_end, lag_s
        ),
        "working_set": _measure_working_set(
            prom_url, start_ts, end_ts, steady_start_ts, steady_end, lag_s
        ),
    }


def extract_and_record_server_telemetry(
    prom_url: str,
    start_ts: int,
    end_ts: int,
    stats_history_csv: Path,
    output_json_path: Path,
    jsonl_path: Path,
    data_ts: str,
    tag: str,
    test_name: str,
    logs: TextIO | None = None,
    lag_s: int = 70,
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
        f"({steady_end - steady_start}s), atelet-exported reads (snapshots, "
        f"active actors) at [{steady_start + lag_s // 2}, {steady_end + lag_s // 2}]"
    )

    telemetry = harvest_server_telemetry(
        prom_url, start_ts, end_ts, steady_start, lag_s,
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
    active = telemetry.get("active_actors", {}).get("summary", {})
    for p in ps:
        measurements[f"active_actors_{p}"] = active.get(p)
    working_set = telemetry.get("working_set", {})
    for block in WORKING_SET_KEYS:
        ws = working_set.get(block, {}).get("summary", {})
        for p in (*ps, "max"):
            measurements[f"working_set_{block}_{p}_gb"] = ws.get(p)

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
