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
"""Collects server-side and node-level evidence for one disk-contention run.

Queries the in-cluster Prometheus (port-forwarded) over the measurement window
[start + warmup, end] and, optionally, GCE Cloud Monitoring disk throttling
metrics for the node's boot disk. Writes one JSON file.

Only the Python standard library is used, so it runs on any workstation.
"""

import argparse
import json
import re
import subprocess
import sys
import time
import urllib.parse
import urllib.request

CHECKPOINT = "ate_actor_checkpoint_duration_seconds"
RESTORE = "ate_actor_restore_duration_seconds"


def prom(url: str, query: str, at: float) -> list:
    data = urllib.parse.urlencode({"query": query, "time": f"{at:.3f}"}).encode()
    with urllib.request.urlopen(f"{url}/api/v1/query", data=data, timeout=60) as r:
        body = json.load(r)
    if body.get("status") != "success":
        raise RuntimeError(f"prometheus error for {query}: {body}")
    return body["data"]["result"]


def prom_range(url: str, query: str, start: float, end: float, step: int) -> list:
    data = urllib.parse.urlencode(
        {"query": query, "start": f"{start:.3f}", "end": f"{end:.3f}", "step": str(step)}
    ).encode()
    with urllib.request.urlopen(f"{url}/api/v1/query_range", data=data, timeout=60) as r:
        body = json.load(r)
    if body.get("status") != "success":
        raise RuntimeError(f"prometheus error for {query}: {body}")
    return body["data"]["result"]


ONE_NODE = "the suite expects every actor on one node (see README)"


def scalar(result: list) -> float | None:
    if not result:
        return None
    if len(result) > 1:
        raise SystemExit(f"a node query returned {len(result)} series; {ONE_NODE}")
    v = float(result[0]["value"][1])
    return None if v != v else v  # NaN -> None


def by_label(result: list, label: str) -> dict:
    out = {}
    for r in result:
        v = float(r["value"][1])
        out[r["metric"].get(label, "")] = None if v != v else v
    return out


def phase_stats(url: str, metric: str, w: int, at: float, template: str = "glutton") -> dict:
    """Per-phase count, mean and p50/p95/p99 (seconds) over the window for the probe template."""
    lbl = "ate_snapshot_phase"
    flt = f'{{ate_template_name="{template}"}}' if template else ""
    stats = {}
    counts = by_label(prom(url, f"sum by ({lbl}) (increase({metric}_count{flt}[{w}s]))", at), lbl)
    sums = by_label(prom(url, f"sum by ({lbl}) (increase({metric}_sum{flt}[{w}s]))", at), lbl)
    qs = {}
    for q in (0.5, 0.95, 0.99):
        qs[q] = by_label(
            prom(
                url,
                f"histogram_quantile({q}, sum by ({lbl}, le) (increase({metric}_bucket{flt}[{w}s])))",
                at,
            ),
            lbl,
        )
    for phase, n in counts.items():
        s = sums.get(phase)
        stats[phase] = {
            "count": n,
            "mean_s": (s / n) if (s is not None and n) else None,
            "p50_s": qs[0.5].get(phase),
            "p95_s": qs[0.95].get(phase),
            "p99_s": qs[0.99].get(phase),
        }
    return stats


def _whole_disk(dev: str) -> str:
    """Strips partition suffixes (nvme0n1p1 -> nvme0n1, sda1 -> sda) while keeping md0/dm-0 intact."""
    dev = dev.removeprefix("/dev/")
    if dev.startswith(("nvme", "mmcblk", "loop")):
        return re.sub(r"p\d+$", "", dev)
    if re.match(r"^(?:sd|vd|xvd|hd)[a-z]+\d+$", dev):
        return re.sub(r"\d+$", "", dev)
    return dev


def mounted_device(url: str, suffix: str, at: float) -> str | None:
    """The block device mounted at a mount point ending in suffix ("" for /), if any."""
    query = (
        f'node_filesystem_avail_bytes{{mountpoint=~".*{suffix}$"}}'
        if suffix
        else 'node_filesystem_avail_bytes{mountpoint="/"}'
    )
    try:
        for r in prom(url, query, at):
            dev = r.get("metric", {}).get("device", "")
            if dev.startswith("/dev/"):
                return _whole_disk(dev) or None
    except Exception:  # pylint: disable=broad-except
        pass
    return None


def detect_device(url: str, mount_suffixes: tuple[str, ...], at: float, default: str = "nvme0n1") -> str:
    """Finds the block device mounted at the first of mount_suffixes that is a mount point.

    Falls back to the stateful partition, then to the root filesystem.
    """
    for suffix in (*mount_suffixes, "/mnt/stateful_partition", ""):
        dev = mounted_device(url, suffix, at)
        if dev:
            return dev
    return default


def disk_stats(url: str, device: str, w: int, at: float) -> dict:
    d = f'device="{device}"'
    q = {
        "write_MBps_avg": f"increase(node_disk_written_bytes_total{{{d}}}[{w}s]) / {w} / 1e6",
        "read_MBps_avg": f"increase(node_disk_read_bytes_total{{{d}}}[{w}s]) / {w} / 1e6",
        "write_MBps_p95_30s": (
            f"quantile_over_time(0.95, (rate(node_disk_written_bytes_total{{{d}}}[30s]) / 1e6)[{w}s:15s])"
        ),
        "write_iops_avg": f"increase(node_disk_writes_completed_total{{{d}}}[{w}s]) / {w}",
        "read_iops_avg": f"increase(node_disk_reads_completed_total{{{d}}}[{w}s]) / {w}",
        "util_frac": f"increase(node_disk_io_time_seconds_total{{{d}}}[{w}s]) / {w}",
        "write_await_ms": (
            f"1000 * increase(node_disk_write_time_seconds_total{{{d}}}[{w}s])"
            f" / increase(node_disk_writes_completed_total{{{d}}}[{w}s])"
        ),
        "read_await_ms": (
            f"1000 * increase(node_disk_read_time_seconds_total{{{d}}}[{w}s])"
            f" / increase(node_disk_reads_completed_total{{{d}}}[{w}s])"
        ),
        "queue_depth_avg": f"increase(node_disk_io_time_weighted_seconds_total{{{d}}}[{w}s]) / {w}",
        "psi_io_some_frac": f"increase(node_pressure_io_waiting_seconds_total[{w}s]) / {w}",
        "psi_io_full_frac": f"increase(node_pressure_io_stalled_seconds_total[{w}s]) / {w}",
        "psi_cpu_some_frac": f"increase(node_pressure_cpu_waiting_seconds_total[{w}s]) / {w}",
        "psi_mem_some_frac": f"increase(node_pressure_memory_waiting_seconds_total[{w}s]) / {w}",
        "net_tx_MBps_avg": f'increase(node_network_transmit_bytes_total{{device="eth0"}}[{w}s]) / {w} / 1e6',
        "net_rx_MBps_avg": f'increase(node_network_receive_bytes_total{{device="eth0"}}[{w}s]) / {w} / 1e6',
        "cpu_busy_frac": (
            f'1 - sum(increase(node_cpu_seconds_total{{mode="idle"}}[{w}s]))'
            f" / sum(increase(node_cpu_seconds_total[{w}s]))"
        ),
        "dirty_MB_avg": f"avg_over_time(node_memory_Dirty_bytes[{w}s]) / 1e6",
        "writeback_MB_max": f"max_over_time(node_memory_Writeback_bytes[{w}s]) / 1e6",
        # Boot-disk (kubelet ephemeral storage) headroom; the kubelet evicts pods below ~10%.
        "rootfs_free_frac_min": (
            f'min_over_time(min(node_filesystem_avail_bytes{{mountpoint="/mnt/stateful_partition"}}'
            f' / node_filesystem_size_bytes{{mountpoint="/mnt/stateful_partition"}})[{w}s:15s])'
        ),
        # Bytes of every snapshot file per checkpoint: all files over the count
        # of memory images (pages.img for gVisor, memory-ranges for micro-VM).
        "snapshot_MB_per_checkpoint": (
            f'sum(increase(atelet_snapshot_size_bytes_sum{{ate_template_name="glutton"}}[{w}s]))'
            f' / sum(increase(atelet_snapshot_size_bytes_count{{ate_template_name="glutton",file_name=~"pages.img|memory-ranges"}}[{w}s])) / 1e6'
        ),
    }
    return {k: scalar(prom(url, v, at)) for k, v in q.items()}


def series(url: str, start: float, end: float, device: str) -> dict:
    d = f'device="{device}"'
    q = {
        "disk_write_MBps": f"rate(node_disk_written_bytes_total{{{d}}}[30s]) / 1e6",
        "disk_read_MBps": f"rate(node_disk_read_bytes_total{{{d}}}[30s]) / 1e6",
        "psi_io_full_frac": "rate(node_pressure_io_stalled_seconds_total[30s])",
        "dirty_MB": "node_memory_Dirty_bytes / 1e6",
    }
    out = {}
    for k, v in q.items():
        res = prom_range(url, v, start, end, 15)
        if len(res) > 1:
            raise SystemExit(f"a node query returned {len(res)} series; {ONE_NODE}")
        out[k] = [[float(t), float(x)] for t, x in res[0]["values"]] if res else []
    return out


def _gce_series(project: str, token: str, metric: str, instance_id: str, start: float, end: float):
    iso = lambda t: time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(t))
    params = {
        "filter": (
            f'metric.type="compute.googleapis.com/instance/disk/{metric}" '
            f'AND resource.labels.instance_id="{instance_id}"'
        ),
        "interval.startTime": iso(start),
        "interval.endTime": iso(end),
    }
    req = urllib.request.Request(
        f"https://monitoring.googleapis.com/v3/projects/{project}/timeSeries?"
        + urllib.parse.urlencode(params),
        headers={"Authorization": f"Bearer {token}"},
    )
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.load(r).get("timeSeries", [])


def _pv(p: dict) -> float:
    v = p["value"]
    if "boolValue" in v:
        return 1.0 if v["boolValue"] else 0.0
    return float(v.get("int64Value", v.get("doubleValue", 0)))


def gce_disk(project: str, instance_id: str, start: float, end: float) -> dict:
    """GCE-side view of the node's disks over [start, end] (1-minute samples).

    Hyperdisk exposes no throttled_* counters, so throttling shows up as
    performance_status != Healthy and as per-minute max throughput near the
    provisioned limit.
    """
    token = subprocess.check_output(
        ["gcloud", "auth", "print-access-token"], text=True, stderr=subprocess.DEVNULL
    ).strip()
    out: dict = {}
    for m, how in (
        ("write_bytes_count", "sum"),
        ("read_bytes_count", "sum"),
        ("write_ops_count", "sum"),
        ("max_write_bytes_count", "max"),
        ("max_write_ops_count", "max"),
        ("average_io_latency", "mean"),
        ("average_io_queue_depth", "mean"),
        ("provisioning/throughput", "max"),
        ("provisioning/iops", "max"),
    ):
        try:
            tss = _gce_series(project, token, m, instance_id, start, end)
        except Exception as e:  # pylint: disable=broad-except
            out[m] = {"error": str(e)}
            continue
        per_disk = {}
        for ts in tss:
            dev = ts["metric"]["labels"].get("device_name", "?")
            vals = [_pv(p) for p in ts.get("points", [])]
            if not vals:
                continue
            agg = {"sum": sum(vals), "max": max(vals), "mean": sum(vals) / len(vals)}[how]
            per_disk[dev] = agg
        out[m] = per_disk
    # performance_status: fraction of samples per status, per disk.
    try:
        status: dict = {}
        for ts in _gce_series(project, token, "performance_status", instance_id, start, end):
            dev = ts["metric"]["labels"].get("device_name", "?")
            st = ts["metric"]["labels"].get("performance_status", "?")
            vals = [_pv(p) for p in ts.get("points", [])]
            if vals:
                status.setdefault(dev, {})[st] = sum(vals) / len(vals)
        out["performance_status_frac"] = status
    except Exception as e:  # pylint: disable=broad-except
        out["performance_status_frac"] = {"error": str(e)}
    return out


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--prom", default="http://localhost:19090")
    p.add_argument("--start", type=float, required=True, help="run start, unix seconds")
    p.add_argument("--end", type=float, required=True, help="run end, unix seconds")
    p.add_argument("--warmup", type=float, default=180, help="seconds excluded at start")
    p.add_argument(
        "--device",
        default="auto",
        help="node block device holding /var/lib/ate/actors (default: auto-detect, else nvme0n1)",
    )
    p.add_argument("--project", default="")
    p.add_argument("--instance-id", default="", help="GCE instance id of the node (optional)")
    p.add_argument("--out", required=True)
    a = p.parse_args()

    ws = a.start + a.warmup
    w = int(a.end - ws)
    if w < 60:
        print(f"measurement window too short: {w}s", file=sys.stderr)
        return 1
    boot_device = detect_device(a.prom, (), a.end, "nvme0n1")
    # Actor data is on the disk mounted at /var/lib/ate/actors or
    # /var/lib/ate, whichever is a mount point, else on the boot disk.
    device = (
        detect_device(a.prom, ("/var/lib/ate/actors", "/var/lib/ate"), a.end, boot_device)
        if a.device in ("", "auto")
        else a.device
    )
    node_stats = disk_stats(a.prom, device, w, a.end)
    node_stats["device"] = device
    res = {
        "window": {"start": ws, "end": a.end, "seconds": w, "warmup": a.warmup},
        "checkpoint_phases": phase_stats(a.prom, CHECKPOINT, w, a.end),
        "restore_phases": phase_stats(a.prom, RESTORE, w, a.end),
        "node": node_stats,
        "series": series(a.prom, a.start, a.end, device),
    }
    if device != boot_device:
        res["boot_node"] = disk_stats(a.prom, boot_device, w, a.end)
    # A disk of its own at /var/lib/ate/snapshots takes the snapshot staging
    # I/O off the actor disk.
    snapshots_device = mounted_device(a.prom, "/var/lib/ate/snapshots", a.end)
    if snapshots_device and snapshots_device != device:
        res["snapshots_node"] = disk_stats(a.prom, snapshots_device, w, a.end)
        res["snapshots_node"]["device"] = snapshots_device
    if a.project and a.instance_id:
        res["gce_disk"] = gce_disk(a.project, a.instance_id, ws, a.end)
    with open(a.out, "w") as f:
        json.dump(res, f, indent=1)
    print(json.dumps({k: res[k] for k in ("window", "node")}, indent=1))
    return 0


if __name__ == "__main__":
    sys.exit(main())
