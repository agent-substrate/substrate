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

"""Headless locust runner that publishes results as JSONL + CSVs.

Runs locust with the given flags, converts the resulting stats CSV
to JSONL, and uploads everything to either GCS or local disk under
<dest>/runs/<tag>/<timestamp>/.

When the test target is glutton.py, also spawns the boomer-glutton Go
worker as a subprocess (locust runs in --master + --expect-workers=1
mode) so the GluttonUser load comes from boomer instead of Python+gevent.
"""

import argparse
import csv
import json
import os
import re
import shutil
import signal
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import IO, TextIO

from common.boomer_config import build_config_json

# Path inside the locust image to the boomer-glutton binary baked in by
# benchmarking/locust/Dockerfile.
BOOMER_BINARY = "/app/boomer-glutton"

# Tab-separated columns written to traces.txt. Order matters — readers split
# on \t and index positionally.
TRACE_COLUMNS = ("time", "name", "duration_ms", "latency_source", "trace_id", "err")

# Python locust per-trace log line. Emitted by common/grpc_tracing.py and
# tests/counter_demo.py as:
#   Traced {name}[ (failed)]: trace_id={32hex}, duration_ms={float} ({src})
PY_TRACE_RE = re.compile(
    r"Traced\s+(?P<name>\S+)(?:\s+\(failed\))?:\s*"
    r"trace_id=(?P<trace_id>[0-9a-f]{32}),\s*"
    r"duration_ms=(?P<duration>[0-9.]+)\s+"
    r"\((?P<source>\w+)\)"
)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("-f", required=True, dest="file", help="Locust test file (-f)")
    p.add_argument("-t", required=True, dest="duration", help="Run duration (-t)")
    p.add_argument(
        "-u", required=True, type=int, dest="users", help="Number of users (-u)"
    )
    p.add_argument("--tag", required=True, help="Tag for this run")
    p.add_argument(
        "--name", required=True, help="Name for this run; used as locust --csv prefix"
    )
    p.add_argument(
        "--dest",
        required=True,
        help="Root destination (gs://bucket/path or local path)",
    )
    args, extra = p.parse_known_args()
    args.locust_extra = extra
    return args


def needs_boomer(test_file: str) -> bool:
    """Return True if the test file is the glutton stub; the real GluttonUser
    implementation lives in the boomer-glutton binary."""
    return os.path.basename(test_file) == "glutton.py"


def tee(logs: TextIO, msg: str) -> None:
    print(msg, flush=True)
    logs.write(msg + "\n")
    logs.flush()


def log_run_config(args: argparse.Namespace, dest_prefix: str, work_dir: Path, logs: TextIO) -> None:
    """Emit a structured summary of the test config at the top of every run
    so anyone reading logs.txt later can see exactly what was executed
    without cross-referencing tests.yaml + the orchestrator's invocation."""
    lines = [
        "==== Run config ====",
        f"  name:           {args.name}",
        f"  tag:            {args.tag}",
        f"  test_file:      {args.file}",
        f"  duration:       {args.duration}",
        f"  users:          {args.users}",
        f"  uses_boomer:    {needs_boomer(args.file)}",
        f"  dest_prefix:    {dest_prefix}",
        f"  work_dir:       {work_dir}",
        f"  extra flags:    {' '.join(args.locust_extra) if args.locust_extra else '(none)'}",
        "====================",
    ]
    for line in lines:
        tee(logs, line)


def extract_trace_record(prefix: str, line: str) -> dict[str, str] | None:
    """Parse `line` into a trace record dict (TRACE_COLUMNS keys) if it
    describes a sampled span, else return None. Handles boomer slog JSON
    lines (msg starts with 'traced span') and Python locust 'Traced ...:'
    free-form lines. Failed-span fields land in `err`."""
    if prefix == "boomer":
        if "trace_id" not in line:
            return None
        try:
            obj = json.loads(line)
        except ValueError:
            return None
        if not isinstance(obj, dict) or not str(obj.get("msg", "")).startswith("traced span"):
            return None
        if "trace_id" not in obj:
            return None
        duration = obj.get("duration_ms")
        return {
            "time": str(obj.get("time", "")),
            "name": str(obj.get("name", "")),
            "duration_ms": "" if duration is None else f"{float(duration):.3f}",
            "latency_source": str(obj.get("source", "")),
            "trace_id": str(obj["trace_id"]),
            "err": str(obj.get("err", "")),
        }
    m = PY_TRACE_RE.search(line)
    if not m:
        return None
    return {
        "time": "",
        "name": m.group("name"),
        "duration_ms": m.group("duration"),
        "latency_source": m.group("source"),
        "trace_id": m.group("trace_id"),
        "err": "failed" if "(failed)" in line else "",
    }


def pump_stream(prefix: str, stream: IO[str], logs: TextIO, traces: TextIO) -> None:
    """Forward each line of `stream` to stdout + logs (with a per-source
    prefix) and append any extracted trace records to `traces` as TSV rows."""
    for line in stream:
        line = line.rstrip("\n")
        tagged = f"[{prefix}] {line}"
        sys.stdout.write(tagged + "\n")
        sys.stdout.flush()
        logs.write(tagged + "\n")
        logs.flush()
        record = extract_trace_record(prefix, line)
        if record is not None:
            traces.write("\t".join(record[c] for c in TRACE_COLUMNS) + "\n")
            traces.flush()


def run_test(args: argparse.Namespace, csv_prefix: Path, logs: TextIO, traces: TextIO) -> int:
    """Run locust (and boomer, when needed). Returns locust's exit code.

    Stdout from each subprocess is forwarded to logs.txt with a `[locust]` /
    `[boomer]` prefix so they're distinguishable; trace_id matches are
    siphoned into traces.txt as a deduped one-per-line list.
    """
    with_boomer = needs_boomer(args.file)

    locust_cmd = [
        sys.executable, "-m", "locust",
        "--headless",
        "-f", args.file,
        "-t", args.duration,
        "-u", str(args.users),
        "--csv", str(csv_prefix),
    ]
    if with_boomer:
        # Master mode so boomer can connect as a worker on localhost:5557.
        # --expect-workers=1 makes locust wait for boomer before starting.
        locust_cmd += ["--master", "--expect-workers", "1"]
    locust_cmd += list(args.locust_extra)

    tee(logs, f"Running: {' '.join(locust_cmd)}")
    locust_proc = subprocess.Popen(
        locust_cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        bufsize=1,
        text=True,
    )

    pumps = []
    pumps.append(threading.Thread(
        target=pump_stream,
        args=("locust", locust_proc.stdout, logs, traces),
        daemon=True,
    ))

    boomer_proc = None
    if with_boomer:
        boomer_cmd = [BOOMER_BINARY]
        cfg_json = build_config_json(args.locust_extra)
        if cfg_json:
            boomer_cmd += ["--config-json", cfg_json]
        tee(logs, f"Running: {' '.join(boomer_cmd)}")
        boomer_proc = subprocess.Popen(
            boomer_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            bufsize=1,
            text=True,
        )
        pumps.append(threading.Thread(
            target=pump_stream,
            args=("boomer", boomer_proc.stdout, logs, traces),
            daemon=True,
        ))

    for t in pumps:
        t.start()

    locust_exit = locust_proc.wait()
    tee(logs, f"Locust exited with code {locust_exit}")

    if boomer_proc is not None:
        # Locust finishing means the test window is over; let boomer drain
        # its actor cleanup before tearing it down hard. The boomer process
        # suspends+deletes every actor it created on SIGTERM.
        tee(logs, "Stopping boomer...")
        boomer_proc.send_signal(signal.SIGTERM)
        try:
            boomer_proc.wait(timeout=90)
        except subprocess.TimeoutExpired:
            tee(logs, "Boomer did not exit within 90s; killing")
            boomer_proc.kill()
            boomer_proc.wait()
        tee(logs, f"Boomer exited with code {boomer_proc.returncode}")

    for t in pumps:
        t.join(timeout=5)

    return locust_exit


def stats_to_jsonl(stats_csv: Path, jsonl_path: Path, timestamp: str, tag: str, test_name: str) -> int:
    rows_written = 0
    with open(stats_csv) as f, open(jsonl_path, "w") as out:
        reader = csv.DictReader(f)
        for row in reader:
            type_val = row.pop("Type", "") or ""
            name_val = row.pop("Name", "") or ""
            if name_val == "Aggregated":
                continue
            measurements = {}
            for k, v in row.items():
                if k is None:
                    continue
                if k.endswith("%"):
                    # Percentile columns: "50%" -> "p50", "99.99%" -> "p99_99"
                    key = "p" + k[:-1].replace(".", "_")
                else:
                    # avoid non-alphanumeric characters
                    key = k.lower().replace("/", "_per_")
                    key = re.sub(r"[^a-z0-9]+", "_", key).strip("_")
                measurements[key] = v
            entry = {
                "timestamp": timestamp,
                "tag": tag,
                "test_name": test_name,
                "metric": f"{type_val}_{name_val}",
                "measurements": measurements,
            }
            out.write(json.dumps(entry) + "\n")
            rows_written += 1
    return rows_written


def upload_to_gcs(local_path: Path, gcs_uri: str) -> None:
    # Imported here so non-GCS use doesn't require google-cloud-storage.
    from google.cloud import storage

    bucket_name, _, blob_path = gcs_uri[len("gs://"):].partition("/")
    storage.Client().bucket(bucket_name).blob(blob_path).upload_from_filename(
        str(local_path)
    )


def upload(src: Path, dest: str) -> None:
    if dest.startswith("gs://"):
        upload_to_gcs(src, dest)
    else:
        dest_path = Path(dest)
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(src, dest_path)


def main() -> None:
    args = parse_args()
    now = datetime.now(timezone.utc)
    # Path-safe timestamp for the local work dir (no Hive semantics here).
    path_ts = now.strftime("%Y%m%dT%H%M%SZ")
    # RFC 3339 / ISO 8601 extended for the JSONL data column
    data_ts = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    # Hive partition values for the GCS layout
    run_date = now.strftime("%Y-%m-%d")
    run_ts = int(now.timestamp())

    work_dir = Path(f"/tmp/{path_ts}-locust-runner")
    work_dir.mkdir(parents=True, exist_ok=True)
    csv_prefix = work_dir / args.name
    stats_csv = work_dir / f"{args.name}_stats.csv"
    jsonl_path = work_dir / f"{args.name}.jsonl"
    logs_path = work_dir / f"{args.name}_logs.txt"
    traces_path = work_dir / f"{args.name}_traces.txt"
    status_path = work_dir / f"{args.name}_status.json"

    prefix = (
        f"{args.dest.rstrip('/')}/runs/{args.name}"
        f"/run_date={run_date}/run_ts={run_ts}/run_tag={args.tag}"
    )

    with open(logs_path, "w") as logs, open(traces_path, "w") as traces:
        traces.write("\t".join(TRACE_COLUMNS) + "\n")
        traces.flush()
        log_run_config(args, prefix, work_dir, logs)
        exit_code = run_test(args, csv_prefix, logs, traces)

        stats_generated = False
        if stats_csv.exists():
            try:
                rows = stats_to_jsonl(
                    stats_csv, jsonl_path, data_ts, args.tag, args.name
                )
                if rows == 0:
                    tee(
                        logs,
                        f"Stats CSV {stats_csv} had no measurement rows; "
                        f"treating as not produced",
                    )
                    if jsonl_path.exists():
                        jsonl_path.unlink()
                else:
                    stats_generated = jsonl_path.exists()
            except Exception as e:
                tee(logs, f"Failed to generate JSONL from {stats_csv}: {e}")
                if jsonl_path.exists():
                    jsonl_path.unlink()
        else:
            tee(logs, f"Stats CSV {stats_csv} not produced; skipping JSONL")

    status_path.write_text(
        json.dumps(
            {"locust_exit_code": exit_code, "stats_generated": stats_generated}
        )
    )

    files: list[tuple[Path, str]] = [
        (status_path, "status.json"),
        (logs_path, "logs.txt"),
        (traces_path, "traces.txt"),
        (jsonl_path, "stats.jsonl"),
        (stats_csv, "stats.csv"),
        (work_dir / f"{args.name}_exceptions.csv", "exceptions.csv"),
        (work_dir / f"{args.name}_failures.csv", "failures.csv"),
        (work_dir / f"{args.name}_stats_history.csv", "stats_history.csv"),
        # TODO: remove after data migration
        (jsonl_path, f"{args.name}.jsonl"),
    ]
    for src, basename in files:
        if not src.exists():
            print(f"Skipping {src}: not produced", flush=True)
            continue
        dest = f"{prefix}/{basename}"
        upload(src, dest)
        print(f"Uploaded {src} -> {dest}", flush=True)

    if not stats_generated:
        sys.exit(1)


if __name__ == "__main__":
    main()
