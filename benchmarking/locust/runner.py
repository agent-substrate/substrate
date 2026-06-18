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
"""

import argparse
import csv
import json
import re
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path


def parse_args():
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


def tee(logs, msg):
    print(msg, flush=True)
    logs.write(msg + "\n")
    logs.flush()


def run_locust(args, csv_prefix, logs):
    cmd = [
        sys.executable,
        "-m",
        "locust",
        "--headless",
        "-f",
        args.file,
        "-t",
        args.duration,
        "-u",
        str(args.users),
        "--csv",
        str(csv_prefix),
        *args.locust_extra,
    ]
    tee(logs, f"Running: {' '.join(cmd)}")
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        bufsize=1,
        text=True,
    )
    for line in proc.stdout:
        sys.stdout.write(line)
        sys.stdout.flush()
        logs.write(line)
        logs.flush()
    return proc.wait()


def stats_to_jsonl(stats_csv, jsonl_path, timestamp, tag, test_name):
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


def upload_to_gcs(local_path, gcs_uri):
    # Imported here so non-GCS use doesn't require google-cloud-storage.
    from google.cloud import storage

    bucket_name, _, blob_path = gcs_uri[len("gs://"):].partition("/")
    storage.Client().bucket(bucket_name).blob(blob_path).upload_from_filename(
        str(local_path)
    )


def upload(src, dest):
    if dest.startswith("gs://"):
        upload_to_gcs(src, dest)
    else:
        dest_path = Path(dest)
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(src, dest_path)


def main():
    args = parse_args()
    now = datetime.now(timezone.utc)
    # Path-safe timestamp for filesystem
    path_ts = now.strftime("%Y%m%dT%H%M%SZ")
    # RFC 3339 / ISO 8601 extended for the JSONL data column
    data_ts = now.strftime("%Y-%m-%dT%H:%M:%SZ")

    work_dir = Path(f"/tmp/locust-runner-{path_ts}")
    work_dir.mkdir(parents=True, exist_ok=True)
    csv_prefix = work_dir / args.name
    stats_csv = work_dir / f"{args.name}_stats.csv"
    jsonl_path = work_dir / f"{args.name}.jsonl"
    run_id = f"{args.tag}_{path_ts}"
    logs_path = work_dir / f"{args.name}_logs.txt"
    status_path = work_dir / f"{args.name}_status.json"

    with open(logs_path, "w") as logs:
        exit_code = run_locust(args, csv_prefix, logs)
        tee(logs, f"Locust exited with code {exit_code}")

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

    prefix = f"{args.dest.rstrip('/')}/runs/{args.name}/{run_id}"
    files = [
        status_path,
        logs_path,
        jsonl_path,
        stats_csv,
        work_dir / f"{args.name}_exceptions.csv",
        work_dir / f"{args.name}_failures.csv",
        work_dir / f"{args.name}_stats_history.csv",
    ]
    for src in files:
        if not src.exists():
            print(f"Skipping {src}: not produced", flush=True)
            continue
        dest = f"{prefix}/{src.name}"
        upload(src, dest)
        print(f"Uploaded {src} -> {dest}", flush=True)

    if not stats_generated:
        sys.exit(1)


if __name__ == "__main__":
    main()
