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

"""Nighthawk adaptive router-capacity benchmark runner.

One Kubernetes Job per tests.yaml entry (submitted by
benchmarking/automation/orchestrator.py):
  1. Create + warm N glutton actors through the router.
  2. Build the AdaptiveLoadSessionSpec (spec.py).
  3. Run nighthawk_service + nighthawk_adaptive_load_client.
  4. Convert the output (output.py) and upload to <dest>/runs/<name>/...
     (same Hive-partitioned layout as the locust runner).

Exit 0 iff the session converged (client exit 0 AND a testing-stage result
parsed) and all artifacts uploaded.
"""

import argparse
import json
import os
import re
import shutil
import socket
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import IO, TextIO

import actors
import output as output_mod
import spec as spec_mod

NIGHTHAWK_SERVICE = "nighthawk_service"
ADAPTIVE_CLIENT = "nighthawk_adaptive_load_client"
SERVICE_ADDRESS = "127.0.0.1:8443"

DEFAULT_ROUTER_URL = "http://atenet-router.ate-system.svc.cluster.local:80"
DESC_PATH = os.environ.get("NIGHTHAWK_DESC", "/app/nighthawk.desc")


def parse_duration_seconds(s: str) -> int:
    """Same grammar as the orchestrator: <int>[smh], default seconds."""
    m = re.fullmatch(r"(\d+)\s*([smh]?)", s.strip())
    if not m:
        raise ValueError(f"unrecognized duration: {s}")
    return int(m.group(1)) * {"s": 1, "m": 60, "h": 3600}[m.group(2) or "s"]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--name", required=True, help="Test name (GCS path component)")
    p.add_argument("--tag", required=True, help="Tag for this run (commit sha)")
    p.add_argument(
        "--dest",
        required=True,
        help="Root destination (gs://bucket/path or local path)",
    )
    p.add_argument("--envoy-cpu", required=True, type=int, dest="envoy_cpu")
    p.add_argument("--actors", type=int, default=100)
    # Event loops and per-loop pools are sized so the client never binds
    # before the router; decoupled from --envoy-cpu.
    p.add_argument(
        "--client-concurrency", type=int, default=16, dest="client_concurrency"
    )
    p.add_argument("--connections", type=int, default=1000)
    p.add_argument("--max-pending", type=int, default=10000, dest="max_pending")
    p.add_argument("--initial-rps", type=int, default=500, dest="initial_rps")
    p.add_argument("--exp-factor", type=float, default=2.0, dest="exp_factor")
    p.add_argument("--measuring-period", default="10s", dest="measuring_period")
    p.add_argument(
        "--convergence-deadline", default="600s", dest="convergence_deadline"
    )
    p.add_argument(
        "--testing-stage-duration", default="60s", dest="testing_stage_duration"
    )
    p.add_argument(
        "--success-rate-threshold",
        type=float,
        default=0.999,
        dest="success_rate",
    )
    # Open-loop backstop; without it the search diverges (see spec.py).
    p.add_argument(
        "--send-rate-threshold", type=float, default=0.9, dest="send_rate"
    )
    # SLO bound on latency-ns-mean-plus-2stdev, in ms; <= 0 disables.
    p.add_argument(
        "--tail-latency-slo-ms",
        type=float,
        default=0,
        dest="tail_latency_slo_ms",
    )
    p.add_argument("--router-url", default=DEFAULT_ROUTER_URL, dest="router_url")
    p.add_argument("--warm-deadline", default="600s", dest="warm_deadline")
    p.add_argument("--atespace", default=actors.ATESPACE)
    args, extra = p.parse_known_args()
    args.extra = extra
    return args


def tee(logs: TextIO, msg: str) -> None:
    print(msg, flush=True)
    logs.write(msg + "\n")
    logs.flush()


def pump_stream(prefix: str, stream: IO[str], logs: TextIO) -> None:
    for line in stream:
        tagged = f"[{prefix}] {line.rstrip()}"
        sys.stdout.write(tagged + "\n")
        sys.stdout.flush()
        logs.write(tagged + "\n")
        logs.flush()


def start_service(logs: TextIO) -> subprocess.Popen:
    proc = subprocess.Popen(
        [NIGHTHAWK_SERVICE, "--listen", SERVICE_ADDRESS],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        bufsize=1,
        text=True,
    )
    threading.Thread(
        target=pump_stream, args=("service", proc.stdout, logs), daemon=True
    ).start()
    host, port = SERVICE_ADDRESS.split(":")
    deadline = time.time() + 30
    while time.time() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(
                f"{NIGHTHAWK_SERVICE} exited early with {proc.returncode}"
            )
        try:
            with socket.create_connection((host, int(port)), timeout=1):
                tee(logs, f"{NIGHTHAWK_SERVICE} ready on {SERVICE_ADDRESS}")
                return proc
        except OSError:
            time.sleep(0.5)
    proc.terminate()
    raise RuntimeError(f"{NIGHTHAWK_SERVICE} not listening within 30s")


def run_adaptive_client(
    spec_path: Path, output_path: Path, logs: TextIO
) -> int:
    cmd = [
        ADAPTIVE_CLIENT,
        "--spec-file",
        str(spec_path),
        "--output-file",
        str(output_path),
        "--nighthawk-service-address",
        SERVICE_ADDRESS,
    ]
    tee(logs, f"Running: {' '.join(cmd)}")
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        bufsize=1,
        text=True,
    )
    pump_stream("adaptive", proc.stdout, logs)
    return proc.wait()


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


def log_run_config(args: argparse.Namespace, prefix: str, logs: TextIO) -> None:
    lines = [
        "==== Run config ====",
        f"  name:                   {args.name}",
        f"  tag:                    {args.tag}",
        f"  envoy_cpu (router limits + envoy --concurrency): {args.envoy_cpu}",
        f"  client_concurrency (nighthawk event loops): {args.client_concurrency}",
        f"  actors:                 {args.actors}",
        f"  connections/loop:       {args.connections}",
        f"  max_pending/loop:       {args.max_pending}",
        f"  initial_total_rps:      {args.initial_rps}",
        f"  exponential_factor:     {args.exp_factor}",
        f"  measuring_period:       {args.measuring_period}",
        f"  convergence_deadline:   {args.convergence_deadline}",
        f"  testing_stage_duration: {args.testing_stage_duration}",
        f"  success_rate_threshold: {args.success_rate}",
        f"  send_rate_threshold:    {args.send_rate}",
        f"  tail_latency_slo_ms:    {args.tail_latency_slo_ms or '(disabled)'}",
        f"  atespace:               {args.atespace}",
        f"  router_url:             {args.router_url}",
        f"  dest_prefix:            {prefix}",
        f"  extra flags (ignored):  {' '.join(args.extra) or '(none)'}",
        "====================",
    ]
    for line in lines:
        tee(logs, line)


def main() -> None:
    args = parse_args()
    now = datetime.now(timezone.utc)
    work_dir = Path(f"/tmp/{now.strftime('%Y%m%dT%H%M%SZ')}-nighthawk-runner")
    work_dir.mkdir(parents=True, exist_ok=True)

    prefix = (
        f"{args.dest.rstrip('/')}/runs/{args.name}"
        f"/run_date={now.strftime('%Y-%m-%d')}/run_ts={int(now.timestamp())}"
        f"/run_tag={args.tag}"
    )

    logs_path = work_dir / "logs.txt"
    spec_path = work_dir / "spec.textproto"
    output_path = work_dir / "output.textproto"
    results_path = work_dir / "results.json"
    stats_path = work_dir / "stats.jsonl"
    capacity_path = work_dir / "capacity.json"
    status_path = work_dir / "status.json"

    adaptive_exit: int | None = None
    testing_stage_parsed = False
    actor_names: list[str] = []
    service_proc = None

    with open(logs_path, "w") as logs:
        log_run_config(args, prefix, logs)
        auth_mode = os.environ.get("ATE_ATEAPI_CLIENT_AUTH", "cert")
        stub = actors.ateapi_stub(auth_mode)
        try:
            # Filled incrementally so the finally-block cleanup covers a
            # fleet that failed partway through warming.
            actors.create_and_warm(
                stub,
                args.router_url,
                args.actors,
                created=actor_names,
                atespace=args.atespace,
                warm_deadline_s=parse_duration_seconds(args.warm_deadline),
                log=lambda m: tee(logs, m),
            )

            hosts = [
                spec_mod.actor_host(n, args.atespace) for n in actor_names
            ]
            spec_dict = spec_mod.build_spec_dict(
                uri=f"{args.router_url.rstrip('/')}/ping",
                hosts=hosts,
                client_concurrency=args.client_concurrency,
                connections=args.connections,
                max_pending_requests=args.max_pending,
                initial_total_rps=args.initial_rps,
                exponential_factor=args.exp_factor,
                measuring_period_s=parse_duration_seconds(args.measuring_period),
                convergence_deadline_s=parse_duration_seconds(
                    args.convergence_deadline
                ),
                testing_stage_duration_s=parse_duration_seconds(
                    args.testing_stage_duration
                ),
                success_rate_threshold=args.success_rate,
                send_rate_threshold=args.send_rate,
                tail_latency_slo_ms=(
                    args.tail_latency_slo_ms
                    if args.tail_latency_slo_ms > 0
                    else None
                ),
            )
            spec_path.write_text(
                spec_mod.spec_dict_to_textproto(spec_dict, DESC_PATH)
            )
            tee(logs, f"Wrote spec to {spec_path}")

            service_proc = start_service(logs)
            adaptive_exit = run_adaptive_client(spec_path, output_path, logs)
            tee(logs, f"{ADAPTIVE_CLIENT} exited with code {adaptive_exit}")

            if output_path.exists():
                output_dict = output_mod.parse_output_textproto(
                    output_path.read_text(), DESC_PATH
                )
                results_path.write_text(json.dumps(output_dict, indent=2))
                rows = output_mod.stage_rows(output_dict)
                output_mod.write_jsonl(
                    output_mod.stats_records(rows, args.tag, args.name),
                    stats_path,
                )
                summary = output_mod.capacity_summary(
                    output_dict,
                    envoy_cpu=args.envoy_cpu,
                    actors=args.actors,
                    client_concurrency=args.client_concurrency,
                    tail_latency_slo_ms=(
                        args.tail_latency_slo_ms
                        if args.tail_latency_slo_ms > 0
                        else None
                    ),
                )
                capacity_path.write_text(json.dumps(summary, indent=2))
                testing_stage_parsed = any(
                    r["stage"] == "testing" for r in rows
                )
                tee(logs, f"Capacity summary: {json.dumps(summary)}")
            else:
                tee(logs, f"No output file at {output_path}")
        except Exception as e:
            tee(logs, f"Run failed: {e}")
        finally:
            if service_proc is not None:
                service_proc.terminate()
                try:
                    service_proc.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    service_proc.kill()
            if actor_names:
                tee(logs, f"Cleaning up {len(actor_names)} actors")
                actors.cleanup(
                    stub,
                    actor_names,
                    atespace=args.atespace,
                    log=lambda m: tee(logs, m),
                )

    converged = adaptive_exit == 0 and testing_stage_parsed
    status_path.write_text(
        json.dumps(
            {
                "adaptive_client_exit_code": adaptive_exit,
                "converged": converged,
                "actors_created": len(actor_names),
            }
        )
    )

    upload_ok = True
    files = [
        (status_path, "status.json"),
        (logs_path, "logs.txt"),
        (spec_path, "spec.textproto"),
        (output_path, "output.textproto"),
        (results_path, "results.json"),
        (stats_path, "stats.jsonl"),
        (capacity_path, "capacity.json"),
    ]
    for src, basename in files:
        if not src.exists():
            print(f"Skipping {src}: not produced", flush=True)
            continue
        dest = f"{prefix}/{basename}"
        try:
            upload(src, dest)
            print(f"Uploaded {src} -> {dest}", flush=True)
        except Exception as e:
            print(f"Upload of {src} failed: {e}", flush=True)
            upload_ok = False

    sys.exit(0 if (converged and upload_ok) else 1)


if __name__ == "__main__":
    main()
