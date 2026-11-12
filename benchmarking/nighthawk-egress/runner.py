#!/usr/bin/env python3
# Copyright 2026 The Agent Substrate Authors
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

"""Nighthawk adaptive egress-gateway capacity benchmark runner.

One Kubernetes Job per tests.yaml entry, submitted by
benchmarking/automation/orchestrator.py:
  1. Build the AdaptiveLoadSessionSpec for the mode (spec.py).
  2. Run nighthawk_adaptive_load_client against the nighthawk_service in the
     nighthawk/tester actor, through the invoker sidecar of the Job.
  3. Convert the output (nighthawk_common/output.py) and upload it to
     <dest>/runs/<name>/..., the layout of the nighthawk-ingress runner.

Exit 0 only when the adaptive client exits 0, its output has a testing stage,
the runner raises no error, and every upload works.
"""

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import time
import traceback

from nighthawk_common.output import upload_files, write_artifacts
from spec import (
    DEFAULT_CONCURRENCY,
    DEFAULT_TAIL_LATENCY_SLO_MS,
    MODE_URIS,
    build_egress_session_spec,
    spec_dict_to_textproto,
)

ADAPTIVE_CLIENT = "nighthawk_adaptive_load_client"
INVOKER_ADDR = "127.0.0.1:19980"
SERVICEDNS_CA_CERT = "/run/servicedns-ca/ca.crt"
DESC_PATH = os.environ.get("NIGHTHAWK_DESC", "/app/nighthawk.desc")


def load_servicedns_ca_pem(path: str | os.PathLike[str]) -> str:
    if not os.path.isfile(path):
        raise ValueError(f"servicedns CA bundle not found at {path}")
    with open(path, encoding="utf-8") as f:
        pem = f.read()
    if not pem.strip():
        raise ValueError(f"servicedns CA bundle at {path} is empty")
    return pem


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--name", required=True)
    p.add_argument("--tag", required=True)
    p.add_argument("--dest", required=True)
    p.add_argument("--mode", required=True, choices=sorted(MODE_URIS))
    p.add_argument("--envoy-cpu", required=True, type=int, dest="envoy_cpu")
    return p.parse_args(argv)


def wait_for_tcp(host: str, port: int, timeout_s: float) -> None:
    deadline = time.monotonic() + timeout_s
    last_err: Exception | None = None
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=1.0):
                return
        except OSError as exc:
            last_err = exc
            time.sleep(0.25)
    raise RuntimeError(f"Timed out waiting for {host}:{port}: {last_err}")


def run_adaptive_client(spec_path: Path, output_path: Path, logs_path: Path) -> int:
    """Runs the adaptive session through the invoker sidecar and returns the client's exit code."""
    host, port = INVOKER_ADDR.rsplit(":", 1)
    with open(logs_path, "w") as logs:
        wait_for_tcp(host, int(port), timeout_s=30.0)
        cmd = [
            ADAPTIVE_CLIENT,
            "--nighthawk-service-address",
            INVOKER_ADDR,
            "--spec-file",
            str(spec_path),
            "--output-file",
            str(output_path),
        ]
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
        for line in proc.stdout or ():
            sys.stdout.write(line)
            sys.stdout.flush()
            logs.write(line)
        return proc.wait()


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    now = datetime.now(timezone.utc)
    work_dir = Path(f"/tmp/{now.strftime('%Y%m%dT%H%M%SZ')}-nighthawk-egress-{args.mode}")
    work_dir.mkdir(parents=True, exist_ok=True)
    prefix = (
        f"{args.dest.rstrip('/')}/runs/{args.name}"
        f"/run_date={now.strftime('%Y-%m-%d')}/run_ts={int(now.timestamp())}/run_tag={args.tag}"
    )
    print(f"Uploading to {prefix}", flush=True)
    logs_path = work_dir / "logs.txt"
    spec_path = work_dir / "spec.textproto"
    output_path = work_dir / "output.textproto"
    results_path = work_dir / "results.json"
    stats_path = work_dir / "stats.jsonl"
    capacity_path = work_dir / "capacity.json"
    status_path = work_dir / "status.json"

    # Each entry is one reason the run does not count. The exit code derives
    # from this list alone.
    failures: list[str] = []
    adaptive_exit = None
    slo_max_rps = None
    try:
        servicedns_ca_pem = (
            load_servicedns_ca_pem(SERVICEDNS_CA_CERT) if args.mode == "passthrough" else None
        )
        spec_dict = build_egress_session_spec(args.mode, servicedns_ca_pem=servicedns_ca_pem)
        spec_path.write_text(spec_dict_to_textproto(spec_dict, DESC_PATH))
        adaptive_exit = run_adaptive_client(spec_path, output_path, logs_path)
        if adaptive_exit != 0:
            failures.append(f"{ADAPTIVE_CLIENT} exited with code {adaptive_exit}")
        else:
            # The client writes its output only when the session succeeds.
            summary, has_testing_stage = write_artifacts(
                output_path,
                DESC_PATH,
                tag=args.tag,
                name=args.name,
                envoy_cpu=args.envoy_cpu,
                # The rig runs one actor, nighthawk/tester.
                actors=1,
                client_concurrency=DEFAULT_CONCURRENCY,
                tail_latency_slo_ms=DEFAULT_TAIL_LATENCY_SLO_MS,
                extra={"mode": args.mode},
            )
            slo_max_rps = summary["slo_max_rps"]
            print(f"Capacity summary: {json.dumps(summary)}", flush=True)
            # No testing stage means that the search did not finish, as in the nighthawk-ingress runner.
            if not has_testing_stage:
                failures.append(f"the output of {ADAPTIVE_CLIENT} has no testing stage")
    except Exception as exc:
        traceback.print_exc()
        failures.append(f"{type(exc).__name__}: {exc}")

    # Upload before the exit decision, so a failed run keeps its artifacts.
    failures += upload_files(
        [
            logs_path,
            spec_path,
            output_path,
            results_path,
            stats_path,
            capacity_path,
        ],
        prefix,
    )
    # status.json is last, so it names the uploads that failed before it.
    status_path.write_text(
        json.dumps(
            {
                "adaptive_client_exit_code": adaptive_exit,
                "slo_max_rps": slo_max_rps,
                "failures": failures,
            },
            indent=2,
        )
        + "\n"
    )
    failures += upload_files([status_path], prefix)

    for reason in failures:
        print(f"Run failed: {reason}", flush=True)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
