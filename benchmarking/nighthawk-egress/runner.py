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

"""Nighthawk adaptive egress-gateway capacity benchmark runner."""

import argparse
import contextlib
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import shutil
import socket
import subprocess
import sys
import time
import urllib.request
import urllib.parse

from output import (
    capacity_summary,
    gateway_counter_deltas,
    parse_gateway_prometheus,
    parse_output_textproto,
    stage_rows,
    stats_records,
    write_jsonl,
)
from spec import (
    DEFAULT_CONCURRENCY,
    DEFAULT_CONNECTIONS,
    DEFAULT_CONVERGENCE_DEADLINE_S,
    DEFAULT_EXP_FACTOR,
    DEFAULT_INITIAL_RPS,
    DEFAULT_MAX_PENDING,
    DEFAULT_MEASURING_PERIOD_S,
    DEFAULT_SEND_RATE_THRESHOLD,
    DEFAULT_SUCCESS_RATE_THRESHOLD,
    DEFAULT_TAIL_LATENCY_SLO_MS,
    DEFAULT_TESTING_STAGE_S,
    MODE_URIS,
    build_egress_session_spec,
    spec_dict_to_textproto,
)

ADAPTIVE_CLIENT = "nighthawk_adaptive_load_client"
DEFAULT_INVOKER_ADDR = "traffic-driver-invoker.nighthawk.svc:19980"
DEFAULT_GATEWAY_METRICS_URL = "http://atenet-egress-metrics.ate-system.svc:15090/stats/prometheus"
DEFAULT_SERVICEDNS_CA_CERT = "/run/servicedns-ca/ca.crt"
DEFAULT_DESC_PATH = os.environ.get("NIGHTHAWK_DESC", "/app/nighthawk.desc")


def parse_duration_seconds(s: str) -> int:
    m = re.fullmatch(r"(\d+)\s*([smh]?)", str(s).strip())
    if not m:
        raise ValueError(f"unrecognized duration: {s!r}")
    return int(m.group(1)) * {"s": 1, "m": 60, "h": 3600}[m.group(2) or "s"]


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
    p.add_argument("--client-concurrency", type=int, default=DEFAULT_CONCURRENCY, dest="client_concurrency")
    p.add_argument("--connections", type=int, default=DEFAULT_CONNECTIONS)
    p.add_argument("--max-pending", type=int, default=DEFAULT_MAX_PENDING, dest="max_pending")
    p.add_argument("--initial-rps", type=int, default=DEFAULT_INITIAL_RPS, dest="initial_rps")
    p.add_argument("--exp-factor", type=float, default=DEFAULT_EXP_FACTOR, dest="exp_factor")
    p.add_argument("--measuring-period", default=f"{DEFAULT_MEASURING_PERIOD_S}s", dest="measuring_period")
    p.add_argument("--convergence-deadline", default=f"{DEFAULT_CONVERGENCE_DEADLINE_S}s", dest="convergence_deadline")
    p.add_argument("--testing-stage-duration", default=f"{DEFAULT_TESTING_STAGE_S}s", dest="testing_stage_duration")
    p.add_argument("--success-rate-threshold", type=float, default=DEFAULT_SUCCESS_RATE_THRESHOLD, dest="success_rate")
    p.add_argument("--send-rate-threshold", type=float, default=DEFAULT_SEND_RATE_THRESHOLD, dest="send_rate")
    p.add_argument("--tail-latency-slo-ms", type=float, default=DEFAULT_TAIL_LATENCY_SLO_MS, dest="tail_latency_slo_ms")
    p.add_argument("--invoker-addr", default=DEFAULT_INVOKER_ADDR, dest="invoker_addr")
    p.add_argument("--gateway-metrics-url", default=DEFAULT_GATEWAY_METRICS_URL, dest="gateway_metrics_url")
    p.add_argument("--desc-path", default=DEFAULT_DESC_PATH, dest="desc_path")
    p.add_argument("--servicedns-ca-cert", default=DEFAULT_SERVICEDNS_CA_CERT, dest="servicedns_ca_cert")
    p.add_argument("--port-forward", action="store_true", dest="port_forward")
    return p.parse_args(argv)


def wait_for_tcp(
    host: str,
    port: int,
    timeout_s: float = 15.0,
    proc: subprocess.Popen[str] | None = None,
) -> None:
    deadline = time.monotonic() + timeout_s
    last_err: Exception | None = None
    while time.monotonic() < deadline:
        if proc is not None and proc.poll() is not None:
            err = (proc.stderr.read() if proc.stderr else "").strip()
            raise RuntimeError(f"port-forward exited with code {proc.returncode}: {err}")
        try:
            with socket.create_connection((host, port), timeout=1.0):
                return
        except OSError as exc:
            last_err = exc
            time.sleep(0.25)
    raise RuntimeError(f"Timed out waiting for {host}:{port}: {last_err}")


@contextlib.contextmanager
def port_forward(namespace: str, resource: str, local_port: int, remote_port: int):
    proc = subprocess.Popen(
        ["kubectl", "port-forward", "-n", namespace, resource, f"{local_port}:{remote_port}"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        wait_for_tcp("127.0.0.1", local_port, proc=proc)
        yield
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()


def scrape_gateway_prometheus(url: str, use_port_forward: bool) -> str:
    target = "http://127.0.0.1:15090/stats/prometheus" if use_port_forward else url
    ctx = (
        port_forward("ate-system", "svc/atenet-egress-metrics", 15090, 15090)
        if use_port_forward
        else contextlib.nullcontext()
    )
    with ctx:
        parsed = urllib.parse.urlparse(target)
        host = parsed.hostname
        if not host:
            raise ValueError(f"gateway metrics URL missing hostname: {target!r}")
        port = parsed.port or 80
        path = parsed.path or "/stats/prometheus"
        host_header = f"[{host}]" if ":" in host else host

        addr_infos = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
        v4_ips = sorted({info[4][0] for info in addr_infos if info[0] == socket.AF_INET})
        v6_ips = sorted({info[4][0] for info in addr_infos if info[0] == socket.AF_INET6})
        ips = v4_ips or v6_ips
        if not ips:
            raise RuntimeError(f"no TCP endpoints resolved for {host}:{port}")

        lines = [f"# SCRAPED_IPS: {','.join(ips)}"]
        for ip in ips:
            ip_str = f"[{ip}]" if ":" in ip else ip
            ip_target = urllib.parse.urlunparse(
                (parsed.scheme, f"{ip_str}:{port}", path, parsed.params, parsed.query, parsed.fragment)
            )
            req = urllib.request.Request(ip_target, headers={"Accept": "text/plain", "Host": host_header})
            with urllib.request.urlopen(req, timeout=10) as resp:
                lines.append(resp.read().decode("utf-8", errors="replace"))
        return "\n".join(lines)


def upload(src: Path, dest: str) -> None:
    if dest.startswith("gs://"):
        from google.cloud import storage

        bucket_name, _, blob_path = dest[len("gs://") :].partition("/")
        storage.Client().bucket(bucket_name).blob(blob_path).upload_from_filename(str(src))
    else:
        dest_path = Path(dest)
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(src, dest_path)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    servicedns_ca_pem = (
        load_servicedns_ca_pem(args.servicedns_ca_cert)
        if args.mode == "passthrough"
        else None
    )
    now = datetime.now(timezone.utc)
    work_dir = Path(f"/tmp/{now.strftime('%Y%m%dT%H%M%SZ')}-nighthawk-egress-{args.mode}")
    work_dir.mkdir(parents=True, exist_ok=True)
    prefix = (
        f"{args.dest.rstrip('/')}/runs/{args.name}"
        f"/run_date={now.strftime('%Y-%m-%d')}/run_ts={int(now.timestamp())}/run_tag={args.tag}"
    )
    logs_path = work_dir / "logs.txt"
    spec_path = work_dir / "spec.textproto"
    output_path = work_dir / "output.textproto"
    results_path = work_dir / "results.json"
    stats_path = work_dir / "stats.jsonl"
    capacity_path = work_dir / "capacity.json"
    status_path = work_dir / "status.json"
    status_tmp_path = work_dir / ".status.json.tmp"
    gw_before_path = work_dir / "gateway_before.prom"
    gw_after_path = work_dir / "gateway_after.prom"

    invoker_addr = "127.0.0.1:19980" if args.port_forward else args.invoker_addr
    slo_ms = args.tail_latency_slo_ms if args.tail_latency_slo_ms > 0 else None
    spec_dict = build_egress_session_spec(
        args.mode,
        concurrency=args.client_concurrency,
        connections=args.connections,
        max_pending=args.max_pending,
        initial_rps=args.initial_rps,
        exp_factor=args.exp_factor,
        measuring_period_s=parse_duration_seconds(args.measuring_period),
        convergence_deadline_s=parse_duration_seconds(args.convergence_deadline),
        testing_stage_s=parse_duration_seconds(args.testing_stage_duration),
        success_rate_threshold=args.success_rate,
        send_rate_threshold=args.send_rate,
        tail_latency_slo_ms=slo_ms,
        servicedns_ca_pem=servicedns_ca_pem,
    )
    spec_path.write_text(spec_dict_to_textproto(spec_dict, args.desc_path))

    before_prom = scrape_gateway_prometheus(args.gateway_metrics_url, args.port_forward)
    gw_before_path.write_text(before_prom)
    before_counters = parse_gateway_prometheus(before_prom)

    invoker_host, invoker_port = invoker_addr.rsplit(":", 1)
    invoker_host = invoker_host.strip("[]")
    invoker_ctx = (
        port_forward("nighthawk", "svc/traffic-driver-invoker", 19980, 19980)
        if args.port_forward
        else contextlib.nullcontext()
    )
    with invoker_ctx, open(logs_path, "w") as logs:
        wait_for_tcp(invoker_host, int(invoker_port), timeout_s=30.0)
        cmd = [
            ADAPTIVE_CLIENT,
            "--nighthawk-service-address",
            invoker_addr,
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
        adaptive_exit = proc.wait()

    after_scrape_error: Exception | None = None
    try:
        after_prom = scrape_gateway_prometheus(args.gateway_metrics_url, args.port_forward)
    except Exception as exc:
        after_scrape_error = exc
        after_prom = ""
    else:
        gw_after_path.write_text(after_prom)

    ip_mismatch_error = None
    counter_reset_error = None
    pod_ips_changed = False
    if after_scrape_error is not None:
        deltas = None
    else:
        before_ips = before_prom.split("\n", 1)[0] if before_prom.startswith("# SCRAPED_IPS") else ""
        after_ips = after_prom.split("\n", 1)[0] if after_prom.startswith("# SCRAPED_IPS") else ""
        pod_ips_changed = before_ips != after_ips
        if pod_ips_changed:
            deltas = None
            ip_mismatch_error = RuntimeError(
                f"Pod IP set changed between before and after scrapes: {before_ips} vs {after_ips}"
            )
        else:
            deltas = gateway_counter_deltas(before_counters, parse_gateway_prometheus(after_prom))

    scrape_err_str = str(after_scrape_error) if after_scrape_error is not None else None
    converged = False
    testing_stage_completed = False
    slo_max_rps = None

    if output_path.exists():
        output_dict = parse_output_textproto(output_path.read_text(), args.desc_path)
        results_path.write_text(json.dumps(output_dict, indent=2) + "\n")
        rows = stage_rows(output_dict)
        write_jsonl(stats_records(rows, args.tag, args.name), stats_path)
        summary = capacity_summary(
            output_dict,
            mode=args.mode,
            envoy_cpu=args.envoy_cpu,
            client_concurrency=args.client_concurrency,
            connections=args.connections,
            tail_latency_slo_ms=slo_ms,
            gateway_counters=deltas,
        )
        summary["gateway_pod_ips_changed"] = pod_ips_changed
        summary["gateway_scrape_error"] = scrape_err_str
        if deltas is None:
            summary["tunnels_per_stage"] = None
        capacity_path.write_text(json.dumps(summary, indent=2) + "\n")
        testing_stage_completed = bool(summary.get("testing_stage_completed"))
        if summary.get("gateway_counter_reset"):
            counter_reset_error = RuntimeError(
                "Gateway Prometheus counters decreased during benchmark (container restart)"
            )
        slo_max_rps = summary.get("slo_max_rps")
        converged = adaptive_exit == 0 and bool(summary.get("converged"))
        print(f"Capacity summary: {json.dumps(summary)}", flush=True)

    status_tmp_path.write_text(
        json.dumps(
            {
                "adaptive_client_exit_code": adaptive_exit,
                "converged": converged,
                "testing_stage_completed": testing_stage_completed,
                "slo_max_rps": slo_max_rps,
                "gateway_pod_ips_changed": pod_ips_changed,
                "gateway_scrape_error": scrape_err_str,
            },
            indent=2,
        )
        + "\n"
    )
    os.replace(status_tmp_path, status_path)
    for src in (
        logs_path,
        spec_path,
        output_path,
        results_path,
        stats_path,
        capacity_path,
        gw_before_path,
        gw_after_path,
        status_path,
    ):
        if src.exists():
            upload(src, f"{prefix}/{src.name}")

    if after_scrape_error is not None:
        raise after_scrape_error
    if ip_mismatch_error is not None:
        raise ip_mismatch_error
    if counter_reset_error is not None:
        raise counter_reset_error

    # Contract: runner exits 0 if and only if the underlying adaptive client exited 0
    # AND a valid capacity benchmark result was produced (converged, or testing stage ran
    # and a clean prior stage provided an slo_max_rps reference).
    return 0 if (adaptive_exit == 0 and testing_stage_completed and slo_max_rps is not None) else 1


if __name__ == "__main__":
    sys.exit(main())
