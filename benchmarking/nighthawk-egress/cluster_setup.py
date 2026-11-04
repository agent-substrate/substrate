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

import json
import subprocess
import os
import sys
import time
import argparse
from string import Template
from pathlib import Path

# Allow importing spec, runner, and nighthawk_common when executed directly as a script.
sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from spec import (
    DEFAULT_CONCURRENCY,
    DEFAULT_EXP_FACTOR,
    DEFAULT_INITIAL_RPS,
    DEFAULT_SEND_RATE_THRESHOLD,
    DEFAULT_SUCCESS_RATE_THRESHOLD,
)
from runner import parse_duration_seconds

def kubectl_json(args: list[str]) -> dict:
    res = subprocess.run(["kubectl", *args, "-o", "json"], check=True, capture_output=True, text=True)
    return json.loads(res.stdout)

def _cpu_millicores(val: str | int | None) -> int | None:
    if val is None:
        return None
    s = str(val)
    if s.endswith("m"):
        return int(s[:-1])
    return int(s) * 1000

def shape_atenet_egress(cpu: int) -> None:
    dep = kubectl_json(["get", "deployment/atenet-egress", "-n", "ate-system"])
    # Single-pod gateway sizing requires replicas == 1.
    replicas = dep["spec"].get("replicas", 1)
    containers = dep["spec"]["template"]["spec"]["containers"]
    envoy = next(c for c in containers if c["name"] == "envoy")
    ext_proc = next((c for c in containers if c["name"] == "ext-proc"), None)

    args = list(envoy.get("args", []))
    cpu_str = str(cpu)

    current_concurrency = None
    skip_next = False
    for i, a in enumerate(args):
        if skip_next:
            current_concurrency = a
            skip_next = False
            continue
        if a == "--concurrency" and i + 1 < len(args):
            skip_next = True
            continue
        if a.startswith("--concurrency="):
            current_concurrency = a.split("=", 1)[1]

    target_m = cpu * 1000

    def _is_matching(c: dict) -> bool:
        req = _cpu_millicores(c.get("resources", {}).get("requests", {}).get("cpu"))
        lim = _cpu_millicores(c.get("resources", {}).get("limits", {}).get("cpu"))
        return req == target_m and lim == target_m

    envoy_match = _is_matching(envoy)
    ext_proc_match = _is_matching(ext_proc) if ext_proc else True

    if replicas != 1 or current_concurrency != cpu_str or not envoy_match or not ext_proc_match:
        _patch_atenet_egress(cpu, args, containers)
    # Always wait: a spec that already matches may still be mid-rollout or
    # crash-looping from an earlier patch.
    subprocess.run(["kubectl", "rollout", "status", "deployment/atenet-egress", "-n", "ate-system", "--timeout=180s"], check=True)


def _patch_atenet_egress(cpu: int, args: list[str], containers: list[dict]) -> None:
    filtered: list[str] = []
    skip_next = False
    for i, a in enumerate(args):
        if skip_next:
            skip_next = False
            continue
        if a == "--concurrency" and i + 1 < len(args):
            skip_next = True
            continue
        if a.startswith("--concurrency="):
            continue
        filtered.append(a)
    # Envoy takes "--concurrency N", not "--concurrency=N".
    filtered.extend(["--concurrency", str(cpu)])

    cpu_res = {"requests": {"cpu": f"{cpu * 1000}m"}, "limits": {"cpu": f"{cpu * 1000}m"}}
    patch_containers = [{"name": "envoy", "args": filtered, "resources": cpu_res}]
    if any(c["name"] == "ext-proc" for c in containers):
        patch_containers.append({"name": "ext-proc", "resources": cpu_res})
    patch = {"spec": {"replicas": 1, "template": {"spec": {"containers": patch_containers}}}}

    subprocess.run(["kubectl", "patch", "deployment/atenet-egress", "-n", "ate-system", "--type=strategic", "-p", json.dumps(patch)], check=True)

def authorize_nighthawk_spiffe() -> None:
    principal = "spiffe://cluster.local/ns/nighthawk/sa/traffic-driver-invoker"
    dep = kubectl_json(["get", "deployment/atenet-router", "-n", "ate-system"])
    containers = dep["spec"]["template"]["spec"]["containers"]
    router = next((c for c in containers if c["name"] == "atenet-router"), None)
    if router is None:
        raise ValueError("atenet-router container not found")
    args = list(router.get("args", []))
    prefix = "--ingress-allowed-spiffe-ids="
    updated = False
    for i, a in enumerate(args):
        if a.startswith(prefix):
            existing = [x for x in a[len(prefix):].split(",") if x]
            if principal not in existing:
                existing.append(principal)
                args[i] = prefix + ",".join(existing)
                updated = True
            break
    else:
        args.append(prefix + principal)
        updated = True
    if updated:
        patch = {"spec": {"template": {"spec": {"containers": [{"name": "atenet-router", "args": args}]}}}}
        subprocess.run(["kubectl", "patch", "deployment/atenet-router", "-n", "ate-system", "--type=strategic", "-p", json.dumps(patch)], check=True)
        subprocess.run(["kubectl", "rollout", "status", "deployment/atenet-router", "-n", "ate-system", "--timeout=180s"], check=True)

def run_orchestration(name, job_name, image, tag, dest, mode, envoy_cpu, tail_slo_ms, meas, test_dur, deadline, conns, max_pend, skip_deploy):
    if mode not in ("http", "mitm", "passthrough"):
        raise SystemExit(f"unsupported --mode {mode!r}; expected http, mitm, or passthrough")
    cpu_int = int(envoy_cpu)
    if cpu_int < 1:
        raise SystemExit(f"--envoy-cpu must be >= 1, got {envoy_cpu!r}")

    shape_atenet_egress(cpu_int)
    authorize_nighthawk_spiffe()

    if not skip_deploy:
        ctx = subprocess.run(["kubectl", "config", "current-context"], check=True, capture_output=True, text=True).stdout.strip()
        env = {**os.environ, **({"KUBECTL_CONTEXT": ctx} if ctx else {})}
        subprocess.run(["hack/setup-e2e-egress-tls-kind.sh"], env=env, check=True)
        subprocess.run(["benchmarking/nighthawk-egress/deploy.sh", "--deploy"], check=True)
        subprocess.run(["benchmarking/nighthawk-egress/deploy-traffic-driver.sh", "--deploy"], check=True)

    subs = {
        "JOB_NAME": job_name,
        "IMAGE": image,
        "TAG": tag,
        "NAME": name,
        "DEST": dest,
        "ENVOY_CPU": str(cpu_int),
        "MODE": mode,
        "CLIENT_CONCURRENCY": str(DEFAULT_CONCURRENCY),
        "CONNECTIONS": str(conns),
        "MAX_PENDING": str(max_pend),
        "INITIAL_RPS": str(DEFAULT_INITIAL_RPS),
        "EXP_FACTOR": str(DEFAULT_EXP_FACTOR),
        "MEASURING_PERIOD": meas,
        "CONVERGENCE_DEADLINE": deadline,
        "TESTING_STAGE_DURATION": test_dur,
        "SUCCESS_RATE_THRESHOLD": str(DEFAULT_SUCCESS_RATE_THRESHOLD),
        "SEND_RATE_THRESHOLD": str(DEFAULT_SEND_RATE_THRESHOLD),
        "TAIL_LATENCY_SLO_MS": str(tail_slo_ms),
    }
    with open("benchmarking/automation/manifests/nighthawk-egress-runner-job.yaml.tmpl") as f:
        manifest = Template(f.read()).substitute(subs)

    subprocess.run(["kubectl", "delete", "job", job_name, "-n", "benchmarking", "--ignore-not-found"], check=False)
    subprocess.run(["kubectl", "apply", "-f", "-"], input=manifest, text=True, check=True)

    timeout_s = max(1800, parse_duration_seconds(deadline) + parse_duration_seconds(test_dur) + 600)
    res = "error"
    deadline_t = time.monotonic() + timeout_s
    res = "timeout"
    while time.monotonic() < deadline_t:
        st = kubectl_json(["get", f"job/{job_name}", "-n", "benchmarking"]).get("status", {})
        if st.get("succeeded", 0) >= 1:
            res = "complete"
            break
        if st.get("failed", 0) >= 1:
            res = "failed"
            break
        time.sleep(5)

    print(f"Job {job_name} result: {res}", flush=True)
    subprocess.run(["kubectl", "logs", f"job/{job_name}", "-n", "benchmarking", "--tail=200"], check=False)
    if res in ("complete", "failed"):
        subprocess.run(["kubectl", "delete", "job", job_name, "-n", "benchmarking", "--ignore-not-found"], check=False)
    if res != "complete":
        raise SystemExit(1)

def main():
    parser = argparse.ArgumentParser(description="Nighthawk Egress Cluster Setup")
    subparsers = parser.add_subparsers(dest="command", required=True)

    shape_parser = subparsers.add_parser("shape", help="Shape the atenet-egress deployment")
    shape_parser.add_argument("--envoy-cpu", type=int, required=True, help="CPU limit for Envoy")

    run_dev_parser = subparsers.add_parser("run-dev", help="Run orchestration for dev")
    run_dev_parser.add_argument("--name", required=True)
    run_dev_parser.add_argument("--job-name", required=True)
    run_dev_parser.add_argument("--image", required=True)
    run_dev_parser.add_argument("--tag", required=True)
    run_dev_parser.add_argument("--dest", required=True)
    run_dev_parser.add_argument("--mode", required=True)
    run_dev_parser.add_argument("--envoy-cpu", type=int, required=True)
    run_dev_parser.add_argument("--tail-slo-ms", type=float, required=True)
    run_dev_parser.add_argument("--measuring-period", required=True)
    run_dev_parser.add_argument("--testing-duration", required=True)
    run_dev_parser.add_argument("--convergence-deadline", required=True)
    run_dev_parser.add_argument("--connections", type=int, required=True)
    run_dev_parser.add_argument("--max-pending", type=int, required=True)
    run_dev_parser.add_argument("--skip-deploy", action="store_true")

    args = parser.parse_args()

    if args.command == "shape":
        shape_atenet_egress(args.envoy_cpu)
    elif args.command == "run-dev":
        run_orchestration(
            name=args.name,
            job_name=args.job_name,
            image=args.image,
            tag=args.tag,
            dest=args.dest,
            mode=args.mode,
            envoy_cpu=args.envoy_cpu,
            tail_slo_ms=args.tail_slo_ms,
            meas=args.measuring_period,
            test_dur=args.testing_duration,
            deadline=args.convergence_deadline,
            conns=args.connections,
            max_pend=args.max_pending,
            skip_deploy=args.skip_deploy,
        )
if __name__ == "__main__":
    main()
