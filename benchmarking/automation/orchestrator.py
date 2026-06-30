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

"""Substrate benchmark orchestrator.

Clones a branch of the substrate repo, builds and deploys it to the test
cluster, then submits one Kubernetes Job per entry in tests.yaml that runs
benchmarking/locust/runner.py. Tears down substrate + workloads between
tests so they don't pollute each other.
"""

import argparse
import json
import os
import re
import shlex
import shutil
import subprocess
import sys
import time
import uuid
from collections.abc import Iterable
from pathlib import Path
from typing import Any

import yaml


SUBSTRATE_DIR = "/workspace/substrate"
TARGET_CLUSTER_DIR = "/etc/orchestrator/target-clusters"
ENV_FILE_NAME = ".ate-dev-env.sh"
RUNNER_JOB_TMPL = "/opt/automation/manifests/runner-job.yaml.tmpl"
NAMESPACE = "benchmarking"

# Snapshot the process's initial env so apply_config can return to a known
# baseline before sourcing the next config (avoids stale vars carrying over
# from a prior test if that test's config defined keys the next one doesn't).
_ORIG_ENV = dict(os.environ)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--repo", required=True, help="Git URL of substrate repo to clone")
    p.add_argument("--branch", default="main", help="Branch to benchmark")
    p.add_argument(
        "--dest",
        required=True,
        help="Root destination for results (passed through to runner.py --dest)",
    )
    p.add_argument(
        "--tests",
        default="/etc/orchestrator/tests.yaml",
        help="Path to the tests YAML file (mounted from a ConfigMap)",
    )
    return p.parse_args()


def run(cmd: list[str], **kwargs) -> subprocess.CompletedProcess:
    print(f"$ {' '.join(shlex.quote(c) for c in cmd)}", flush=True)
    return subprocess.run(cmd, check=True, **kwargs)


def run_no_check(cmd: list[str], **kwargs) -> subprocess.CompletedProcess:
    print(f"$ {' '.join(shlex.quote(c) for c in cmd)}", flush=True)
    return subprocess.run(cmd, check=False, **kwargs)


def source_env(path: str) -> None:
    result = subprocess.run(
        ["bash", "-c", f'set -a; source "{path}"; env'],
        check=True,
        capture_output=True,
        text=True,
    )
    for line in result.stdout.splitlines():
        if "=" not in line:
            continue
        k, _, v = line.partition("=")
        os.environ[k] = v


def apply_target_cluster(target_cluster: str) -> None:
    """Copy /etc/orchestrator/target-clusters/<name>.sh into the cloned
    substrate repo as .ate-dev-env.sh (so install-ate.sh / deploy.sh source
    it) and merge it into this process's env (so the orchestrator's own
    gcloud / docker / kubectl calls see the same values). Resets os.environ
    to the startup baseline first so vars defined by a previous target
    cluster don't bleed into the next one."""
    src = Path(TARGET_CLUSTER_DIR) / f"{target_cluster}.sh"
    if not src.exists():
        raise FileNotFoundError(
            f"target cluster {target_cluster!r} not found at {src}"
        )
    os.environ.clear()
    os.environ.update(_ORIG_ENV)
    dst = Path(SUBSTRATE_DIR) / ENV_FILE_NAME
    shutil.copy(src, dst)
    source_env(str(dst))
    for k in ("PROJECT_ID", "CLUSTER_NAME", "CLUSTER_LOCATION", "KO_DOCKER_REPO"):
        if not os.environ.get(k):
            raise RuntimeError(
                f"{k} not set after sourcing target cluster {target_cluster!r}"
            )


def clear_target_cluster() -> None:
    dst = Path(SUBSTRATE_DIR) / ENV_FILE_NAME
    if dst.exists():
        dst.unlink()


def gcloud_setup_for_target_cluster() -> None:
    """Per-target-cluster gcloud setup: configure docker creds for the new
    registry and switch kubectl to the new cluster."""
    run(
        [
            "gcloud",
            "auth",
            "configure-docker",
            registry_host(os.environ["KO_DOCKER_REPO"]),
            "--quiet",
        ]
    )
    run(
        [
            "gcloud",
            "container",
            "clusters",
            "get-credentials",
            os.environ["CLUSTER_NAME"],
            "--location",
            os.environ["CLUSTER_LOCATION"],
            "--project",
            os.environ["PROJECT_ID"],
        ]
    )


def build_locust_image(commit: str) -> str:
    """Build & push the locust image to the current config's registry. The
    image must live in the same project as the test cluster so the runner
    Job can pull it. Returns the fully-qualified image reference."""
    image = f"{os.environ['KO_DOCKER_REPO']}/locust-test:{commit}"
    run(
        [
            "docker",
            "build",
            "-t",
            image,
            "-f",
            "benchmarking/locust/Dockerfile",
            ".",
        ]
    )
    run(["docker", "push", image])
    return image


def wait_for_docker(timeout: int = 120) -> None:
    print("Waiting for DIND sidecar...", flush=True)
    start = time.time()
    while time.time() - start < timeout:
        r = subprocess.run(["docker", "info"], capture_output=True)
        if r.returncode == 0:
            print("DIND ready.", flush=True)
            return
        time.sleep(2)
    raise RuntimeError("DIND sidecar did not become ready within timeout")


def registry_host(ko_docker_repo: str) -> str:
    return ko_docker_repo.split("/", 1)[0]


def sanitize(name: str) -> str:
    return re.sub(r"[^a-z0-9-]+", "-", name.lower()).strip("-")


def render_template(path: str, subs: dict[str, Any], extra_args: Iterable[str] = ()) -> str:
    text = Path(path).read_text()
    for k, v in subs.items():
        text = text.replace("${" + k + "}", str(v))
    if not extra_args:
        return text
    docs = list(yaml.safe_load_all(text))
    for doc in docs:
        if doc and doc.get("kind") == "Job":
            doc["spec"]["template"]["spec"]["containers"][0]["args"].extend(
                str(a) for a in extra_args
            )
    return yaml.safe_dump_all(docs)


def parse_duration_seconds(s: str) -> int:
    m = re.fullmatch(r"(\d+)\s*([smh]?)", s.strip())
    if not m:
        raise ValueError(f"unrecognized duration: {s}")
    n = int(m.group(1))
    unit = m.group(2) or "s"
    return n * {"s": 1, "m": 60, "h": 3600}[unit]


def wait_for_no_active_runners(timeout: int = 300) -> None:
    start = time.time()
    while time.time() - start < timeout:
        r = subprocess.run(
            [
                "kubectl",
                "get",
                "jobs",
                "-n",
                NAMESPACE,
                "-l",
                "app=substrate-benchmark-runner",
                "-o",
                "json",
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        if r.returncode != 0:
            print(f"kubectl get jobs failed: {r.stderr}", flush=True)
            time.sleep(5)
            continue
        items = json.loads(r.stdout).get("items", [])
        active = [
            j["metadata"]["name"]
            for j in items
            if j.get("status", {}).get("succeeded", 0) == 0
            and j.get("status", {}).get("failed", 0) == 0
        ]
        if not active:
            return
        print(f"Waiting for in-progress runner jobs: {active}", flush=True)
        time.sleep(10)
    raise RuntimeError(
        f"Existing runner jobs still active after {timeout}s; aborting"
    )


def wait_for_job(name: str, timeout_seconds: int) -> str:
    start = time.time()
    while time.time() - start < timeout_seconds:
        r = subprocess.run(
            ["kubectl", "get", "job", name, "-n", NAMESPACE, "-o", "json"],
            capture_output=True,
            text=True,
            check=False,
        )
        if r.returncode != 0:
            print(f"kubectl get job failed: {r.stderr}", flush=True)
            time.sleep(5)
            continue
        status = json.loads(r.stdout).get("status", {})
        if status.get("succeeded", 0) >= 1:
            return "complete"
        if status.get("failed", 0) >= 1:
            return "failed"
        time.sleep(10)
    return "timeout"


def deploy_substrate() -> None:
    run(["hack/install-ate.sh", "--deploy-ate-system"])


def teardown_substrate() -> None:
    run_no_check(["hack/install-ate.sh", "--delete-ate-system"])


def deploy_workloads(worker_count: int = 1) -> None:
    run(
        [
            "benchmarking/workloads/deploy.sh",
            "--deploy",
            "--worker-count",
            str(worker_count),
        ]
    )
    # Block until ActorTemplates are Ready
    run(
        [
            "kubectl",
            "wait",
            "--for=condition=Ready",
            "--all",
            "actortemplates",
            "-n",
            "benchmark-workloads",
            "--timeout=300s",
        ]
    )


def teardown_workloads() -> None:
    run_no_check(["benchmarking/workloads/deploy.sh", "--delete"])


def run_test(test: dict[str, Any], image: str, dest: str, commit: str) -> str:
    name = test["name"]
    job_name = f"runner-{sanitize(name)}-{commit[:7]}-{uuid.uuid4().hex[:6]}"
    subs = {
        "JOB_NAME": job_name,
        "IMAGE": image,
        "TEST_FILE": test["file"],
        "DURATION": test["duration"],
        "USERS": test["users"],
        "TAG": commit,
        "NAME": name,
        "DEST": dest,
    }
    manifest = render_template(RUNNER_JOB_TMPL, subs, test.get("flags", []))
    wait_for_no_active_runners()
    print(f"Submitting Job {job_name}", flush=True)
    subprocess.run(
        ["kubectl", "apply", "-f", "-"], input=manifest, text=True, check=True
    )
    timeout = parse_duration_seconds(test["duration"]) + 1800
    result = wait_for_job(job_name, timeout)
    print(f"Job {job_name} result: {result}", flush=True)
    run_no_check(
        ["kubectl", "logs", f"job/{job_name}", "-n", NAMESPACE, "--tail=500"]
    )
    run_no_check(["kubectl", "delete", "job", job_name, "-n", NAMESPACE])
    return result


def main() -> None:
    args = parse_args()

    # Config-independent setup: DIND + clone the substrate branch once.
    wait_for_docker()
    run(
        [
            "git",
            "clone",
            "--depth",
            "1",
            "--branch",
            args.branch,
            args.repo,
            SUBSTRATE_DIR,
        ]
    )
    os.chdir(SUBSTRATE_DIR)
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    print(f"Building commit {commit}", flush=True)

    tests = yaml.safe_load(Path(args.tests).read_text())["tests"]
    print(f"Running {len(tests)} test(s)", flush=True)
    for t in tests:
        if not t.get("targetCluster"):
            sys.exit(
                f"test {t.get('name')!r} missing required 'targetCluster' field"
            )

    # Per-target-cluster caches: re-running setup for the same target
    # cluster is wasted work, so we track what was last set up and only
    # redo it when the target cluster name changes (substrate images get
    # rebuilt by install-ate.sh each test anyway via ko apply).
    last_target = None
    locust_image = None
    results = []

    for i, test in enumerate(tests):
        target_cluster = test["targetCluster"]
        print(
            f"\n=== test {i + 1}/{len(tests)}: {test['name']} (targetCluster={target_cluster}) ===",
            flush=True,
        )

        try:
            apply_target_cluster(target_cluster)
        except Exception as e:
            print(
                f"Failed to apply target cluster {target_cluster!r}: {e}",
                flush=True,
            )
            results.append((test["name"], "config-error"))
            continue

        try:
            if target_cluster != last_target:
                gcloud_setup_for_target_cluster()
                locust_image = build_locust_image(commit)
                last_target = target_cluster

            # Idempotent sweep before anything else: a previous CronJob
            # fire that crashed mid-test (or any other process that left
            # state behind) would otherwise leak its substrate + workloads
            # into this run. Both teardowns use --ignore-not-found, so
            # this is cheap on a clean cluster.
            teardown_workloads()
            teardown_substrate()

            status = "error"
            try:
                deploy_substrate()
                deploy_workloads(test.get("workerCount", 1))
                try:
                    status = run_test(test, locust_image, args.dest, commit)
                except Exception as e:
                    print(f"Test {test['name']} crashed: {e}", flush=True)
            except Exception as e:
                print(f"Test {test['name']} setup failed: {e}", flush=True)
            finally:
                # Always tear down, even if deploy or run failed, so the
                # next test (and the next CronJob fire) starts clean.
                teardown_workloads()
                teardown_substrate()
            results.append((test["name"], status))
        finally:
            # Drop .ate-dev-env.sh so the next test cannot accidentally
            # inherit this one's cluster/project if the next
            # apply_target_cluster fails partway through.
            clear_target_cluster()

    print("\n=== summary ===", flush=True)
    failed = 0
    for name, status in results:
        print(f"  {name}: {status}", flush=True)
        if status != "complete":
            failed += 1
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
