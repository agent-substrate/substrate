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

Owns the generic benchmark lifecycle; everything specific to a test type
lives in a testtypes/ module (see that package's docstring for the hook
contract). The flow, with the type hooks marked:

  1. Clone the substrate branch once; wait for the DIND sidecar.
  2. Validate every tests.yaml entry before any cluster work
     [hook: validate].
  3. Per test entry:
     a. Source the target cluster's env; gcloud/docker/kubectl setup and
        the runner image build [hook: build_image] are cached per
        target cluster.
     b. Sweep leftovers, deploy substrate, let the type shape the
        cluster [hook: pre_test], deploy workloads (+ microvm deps when
        the sandbox class needs them).
     c. Render the type's Job template [hooks: job_tmpl, job_subs],
        submit it, wait, tail logs, delete the Job.
     d. Tear substrate + workloads down again so tests don't pollute
        each other, then drop the env file so the next test can't
        inherit this one's cluster.
  4. Exit non-zero if any test didn't complete.
"""

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import time
import uuid
import xml.etree.ElementTree as ET
from collections.abc import Iterable
from pathlib import Path
from typing import Any

import yaml

from testtypes import TYPES
from util import parse_duration_seconds, run, run_no_check


SUBSTRATE_DIR = "/workspace/substrate"
TARGET_CLUSTER_DIR = "/etc/orchestrator/target-clusters"
ENV_FILE_NAME = ".ate-dev-env.sh"
MANIFESTS_DIR = "/opt/automation/manifests"
NAMESPACE = "benchmarking"

TEST_TYPES = tuple(TYPES)

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
    p.add_argument(
        "--target-cluster-dir",
        default=TARGET_CLUSTER_DIR,
        help="Directory containing target cluster configuration scripts (<cluster>.sh)",
    )
    p.add_argument(
        "--manifests-dir",
        default=MANIFESTS_DIR,
        help="Directory holding the per-type runner Job templates",
    )
    p.add_argument(
        "--junit-output",
        help="Path to write a JUnit XML report summarizing test results and durations",
    )
    return p.parse_args()


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


def apply_target_cluster(
    target_cluster: str, target_cluster_dir: str = TARGET_CLUSTER_DIR
) -> None:
    """Copy target_cluster_dir/<name>.sh into the cloned
    substrate repo as .ate-dev-env.sh (so install-ate.sh / deploy.sh source
    it) and merge it into this process's env (so the orchestrator's own
    gcloud / docker / kubectl calls see the same values). Resets os.environ
    to the startup baseline first so vars defined by a previous target
    cluster don't bleed into the next one."""
    src = Path(target_cluster_dir) / f"{target_cluster}.sh"
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


def test_type(test: dict[str, Any]) -> str:
    return test["type"]


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


SANDBOX_CLASSES = ("gvisor", "microvm")


def validate_and_normalize_tests(tests: list[dict[str, Any]]) -> None:
    """Fail fast (ValueError) on malformed entries, before any cluster
    work: the type-independent fields here, everything else via the
    type's validate hook."""
    for t in tests:
        name = t.get("name")
        if not t.get("targetCluster"):
            raise ValueError(f"test {name!r} missing required 'targetCluster' field")
        sandbox_class = t.get("sandboxClass", "gvisor")
        if sandbox_class not in SANDBOX_CLASSES:
            raise ValueError(
                f"test {name!r} has invalid sandboxClass {sandbox_class!r} "
                f"(want one of {list(SANDBOX_CLASSES)})"
            )
        if "type" not in t:
            raise ValueError(
                f"test {name!r} missing required 'type' field "
                f"(one of {list(TEST_TYPES)})"
            )
        ttype = test_type(t)
        if ttype not in TEST_TYPES:
            raise ValueError(
                f"test {name!r} has invalid type {ttype!r} "
                f"(want one of {list(TEST_TYPES)})"
            )
        TYPES[ttype].validate(t)


def deploy_substrate(ate_args: Iterable[str] = ()) -> None:
    run(["hack/install-ate.sh", "--deploy-ate-system", *(str(a) for a in ate_args)])


def teardown_substrate() -> None:
    run_no_check(["hack/install-ate.sh", "--delete-ate-system"])


def install_microvm_deps() -> None:
    """Stage kata/cloud-hypervisor assets and apply the cluster-wide
    microvm SandboxConfig. Required before a microvm ActorTemplate can
    boot; must run after deploy_substrate() (which installs the CRDs)."""
    run(["hack/install-microvm-deps.sh", "--install"])


def teardown_microvm_deps() -> None:
    """Remove the microvm SandboxConfig. Must run before
    teardown_substrate(), which deletes the SandboxConfig CRD (and would
    prevent this from succeeding via kubectl)."""
    run_no_check(["hack/install-microvm-deps.sh", "--delete"])


def deploy_workloads(
    worker_count: int = 1,
    sandbox_class: str = "gvisor",
    actor_memory: str = "",
    wait_timeout_secs: int | str = "",
) -> None:
    cmd = [
        "benchmarking/workloads/deploy.sh",
        "--deploy",
        "--worker-count",
        str(worker_count),
        "--sandbox-class",
        sandbox_class,
    ]
    # Empty keeps the default in workloads/deploy.sh (256Mi, the microvm
    # minimum); RAM-consuming suites set actorMemory in tests.yaml.
    if actor_memory:
        cmd += ["--actor-memory", actor_memory]
    # Empty keeps deploy.sh's own default; large fleets set workerWaitTimeout
    # (whole seconds).
    if wait_timeout_secs != "":
        cmd += ["--wait-timeout", str(wait_timeout_secs)]
    # deploy.sh itself blocks until every template's golden snapshot exists.
    run(cmd)


def teardown_workloads() -> None:
    run_no_check(["benchmarking/workloads/deploy.sh", "--delete"])


def run_test(
    test: dict[str, Any],
    image: str,
    dest: str,
    commit: str,
    manifests_dir: str = MANIFESTS_DIR,
) -> str:
    name = test["name"]
    job_name = f"runner-{sanitize(name)}-{commit[:7]}-{uuid.uuid4().hex[:6]}"
    subs = {
        "JOB_NAME": job_name,
        "IMAGE": image,
        "TAG": commit,
        "NAME": name,
        "DEST": dest,
    }
    mod = TYPES[test_type(test)]
    tmpl = mod.job_tmpl(manifests_dir)
    subs.update(mod.job_subs(test))
    manifest = render_template(tmpl, subs, test.get("flags", []))
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


def write_junit_xml(
    path: str,
    results: list[tuple[str, str, float, str | None]],
    classname: str = "substrate.benchmarks",
) -> None:
    total_tests = len(results)
    failures = sum(1 for _, status, _, _ in results if status != "complete")
    total_time = sum(duration for _, _, duration, _ in results)

    testsuite = ET.Element(
        "testsuite",
        name="substrate-benchmarks",
        tests=str(total_tests),
        failures=str(failures),
        errors="0",
        time=f"{total_time:.2f}",
    )

    for name, status, duration, failure_msg in results:
        testcase = ET.SubElement(
            testsuite,
            "testcase",
            name=name,
            classname=classname,
            time=f"{duration:.2f}",
        )
        if status != "complete":
            failure = ET.SubElement(
                testcase,
                "failure",
                message=failure_msg or f"Test failed with status: {status}",
            )
            failure.text = failure_msg or f"Test failed with status: {status}"

    tree = ET.ElementTree(testsuite)
    ET.indent(tree, space="  ", level=0)
    out_path = Path(path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tree.write(str(out_path), encoding="utf-8", xml_declaration=True)


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
    try:
        validate_and_normalize_tests(tests)
    except ValueError as e:
        sys.exit(str(e))

    # Per-target-cluster caches: re-running setup for the same target
    # cluster is wasted work, so we track what was last set up and only
    # redo it when the target cluster name changes (substrate images get
    # rebuilt by install-ate.sh each test anyway via ko apply). Runner
    # images are built lazily per test type, so a locust-only tests.yaml
    # never builds the nighthawk image and vice versa.
    last_target = None
    images: dict[str, str] = {}
    results = []

    for i, test in enumerate(tests):
        target_cluster = test["targetCluster"]
        print(
            f"\n=== test {i + 1}/{len(tests)}: {test['name']} (targetCluster={target_cluster}) ===",
            flush=True,
        )

        try:
            apply_target_cluster(target_cluster, args.target_cluster_dir)
        except Exception as e:
            print(
                f"Failed to apply target cluster {target_cluster!r}: {e}",
                flush=True,
            )
            results.append((test["name"], "config-error", 0.0, str(e)))
            continue

        try:
            if target_cluster != last_target:
                gcloud_setup_for_target_cluster()
                images = {}
                last_target = target_cluster
            ttype = test_type(test)
            if ttype not in images:
                images[ttype] = TYPES[ttype].build_image(commit)

            # Idempotent sweep before anything else: a previous CronJob
            # fire that crashed mid-test (or any other process that left
            # state behind) would otherwise leak its substrate + workloads
            # into this run. All teardowns use --ignore-not-found, so
            # this is cheap on a clean cluster. Order matters:
            # microvm-deps deletes a SandboxConfig CR, which requires the
            # SandboxConfig CRD that teardown_substrate removes.
            teardown_workloads()
            teardown_microvm_deps()
            teardown_substrate()

            sandbox_class = test.get("sandboxClass", "gvisor")
            status = "error"
            failure_msg = None
            start_time = time.time()
            try:
                deploy_substrate(test.get("ateArgs", []))
                TYPES[ttype].pre_test(test)
                # install-microvm-deps needs the CRDs from deploy_substrate;
                # deploy_workloads needs the microvm SandboxConfig.
                if sandbox_class == "microvm":
                    install_microvm_deps()
                deploy_workloads(
                    test.get("workerCount", 1),
                    sandbox_class,
                    test.get("actorMemory", ""),
                    test.get("workerWaitTimeout", ""),
                )
                try:
                    status = run_test(
                        test,
                        images[ttype],
                        args.dest,
                        commit,
                        args.manifests_dir,
                    )
                    if status != "complete":
                        failure_msg = f"Test finished with status: {status}"
                except Exception as e:
                    print(f"Test {test['name']} crashed: {e}", flush=True)
                    failure_msg = str(e)
            except Exception as e:
                print(f"Test {test['name']} setup failed: {e}", flush=True)
                failure_msg = str(e)
            finally:
                # Always tear down, even if deploy or run failed, so the
                # next test (and the next CronJob fire) starts clean.
                # microvm-deps must go before substrate for the same reason
                # as above.
                teardown_workloads()
                teardown_microvm_deps()
                teardown_substrate()
            duration = time.time() - start_time
            results.append((test["name"], status, duration, failure_msg))
        finally:
            # Drop .ate-dev-env.sh so the next test cannot accidentally
            # inherit this one's cluster/project if the next
            # apply_target_cluster fails partway through.
            clear_target_cluster()

    print("\n=== summary ===", flush=True)
    failed = 0
    for name, status, duration, _ in results:
        print(f"  {name}: {status} ({duration:.2f}s)", flush=True)
        if status != "complete":
            failed += 1

    if args.junit_output:
        print(f"Writing JUnit XML report to {args.junit_output}", flush=True)
        write_junit_xml(args.junit_output, results)

    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
