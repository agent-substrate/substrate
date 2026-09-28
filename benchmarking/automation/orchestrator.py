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

"""Substrate benchmark orchestrator.

Owns the generic benchmark lifecycle; everything specific to a test type
lives in a testtypes/ module (see that package's docstring for the hook
contract). The flow, with the type hooks marked:

  1. Pick the tests.yaml entries named by --test-name (default: all) and
     validate them before any other work [hook: validate].
  2. Wait for docker; clone the substrate branch once, or build
     --substrate-dir as it is.
  3. Per test entry:
     a. Source the target cluster's env file, or --env-file; gcloud/
        docker/kubectl setup and the runner image build [hook:
        build_image] are cached per env file.
     b. Sweep leftovers, deploy substrate, let the type shape the
        cluster [hook: pre_test], deploy workloads (+ microvm deps when
        the sandbox class needs them).
     c. Render the type's Job template [hooks: job_tmpl, job_subs],
        submit it, wait, tail logs, delete the Job.
     d. Tear substrate + workloads down again so tests don't pollute
        each other, then drop the copied env file so the next test can't
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
import tempfile
import time
import uuid
import xml.etree.ElementTree as ET
from collections.abc import Iterable
from pathlib import Path
from typing import Any

import yaml

from testtypes import TYPES
from util import (
    isolate_block,
    isolate_install_args,
    isolate_workload_args,
    parse_duration_seconds,
    run,
    run_no_check,
)


SUBSTRATE_DIR = "/workspace/substrate"
TARGET_CLUSTER_DIR = "/etc/orchestrator/target-clusters"
ENV_FILE_NAME = ".ate-dev-env.sh"
# The runner Job templates ship next to this script, both in the image and
# in the checkout.
MANIFESTS_DIR = str(Path(__file__).resolve().parent / "manifests")
NAMESPACE = "benchmarking"

TEST_TYPES = tuple(TYPES)

# Snapshot the process's initial env so apply_env_file can return to a known
# baseline before sourcing the next env file (avoids stale vars carrying over
# from a prior test if that test's env file defined keys the next one doesn't).
_ORIG_ENV = dict(os.environ)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--repo", help="Git URL of substrate repo to clone")
    p.add_argument("--branch", help="Branch to benchmark (default: main)")
    p.add_argument(
        "--substrate-dir",
        help="Build this checkout as it is, uncommitted changes included, "
        "instead of cloning --repo",
    )
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
        "--test-name",
        action="append",
        default=[],
        metavar="NAME",
        help="Run only the named test entry; repeatable. Default: all entries",
    )
    p.add_argument(
        "--target-cluster-dir",
        help="Directory containing target cluster configuration scripts "
        f"(<cluster>.sh); default {TARGET_CLUSTER_DIR}",
    )
    p.add_argument(
        "--env-file",
        help="Source this file for every test, whatever its targetCluster, "
        "instead of --target-cluster-dir/<targetCluster>.sh; for example a "
        f"checkout's own {ENV_FILE_NAME}",
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
    args = p.parse_args(argv)
    # Reject flags the chosen mode would ignore.
    if args.substrate_dir:
        if args.repo or args.branch:
            p.error("--substrate-dir cannot be combined with --repo or --branch")
        # .git is a file in a worktree, so test for either.
        if not os.path.exists(os.path.join(args.substrate_dir, ".git")):
            p.error(f"--substrate-dir {args.substrate_dir} is not a git checkout")
    elif not args.repo:
        p.error("one of --repo or --substrate-dir is required")
    else:
        args.branch = args.branch or "main"
    if args.env_file:
        if args.target_cluster_dir:
            p.error("--env-file cannot be combined with --target-cluster-dir")
    else:
        args.target_cluster_dir = args.target_cluster_dir or TARGET_CLUSTER_DIR
    # main chdirs into the substrate checkout, so keep paths relative to the
    # caller's cwd meaning what the caller meant.
    for name in (
        "substrate_dir",
        "tests",
        "target_cluster_dir",
        "env_file",
        "manifests_dir",
        "junit_output",
    ):
        if getattr(args, name):
            setattr(args, name, os.path.abspath(getattr(args, name)))
    return args


def env_file_for(test: dict[str, Any], args: argparse.Namespace) -> Path:
    """The env file a test runs with: --env-file, or the test's
    targetCluster under --target-cluster-dir."""
    if args.env_file:
        return Path(args.env_file)
    return Path(args.target_cluster_dir) / f"{test['targetCluster']}.sh"


def check_env_files(
    tests: list[dict[str, Any]], args: argparse.Namespace, substrate_dir: str
) -> None:
    """Fail fast (ValueError) before any docker or cluster work on an env
    file that is missing, and on a checkout whose own .ate-dev-env.sh a
    copied env file would overwrite."""
    files = {env_file_for(t, args) for t in tests}
    missing = sorted(str(f) for f in files if not f.exists())
    if missing:
        raise ValueError(f"env file(s) not found: {', '.join(missing)}")
    own = Path(substrate_dir) / ENV_FILE_NAME
    if own.exists() and any(not is_own_env_file(f, substrate_dir) for f in files):
        raise ValueError(
            f"{own} exists and a test would overwrite it; pass it as "
            "--env-file or move it away"
        )


def select_tests(tests: list[dict], names: list[str]) -> list[dict]:
    """Return the entries of tests named in names, in file order, or all of
    them when names is empty. Raise ValueError on a name not in tests."""
    if not names:
        return tests
    unknown = sorted(set(names) - {t.get("name") for t in tests})
    if unknown:
        raise ValueError(
            f"--test-name values not in the tests file: {', '.join(unknown)}"
        )
    return [t for t in tests if t.get("name") in names]


def source_env(path: str) -> None:
    # env -0: a developer shell exports multi-line bash functions, which
    # newline splitting would turn into bogus keys.
    result = subprocess.run(
        ["bash", "-c", f'set -a; source "{path}"; env -0'],
        check=True,
        capture_output=True,
        text=True,
    )
    for entry in result.stdout.split("\0"):
        if "=" not in entry:
            continue
        k, _, v = entry.partition("=")
        os.environ[k] = v


def is_own_env_file(src: Path, substrate_dir: str) -> bool:
    """Whether src is substrate_dir's own .ate-dev-env.sh. That file is
    sourced where it is and left alone; any other env file is copied in for
    the test and removed after it."""
    return src.resolve() == (Path(substrate_dir) / ENV_FILE_NAME).resolve()


def apply_env_file(src: Path, substrate_dir: str = SUBSTRATE_DIR) -> None:
    """Install src as the substrate checkout's .ate-dev-env.sh (so ate-setup
    / deploy.sh source it) and merge it into this process's env (so the
    orchestrator's own gcloud / docker / kubectl calls see the same values).
    Resets os.environ to the startup baseline first so vars defined by a
    previous env file don't bleed into the next one."""
    os.environ.clear()
    os.environ.update(_ORIG_ENV)
    dst = Path(substrate_dir) / ENV_FILE_NAME
    if not is_own_env_file(src, substrate_dir):
        shutil.copy(src, dst)
    source_env(str(dst))
    for k in ("PROJECT_ID", "CLUSTER_NAME", "CLUSTER_LOCATION", "KO_DOCKER_REPO"):
        if not os.environ.get(k):
            raise RuntimeError(f"{k} not set after sourcing {src}")


def clear_env_file(src: Path, substrate_dir: str = SUBSTRATE_DIR) -> None:
    dst = Path(substrate_dir) / ENV_FILE_NAME
    if not is_own_env_file(src, substrate_dir) and dst.exists():
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


def _json_object(raw: str, what: str) -> dict[str, Any]:
    """Parses raw as a JSON object, naming `what` in the error otherwise."""
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as e:
        raise ValueError(f"{what} is not valid JSON: {e}") from e
    if not isinstance(value, dict):
        raise ValueError(f"{what} must be a JSON object")
    return value


def run_metadata() -> dict[str, Any]:
    """Free-form run facts handed to the runner as --metadata.

    Read per test, after apply_env_file has reset os.environ;
    BENCHMARK_METADATA survives that reset because it is part of the startup
    env (_ORIG_ENV).
    """
    md: dict[str, Any] = {
        k.lower(): os.environ[k]
        for k in ("CLUSTER_NAME", "CLUSTER_LOCATION", "PROJECT_ID")
        if os.environ.get(k)
    }
    raw = os.environ.get("BENCHMARK_METADATA")
    if raw:
        md.update(_json_object(raw, "BENCHMARK_METADATA"))
    return md


def split_metadata_flag(test: dict[str, Any]) -> tuple[list[Any], dict[str, Any]]:
    """Separates a test's own --metadata from the rest of its flags.

    Accepts `--metadata <json>` and `--metadata=<json>`. With more than one,
    later keys win. Raises ValueError when the value is missing or is not a
    JSON object, rather than dropping it.
    """
    name = test.get("name")
    rest: list[Any] = []
    md: dict[str, Any] = {}
    flags = list(test.get("flags", []))
    i = 0
    while i < len(flags):
        flag = str(flags[i])
        if flag == "--metadata":
            if i + 1 >= len(flags):
                raise ValueError(f"test {name!r}: --metadata needs a JSON value")
            raw, i = str(flags[i + 1]), i + 2
        elif flag.startswith("--metadata="):
            raw, i = flag.split("=", 1)[1], i + 1
        else:
            rest.append(flags[i])
            i += 1
            continue
        md.update(_json_object(raw, f"test {name!r}: --metadata"))
    return rest, md


def wait_for_docker(timeout: int = 120) -> None:
    """Poll docker info: the CronJob's DIND sidecar starts alongside the
    orchestrator, and a workstation's Docker may still be starting too."""
    print("Waiting for docker...", flush=True)
    start = time.time()
    while time.time() - start < timeout:
        r = subprocess.run(["docker", "info"], capture_output=True)
        if r.returncode == 0:
            print("docker ready.", flush=True)
            return
        time.sleep(2)
    raise RuntimeError(f"docker did not become ready within {timeout}s")


def build_tag(substrate_dir: str) -> str:
    """HEAD's commit, labeling the runner image and the results. Uncommitted
    changes get a unique -dirty suffix: the Job templates pull IfNotPresent,
    so a tag reused for different content would run a stale image."""
    git = ["git", "-C", substrate_dir]
    commit = subprocess.check_output([*git, "rev-parse", "HEAD"], text=True).strip()
    if subprocess.check_output([*git, "status", "--porcelain"], text=True).strip():
        commit += f"-dirty-{uuid.uuid4().hex[:6]}"
    return commit


def registry_host(ko_docker_repo: str) -> str:
    return ko_docker_repo.split("/", 1)[0]


def sanitize(name: str) -> str:
    return re.sub(r"[^a-z0-9-]+", "-", name.lower()).strip("-")


# Kubernetes copies a Job's name into the job-name label on the Job and its
# pods, and a label value holds at most 63 bytes.
MAX_JOB_NAME_LEN = 63


def job_name(name: str, commit: str) -> str:
    """Return a unique Job name for a test run that fits a Kubernetes label.

    The commit and random suffix always survive; a long test name is cut to
    make room, so two runs of the same test never collide.
    """
    suffix = f"-{commit[:7]}-{uuid.uuid4().hex[:6]}"
    prefix = f"runner-{sanitize(name)}"
    return prefix[: MAX_JOB_NAME_LEN - len(suffix)].rstrip("-") + suffix


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
        if ttype == "locust":
            # A malformed --metadata fails here rather than mid-sweep.
            split_metadata_flag(t)
        isolate_block(t)


def substrate_args(test: dict[str, Any]) -> list[str]:
    """Return the ate-setup deploy ate-system arguments for a
    tests.yaml entry: its ateArgs, the fake data plane flag its isolate block
    asks for, and --sandbox-classes=<sandboxClass> so gVisor runs do not need
    write access to <BUCKET_NAME>-assets. The teardown needs no matching flag:
    delete ate-system removes the fake data plane and any SandboxConfigs
    whether or not they were installed."""
    args = [*test.get("ateArgs", []), *isolate_install_args(test)]
    if not any(a == "--sandbox-classes" or a.startswith("--sandbox-classes=") for a in args):
        args.append(f"--sandbox-classes={test.get('sandboxClass', 'gvisor')}")
    return args


# ate-api-server enforces authorization, and the runner Job calls it with its
# pod certificate as BENCHMARK_RUNNER_PRINCIPAL. Each test reinstalls
# Substrate, so making the runner a bootstrap owner on every install grants it
# global owner for exactly the life of that install. kubectl-ate's principal
# stays an owner for the workload and teardown steps. The Envoy ingress router
# also requires an allowlist of client SPIFFE IDs on every install.
BENCHMARK_RUNNER_PRINCIPAL = "spiffe://cluster.local/ns/benchmarking/sa/benchmark-runner"
CLIENT_PRINCIPAL = "system:serviceaccount:ate-system:ate-client"


def deploy_substrate(ate_args: Iterable[str] = ()) -> None:
    env = {
        **os.environ,
        "ATE_API_AUTHZ_BOOTSTRAP_OWNERS": f"{CLIENT_PRINCIPAL},{BENCHMARK_RUNNER_PRINCIPAL}",
        "ATE_INGRESS_ALLOWED_SPIFFE_IDS": BENCHMARK_RUNNER_PRINCIPAL,
    }
    run(["go", "run", "./cmd/ate-setup", "deploy", "ate-system", *(str(a) for a in ate_args)], env=env)


def teardown_substrate() -> None:
    run_no_check(["go", "run", "./cmd/ate-setup", "delete", "ate-system"])


def deploy_workloads(
    worker_count: int = 1,
    sandbox_class: str = "gvisor",
    actor_memory: str = "",
    wait_timeout_secs: int | str = "",
    worker_memory: str = "",
    fake_atelet_args: Iterable[str] = (),
    actor_cpu: str = "",
) -> None:
    cmd = [
        "benchmarking/workloads/deploy.sh",
        "--deploy",
        "--worker-count",
        str(worker_count),
        "--sandbox-class",
        sandbox_class,
    ]
    # Empty keeps the default in workloads/deploy.sh (256Mi); RAM-consuming
    # suites set actorMemory in tests.yaml.
    if actor_memory:
        cmd += ["--actor-memory", actor_memory]
    # Empty keeps the default in workloads/deploy.sh (1 CPU). The scheduler
    # books the limit against worker CPU capacity, so dense suites set
    # actorCPU in tests.yaml to fit more actors per worker.
    # str() because a whole number of cores (actorCPU: 1) parses as an int.
    if actor_cpu:
        cmd += ["--actor-cpu", str(actor_cpu)]
    # Empty leaves the worker pods unsized; set workerMemory in tests.yaml to
    # control how many workers the scheduler packs onto a node.
    if worker_memory:
        cmd += ["--worker-memory", worker_memory]
    # Empty keeps deploy.sh's own default; large fleets set workerWaitTimeout
    # (whole seconds).
    if wait_timeout_secs != "":
        cmd += ["--wait-timeout", str(wait_timeout_secs)]
    # An isolate block's tuning of fake-atelet, on the fake data plane.
    cmd += list(fake_atelet_args)
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
    job = job_name(name, commit)
    subs = {
        "JOB_NAME": job,
        "IMAGE": image,
        "TAG": commit,
        "NAME": name,
        "DEST": dest,
    }
    mod = TYPES[test_type(test)]
    tmpl = mod.job_tmpl(manifests_dir)
    subs.update(mod.job_subs(test))
    flags = list(test.get("flags", []))
    # Only the locust runner knows --metadata. It keeps only the last one it
    # is given, so a test's own --metadata is folded into a single merged
    # flag; the test's keys win over the run's.
    if test_type(test) == "locust":
        flags, own = split_metadata_flag(test)
        flags += ["--metadata", json.dumps({**run_metadata(), **own})]
    manifest = render_template(tmpl, subs, flags)
    wait_for_no_active_runners()
    print(f"Submitting Job {job}", flush=True)
    subprocess.run(
        ["kubectl", "apply", "-f", "-"], input=manifest, text=True, check=True
    )
    timeout = parse_duration_seconds(test["duration"]) + 1800
    result = wait_for_job(job, timeout)
    print(f"Job {job} result: {result}", flush=True)
    run_no_check(
        ["kubectl", "logs", f"job/{job}", "-n", NAMESPACE, "--tail=500"]
    )
    run_no_check(["kubectl", "delete", "job", job, "-n", NAMESPACE])
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
    substrate_dir = args.substrate_dir or SUBSTRATE_DIR
    tests = yaml.safe_load(Path(args.tests).read_text())["tests"]
    try:
        tests = select_tests(tests, args.test_name)
        print(f"Running {len(tests)} test(s)", flush=True)
        validate_and_normalize_tests(tests)
        # Checks BENCHMARK_METADATA only; cluster vars may not be set yet.
        run_metadata()
        check_env_files(tests, args, substrate_dir)
    except ValueError as e:
        sys.exit(str(e))

    # gcloud get-credentials writes the cluster into KUBECONFIG and makes it
    # the current context, so give the run its own file rather than the
    # caller's ~/.kube/config. Set in _ORIG_ENV too, so it survives
    # apply_env_file's reset.
    kubeconfig = str(Path(tempfile.mkdtemp(prefix="orchestrator-")) / "kubeconfig")
    _ORIG_ENV["KUBECONFIG"] = os.environ["KUBECONFIG"] = kubeconfig

    # Config-independent setup: docker, then the substrate checkout.
    wait_for_docker()
    if not args.substrate_dir:
        run(
            [
                "git",
                "clone",
                "--depth",
                "1",
                "--branch",
                args.branch,
                args.repo,
                substrate_dir,
            ]
        )
    os.chdir(substrate_dir)
    tag = build_tag(substrate_dir)
    print(f"Building {tag} from {substrate_dir}", flush=True)

    # Per-env-file caches: re-running setup for the same env file is
    # wasted work, so we track what was last set up and only redo it when
    # the env file changes (substrate images get rebuilt by ate-setup each
    # test anyway via ko). Runner images are built lazily per test type, so
    # a locust-only tests.yaml never builds the nighthawk image and vice
    # versa.
    last_env_file = None
    images: dict[str, str] = {}
    results = []

    for i, test in enumerate(tests):
        env_file = env_file_for(test, args)
        print(
            f"\n=== test {i + 1}/{len(tests)}: {test['name']} (env file {env_file}) ===",
            flush=True,
        )

        try:
            apply_env_file(env_file, substrate_dir)
        except Exception as e:
            print(f"Failed to apply env file {env_file}: {e}", flush=True)
            results.append((test["name"], "config-error", 0.0, str(e)))
            continue

        try:
            if env_file != last_env_file:
                gcloud_setup_for_target_cluster()
                images = {}
                last_env_file = env_file
            ttype = test_type(test)
            if ttype not in images:
                images[ttype] = TYPES[ttype].build_image(tag)

            # Idempotent sweep before anything else: a previous CronJob
            # fire that crashed mid-test (or any other process that left
            # state behind) would otherwise leak its substrate + workloads
            # into this run. All teardowns use --ignore-not-found, so
            # this is cheap on a clean cluster.
            teardown_workloads()
            teardown_substrate()

            sandbox_class = test.get("sandboxClass", "gvisor")
            status = "error"
            failure_msg = None
            start_time = time.time()
            try:
                deploy_substrate(substrate_args(test))
                TYPES[ttype].pre_test(test)
                deploy_workloads(
                    test.get("workerCount", 1),
                    sandbox_class,
                    test.get("actorMemory", ""),
                    test.get("workerWaitTimeout", ""),
                    test.get("workerMemory", ""),
                    isolate_workload_args(test),
                    test.get("actorCPU", ""),
                )
                try:
                    status = run_test(
                        test,
                        images[ttype],
                        args.dest,
                        tag,
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
                teardown_workloads()
                teardown_substrate()
            duration = time.time() - start_time
            results.append((test["name"], status, duration, failure_msg))
        finally:
            # Drop the copied .ate-dev-env.sh so the next test cannot
            # accidentally inherit this one's cluster/project if the next
            # apply_env_file fails partway through.
            clear_env_file(env_file, substrate_dir)

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
