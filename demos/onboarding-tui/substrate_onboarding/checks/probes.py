"""Pre-flight diagnostic probes for developer environment verification with plain-language feedback and actionable remedies."""

from __future__ import annotations

import asyncio
import os
import shutil
import socket
import sys
import time
from typing import List
from substrate_onboarding.config import CheckResult


async def _run_command(cmd: List[str], timeout_sec: float = 3.0) -> tuple[int, str, str]:
    """Safely run a sub-process command asynchronously with timeout."""
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout_sec)
        return proc.returncode or 0, stdout.decode(errors="replace").strip(), stderr.decode(errors="replace").strip()
    except (asyncio.TimeoutError, FileNotFoundError, PermissionError) as e:
        return -1, "", str(e)


class SystemProbes:
    """Collection of pre-flight environment checks explained in plain language."""

    @classmethod
    async def check_git(cls) -> CheckResult:
        """Verify Git version control and author identity."""
        t0 = time.time()
        git_path = shutil.which("git")
        if not git_path:
            return CheckResult(
                name="Version Control (Git)",
                category="Version Control",
                status="failed",
                message="Git is not installed on this machine",
                details="Git is required so code changes, agent templates, and workspace states are tracked.",
                plain_description="Tracks your code and configuration history.",
                fix_command="brew install git",
                doc_url="https://git-scm.com/doc",
                duration_ms=int((time.time() - t0) * 1000),
                is_fatal=False,
            )

        code, version_out, _ = await _run_command(["git", "--version"])
        _, user_name, _ = await _run_command(["git", "config", "user.name"])

        duration = int((time.time() - t0) * 1000)
        version_str = version_out.replace("git version ", "v").split()[0] if version_out else "Installed"

        if not user_name:
            return CheckResult(
                name="Version Control (Git)",
                category="Version Control",
                status="warning",
                message=f"Git {version_str} is installed, but your name is not set",
                details="Set your name and email so workspace commits and agent templates are attributed to you.",
                plain_description="Attaches your name to version history.",
                fix_command='git config --global user.name "Your Name" && git config --global user.email "you@example.com"',
                doc_url="https://git-scm.com/book/en/v2/Getting-Started-First-Time-Git-Setup",
                duration_ms=duration,
                is_fatal=False,
            )

        return CheckResult(
            name="Version Control (Git)",
            category="Version Control",
            status="ok",
            message=f"Git {version_str} ready (Identity: {user_name})",
            plain_description="Version control is configured and ready.",
            doc_url="https://git-scm.com/doc",
            duration_ms=duration,
            is_fatal=False,
        )

    @classmethod
    async def check_python(cls) -> CheckResult:
        """Verify Python environment version."""
        t0 = time.time()
        vi = sys.version_info
        duration = int((time.time() - t0) * 1000)
        version_str = f"v{vi.major}.{vi.minor}.{vi.micro}"

        if vi < (3, 10):
            return CheckResult(
                name="Python Environment",
                category="Execution Engine",
                status="failed",
                message=f"Python {version_str} is too old (requires Python 3.10 or newer)",
                details="Onboarding tools and developer scripts require Python 3.10+.",
                plain_description="Runs the onboarding prototype and developer scripts.",
                fix_command="brew install python@3.12",
                doc_url="https://www.python.org/downloads/",
                duration_ms=duration,
                is_fatal=True,
            )

        return CheckResult(
            name="Python Environment",
            category="Execution Engine",
            status="ok",
            message=f"Python {version_str} ready",
            plain_description="Python runtime meets all requirements.",
            doc_url="https://docs.python.org/3/",
            duration_ms=duration,
            is_fatal=False,
        )

    @classmethod
    async def check_container_runtime(cls) -> CheckResult:
        """Verify container engine (Docker, Podman, or Colima)."""
        t0 = time.time()
        docker_path = shutil.which("docker") or shutil.which("podman")
        if not docker_path:
            return CheckResult(
                name="Container Runtime (Docker / Podman)",
                category="Sandboxing",
                status="warning",
                message="Docker or Podman is not installed or not in PATH",
                details="Local container engines are required for building and running workload containers locally.",
                plain_description="Runs containerized workloads.",
                fix_command="brew install --cask docker",
                doc_url="https://docs.docker.com/get-docker/",
                duration_ms=int((time.time() - t0) * 1000),
                is_fatal=False,
            )

        code, out, _ = await _run_command([docker_path, "version", "--format", "{{.Server.Version}}"])
        duration = int((time.time() - t0) * 1000)

        if code == 0 and out:
            return CheckResult(
                name="Container Runtime (Docker / Podman)",
                category="Sandboxing",
                status="ok",
                message=f"Docker Engine v{out} running",
                plain_description="Container engine daemon is active and responsive.",
                doc_url="https://docs.docker.com/",
                duration_ms=duration,
                is_fatal=False,
            )

        code_cli, out_cli, _ = await _run_command([docker_path, "--version"])
        if code_cli == 0:
            return CheckResult(
                name="Container Runtime (Docker / Podman)",
                category="Sandboxing",
                status="warning",
                message="Docker CLI installed, but background engine daemon is stopped",
                details="Start Docker Desktop or Colima to enable local container execution.",
                plain_description="Start the container engine to run local containers.",
                fix_command="open -a Docker || colima start",
                doc_url="https://docs.docker.com/",
                duration_ms=duration,
                is_fatal=False,
            )

        return CheckResult(
            name="Container Runtime (Docker / Podman)",
            category="Sandboxing",
            status="warning",
            message="Container engine not responding",
            plain_description="Start Docker Desktop to enable local sandboxes.",
            fix_command="open -a Docker",
            doc_url="https://docs.docker.com/",
            duration_ms=duration,
            is_fatal=False,
        )

    @classmethod
    async def check_kubernetes_tooling(cls) -> CheckResult:
        """Verify Kubernetes CLI tooling and active context."""
        t0 = time.time()
        kubectl_path = shutil.which("kubectl")
        atectl_path = shutil.which("kubectl-ate")
        duration = int((time.time() - t0) * 1000)

        if not kubectl_path:
            return CheckResult(
                name="Kubernetes Tooling (kubectl)",
                category="Control Plane",
                status="warning",
                message="kubectl is not installed (optional for local prototype, required for cluster deployment)",
                details="Install kubectl to connect to your Kubernetes cluster.",
                plain_description="Manages Kubernetes cluster communication.",
                fix_command="brew install kubectl",
                doc_url="https://kubernetes.io/docs/tasks/tools/",
                duration_ms=duration,
                is_fatal=False,
            )

        # Check current context if kubectl is available
        code, current_ctx, _ = await _run_command(["kubectl", "config", "current-context"])
        if code == 0 and current_ctx:
            msg = f"kubectl installed (Active context: {current_ctx})"
            if atectl_path:
                msg += " • kubectl-ate plugin detected"
            return CheckResult(
                name="Kubernetes Tooling (kubectl)",
                category="Control Plane",
                status="ok",
                message=msg,
                plain_description="Kubectl is configured with an active cluster context.",
                doc_url="https://kubernetes.io/docs/tasks/tools/",
                duration_ms=duration,
                is_fatal=False,
            )

        return CheckResult(
            name="Kubernetes Tooling (kubectl)",
            category="Control Plane",
            status="warning",
            message="kubectl installed, but no active cluster context is set",
            details="Configure a cluster context in ~/.kube/config to target a specific cluster.",
            plain_description="Allows remote deployment to Kubernetes clusters.",
            fix_command="kubectl config get-contexts",
            doc_url="https://kubernetes.io/docs/tasks/tools/",
            duration_ms=duration,
            is_fatal=False,
        )

    @classmethod
    async def check_cloud_cli(cls) -> CheckResult:
        """Verify Google Cloud SDK / gcloud CLI."""
        t0 = time.time()
        gcloud_path = shutil.which("gcloud")
        duration = int((time.time() - t0) * 1000)

        if not gcloud_path:
            return CheckResult(
                name="Google Cloud SDK (gcloud)",
                category="Cloud SDK",
                status="warning",
                message="gcloud CLI is not installed (optional for local prototype)",
                details="gcloud is needed for provisioning GKE clusters and GCP resources.",
                plain_description="Authenticates and manages GCP resources.",
                fix_command="brew install --cask google-cloud-sdk",
                doc_url="https://cloud.google.com/sdk/docs/install",
                duration_ms=duration,
                is_fatal=False,
            )

        code, project_out, _ = await _run_command(["gcloud", "config", "get-value", "project"])
        project = project_out.strip() if code == 0 and project_out else "None"

        return CheckResult(
            name="Google Cloud SDK (gcloud)",
            category="Cloud SDK",
            status="ok",
            message=f"gcloud CLI detected (Active Project: {project})",
            plain_description="Google Cloud SDK is ready.",
            doc_url="https://cloud.google.com/sdk",
            duration_ms=duration,
            is_fatal=False,
        )

    @classmethod
    async def check_network_connectivity(cls) -> CheckResult:
        """Verify internet and DNS connectivity."""
        t0 = time.time()
        try:
            loop = asyncio.get_running_loop()
            await loop.getaddrinfo("github.com", 443, family=socket.AF_INET)
            duration = int((time.time() - t0) * 1000)
            return CheckResult(
                name="Network & DNS Connectivity",
                category="Connectivity",
                status="ok",
                message=f"Internet and DNS resolution healthy ({duration}ms)",
                plain_description="Outbound network access is verified.",
                doc_url="https://github.com/agent-substrate/substrate",
                duration_ms=duration,
                is_fatal=False,
            )
        except Exception as e:
            duration = int((time.time() - t0) * 1000)
            return CheckResult(
                name="Network & DNS Connectivity",
                category="Connectivity",
                status="warning",
                message=f"Internet / DNS resolution unreachable ({e})",
                details="Offline mode will be used.",
                plain_description="Verifies internet reachability.",
                fix_command="ping -c 3 8.8.8.8",
                doc_url="https://github.com/agent-substrate/substrate",
                duration_ms=duration,
                is_fatal=False,
            )
