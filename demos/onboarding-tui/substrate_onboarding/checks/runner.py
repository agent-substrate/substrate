"""Pre-flight check execution engine and runner."""

from __future__ import annotations

import asyncio
from typing import Callable, Coroutine, Dict, List, Optional
from substrate_onboarding.config import CheckResult
from substrate_onboarding.checks.probes import SystemProbes


class PreflightRunner:
    """Orchestrates async execution of pre-flight environment checks with live progress callbacks."""

    CHECK_DEFINITIONS = [
        ("git", "Version Control (Git)", SystemProbes.check_git),
        ("python", "Python Environment", SystemProbes.check_python),
        ("container", "Container Runtime (Docker / Podman)", SystemProbes.check_container_runtime),
        ("kubernetes", "Kubernetes Tooling (kubectl)", SystemProbes.check_kubernetes_tooling),
        ("cloud", "Google Cloud SDK (gcloud)", SystemProbes.check_cloud_cli),
        ("network", "Network & DNS Connectivity", SystemProbes.check_network_connectivity),
    ]

    def __init__(self):
        self.results: Dict[str, CheckResult] = {}
        self.is_running: bool = False
        self._on_check_start: Optional[Callable[[str, str], None]] = None
        self._on_check_complete: Optional[Callable[[str, CheckResult], None]] = None
        self._on_all_complete: Optional[Callable[[Dict[str, CheckResult]], None]] = None

    def set_callbacks(
        self,
        on_start: Optional[Callable[[str, str], None]] = None,
        on_complete: Optional[Callable[[str, CheckResult], None]] = None,
        on_all_done: Optional[Callable[[Dict[str, CheckResult]], None]] = None,
    ) -> None:
        self._on_check_start = on_start
        self._on_check_complete = on_complete
        self._on_all_complete = on_all_done

    async def run_all(self, delay_between_sec: float = 0.2) -> Dict[str, CheckResult]:
        """Execute each preflight diagnostic check in order."""
        self.is_running = True
        self.results.clear()

        for key, name, probe_func in self.CHECK_DEFINITIONS:
            if self._on_check_start:
                self._on_check_start(key, name)

            # Stagger slightly for pleasant visual cadence
            if delay_between_sec > 0:
                await asyncio.sleep(delay_between_sec)

            try:
                result = await probe_func()
            except Exception as e:
                result = CheckResult(
                    name=name,
                    category="System",
                    status="failed",
                    message=f"Check failed unexpectedly: {e}",
                    is_fatal=False,
                )

            self.results[key] = result

            if self._on_check_complete:
                self._on_check_complete(key, result)

        self.is_running = False
        if self._on_all_complete:
            self._on_all_complete(self.results)

        return self.results
