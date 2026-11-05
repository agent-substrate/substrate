"""Unit and async tests for preflight diagnostic probes and runner."""

import pytest
from substrate_onboarding.checks.probes import SystemProbes
from substrate_onboarding.checks.runner import PreflightRunner
from substrate_onboarding.config import CheckResult


@pytest.mark.asyncio
async def test_python_probe():
    res = await SystemProbes.check_python()
    assert isinstance(res, CheckResult)
    assert res.name == "Python Environment"
    assert res.status == "ok"
    assert "Python" in res.message
    assert res.doc_url is not None


@pytest.mark.asyncio
async def test_git_probe():
    res = await SystemProbes.check_git()
    assert isinstance(res, CheckResult)
    assert res.name == "Version Control (Git)"
    assert res.status in ("ok", "warning", "failed")
    assert res.doc_url is not None


@pytest.mark.asyncio
async def test_container_probe():
    res = await SystemProbes.check_container_runtime()
    assert isinstance(res, CheckResult)
    assert res.name == "Container Runtime (Docker / Podman)"
    assert res.status in ("ok", "warning", "failed")
    assert res.doc_url is not None


@pytest.mark.asyncio
async def test_preflight_runner_execution():
    runner = PreflightRunner()
    started_checks = []
    completed_checks = []

    runner.set_callbacks(
        on_start=lambda k, n: started_checks.append(k),
        on_complete=lambda k, r: completed_checks.append((k, r.status)),
    )

    results = await runner.run_all(delay_between_sec=0.01)

    assert len(results) == len(PreflightRunner.CHECK_DEFINITIONS)
    assert len(started_checks) == len(PreflightRunner.CHECK_DEFINITIONS)
    assert len(completed_checks) == len(PreflightRunner.CHECK_DEFINITIONS)
    assert "python" in results
    assert "git" in results
