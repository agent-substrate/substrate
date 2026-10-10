"""Delightful, high-taste Terminal User Interface (TUI) for Agent Substrate Onboarding."""

from substrate_onboarding.app import SubstrateOnboardingApp, run_onboarding
from substrate_onboarding.config import UserSetupState, OnboardingStep
from substrate_onboarding.checks.probes import SystemProbes
from substrate_onboarding.checks.runner import PreflightRunner

__version__ = "0.1.0"
__all__ = [
    "SubstrateOnboardingApp",
    "run_onboarding",
    "UserSetupState",
    "OnboardingStep",
    "SystemProbes",
    "PreflightRunner",
]
