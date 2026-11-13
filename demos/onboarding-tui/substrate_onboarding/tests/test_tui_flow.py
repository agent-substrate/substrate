"""End-to-end headless Textual test suite for Substrate Onboarding TUI with Welcome Screen & 7 steps."""

import pytest
from substrate_onboarding.app import SubstrateOnboardingApp
from substrate_onboarding.config import OnboardingStep, UserSetupState
from substrate_onboarding.screens.welcome_screen import WelcomeScreen
from substrate_onboarding.screens.step_screen import GenericStepScreen


@pytest.mark.asyncio
async def test_tui_full_flow():
    state = UserSetupState(current_step=OnboardingStep.WELCOME)
    app = SubstrateOnboardingApp(initial_state=state)

    async with app.run_test() as pilot:
        # Step 0: Welcome Screen
        assert app.state_machine.current_step == OnboardingStep.WELCOME
        assert isinstance(app.screen, WelcomeScreen)

        # Step 1: Check your setup
        await pilot.press("enter")
        await pilot.pause(0.1)
        assert app.state_machine.current_step == OnboardingStep.CHECK_SETUP
        assert isinstance(app.screen, GenericStepScreen)

        # Step 2: Connect pre-existing cluster (portability & GKE agreement)
        await pilot.press("enter")
        await pilot.pause(0.1)
        assert app.state_machine.current_step == OnboardingStep.CONNECT_CLUSTER

        # Step 3: Turn on Substrate
        await pilot.press("enter")
        await pilot.pause(0.1)
        assert app.state_machine.current_step == OnboardingStep.TURN_ON_SUBSTRATE

        # Step 4: Compatible Node Pool (CCC / Nested-Virt)
        await pilot.press("enter")
        await pilot.pause(0.1)
        assert app.state_machine.current_step == OnboardingStep.COMPATIBLE_NODEPOOL

        # Step 5: Configure Autoscaling (HPA & CapacityBuffer)
        await pilot.press("enter")
        await pilot.pause(0.1)
        assert app.state_machine.current_step == OnboardingStep.CONFIG_AUTOSCALING

        # Step 6: Deploy WorkerPool
        await pilot.press("enter")
        await pilot.pause(0.1)
        assert app.state_machine.current_step == OnboardingStep.DEPLOY_WORKERPOOL

        # Step 7: Installation Complete
        await pilot.press("enter")
        await pilot.pause(0.1)
        assert app.state_machine.current_step == OnboardingStep.COMPLETE
        assert app.state.is_complete is True


@pytest.mark.asyncio
async def test_tui_help_modal():
    app = SubstrateOnboardingApp()
    async with app.run_test() as pilot:
        initial_depth = len(app.screen_stack)
        await pilot.press("f1")
        await pilot.pause(0.05)
        assert len(app.screen_stack) == initial_depth + 1
        # Close help modal with escape
        await pilot.press("escape")
        await pilot.pause(0.05)
        assert len(app.screen_stack) == initial_depth
