"""Generate high-resolution SVG snapshots of all onboarding screens for visual inspection."""

import asyncio
import os
import sys

# Ensure repository root is on sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from substrate_onboarding.app import SubstrateOnboardingApp
from substrate_onboarding.config import OnboardingStep, UserSetupState
from substrate_onboarding.screens.auth_screen import AuthScreen
from substrate_onboarding.screens.doctor_screen import DoctorScreen
from substrate_onboarding.screens.help_modal import HelpModal
from substrate_onboarding.screens.summary_screen import SummaryScreen
from substrate_onboarding.screens.welcome_screen import WelcomeScreen
from substrate_onboarding.screens.wizard_screen import QuestionnaireScreen


OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "screenshots")


async def generate_snapshots():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    print(f"Generating SVG screenshots in: {OUTPUT_DIR}\n")

    # 1. Welcome Screen
    app1 = SubstrateOnboardingApp()
    async with app1.run_test(size=(100, 30)) as pilot:
        tw = app1.screen.query_one("TypewriterWidget")
        tw.skip()
        await pilot.pause(0.1)
        svg = app1.export_screenshot()
        path = os.path.join(OUTPUT_DIR, "01_welcome_screen.svg")
        with open(path, "w", encoding="utf-8") as f:
            f.write(svg)
        print(f"  ✓ Saved {path}")

    # 2. Questionnaire Screen
    state2 = UserSetupState(current_step=OnboardingStep.QUESTIONNAIRE)
    app2 = SubstrateOnboardingApp(initial_state=state2)
    async with app2.run_test(size=(100, 30)) as pilot:
        await pilot.pause(0.1)
        svg = app2.export_screenshot()
        path = os.path.join(OUTPUT_DIR, "02_wizard_screen.svg")
        with open(path, "w", encoding="utf-8") as f:
            f.write(svg)
        print(f"  ✓ Saved {path}")

    # 3. Doctor Screen
    state3 = UserSetupState(current_step=OnboardingStep.DOCTOR)
    app3 = SubstrateOnboardingApp(initial_state=state3)
    async with app3.run_test(size=(100, 30)) as pilot:
        await pilot.pause(1.5)
        svg = app3.export_screenshot()
        path = os.path.join(OUTPUT_DIR, "03_doctor_screen.svg")
        with open(path, "w", encoding="utf-8") as f:
            f.write(svg)
        print(f"  ✓ Saved {path}")

    # 4. Auth Screen
    state4 = UserSetupState(current_step=OnboardingStep.AUTH)
    app4 = SubstrateOnboardingApp(initial_state=state4)
    async with app4.run_test(size=(100, 30)) as pilot:
        await pilot.pause(0.1)
        svg = app4.export_screenshot()
        path = os.path.join(OUTPUT_DIR, "04_auth_screen.svg")
        with open(path, "w", encoding="utf-8") as f:
            f.write(svg)
        print(f"  ✓ Saved {path}")

    # 5. Summary Screen
    state5 = UserSetupState(current_step=OnboardingStep.SUMMARY)
    app5 = SubstrateOnboardingApp(initial_state=state5)
    async with app5.run_test(size=(100, 30)) as pilot:
        await pilot.pause(1.5)
        svg = app5.export_screenshot()
        path = os.path.join(OUTPUT_DIR, "05_summary_screen.svg")
        with open(path, "w", encoding="utf-8") as f:
            f.write(svg)
        print(f"  ✓ Saved {path}")

    # 6. Help Modal
    app6 = SubstrateOnboardingApp()
    async with app6.run_test(size=(100, 30)) as pilot:
        app6.action_show_help()
        await pilot.pause(0.1)
        svg = app6.export_screenshot()
        path = os.path.join(OUTPUT_DIR, "06_help_modal.svg")
        with open(path, "w", encoding="utf-8") as f:
            f.write(svg)
        print(f"  ✓ Saved {path}")

    print("\nAll 6 SVG screenshots successfully generated!")


if __name__ == "__main__":
    asyncio.run(generate_snapshots())
