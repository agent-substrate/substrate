"""Interactive autopilot simulator that walks through the onboarding TUI automatically."""

import asyncio
import os
import sys

# Ensure directory is on sys.path
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from substrate_onboarding.app import SubstrateOnboardingApp


class SimulatedOnboardingApp(SubstrateOnboardingApp):
    """Substrate Onboarding App with an autonomous autopilot worker for live demo recordings."""

    def on_mount(self) -> None:
        super().on_mount()
        self.set_timer(0.5, self._start_autopilot)

    def _start_autopilot(self) -> None:
        asyncio.create_task(self._autopilot_loop())

    async def _autopilot_loop(self) -> None:
        """Autonomously drive the TUI flow with natural user-like delays."""
        # 1. Welcome Screen
        await asyncio.sleep(2.0)
        # Advance to Wizard
        self.advance_step()

        # 2. Wizard - Step 1 (Track Selection)
        await asyncio.sleep(1.0)
        wizard_screen = self.screen
        wizard_screen.action_cursor_down()
        await asyncio.sleep(0.8)
        wizard_screen.action_cursor_down()
        await asyncio.sleep(0.8)
        wizard_screen.action_select_and_next()

        # Wizard - Step 2 (Editor Selection)
        await asyncio.sleep(1.0)
        wizard_screen.action_cursor_down()
        await asyncio.sleep(0.8)
        wizard_screen.action_select_and_next()

        # Wizard - Step 3 (Sandbox Tier)
        await asyncio.sleep(1.0)
        wizard_screen.action_select_and_next()

        # 3. Doctor Pre-flight Screen
        # Let diagnostics run and show off animated spinners
        await asyncio.sleep(3.5)
        self.advance_step()

        # 4. Auth Screen
        await asyncio.sleep(1.0)
        if isinstance(self.screen, AuthScreen):
            # Demonstrate OAuth flow
            auth_screen = self.screen
            auth_screen.action_start_oauth()
            await asyncio.sleep(3.5)

        # 5. Summary Screen
        # Let workspace compilation progress bar animate
        await asyncio.sleep(4.0)
        self.finish_onboarding()


def main():
    print("Launching autonomous Onboarding TUI simulator...")
    app = SimulatedOnboardingApp()
    app.run()


if __name__ == "__main__":
    main()
