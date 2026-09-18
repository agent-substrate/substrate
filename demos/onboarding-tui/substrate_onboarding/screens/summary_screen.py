"""Step 6: Launchpad & Live Cluster Verification Screen."""

from __future__ import annotations

from textual.app import ComposeResult
from textual.containers import Horizontal, Vertical
from textual.screen import Screen
from textual.widgets import Label, Static, Button
from rich.text import Text
from substrate_onboarding.config import OnboardingStep
from substrate_onboarding.widgets.status_bar import TopHeader, BottomBar
from substrate_onboarding.widgets.sidebar_nav import SidebarNav


class SummaryScreen(Screen[None]):
    """Step 6: Launchpad & Live Verification Screen."""

    BINDINGS = [
        ("enter", "finish_onboarding", "Finish & Launch"),
        ("space", "finish_onboarding", "Finish & Launch"),
        ("q", "finish_onboarding", "Finish & Launch"),
        ("b", "previous_step", "Back"),
        ("left", "previous_step", "Back"),
        ("backspace", "previous_step", "Back"),
        ("0", "return_to_start", "Return to Start"),
        ("question_mark", "show_help", "Help"),
        ("?", "show_help", "Help"),
        ("slash", "show_help", "Help"),
        ("f1", "show_help", "Help"),
    ]

    def on_key(self, event) -> None:
        """Handle question mark and help keypresses."""
        if event.key in ("question_mark", "?") or getattr(event, "character", "") == "?":
            self.action_show_help()
            event.prevent_default()
            event.stop()

    def __init__(self, name: str = "launchpad"):
        super().__init__(name=name)

    def compose(self) -> ComposeResult:
        yield TopHeader(initial_step=OnboardingStep.LAUNCHPAD)
        with Horizontal(id="workspace-layout"):
            yield SidebarNav(current_step=OnboardingStep.LAUNCHPAD)
            with Vertical(id="content-area"):
                with Vertical(id="content-panel"):
                    yield Label("[6/6] PROTOTYPE WALKTHROUGH COMPLETE", classes="wizard-step-title")
                    yield Label(
                        "✔ Interactive Onboarding Prototype Complete! This concludes the simulated walkthrough.",
                        classes="wizard-step-subtitle",
                    )

                    # Live Verification Table (Simulated)
                    yield Static(self._render_verification_card(), id="terminal-log-card")

                    # Runbook Card
                    yield Static(self._render_runbook_card(), id="remedy-card")

                    # Button Row
                    with Horizontal(classes="action-button-row"):
                        btn_back = Button("← Back (b)", id="btn-back", classes="secondary-button")
                        btn_back.can_focus = False
                        yield btn_back

                        btn_finish = Button(
                            "Finish Simulation (Enter) →",
                            variant="primary",
                            id="btn-finish",
                            classes="action-button",
                        )
                        btn_finish.can_focus = False
                        yield btn_finish

        yield BottomBar(
            initial_tip="Interactive simulation finished. Press [Enter] to exit.",
            initial_hints="[Enter] Finish  [b] Back  [/help] Help  [q] Quit",
        )

    def _render_verification_card(self) -> Text:
        t = Text()
        t.append("╭── 📊 SIMULATED WORKERPOOL STATUS (Prototype Mockup) ─────────────────────────────╮\n", style="bold #8ab4f8")
        t.append("│                                                                                  │\n", style="#8ab4f8")
        t.append("│  ", style="#8ab4f8")
        t.append("WORKERPOOL           NAMESPACE         ISOLATION  READY  STANDBY  CPU  MEM  QUEUE", style="bold #d3e3fd")
        t.append("  │\n", style="#8ab4f8")
        t.append("│  ", style="#8ab4f8")
        t.append("default-worker-pool  substrate-system  microvm    10/10  10       4%   8%   0    ", style="bold #81c995")
        t.append("  │\n", style="#8ab4f8")
        t.append("│                                                                                  │\n", style="#8ab4f8")
        t.append("╰──────────────────────────────────────────────────────────────────────────────────╯", style="bold #8ab4f8")
        return t

    def _render_runbook_card(self) -> Text:
        t = Text()
        t.append("🚀 REAL SUBSTRATE DEVELOPER RESOURCES & DEMOS:\n\n", style="bold #81c995")
        t.append("  1. Explore runnable agent demos:\n", style="bold #ffffff")
        t.append("     $ cd demos/claude-code-multiplex && ./run.sh\n", style="bold #8ab4f8")
        t.append("     $ cd demos/sandbox && ./run.sh\n\n", style="bold #8ab4f8")
        t.append("  2. Architecture & Design Documentation:\n", style="bold #ffffff")
        t.append("     $ cat docs/architecture.md\n\n", style="bold #8ab4f8")
        t.append("  3. Build Substrate Kubernetes CLI plugin:\n", style="bold #ffffff")
        t.append("     $ make build-kubectl-ate", style="bold #8ab4f8")
        return t

    def action_finish_onboarding(self) -> None:
        if hasattr(self.app, "finish_onboarding"):
            self.app.finish_onboarding()

    def action_previous_step(self) -> None:
        if hasattr(self.app, "previous_step"):
            self.app.previous_step()

    def action_show_help(self) -> None:
        if hasattr(self.app, "action_show_help"):
            self.app.action_show_help()

    def action_return_to_start(self) -> None:
        if hasattr(self.app, "state_machine"):
            self.app.state_machine.transition_to(OnboardingStep.WELCOME)

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "btn-finish":
            self.action_finish_onboarding()
        elif event.button.id == "btn-back":
            self.action_previous_step()
