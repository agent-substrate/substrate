"""Left Navigation Sidebar Widget for the Substrate Onboarding Journey.

Renders the clean, minimal sidebar matching the design reference:
- Substrate (bold cyan header)
- Getting set up (muted cyan subtext)
- Progress bar (dynamic filled width based on active step)
- X of 7 steps
- 7 numbered steps with ✓, active cyan number, and muted upcoming steps.
"""

from __future__ import annotations

from typing import List, Tuple
from textual.app import ComposeResult
from textual.containers import Vertical
from textual.reactive import reactive
from textual.widget import Widget
from textual.widgets import Static
from rich.text import Text
from substrate_onboarding.config import OnboardingStep


STEPS_LIST: List[Tuple[OnboardingStep, str]] = [
    (OnboardingStep.CHECK_SETUP, "Check your setup"),
    (OnboardingStep.CONNECT_CLUSTER, "Connect your cluster"),
    (OnboardingStep.TURN_ON_SUBSTRATE, "Turn on Substrate"),
    (OnboardingStep.COMPATIBLE_NODEPOOL, "Compatible Node Pool"),
    (OnboardingStep.CONFIG_AUTOSCALING, "Configure Autoscaling"),
    (OnboardingStep.DEPLOY_WORKERPOOL, "Deploy WorkerPool"),
    (OnboardingStep.COMPLETE, "Installation Complete"),
]

STEP_MAP = {step: idx + 1 for idx, (step, _) in enumerate(STEPS_LIST)}


class SidebarNav(Widget):
    """Left navigation sidebar showing 7 onboarding steps and active progress."""

    current_step: reactive[OnboardingStep] = reactive(OnboardingStep.CHECK_SETUP)

    def __init__(
        self,
        current_step: OnboardingStep = OnboardingStep.CHECK_SETUP,
        id: str = "sidebar-nav",
    ):
        super().__init__(id=id)
        self.current_step = current_step

    def compose(self) -> ComposeResult:
        with Vertical(id="sidebar-container"):
            yield Static(self._render_sidebar_content(), id="sidebar-content")

    def _render_sidebar_content(self) -> Text:
        t = Text()
        curr_idx = STEP_MAP.get(self.current_step, 1)

        # Header Title
        t.append("Agent Substrate\n", style="bold #38bdf8")
        t.append("Onboarding Journey\n\n", style="#94a3b8")

        # Thin Progress Bar
        bar_len = 24
        filled = max(1, int((curr_idx / len(STEPS_LIST)) * bar_len))
        t.append("━" * filled, style="bold #38bdf8")
        t.append("─" * (bar_len - filled), style="#1e293b")
        t.append("\n\n")

        # Step Count Indicator
        completed_count = max(0, curr_idx - 1)
        t.append(f"Step {curr_idx} of 7 ({completed_count} done)\n\n", style="#94a3b8")

        # 7 Steps List with Timeline Glyphs
        for i, (step_enum, title) in enumerate(STEPS_LIST):
            step_num = i + 1
            if step_num < curr_idx:
                # Completed
                t.append("✓ ", style="bold #34d399")
                t.append(f"{title}\n", style="bold #94a3b8")
            elif step_num == curr_idx:
                # Active
                t.append("● ", style="bold #38bdf8")
                t.append(f"{title}\n", style="bold #f8fafc")
            else:
                # Upcoming
                t.append("○ ", style="#64748b")
                t.append(f"{title}\n", style="#64748b")

        return t

    def watch_current_step(self, step: OnboardingStep) -> None:
        try:
            content = self.query_one("#sidebar-content", Static)
            content.update(self._render_sidebar_content())
        except Exception:
            pass
