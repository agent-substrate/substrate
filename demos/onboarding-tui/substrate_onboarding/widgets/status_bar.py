"""Header and status bar widgets with Google Material 3 styling and brand tokens.

Provides:
- TopHeader: Google Cloud branding, active cluster context, and quick action shortcuts.
- BottomBar: Contextual interactive tip and keyboard shortcuts legend.
"""

from __future__ import annotations

from typing import Optional
from textual.app import ComposeResult
from textual.containers import Horizontal
from textual.reactive import reactive
from textual.widget import Widget
from textual.widgets import Label
from rich.text import Text
from substrate_onboarding.config import OnboardingStep


class TopHeader(Widget):
    """Global top navigation bar with Google Cloud branding and quick status badges."""

    current_step: reactive[OnboardingStep] = reactive(OnboardingStep.CLUSTER)

    def __init__(
        self,
        initial_step: OnboardingStep = OnboardingStep.CLUSTER,
        cluster_name: str = "demo-cluster",
        id: str = "top-header",
    ):
        super().__init__(id=id)
        self.current_step = initial_step
        self.cluster_name = cluster_name

    def compose(self) -> ComposeResult:
        with Horizontal():
            yield Label(self._render_brand(), id="header-brand")
            yield Label(self._render_quick_actions(), id="header-stepper")

    def _render_brand(self) -> Text:
        t = Text("⚡ Agent Substrate ", style="bold #38bdf8")
        t.append("│ Onboarding UX Prototype", style="bold #f8fafc")
        return t

    def _render_quick_actions(self) -> Text:
        t = Text()
        t.append(" [ 🌐 Context: ", style="#94a3b8")
        t.append(f"{self.cluster_name} ", style="bold #38bdf8")
        t.append("] ", style="#94a3b8")
        t.append(" [ ⏻ Exit ] ", style="#64748b")
        return t

    def watch_current_step(self, step: OnboardingStep) -> None:
        try:
            stepper_label = self.query_one("#header-stepper", Label)
            stepper_label.update(self._render_quick_actions())
        except Exception:
            pass


class BottomBar(Widget):
    """Dynamic persistent bottom Keymap Footer dock (k9s/lazygit pattern)."""

    def __init__(
        self,
        keymaps: Optional[List[tuple]] = None,
        step_badge: str = "Step 1 of 7",
        initial_tip: Optional[str] = None,
        initial_hints: Optional[str] = None,
        tip: Optional[str] = None,
        hints: Optional[str] = None,
        id: str = "bottom-bar",
    ):
        super().__init__(id=id)
        self.keymaps = keymaps
        self.step_badge = step_badge
        self.tip_text = tip or initial_tip or ""
        self.hint_text = hints or initial_hints or ""

    def compose(self) -> ComposeResult:
        with Horizontal(id="bottom-bar-inner"):
            yield Label(self._render_keymaps(), id="keyboard-keymaps")
            yield Label(self._render_badge(), id="keyboard-step-badge")

    def _render_keymaps(self) -> Text:
        t = Text()
        if self.keymaps:
            for idx, item in enumerate(self.keymaps):
                key = item[0]
                label = item[1]
                is_primary = item[2] if len(item) > 2 else False
                if idx > 0:
                    t.append("   ")
                if is_primary:
                    t.append(f" {key} ", style="bold #ffffff on #2563eb")
                    t.append(f" {label}", style="bold #f8fafc")
                else:
                    t.append(f" {key} ", style="bold #38bdf8 on #1e293b")
                    t.append(f" {label}", style="#94a3b8")
        else:
            hint = self.hint_text or self.tip_text or "[Enter ↵] Proceed   [b] Back   [?] Help"
            t.append(hint, style="#94a3b8")
        return t

    def _render_badge(self) -> Text:
        t = Text()
        if self.step_badge:
            t.append(f" {self.step_badge} ", style="bold #38bdf8 on #131b2e")
        return t

    def set_tip(self, text: str) -> None:
        self.tip_text = text

    def set_hints(self, text: str) -> None:
        self.hint_text = text
