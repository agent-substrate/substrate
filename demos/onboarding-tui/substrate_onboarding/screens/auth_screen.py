"""Step 4: WorkerPool Autoscaling and CapacityBuffer Configuration Screen."""

from __future__ import annotations

from textual.app import ComposeResult
from textual.containers import Horizontal, Vertical
from textual.screen import Screen
from textual.widgets import Label, Static, Button
from rich.text import Text
from substrate_onboarding.config import OnboardingStep, AUTOSCALING_OPTIONS
from substrate_onboarding.widgets.status_bar import TopHeader, BottomBar
from substrate_onboarding.widgets.sidebar_nav import SidebarNav


class AuthScreen(Screen[None]):
    """Step 4: Autoscaling & CapacityBuffer Screen."""

    BINDINGS = [
        ("up", "cursor_up", "Move Up"),
        ("k", "cursor_up", "Move Up"),
        ("down", "cursor_down", "Move Down"),
        ("j", "cursor_down", "Move Down"),
        ("enter", "confirm_and_next", "Confirm & Next"),
        ("space", "confirm_and_next", "Confirm & Next"),
        ("b", "previous_step", "Back"),
        ("1", "select_opt_1", "Option 1"),
        ("2", "select_opt_2", "Option 2"),
        ("3", "select_opt_3", "Option 3"),
    ]

    def __init__(self, name: str = "autoscaling"):
        super().__init__(name=name)
        self.selected_index = 0

    def compose(self) -> ComposeResult:
        yield TopHeader(initial_step=OnboardingStep.AUTOSCALING)
        with Horizontal(id="workspace-layout"):
            yield SidebarNav(current_step=OnboardingStep.AUTOSCALING)
            with Vertical(id="content-area"):
                with Vertical(id="content-panel"):
                    yield Label("[4/6] WORKERPOOL AUTOSCALING & CAPACITY BUFFERS", classes="wizard-step-title")
                    yield Label(
                        "Substrate uses Kubernetes HPA along with an upstream CapacityBuffer to maintain pre-warmed standby Worker Pods for instant (<100ms) agent session injection.",
                        classes="wizard-step-subtitle",
                    )

                    # Options Container
                    yield Vertical(id="options-container")

                    # Live Policy Card
                    yield Static(self._render_policy_card(), id="terminal-log-card")

                    # Button Row
                    with Horizontal(classes="action-button-row"):
                        btn_back = Button("← Back (b)", id="btn-back", classes="secondary-button")
                        btn_back.can_focus = False
                        yield btn_back

                        btn_proceed = Button(
                            "Configure Autoscaling & Proceed (Enter) →",
                            variant="primary",
                            id="btn-proceed",
                            classes="action-button",
                        )
                        btn_proceed.can_focus = False
                        yield btn_proceed

        yield BottomBar(
            initial_tip="OneHPA scales 10-100 pods with 3 standby warm replicas ready.",
            initial_hints="[Enter] Apply  [↑/↓] Select  [b] Back  [/help] Help  [Ctrl+C] Exit",
        )

    def on_mount(self) -> None:
        self._refresh_options()

    def _refresh_options(self) -> None:
        try:
            container = self.query_one("#options-container", Vertical)
            container.remove_children()

            for i, opt in enumerate(AUTOSCALING_OPTIONS):
                is_active = i == self.selected_index
                prefix = "▶ " if is_active else "  "
                style_title = "bold #ffffff" if is_active else "#e3e3e3"
                style_desc = "#d3e3fd" if is_active else "#9aa0a6"

                t = Text()
                t.append(f"{prefix}{opt.icon} ", style="bold #8ab4f8" if is_active else "#9aa0a6")
                t.append(f"{opt.title}\n", style=style_title)
                t.append(f"    {opt.description}", style=style_desc)

                card = Static(t, classes=f"option-card {'-active' if is_active else ''}")
                container.mount(card)
        except Exception:
            pass

    def _render_policy_card(self) -> Text:
        t = Text()
        t.append("╭── ⚙️ AUTOSCALING POLICY SPECIFICATIONS ──────────────────────────────────────────╮\n", style="bold #8ab4f8")
        t.append("│                                                                                  │\n", style="#8ab4f8")
        t.append("│  ✓ Applying HorizontalPodAutoscaler (OneHPA: minReplicas=10, maxReplicas=100)   │\n", style="bold #81c995")
        t.append("│  ✓ Applying CapacityBuffer (fixed-replica-buffer: 3 standby replicas)           │\n", style="bold #81c995")
        t.append("│  ✓ Standby buffer ready for instant (<100ms) agent session injection             │\n", style="bold #81c995")
        t.append("│                                                                                  │\n", style="#8ab4f8")
        t.append("│  NOTE: Modify and re-apply YAML manifests at manifests/workerpool-ccc.yaml       │\n", style="#fdd663")
        t.append("╰──────────────────────────────────────────────────────────────────────────────────╯", style="bold #8ab4f8")
        return t

    def action_cursor_up(self) -> None:
        if self.selected_index > 0:
            self.selected_index -= 1
            self._refresh_options()

    def action_cursor_down(self) -> None:
        if self.selected_index < len(AUTOSCALING_OPTIONS) - 1:
            self.selected_index += 1
            self._refresh_options()

    def action_select_opt_1(self) -> None:
        self.selected_index = 0
        self._refresh_options()

    def action_select_opt_2(self) -> None:
        self.selected_index = 1
        self._refresh_options()

    def action_select_opt_3(self) -> None:
        self.selected_index = 2
        self._refresh_options()

    def action_confirm_and_next(self) -> None:
        if hasattr(self.app, "advance_step"):
            self.app.advance_step()

    def action_previous_step(self) -> None:
        if hasattr(self.app, "previous_step"):
            self.app.previous_step()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "btn-proceed":
            self.action_confirm_and_next()
        elif event.button.id == "btn-back":
            self.action_previous_step()
