"""Step 3: Node Pool and Hardware Nested Virtualization Screen."""

from __future__ import annotations

from textual.app import ComposeResult
from textual.containers import Horizontal, Vertical
from textual.screen import Screen
from textual.widgets import Label, Static, Button
from rich.text import Text
from substrate_onboarding.config import OnboardingStep, NODEPOOL_OPTIONS
from substrate_onboarding.widgets.status_bar import TopHeader, BottomBar
from substrate_onboarding.widgets.sidebar_nav import SidebarNav


class QuestionnaireScreen(Screen[None]):
    """Step 3: Node Pool & Custom Compute Class (CCC) Screen."""

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

    def __init__(self, name: str = "node_pool"):
        super().__init__(name=name)
        self.selected_index = 0

    def compose(self) -> ComposeResult:
        yield TopHeader(initial_step=OnboardingStep.NODE_POOL)
        with Horizontal(id="workspace-layout"):
            yield SidebarNav(current_step=OnboardingStep.NODE_POOL)
            with Vertical(id="content-area"):
                with Vertical(id="content-panel"):
                    yield Label("[3/6] NODE POOL & HARDWARE NESTED VIRTUALIZATION", classes="wizard-step-title")
                    yield Label(
                        "A Substrate WorkerPool requires a compatible GKE Node Pool with hardware nested virtualization (--nested-virt) enabled for microVM sandbox isolation.",
                        classes="wizard-step-subtitle",
                    )

                    # Diagnostic scan result
                    yield Static(self._render_scan_box(), id="terminal-log-card")

                    # Options Container
                    yield Vertical(id="options-container")

                    # Button Row
                    with Horizontal(classes="action-button-row"):
                        btn_back = Button("← Back (b)", id="btn-back", classes="secondary-button")
                        btn_back.can_focus = False
                        yield btn_back

                        btn_proceed = Button(
                            "Apply CCC & Proceed (Enter) →",
                            variant="primary",
                            id="btn-proceed",
                            classes="action-button",
                        )
                        btn_proceed.can_focus = False
                        yield btn_proceed

        yield BottomBar(
            initial_tip="Select Node Pool configuration. CCC auto-provisions nested-virt N2 Spot instances.",
            initial_hints="[Enter] Apply  [↑/↓] Select  [b] Back  [/help] Help  [Ctrl+C] Exit",
        )

    def on_mount(self) -> None:
        self._refresh_options()

    def _refresh_options(self) -> None:
        try:
            container = self.query_one("#options-container", Vertical)
            container.remove_children()

            for i, opt in enumerate(NODEPOOL_OPTIONS):
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

    def _render_scan_box(self) -> Text:
        t = Text()
        t.append("Scanning cluster [demo-cluster] node pools...\n", style="#9aa0a6")
        t.append("▲ No node pool detected with hardware nested virtualization enabled.\n\n", style="bold #fdd663")
        t.append("┌─ 💡 Action Required ─────────────────────────────────────────────────────────────┐\n", style="bold #fdd663")
        t.append("│ Automatically configure a compatible Node Pool using Custom Compute Class (CCC). │\n", style="#ffffff")
        t.append("│ 📋 atectl create ccc agent-spot-ccc --machine-type=n2-standard-48 --nested-virt  │\n", style="bold #8ab4f8")
        t.append("└──────────────────────────────────────────────────────────────────────────────────┘", style="bold #fdd663")
        return t

    def action_cursor_up(self) -> None:
        if self.selected_index > 0:
            self.selected_index -= 1
            self._refresh_options()

    def action_cursor_down(self) -> None:
        if self.selected_index < len(NODEPOOL_OPTIONS) - 1:
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
