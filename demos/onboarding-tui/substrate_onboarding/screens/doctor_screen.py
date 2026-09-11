"""Step 2: Control Plane Installation Screen."""

from __future__ import annotations

import asyncio
from typing import Optional
from textual.app import ComposeResult
from textual.containers import Horizontal, Vertical
from textual.screen import Screen
from textual.widgets import Label, Static, Button
from rich.text import Text
from substrate_onboarding.config import OnboardingStep
from substrate_onboarding.widgets.status_bar import TopHeader, BottomBar
from substrate_onboarding.widgets.sidebar_nav import SidebarNav


INSTALL_STEPS = [
    ("crds", "Applying Substrate CustomResourceDefinitions (ate.dev/v1alpha1: WorkerPool, ActorTemplate, Actor)"),
    ("valkey", "Deploying Valkey Metadata & State Registry"),
    ("gateway", "Bootstrapping Substrate Gateway & API Server (listening on :8080)"),
    ("ebpf", "Initializing eBPF Network & Ingress/Egress Proxy Controller"),
    ("done", "All control plane components successfully deployed in namespace [substrate-system]."),
]


class DoctorScreen(Screen[None]):
    """Step 2: Control Plane Installation Screen."""

    BINDINGS = [
        ("enter", "proceed_next", "Proceed"),
        ("space", "proceed_next", "Proceed"),
        ("b", "previous_step", "Back"),
        ("left", "previous_step", "Back"),
        ("backspace", "previous_step", "Back"),
        ("0", "return_to_start", "Return to Start"),
        ("question_mark", "show_help", "Help"),
        ("?", "show_help", "Help"),
        ("slash", "show_help", "Help"),
        ("f1", "show_help", "Help"),
        ("q", "request_exit", "Exit"),
    ]

    def on_key(self, event) -> None:
        """Handle question mark and help keypresses."""
        if event.key in ("question_mark", "?") or getattr(event, "character", "") == "?":
            self.action_show_help()
            event.prevent_default()
            event.stop()

    def __init__(self, name: str = "control_plane"):
        super().__init__(name=name)
        self._step_progress = 0
        self._timer = None

    def compose(self) -> ComposeResult:
        yield TopHeader(initial_step=OnboardingStep.CONTROL_PLANE)
        with Horizontal(id="workspace-layout"):
            yield SidebarNav(current_step=OnboardingStep.CONTROL_PLANE)
            with Vertical(id="content-area"):
                with Vertical(id="content-panel"):
                    yield Label("[2/6] CONTROL PLANE INSTALLATION", classes="wizard-step-title")
                    yield Label(
                        "Installing Agent Substrate Control Plane on cluster [demo-cluster] in namespace [substrate-system]...",
                        classes="wizard-step-subtitle",
                    )

                    # Live Installation Progress Log Card
                    yield Static(self._render_install_log(), id="terminal-log-card")

                    # Button Row
                    with Horizontal(classes="action-button-row"):
                        btn_back = Button("← Back (b)", id="btn-back", classes="secondary-button")
                        btn_back.can_focus = False
                        yield btn_back

                        btn_proceed = Button(
                            "Proceed to WorkerPool Setup (Enter) →",
                            variant="primary",
                            id="btn-proceed",
                            classes="action-button",
                        )
                        btn_proceed.can_focus = False
                        yield btn_proceed

        yield BottomBar(
            initial_tip="Control plane components installing in namespace [substrate-system].",
            initial_hints="[Enter] Proceed  [b] Back  [/help] Help  [Ctrl+C] Exit",
        )

    def on_mount(self) -> None:
        self._step_progress = 0
        self._timer = self.set_interval(0.2, self._tick_install)

    def _tick_install(self) -> None:
        if self._step_progress < len(INSTALL_STEPS):
            self._step_progress += 1
            try:
                log_box = self.query_one("#terminal-log-card", Static)
                log_box.update(self._render_install_log())
            except Exception:
                pass
        else:
            if self._timer:
                self._timer.stop()

    def _render_install_log(self) -> Text:
        t = Text()
        t.append("╭── 🚀 SUBSTRATE CONTROL PLANE BOOTSTRAP (Simulation) ──────────────────────────╮\n", style="bold #38bdf8")
        t.append("│                                                                                  │\n", style="#38bdf8")

        for i, (key, desc) in enumerate(INSTALL_STEPS):
            t.append("│  ", style="#38bdf8")
            if i < self._step_progress:
                t.append("✓ ", style="bold #34d399")
                t.append(f"{desc.ljust(76)}", style="bold #34d399" if i == len(INSTALL_STEPS)-1 else "#f8fafc")
            elif i == self._step_progress:
                t.append("⠋ ", style="bold #38bdf8")
                t.append(f"{desc.ljust(76)}", style="bold #38bdf8")
            else:
                t.append("○ ", style="#64748b")
                t.append(f"{desc.ljust(76)}", style="#64748b")
            t.append("│\n", style="#38bdf8")

        t.append("│                                                                                  │\n", style="#38bdf8")
        t.append("╰──────────────────────────────────────────────────────────────────────────────────╯", style="bold #38bdf8")
        return t

    def action_proceed_next(self) -> None:
        if hasattr(self.app, "advance_step"):
            self.app.advance_step()

    def action_previous_step(self) -> None:
        if hasattr(self.app, "previous_step"):
            self.app.previous_step()

    def action_show_help(self) -> None:
        if hasattr(self.app, "action_show_help"):
            self.app.action_show_help()

    def action_return_to_start(self) -> None:
        if hasattr(self.app, "state_machine"):
            self.app.state_machine.transition_to(OnboardingStep.WELCOME)

    def action_request_exit(self) -> None:
        if hasattr(self.app, "action_request_exit"):
            self.app.action_request_exit()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "btn-proceed":
            self.action_proceed_next()
        elif event.button.id == "btn-back":
            self.action_previous_step()
