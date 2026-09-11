"""Confirmation modal dialog for graceful onboarding termination."""

from __future__ import annotations

from textual.app import ComposeResult
from textual.containers import Horizontal, Vertical
from textual.screen import ModalScreen
from textual.widgets import Button, Label, Static
from rich.text import Text


class ExitConfirmModal(ModalScreen[bool]):
    """Modal dialog prompting user to confirm exit upon Ctrl+C or /exit."""

    BINDINGS = [
        ("y", "confirm_exit", "Exit"),
        ("n", "cancel_exit", "Continue"),
        ("escape", "cancel_exit", "Continue"),
    ]

    def compose(self) -> ComposeResult:
        with Vertical(classes="modal-dialog"):
            yield Label("⏸ ONBOARDING PAUSED", classes="modal-title")
            t = Text("Exit setup? (y/n)\nYour selections will be preserved if you return.", style="#f0f6fc")
            yield Static(t, classes="modal-content")

            with Horizontal(classes="auth-button-row"):
                yield Button("Yes, Exit (y)", id="btn-confirm-exit", classes="secondary-button")
                yield Button("No, Continue Setup (n)", id="btn-cancel-exit", classes="action-button")

    def action_confirm_exit(self) -> None:
        self.dismiss(True)

    def action_cancel_exit(self) -> None:
        self.dismiss(False)

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "btn-confirm-exit":
            self.dismiss(True)
        elif event.button.id == "btn-cancel-exit":
            self.dismiss(False)
