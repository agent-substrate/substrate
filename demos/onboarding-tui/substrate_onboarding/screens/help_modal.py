"""Modal overlay for /help slash command and keyboard navigation guide."""

from __future__ import annotations

from textual.app import ComposeResult
from textual.containers import Vertical, Grid, Horizontal
from textual.screen import ModalScreen
from textual.widgets import Button, Label, Static
from rich.text import Text
from substrate_onboarding.engine.commands import CommandRegistry


class HelpModal(ModalScreen[None]):
    """Floating modal overlay displaying slash commands and keyboard shortcuts."""

    BINDINGS = [
        ("escape", "dismiss", "Close Help"),
        ("q", "dismiss", "Close Help"),
    ]

    def compose(self) -> ComposeResult:
        with Vertical(classes="modal-dialog"):
            yield Label("⚡ AGENT SUBSTRATE NAVIGATION & COMMANDS", classes="modal-title")

            yield Label("Global Slash Commands:", classes="wizard-step-title")
            for cmd in CommandRegistry.get_command_list():
                t = Text()
                t.append(f"  {cmd.name.ljust(10)}", style="bold #5eead4")
                t.append(f"Aliases: {', '.join(cmd.aliases).ljust(14)}", style="#8b949e")
                t.append(f" {cmd.description}", style="#f0f6fc")
                yield Static(t)

            yield Label("\nKeyboard Navigation Shortcuts:", classes="wizard-step-title")
            shortcuts = [
                ("[1 - 4]", "Direct instant option/track selection"),
                ("[Up / Down / j / k]", "Navigate items, options, and lists"),
                ("[Enter ↵]", "Select option / Proceed to next state"),
                ("[b / Backspace]", "Step back to previous screen"),
                ("[m]", "Expand / collapse declarative YAML drawer"),
                ("[c]", "Copy active YAML manifest to clipboard"),
                ("[t]", "Run live cold-start test turn (Step 7)"),
                ("[Tab / Shift+Tab]", "Move focus across interactive controls"),
                ("[0]", "Return to Welcome Screen"),
                ("[Ctrl+C / Ctrl+D]", "Pause onboarding & prompt exit confirmation"),
                ("[Esc / ? / F1 / q]", "Close modal dialogs / Toggle Help / Quick Exit"),
            ]
            for key, desc in shortcuts:
                t = Text()
                t.append(f"  {key.ljust(22)}", style="bold #38bdf8")
                t.append(f"{desc}", style="#f8fafc")
                yield Static(t)

            yield Label("\nDocumentation & Guides:", classes="wizard-step-title")
            docs = [
                ("GitHub Repo", "https://github.com/agent-substrate/substrate"),
                ("Architecture Guide", "docs/architecture.md"),
                ("Code Layout", "docs/dev/code-layout.md"),
                ("Roadmap & Sandboxing", "docs/roadmap.md"),
            ]
            for title, path in docs:
                t = Text()
                t.append(f"  📖 {title.ljust(20)}: ", style="bold #70d6ff")
                t.append(f"{path}", style="#8ab4f8")
                yield Static(t)

            with Horizontal(classes="auth-button-row"):
                yield Button("Close (Esc)", id="btn-close-help", classes="secondary-button")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "btn-close-help":
            self.dismiss()
