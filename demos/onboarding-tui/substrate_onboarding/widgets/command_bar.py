"""Command bar and error banner widgets."""

from __future__ import annotations

from typing import Callable, Optional
from textual.app import ComposeResult
from textual.containers import Vertical
from textual.widget import Widget
from textual.widgets import Input, Label
from rich.text import Text


class InlineErrorBanner(Widget):
    """Inline red error banner widget that flashes upon validation errors."""

    def __init__(self, id: str = "error-banner"):
        super().__init__(id=id, classes="error-pill")

    def show_error(self, message: str) -> None:
        self.add_class("-visible")
        try:
            lbl = self.query_one(Label)
            lbl.update(Text(f"⚠ {message}", style="bold #fb7185"))
        except Exception:
            pass

    def clear(self) -> None:
        self.remove_class("-visible")

    def compose(self) -> ComposeResult:
        yield Label("", id="error-label")
