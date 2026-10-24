"""Diagnostic check row widget with animated spinners and remediation actions."""

from __future__ import annotations

from typing import Optional
from textual.app import ComposeResult
from textual.containers import Vertical
from textual.reactive import reactive
from textual.widget import Widget
from textual.widgets import Label, Static
from rich.text import Text
from substrate_onboarding.config import CheckResult


SPINNER_FRAMES = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]


class DoctorItemWidget(Widget):
    """Renderable row for an individual preflight check."""

    status: reactive[str] = reactive("pending")
    spinner_idx: reactive[int] = reactive(0)

    def __init__(self, key: str, name: str, id: Optional[str] = None):
        super().__init__(id=id, classes="doctor-item-row")
        self.check_key = key
        self.check_name = name
        self.result: Optional[CheckResult] = None
        self._spinner_timer = None

    def compose(self) -> ComposeResult:
        with Vertical():
            yield Label(self._render_line(), id=f"check-line-{self.check_key}")
            yield Static("", id=f"check-remedy-{self.check_key}", classes="remedy-box")

    def on_mount(self) -> None:
        remedy_box = self.query_one(f"#check-remedy-{self.check_key}", Static)
        remedy_box.display = False

    def set_running() -> None:
        self.status = "running"
        self.add_class("-running")
        self.remove_class("-ok", "-warning", "-failed")
        if not self._spinner_timer:
            self._spinner_timer = self.set_interval(0.08, self._advance_spinner)
        self._update_ui()

    def set_running(self) -> None:
        self.status = "running"
        self.add_class("-running")
        self.remove_class("-ok", "-warning", "-failed")
        if not self._spinner_timer:
            self._spinner_timer = self.set_interval(0.08, self._advance_spinner)
        self._update_ui()

    def _advance_spinner(self) -> None:
        self.spinner_idx = (self.spinner_idx + 1) % len(SPINNER_FRAMES)
        self._update_ui()

    def set_result(self, result: CheckResult) -> None:
        if self._spinner_timer:
            self._spinner_timer.stop()
            self._spinner_timer = None

        self.result = result
        self.status = result.status
        self.remove_class("-running")

        if result.status == "ok":
            self.add_class("-ok")
        elif result.status == "warning":
            self.add_class("-warning")
        elif result.status == "failed":
            self.add_class("-failed")

        self._update_ui()

        remedy_box = self.query_one(f"#check-remedy-{self.check_key}", Static)
        if (result.status in ("warning", "failed")) and result.fix_command:
            remedy_text = Text()
            remedy_text.append("  ┌─ 💡 Action Required ───────────────────────────────────────────────────\n", style="bold #fdd663")
            if result.details or result.plain_description:
                desc = result.details or result.plain_description
                remedy_text.append(f"  │  ℹ️  {desc}\n", style="#d3e3fd")
            remedy_text.append("  │  📋 Command: ", style="bold #9aa0a6")
            remedy_text.append(f"{result.fix_command}\n", style="bold #8ab4f8")
            if result.doc_url:
                remedy_text.append("  │  📖 Docs:    ", style="bold #9aa0a6")
                remedy_text.append(f"{result.doc_url}\n", style="underline #a8c7fa")
            remedy_text.append("  └────────────────────────────────────────────────────────────────────────", style="bold #fdd663")
            remedy_box.update(remedy_text)
            remedy_box.display = True
        else:
            remedy_box.display = False

    def _render_line(self) -> Text:
        t = Text()
        if self.status == "pending":
            t.append("○ ", style="#9aa0a6")
            t.append(f"Checking {self.check_name}... ", style="#9aa0a6")
            t.append("[WAITING]", style="#5e5e5e")
        elif self.status == "running":
            frame = SPINNER_FRAMES[self.spinner_idx]
            t.append(f"{frame} ", style="bold #a8c7fa")
            t.append(f"Checking {self.check_name}... ", style="bold #f2f2f2")
            t.append(f"[{frame}]", style="bold #a8c7fa")
        elif self.status == "ok":
            t.append("✓ ", style="bold #81c995")
            t.append(f"Checking {self.check_name}... ", style="#f2f2f2")
            t.append("[OK] ", style="bold #81c995")
            if self.result and self.result.message:
                t.append(f"({self.result.message})", style="#9aa0a6")
        elif self.status == "warning":
            t.append("▲ ", style="bold #fdd663")
            t.append(f"Checking {self.check_name}... ", style="#f2f2f2")
            t.append("[WARNING] ", style="bold #fdd663")
            if self.result and self.result.message:
                t.append(f"({self.result.message})", style="#fdd663")
        elif self.status == "failed":
            t.append("✖ ", style="bold #f28b82")
            t.append(f"Checking {self.check_name}... ", style="#f2f2f2")
            t.append("[FAILED] ", style="bold #f28b82")
            if self.result and self.result.message:
                t.append(f"({self.result.message})", style="#f28b82")
        return t

    def _update_ui(self) -> None:
        try:
            lbl = self.query_one(f"#check-line-{self.check_key}", Label)
            lbl.update(self._render_line())
        except Exception:
            pass
