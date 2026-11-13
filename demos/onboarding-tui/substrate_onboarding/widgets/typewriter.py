"""Typewriter animated text widget with auto-wrapping and Google Material 3 styling."""

from __future__ import annotations

import asyncio
from typing import Callable, Optional
from textual.widgets import Static
from rich.text import Text


class TypewriterWidget(Static):
    """Widget that animates text revealing character-by-character with auto word-wrap."""

    def __init__(
        self,
        full_text: str,
        char_delay: float = 0.015,
        on_complete: Optional[Callable[[], None]] = None,
        id: Optional[str] = None,
        classes: Optional[str] = None,
    ):
        super().__init__("", id=id, classes=classes)
        self.full_text = full_text
        self.char_delay = char_delay
        self.on_complete = on_complete
        self.current_length = 0
        self._is_completed = False
        self._anim_task: Optional[asyncio.Task] = None

    def on_mount(self) -> None:
        """Start typewriter animation when mounted."""
        self._anim_task = asyncio.create_task(self._run_typewriter_animation())

    async def _run_typewriter_animation(self) -> None:
        """Gradually increment revealed character count and update widget."""
        try:
            for i in range(1, len(self.full_text) + 1):
                self.current_length = i
                self.update(self._render_text())
                # Slight variable pause for punctuation
                char = self.full_text[i - 1]
                delay = self.char_delay * (2.5 if char in ".!?,:\n" else 1.0)
                await asyncio.sleep(delay)

            self._is_completed = True
            self.update(self._render_text())
            if self.on_complete:
                self.on_complete()
        except asyncio.CancelledError:
            pass

    def skip(self) -> None:
        """Instantly finish typewriter animation."""
        if self._anim_task and not self._anim_task.done():
            self._anim_task.cancel()
        self.current_length = len(self.full_text)
        self._is_completed = True
        self.update(self._render_text())
        if self.on_complete:
            self.on_complete()

    def _render_text(self) -> Text:
        """Render revealed text with clear line wrapping and Google Material 3 styling."""
        revealed = self.full_text[: self.current_length]
        t = Text(revealed, style="#e3e3e3", justify="center")
        if not self._is_completed and self.current_length < len(self.full_text):
            t.append(" ▌", style="bold #a8c7fa")
        return t
