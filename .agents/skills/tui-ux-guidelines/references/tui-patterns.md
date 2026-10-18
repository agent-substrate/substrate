# TUI Design & Implementation Patterns Reference

This reference document provides concrete code patterns and templates for building elite terminal user interfaces with Python Textual, Rich, and standard ANSI terminal libraries.

---

## 1. Google Material 3 Dark & Light-Safe Color Palette

```python
# Semantic color definitions safe for modern terminals
M3_SURFACE = "#131314"
M3_SURFACE_PANEL = "#1e1f20"
M3_SURFACE_CARD = "#28292a"
M3_OUTLINE = "#444746"
M3_PRIMARY = "#a8c7fa"
M3_ON_PRIMARY = "#003062"

GOOGLE_BLUE = "#8ab4f8"
GOOGLE_RED = "#f28b82"
GOOGLE_YELLOW = "#fdd663"
GOOGLE_GREEN = "#81c995"
```

---

## 2. Braille Spinner Patterns

```python
BRAILLE_SPINNER = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]

# Frame rotation cadence: 80ms - 120ms
async def spin_progress(widget, task_name: str):
    idx = 0
    while not widget.is_complete:
        frame = BRAILLE_SPINNER[idx % len(BRAILLE_SPINNER)]
        widget.update(f"{frame} {task_name}...")
        idx += 1
        await asyncio.sleep(0.1)
```

---

## 3. The "Doctor" Diagnostic Pattern with Actionable Remedies

```python
from dataclasses import dataclass
from typing import Optional

@dataclass
class DoctorProbeResult:
    name: str
    status: str  # "ok", "warning", "failed"
    message: str
    remedy_command: Optional[str] = None

# Example formatted probe output
def render_probe(result: DoctorProbeResult) -> str:
    badge = {
        "ok": "[green]✓ [OK][/green]",
        "warning": "[yellow]▲ [WARNING][/yellow]",
        "failed": "[red]✖ [FAILED][/red]"
    }[result.status]
    
    out = f"{badge} Checking {result.name}... ({result.message})\n"
    if result.remedy_command:
        out += f"   ↳ [yellow]Fix:[/yellow] [cyan]code>{result.remedy_command}</code>[/cyan]\n"
    return out
```

---

## 4. Responsive Textual Layout Structure

```css
Screen {
    background: #131314;
    color: #e3e3e3;
    layout: vertical;
}

#top-header {
    dock: top;
    height: 3;
    background: #1e1f20;
    border-bottom: hkey #333538;
    padding: 0 2;
}

#bottom-bar {
    dock: bottom;
    height: 3;
    background: #1e1f20;
    border-top: hkey #333538;
    padding: 0 2;
}

#screen-container {
    width: 100%;
    height: 1fr;
    align: center middle;
    overflow-y: auto;
    padding: 1 2;
}
```
