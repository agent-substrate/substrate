"""Theme, color tokens, and styling for Agent Substrate Onboarding TUI.

Sleek, modern, high-taste dark aesthetic:
- Deep Obsidian base:    #0B0F17 (Velvety dark canvas)
- Elevated Surface:      #131B2E (Soft elevated cards)
- Borders & Outlines:    #1E293B (Subtle slate border)
- Electric Sky & Indigo: #38BDF8 / #818CF8 (Vibrant modern accents)
- Mint / Emerald:        #34D399 (Crisp verified status)
- Warm Amber:            #FBBF24 (Attention warnings)
- Text Hierarchy:        #F8FAFC (Pure White) & #94A3B8 (Muted Slate)
"""

from __future__ import annotations

from typing import List, Tuple
from rich.style import Style
from rich.text import Text

# Base Surfaces
OBSIDIAN_BASE = "#0B0F17"
ELEVATED_SURFACE = "#131B2E"
CARD_BORDER = "#1E293B"
CARD_BORDER_FOCUS = "#38BDF8"

# Accents
ACCENT_ELECTRIC_CYAN = "#38BDF8"
ACCENT_INDIGO = "#818CF8"
SUCCESS_MINT = "#34D399"
WARNING_AMBER = "#FBBF24"
ERROR_ROSE = "#F43F5E"

# Typography
TEXT_PURE_WHITE = "#F8FAFC"
TEXT_MUTED_SLATE = "#94A3B8"
TEXT_DIM = "#64748B"

# Backward compatibility aliases
M3_SURFACE = ELEVATED_SURFACE
M3_SURFACE_PANEL = OBSIDIAN_BASE
M3_SURFACE_CARD = ELEVATED_SURFACE
M3_SURFACE_HOVER = "#1E293B"
M3_OUTLINE = CARD_BORDER
M3_OUTLINE_FOCUS = ACCENT_ELECTRIC_CYAN

GOOGLE_BLUE = ACCENT_ELECTRIC_CYAN
GOOGLE_RED = ERROR_ROSE
GOOGLE_YELLOW = WARNING_AMBER
GOOGLE_GREEN = SUCCESS_MINT

M3_TEXT_PRIMARY = TEXT_PURE_WHITE
M3_TEXT_MUTED = TEXT_MUTED_SLATE
M3_TEXT_WHITE = "#ffffff"


def hex_to_rgb(hex_code: str) -> Tuple[int, int, int]:
    hex_code = hex_code.lstrip("#")
    return tuple(int(hex_code[i : i + 2], 16) for i in (0, 2, 4))  # type: ignore


def get_gradient_color(progress: float) -> Tuple[int, int, int]:
    stops = [
        hex_to_rgb(ACCENT_ELECTRIC_CYAN),
        hex_to_rgb(ACCENT_INDIGO),
        hex_to_rgb(SUCCESS_MINT),
        hex_to_rgb(ACCENT_ELECTRIC_CYAN),
    ]
    p = max(0.0, min(1.0, progress))
    num_segments = len(stops) - 1
    segment_idx = min(int(p * num_segments), num_segments - 1)
    seg_progress = (p * num_segments) - segment_idx
    c1, c2 = stops[segment_idx], stops[segment_idx + 1]
    return (
        int(c1[0] + (c2[0] - c1[0]) * seg_progress),
        int(c1[1] + (c2[1] - c1[1]) * seg_progress),
        int(c1[2] + (c2[2] - c1[2]) * seg_progress),
    )


def apply_google_gradient(text_lines: List[str]) -> Text:
    """Helper for logo gradient rendering."""
    rich_text = Text()
    for y, line in enumerate(text_lines):
        rich_text.append(line, style="bold #38bdf8")
        if y < len(text_lines) - 1:
            rich_text.append("\n")
    return rich_text


apply_pastel_gradient = apply_google_gradient


APP_CSS = """
Screen {
    background: #0B0F17;
    color: #F8FAFC;
    layers: base modal;
    layout: vertical;
}

/* Welcome Screen Hero & Cards */
#welcome-main-container {
    width: 100%;
    height: 1fr;
    background: #0B0F17;
    padding: 1 3;
    overflow-y: auto;
    align: center top;
}

#welcome-hero-logo {
    width: 100%;
    height: auto;
    text-align: center;
    margin-top: 1;
    margin-bottom: 0;
}

#welcome-hero-subtitle {
    width: 100%;
    text-align: center;
    color: #38bdf8;
    text-style: bold;
    margin-bottom: 1;
}

#welcome-tracks-title {
    color: #F8FAFC;
    text-style: bold;
    margin-top: 1;
    margin-bottom: 1;
}

#welcome-tracks-list {
    width: 100%;
    height: auto;
}

.track-option-card {
    width: 100%;
    height: auto;
    background: #131B2E;
    border: round #1E293B;
    padding: 1 2;
    margin-bottom: 1;
}

.track-option-card:focus {
    border: round #38bdf8;
    background: #1E293B;
}

#welcome-preflight-badge {
    width: 100%;
    height: auto;
    background: #131B2E;
    border: round #1E293B;
    padding: 1 2;
    margin-top: 1;
    text-align: center;
}

/* Workspace 2-Column Grid Layout */
#workspace-layout {
    width: 100%;
    height: 1fr;
    layout: horizontal;
    background: #0B0F17;
}

#sidebar-nav {
    width: 32;
    height: 100%;
    background: #0B0F17;
    border-right: solid #1E293B;
    padding: 2 2;
}

#sidebar-container {
    width: 100%;
    height: auto;
}

#sidebar-content {
    width: 100%;
    height: auto;
}

#content-area {
    width: 1fr;
    height: 1fr;
    background: #0F172A;
    padding: 2 3;
    overflow-y: auto;
}

#content-panel {
    width: 100%;
    height: auto;
}

Label {
    width: 100%;
    height: auto;
}

Static {
    width: 100%;
    height: auto;
}

.step-indicator-label {
    color: #94A3B8;
    margin-bottom: 0;
    width: 100%;
}

.wizard-step-title {
    color: #F8FAFC;
    text-style: bold;
    margin-bottom: 1;
    width: 100%;
}

.wizard-step-description {
    color: #94A3B8;
    margin-bottom: 1;
    width: 100%;
}

/* Real Command Callout */
#command-callout-card {
    width: 100%;
    height: auto;
    background: #131B2E;
    border: round #1E293B;
    padding: 1 2;
    margin: 1 0;
}

/* Step 2: Side-by-Side Cluster Layout */
#cluster-side-by-side-layout {
    width: 100%;
    height: auto;
    layout: horizontal;
    margin-top: 1;
    margin-bottom: 1;
}

#cluster-picker-column {
    width: 1fr;
    height: auto;
    margin-right: 1;
}

#cluster-inspection-column {
    width: 1fr;
    height: auto;
    margin-left: 1;
}

.column-header-label {
    color: #F8FAFC;
    text-style: bold;
    margin-bottom: 1;
}

.compact-cluster-card {
    width: 100%;
    height: auto;
    background: #131B2E;
    border: round #1E293B;
    padding: 0 1;
    margin-bottom: 1;
}

#cluster-verification-box {
    width: 100%;
    height: auto;
    background: #131B2E;
    border: round #38bdf8;
    padding: 1 1;
    margin-bottom: 1;
}

#cluster-compact-checklist {
    width: 100%;
    height: auto;
    background: #0B0F17;
    border: round #1E293B;
    padding: 1 1;
}

/* Execution Checklist Card */
#execution-checklist-card {
    width: 100%;
    height: auto;
    background: #131B2E;
    border: round #1E293B;
    padding: 1 2;
    margin: 1 0;
}

/* Button Rows */
.action-button-row {
    width: 100%;
    height: auto;
    align: right middle;
    margin-top: 2;
}

.action-button {
    margin-left: 1;
    background: #2563EB;
    color: #ffffff;
    border: round #38bdf8;
    text-style: bold;
    min-width: 24;
}

.action-button:hover {
    background: #1D4ED8;
    color: #ffffff;
}

.action-button:focus {
    border: round #ffffff;
    background: #1D4ED8;
    color: #ffffff;
    text-style: bold;
}

.secondary-button {
    margin-right: 1;
    background: #131B2E;
    color: #F8FAFC;
    border: round #1E293B;
    min-width: 16;
}

.secondary-button:hover {
    background: #1E293B;
    color: #38bdf8;
    border: round #38bdf8;
}

.secondary-button:focus {
    border: round #38bdf8;
    background: #1E293B;
    color: #38bdf8;
    text-style: bold;
}

Button:focus {
    border: round #ffffff;
    background: #2563EB;
    color: #ffffff;
    text-style: bold;
}

.compact-cluster-card:focus {
    border: round #38bdf8;
    background: #1E293B;
}

/* Bottom TUI Keymap Dock Bar */
#bottom-bar {
    dock: bottom;
    height: 3;
    background: #0B0F17;
    border-top: solid #1E293B;
    padding: 0 1;
}

#bottom-bar-inner {
    width: 100%;
    height: 100%;
    align: left middle;
}

#keyboard-keymaps {
    width: 1fr;
    color: #F8FAFC;
}

#keyboard-step-badge {
    width: auto;
    color: #38bdf8;
    text-align: right;
}

/* Top Header */
#top-header {
    dock: top;
    height: 3;
    background: #0B0F17;
    color: #F8FAFC;
    border-bottom: solid #1E293B;
    padding: 0 2;
}

#header-brand {
    width: auto;
}

#header-stepper {
    color: #94A3B8;
    text-align: right;
    width: 1fr;
}

/* Modals */
#help-modal-container {
    width: 80;
    height: auto;
    background: #131B2E;
    border: round #38bdf8;
    padding: 1 2;
}

#exit-modal-container {
    width: 60;
    height: auto;
    background: #131B2E;
    border: round #F43F5E;
    padding: 1 2;
    align: center middle;
}
"""
