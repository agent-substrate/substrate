# Agent Substrate — High-Taste Onboarding TUI

A high-taste, keyboard-centric Terminal User Interface (TUI) for onboarding developers to **Agent Substrate** and **GKE Agent Sandboxes**. Built with Python and **Textual**, it features rich interactive navigation, Google Material 3 typography and dark surface tokens, non-blocking environment diagnostics, and credential management.

---

## 🎨 Visual & Taste Principles

- **Google Material 3 Dark Palette**: Surface hierarchies (`#131314` base, `#1e1f20` panels, `#28292a` cards, `#444746` outlines).
- **Google 4-Color Gradient Branding**: Smooth gradient flow (Google Blue `#8ab4f8` $\rightarrow$ Google Red `#f28b82` $\rightarrow$ Google Yellow `#fdd663` $\rightarrow$ Google Green `#81c995`).
- **Zero Text Overlap & High Legibility**: Clear line-heights, distinct padding, dynamic option cards, and high contrast.
- **Fluid Keyboard-First Navigation**: Arrow keys (`↑/↓`, `j/k`), `Enter` to confirm, `b` to step back, `/help` overlay, and confirmation modal on `Ctrl+C`.

---

## 🚀 Quickstart

### 1. Interactive Web Simulator
```bash
python3 onboard.py --web
```

### 2. Native Terminal TUI
```bash
python3 onboard.py
```

### 3. Hands-Free Autopilot Simulation
```bash
python3 onboard.py --simulate
```

### 4. Standalone Environment Pre-Flight Doctor
```bash
python3 onboard.py --doctor
```

---

## 🧪 Testing

Run the full pytest suite:
```bash
python3 -m pytest substrate_onboarding/tests/ -v
```
