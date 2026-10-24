# Onboarding TUI Demo & UX Prototype

This directory contains a self-contained **interactive UX prototype and simulation** of the planned Agent Substrate onboarding experience. It is built using [Textual](https://textual.textualize.io/) and [Rich](https://rich.readthedocs.io/) to prototype terminal workflows, keyboard ergonomics, diagnostic probes, and cluster setup interactions before committing to Go-based control plane bindings.

> **Note:** This is a demonstration and design prototype. It runs local diagnostics (Git, Python, Docker, kubectl context, DNS), while cluster mutation and worker pool deployment steps are simulated walkthroughs.

## Installation

Install the required Python dependencies:

```bash
pip install -r demos/onboarding-tui/requirements.txt
```

## Running the Interactive TUI

Launch the interactive terminal UI:

```bash
./demos/onboarding-tui/run.sh
```

Or run directly with Python:

```bash
cd demos/onboarding-tui
python3 onboard.py
```

### CLI Flags & Modes

- **Standalone Doctor Diagnostics:**
  ```bash
  python3 onboard.py --doctor
  ```
- **Autonomous Autopilot Simulation:**
  ```bash
  python3 onboard.py --simulate
  ```
- **Launch Web Simulator (Browser):**
  ```bash
  python3 onboard.py --web
  # or: ./demos/onboarding-tui/open_simulator.sh
  ```

## Running Tests

Run the test suite:

```bash
pytest demos/onboarding-tui/substrate_onboarding/tests
```
