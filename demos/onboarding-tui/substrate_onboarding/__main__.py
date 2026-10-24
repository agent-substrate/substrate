"""CLI entrypoint for running python -m substrate_onboarding or onboard.py."""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
import webbrowser
from rich.console import Console
from rich.panel import Panel
from rich.text import Text

from substrate_onboarding import __version__
from substrate_onboarding.app import SubstrateOnboardingApp
from substrate_onboarding.checks.runner import PreflightRunner


def run_standalone_doctor() -> int:
    """Run pre-flight diagnostics directly in CLI mode without full TUI."""
    console = Console()
    console.print("\n[bold #5eead4]⚡ Agent Substrate Pre-Flight Diagnostics[/bold #5eead4]\n")

    runner = PreflightRunner()

    def on_start(k: str, name: str) -> None:
        console.print(f"  [bold #5eead4]•[/bold #5eead4] Checking {name}...", end="\r")

    def on_complete(k: str, res) -> None:
        if res.status == "ok":
            console.print(f"  [bold #34d399]✓[/bold #34d399] {res.name}: [bold #34d399][OK][/bold #34d399] {res.message}")
        elif res.status == "warning":
            console.print(f"  [bold #fcd34d]▲[/bold #fcd34d] {res.name}: [bold #fcd34d][WARNING][/bold #fcd34d] {res.message}")
            if res.fix_command:
                console.print(f"    [dim #8b949e]↳ Fix:[/dim #8b949e] [bold #58a6ff]{res.fix_command}[/bold #58a6ff]")
        else:
            console.print(f"  [bold #fb7185]✖[/bold #fb7185] {res.name}: [bold #fb7185][FAILED][/bold #fb7185] {res.message}")
            if res.fix_command:
                console.print(f"    [dim #8b949e]↳ Fix:[/dim #8b949e] [bold #fb7185]{res.fix_command}[/bold #fb7185]")

    runner.set_callbacks(on_start=on_start, on_complete=on_complete)
    results = asyncio.run(runner.run_all(delay_between_sec=0.05))

    ok_count = sum(1 for r in results.values() if r.status == "ok")
    total = len(results)
    console.print(f"\n[bold #5eead4]Diagnostics complete:[/bold #5eead4] {ok_count}/{total} probes passed.\n")
    return 0


def open_web_simulator() -> None:
    """Launch the interactive web simulator in default browser."""
    console = Console()
    sim_path = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "index.html")
    )
    if os.path.exists(sim_path):
        console.print(f"\n[bold #5eead4]⚡ Opening Web Simulator in Browser...[/bold #5eead4]")
        console.print(f"[dim #8b949e]File: {sim_path}[/dim #8b949e]\n")
        webbrowser.open(f"file://{sim_path}")
    else:
        console.print(f"[bold #fb7185]Could not find web simulator at {sim_path}[/bold #fb7185]")


def main() -> None:
    """Launch the Agent Substrate Onboarding TUI or CLI tools."""
    parser = argparse.ArgumentParser(
        description="Agent Substrate Onboarding TUI — Interactive Prototype Demonstration",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--doctor",
        action="store_true",
        help="Run environment pre-flight diagnostics in standalone CLI mode",
    )
    parser.add_argument(
        "--simulate",
        action="store_true",
        help="Run autonomous autopilot terminal simulation demonstrating all onboarding states",
    )
    parser.add_argument(
        "--web",
        action="store_true",
        help="Open interactive shareable Web Simulator in your browser",
    )
    parser.add_argument(
        "--version",
        "-v",
        action="version",
        version=f"Agent Substrate Onboarding Prototype v{__version__}",
    )

    args = parser.parse_args()

    if args.doctor:
        sys.exit(run_standalone_doctor())

    if args.web:
        open_web_simulator()
        return

    if args.simulate:
        try:
            # Add demos/onboarding-tui to sys.path
            demo_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
            if demo_dir not in sys.path:
                sys.path.insert(0, demo_dir)
            from simulate_demo import SimulatedOnboardingApp
            app = SimulatedOnboardingApp()
            app.run()
            return
        except Exception as e:
            Console().print(f"[bold #fb7185]Could not launch autopilot simulation: {e}[/bold #fb7185]")
            sys.exit(1)

    console = Console()
    try:
        app = SubstrateOnboardingApp()
        result_state = app.run()

        if result_state and result_state.is_complete:
            # Render a post-TUI success card in the terminal
            t = Text()
            t.append("⚡ Interactive Onboarding Prototype Simulation Completed!\n\n", style="bold #34d399")
            t.append(f"  • Selected Track:     {result_state.get_track_item().title}\n", style="#f0f6fc")
            t.append(f"  • Selected Cluster:   {result_state.get_cluster_item().title}\n", style="#f0f6fc")
            t.append(f"  • Node Pool Setup:    {result_state.get_dataplane_item().title}\n", style="#f0f6fc")
            t.append(f"  • Autoscaling Buffer: {result_state.get_sandbox_item().title}\n", style="#f0f6fc")
            t.append("\nNext Steps (Real Substrate Workloads):\n", style="bold #5eead4")
            t.append("  • Explore demos:    cd demos/sandbox && ./run.sh\n", style="#8b949e")
            t.append("  • Architecture:     cat docs/architecture.md\n", style="#8b949e")
            t.append("  • Build CLI:        make build-kubectl-ate\n", style="#8b949e")

            console.print(Panel(t, title="[bold #5eead4]Agent Substrate Prototype[/bold #5eead4]", border_style="#5eead4"))
        else:
            console.print("[dim #8b949e]Simulation exited. Run anytime with `python3 onboard.py`.[/dim #8b949e]")

    except KeyboardInterrupt:
        console.print("\n[bold #fcd34d]Simulation interrupted by user.[/bold #fcd34d]")
        sys.exit(0)


if __name__ == "__main__":
    main()
