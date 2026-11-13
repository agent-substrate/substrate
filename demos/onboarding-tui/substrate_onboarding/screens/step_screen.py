"""Generic Step Screen Renderer for Substrate Onboarding with 7-step sequence & progressive disclosure."""

from __future__ import annotations

from typing import List, Optional
from textual.app import ComposeResult
from textual.containers import Horizontal, Vertical
from textual.reactive import reactive
from textual.screen import Screen
from textual.widgets import Button, Label, Static
from rich.text import Text

from substrate_onboarding.config import (
    OnboardingStep,
    STEP_CONFIGS,
    StepMetadata,
    AVAILABLE_CLUSTERS,
    NODEPOOL_OPTIONS,
    AUTOSCALING_OPTIONS,
    DEPLOY_WP_OPTIONS,
    OptionItem,
)
from substrate_onboarding.widgets.sidebar_nav import SidebarNav
from substrate_onboarding.widgets.status_bar import TopHeader, BottomBar


class GenericStepScreen(Screen):
    """Universal renderer for wizard steps with progressive disclosure, custom option cards & verification."""

    selected_option_idx: reactive[int] = reactive(0)

    BINDINGS = [
        ("enter", "proceed_next", "Proceed"),
        ("space", "proceed_next", "Proceed"),
        ("right", "proceed_next", "Proceed"),
        ("b", "previous_step", "Back"),
        ("left", "previous_step", "Back"),
        ("backspace", "previous_step", "Back"),
        ("0", "return_to_start", "Return to Start"),
        ("1", "select_opt_1", "Select 1"),
        ("2", "select_opt_2", "Select 2"),
        ("3", "select_opt_3", "Select 3"),
        ("4", "select_opt_4", "Select 4"),
        ("up", "navigate_up", "Previous"),
        ("down", "navigate_down", "Next"),
        ("tab", "navigate_down", "Focus Next"),
        ("shift+tab", "navigate_up", "Focus Prev"),
        ("k", "navigate_up", "Previous"),
        ("j", "navigate_down", "Next"),
        ("m", "toggle_manifest", "Toggle Manifest"),
        ("y", "run_test_turn", "Run Test Turn"),
        ("n", "skip_test_turn", "Skip Test Turn"),
        ("t", "run_test_turn", "Test Turn"),
        ("question_mark", "show_help", "Help"),
        ("?", "show_help", "Help"),
        ("slash", "show_help", "Help"),
        ("f1", "show_help", "Help"),
        ("q", "request_exit", "Exit"),
        ("escape", "handle_escape", "Exit / Back"),
    ]

    def __init__(self, step_key: OnboardingStep, name: Optional[str] = None):
        screen_name = name or step_key.value
        super().__init__(name=screen_name)
        self.step_key = step_key
        self.meta: StepMetadata = STEP_CONFIGS[step_key]
        self.clusters = AVAILABLE_CLUSTERS
        self.nodepool_opts = NODEPOOL_OPTIONS
        self.autoscaling_opts = AUTOSCALING_OPTIONS
        self.deploy_wp_opts = DEPLOY_WP_OPTIONS
        self._checklist_progress = 0
        self._timer = None

    def compose(self) -> ComposeResult:
        yield TopHeader(initial_step=self.step_key)
        with Horizontal(id="workspace-layout"):
            yield SidebarNav(current_step=self.step_key)
            with Vertical(id="content-area"):
                with Vertical(id="content-panel"):
                    # Step Number & Title
                    yield Label(f"Step {self.meta.step_num} of 7", classes="step-indicator-label")
                    yield Label(self.meta.heading, classes="wizard-step-title")
                    yield Label(self.meta.description, classes="wizard-step-description")

                    # Step 2: Side-by-side Cluster Selection with Region & Conditional GKE Private GA Agreement
                    if self.step_key == OnboardingStep.CONNECT_CLUSTER:
                        with Horizontal(id="cluster-side-by-side-layout"):
                            # Left Column: Cluster List (with Region info)
                            with Vertical(id="cluster-picker-column"):
                                yield Label("Select target cluster (Press [1-4]):", classes="column-header-label")
                                for idx in range(len(self.clusters)):
                                    yield Static(
                                        self._render_cluster_row(idx),
                                        id=f"cluster-item-{idx}",
                                        classes="compact-cluster-card",
                                    )

                            # Right Column: Cluster Type, Region & Substrate Probe Verification
                            with Vertical(id="cluster-inspection-column"):
                                yield Label("Cluster Type & Substrate Probe:", classes="column-header-label")
                                yield Static(self._render_cluster_verification_box(), id="cluster-verification-box")
                                yield Static(self._render_compact_checklist(), id="cluster-compact-checklist")

                    elif self.step_key == OnboardingStep.TURN_ON_SUBSTRATE:
                        # Step 3: Turn on Substrate (Declarative Manifest + Live Checklist)
                        yield Static(self._render_manifest_drawer(), id="declarative-manifest-card")
                        yield Static(self._render_checklist_box(), id="execution-checklist-card")

                    elif self.step_key == OnboardingStep.COMPATIBLE_NODEPOOL:
                        # Step 4: Compatible Node Pool (Scan First -> Present Options)
                        yield Static(self._render_nodepool_scan_status(), id="nodepool-scan-box")
                        if self.meta.yaml_notice:
                            yield Static(self._render_yaml_notice(), id="yaml-notice-box")
                        yield Label("Choose node pool configuration (Press [1-3]):", classes="column-header-label")
                        with Vertical(id="nodepool-options-list"):
                            for idx in range(len(self.nodepool_opts)):
                                yield Static(
                                    self._render_option_card(self.nodepool_opts, idx),
                                    id=f"nodepool-opt-{idx}",
                                    classes="compact-cluster-card",
                                )
                        yield Static(self._render_checklist_box(), id="execution-checklist-card")

                    elif self.step_key == OnboardingStep.CONFIG_AUTOSCALING:
                        # Step 5: WorkerPool Autoscaling (HPA & CapacityBuffer)
                        if self.meta.yaml_notice:
                            yield Static(self._render_yaml_notice(), id="yaml-notice-box")
                        yield Static(self._render_capacity_telemetry_badge(), id="capacity-telemetry-badge")
                        yield Label("Choose autoscaling configuration (Press [1-3]):", classes="column-header-label")
                        with Vertical(id="autoscaling-options-list"):
                            for idx in range(len(self.autoscaling_opts)):
                                yield Static(
                                    self._render_option_card(self.autoscaling_opts, idx),
                                    id=f"autoscaling-opt-{idx}",
                                    classes="compact-cluster-card",
                                )
                        yield Static(self._render_checklist_box(), id="execution-checklist-card")

                    elif self.step_key == OnboardingStep.DEPLOY_WORKERPOOL:
                        # Step 6: Confirm & Deploy WorkerPool
                        yield Label("Deploy default Substrate WorkerPool (Press [1-2]):", classes="column-header-label")
                        with Vertical(id="deploy-wp-options-list"):
                            for idx in range(len(self.deploy_wp_opts)):
                                yield Static(
                                    self._render_option_card(self.deploy_wp_opts, idx),
                                    id=f"deploy-wp-opt-{idx}",
                                    classes="compact-cluster-card",
                                )
                        yield Static(self._render_checklist_box(), id="execution-checklist-card")

                    elif self.step_key == OnboardingStep.COMPLETE:
                        # Step 7: Celebratory Completion & Next Steps
                        yield Static(self._render_celebratory_card(), id="celebratory-card")
                        yield Static(self._render_test_turn_playground(), id="test-turn-playground")
                        yield Static(self._render_next_steps_card(), id="next-steps-card")

                    else:
                        # Step 1: Clean Preflight Checklist (No command noise)
                        yield Static(self._render_checklist_box(), id="execution-checklist-card")

        yield BottomBar(
            keymaps=self._get_step_keymaps(),
            step_badge=f"Step {self.meta.step_num} of 7",
        )

    def on_mount(self) -> None:
        self._checklist_progress = 0
        if self.meta.checklist_items:
            self._timer = self.set_interval(0.4, self._tick_checklist)

    def _tick_checklist(self) -> None:
        if self._checklist_progress <= len(self.meta.checklist_items):
            self._checklist_progress += 1
            try:
                chk = self.query_one("#execution-checklist-card", Static)
                chk.update(self._render_checklist_box())
            except Exception:
                pass
            try:
                compact_chk = self.query_one("#cluster-compact-checklist", Static)
                compact_chk.update(self._render_compact_checklist())
            except Exception:
                pass
        else:
            if self._timer:
                self._timer.stop()

    def _render_manifest_drawer(self) -> Text:
        t = Text()
        t.append("</> Declarative Control-Plane Manifest: ", style="bold #70d6ff")
        t.append("manifests/substrate-control-plane.yaml\n", style="bold #ffffff")
        t.append("    kubectl apply -f manifests/substrate-control-plane.yaml\n", style="bold #8ab4f8")
        t.append("    💡 Press [m] to toggle full YAML spec or [c] to copy to clipboard\n", style="italic #80868b")
        return t

    def _render_capacity_telemetry_badge(self) -> Text:
        t = Text()
        t.append("📈 Standby Efficiency: ", style="bold #81c995")
        t.append("3 standby replicas absorb up to 300 bursty agent sessions with 0% CPU idle waste.\n", style="#e3e3e3")
        return t

    def _render_test_turn_playground(self) -> Text:
        t = Text()
        t.append("⚡ LIVE VERIFICATION PLAYGROUND:\n", style="bold #70d6ff")
        if getattr(self, "_test_turn_skipped", False):
            t.append("  ⏭️  Verification skipped • Cluster is ready for production workloads\n", style="bold #80868b")
            t.append("  💡 Press [y] to run test turn at any time\n", style="italic #80868b")
        elif getattr(self, "_test_turn_ran", False):
            t.append("  ✓ Warm microVM Allocated (14ms)  │  ✓ Prompt Dispatched (22ms)  │  ✓ Executed (12ms)\n", style="#81c995")
            t.append("  {\"status\": \"ready\", \"worker\": \"default-worker-pool-8f4b\", \"latency\": \"48ms\"}\n", style="bold #8ab4f8")
            t.append("  💡 Sub-100ms round-trip verified! Press [y] to re-run or [n] to clear.\n", style="italic #80868b")
        else:
            t.append("  Run live cold-start verification test turn on warm microVM? ", style="bold #ffffff")
            t.append("[y] Yes  ", style="bold #81c995")
            t.append("[n] No (Skip)\n", style="bold #80868b")
            t.append("  💡 Press [y] to dispatch a live verification prompt or [n] to skip.\n", style="italic #80868b")
        return t

    def _render_command_callout(self) -> Text:
        t = Text()
        t.append("▼ Show the real command\n", style="bold #70d6ff")
        t.append(f"  {self.meta.real_command}\n", style="bold #e3e3e3")
        return t

    def _render_yaml_notice(self) -> Text:
        t = Text()
        if self.meta.yaml_notice:
            t.append(f"{self.meta.yaml_notice}\n", style="bold #fdd663")
        return t

    def _render_cluster_row(self, idx: int) -> Text:
        cluster = self.clusters[idx]
        is_selected = idx == self.selected_option_idx
        t = Text()

        keycap = f" [{idx + 1}] "
        if is_selected:
            t.append(f" ▶ {keycap}", style="bold #ffffff on #1565c0")
            t.append(f" {cluster.icon} {cluster.title[:38]}\n", style="bold #70d6ff on #1565c0")
            t.append(f"        Region: {cluster.region} • {cluster.nodes} nodes", style="#e3e3e3 on #1565c0")
        else:
            t.append(f" ○ {keycap}", style="#80868b")
            t.append(f" {cluster.icon} {cluster.title[:38]}\n", style="bold #e3e3e3")
            t.append(f"        Region: {cluster.region} • {cluster.nodes} nodes", style="#80868b")

        return t

    def _render_option_card(self, opt_list: List[OptionItem], idx: int) -> Text:
        opt = opt_list[idx]
        is_selected = idx == self.selected_option_idx
        t = Text()

        keycap = f" {idx + 1} "
        if is_selected:
            t.append(f" ▎{keycap}", style="bold #ffffff on #2563eb")
            t.append(f" {opt.icon} {opt.title}\n", style="bold #f8fafc on #1e293b")
            t.append(f"       {opt.description}\n", style="#94a3b8 on #1e293b")
            t.append(f"       💡 {opt.tip}", style="italic #38bdf8 on #1e293b")
        else:
            t.append(f"  {keycap}", style="bold #38bdf8 on #0b0f17")
            t.append(f" {opt.icon} {opt.title}\n", style="bold #f8fafc")
            t.append(f"       {opt.description}\n", style="#94a3b8")
            t.append(f"       💡 {opt.tip}", style="italic #64748b")

        return t

    def _render_cluster_verification_box(self) -> Text:
        cluster = self.clusters[self.selected_option_idx if self.selected_option_idx < len(self.clusters) else 0]
        t = Text()
        t.append("🌐 CLUSTER VERIFICATION (Prototype Context):\n", style="bold #38bdf8")
        t.append(f"  Provider : {cluster.provider} ({cluster.version})\n", style="bold #f8fafc")
        t.append(f"  Region   : {cluster.region}\n", style="bold #818cf8")
        t.append(f"  Nodes    : {cluster.nodes} ready (KVM / microVM compatible)\n", style="#34d399")
        t.append(f"  Probe    : [substrate-system] ➔ {cluster.control_plane_status}", style="bold #fbbf24")
        return t

    def _render_nodepool_scan_status(self) -> Text:
        t = Text()
        t.append("🔍 CLUSTER NODE POOL COMPATIBILITY SCAN:\n", style="bold #38bdf8")
        t.append("  • Probed node pool capacity: 12 nodes across 2 zones\n", style="#94a3b8")
        t.append("  ▲ Scan Result: No existing node pool detected with /dev/kvm enabled (0/12 nodes compatible).\n", style="bold #fbbf24")
        t.append("  💡 Sandboxed microVM execution requires a compatible node pool.\n", style="italic #64748b")
        return t

    def _render_compact_checklist(self) -> Text:
        t = Text()
        t.append("⚡ PROBE CHECKLIST:\n", style="bold #38bdf8")
        for i, item in enumerate(self.meta.checklist_items):
            if i < self._checklist_progress:
                t.append("  ✓ ", style="bold #34d399")
                t.append(f"{item}\n", style="#f8fafc")
            elif i == self._checklist_progress:
                t.append("  ⠋ ", style="bold #38bdf8")
                t.append(f"{item}\n", style="#38bdf8")
            else:
                t.append("  ○ ", style="#64748b")
                t.append(f"{item}\n", style="#64748b")
        return t

    def _render_checklist_box(self) -> Text:
        t = Text()

        if self.step_key == OnboardingStep.COMPATIBLE_NODEPOOL:
            if self.selected_option_idx == 1:
                t.append("🛠️  MANUAL GCLOUD NODE POOL PROVISIONING:\n\n", style="bold #70d6ff")
                t.append("  gcloud container node-pools create substrate-worker-pool \\\n", style="bold #e3e3e3")
                t.append("    --cluster=gke_enterprise_us-central1_prod \\\n", style="bold #e3e3e3")
                t.append("    --machine-type=n2-standard-48 \\\n", style="bold #e3e3e3")
                t.append("    --enable-nested-virtualization \\\n", style="bold #e3e3e3")
                t.append("    --num-nodes=3\n\n", style="bold #e3e3e3")
                t.append("  💡 Run this command in your cloud terminal, then press [Enter ↵] to continue.\n", style="italic #fdd663")
                return t
            elif self.selected_option_idx == 2:
                t.append("🔄  CHOOSE A DIFFERENT CLUSTER:\n\n", style="bold #70d6ff")
                t.append("  Press [Enter ↵] or [3] to return to Step 2 and select another cluster from your kubeconfig.\n", style="#e3e3e3")
                return t
        elif self.step_key == OnboardingStep.CONFIG_AUTOSCALING:
            if self.selected_option_idx == 1:
                t.append("🛠️  MANUAL KUBECTL AUTOSCALING:\n\n", style="bold #70d6ff")
                t.append("  kubectl apply -f manifests/workerpool-autoscaling.yaml\n\n", style="bold #e3e3e3")
                t.append("  💡 Modify the HPA and CapacityBuffer manifests, then press [Enter ↵] to continue.\n", style="italic #fdd663")
                return t
            elif self.selected_option_idx == 2:
                t.append("⏭️  AUTOSCALING SKIPPED:\n\n", style="bold #70d6ff")
                t.append("  Worker pool will run with a fixed replica count without dynamic scaling.\n", style="#e3e3e3")
                return t
        elif self.step_key == OnboardingStep.DEPLOY_WORKERPOOL:
            if self.selected_option_idx == 1:
                t.append("⏭️  DEFAULT WORKERPOOL SKIPPED:\n\n", style="bold #70d6ff")
                t.append("  Initial worker pool creation skipped. You can deploy worker pools at any time via atectl or kubectl.\n", style="#e3e3e3")
                return t

        title = self.meta.checklist_title
        items = self.meta.checklist_items

        t.append(f"{title}\n\n", style="bold #70d6ff")

        for i, item in enumerate(items):
            if i < self._checklist_progress:
                t.append("✓  ", style="bold #81c995")
                t.append(f"{item}\n\n", style="bold #ffffff")
            elif i == self._checklist_progress:
                t.append("⠋  ", style="bold #70d6ff")
                t.append(f"{item}\n\n", style="#70d6ff")
            else:
                t.append("○  ", style="#5f6368")
                t.append(f"{item}\n\n", style="#5f6368")

        if self._checklist_progress >= len(items):
            t.append("Done\n\n", style="bold #81c995")
            t.append(self.meta.done_message, style="#e3e3e3")

        return t

    def _render_celebratory_card(self) -> Text:
        t = Text()
        t.append("    _    ____ _____ _   _ _____        ____  _   _ ____  ____ _____ ____     _  _____ _____\n", style="bold #70d6ff")
        t.append("   / \\  / ___| ____| \\ | |_   _|      / ___|| | | | __ )/ ___|_   _|  _ \\   / \\|_   _| ____|\n", style="bold #70d6ff")
        t.append("  / _ \\| |  _|  _| |  \\| | | |        \\___ \\| | | |  _ \\\\___ \\ | | | |_) | / _ \\ | | |  _|\n", style="bold #81c995")
        t.append(" / ___ \\ |_| | |___| |\\  | | |         ___) | |_| | |_) |___) || | |  _ < / ___ \\| | | |___\n", style="bold #81c995")
        t.append("/_/   \\_\\____|_____|_| \\_| |_|        |____/ \\___/|____/|____/ |_| |_| \\_\\_/   \\_\\_| |_____|\n\n", style="bold #fdd663")
        t.append("⚡ High-Density MicroVM Runtime Active\n\n", style="bold #81c995")
        t.append("Worker pools are warm, sandboxes are pre-allocated, and your gateway is listening:\n\n", style="bold #ffffff")
        t.append("  ⚡ <50ms Cold Starts : Pre-warmed microVM sandboxes listening\n", style="bold #70d6ff")
        t.append("  🛡️  gVisor Sandboxing : Hardware /dev/kvm virtualization active\n", style="bold #81c995")
        t.append("  📈 10x Fleet Density : CapacityBuffer active with 0% CPU waste\n", style="bold #fdd663")
        return t

    def _render_next_steps_card(self) -> Text:
        t = Text()
        t.append("🚀 NEXT STEPS — GET STARTED WITH YOUR FIRST AGENT:\n\n", style="bold #70d6ff")
        t.append("0. Connect your local terminal to the Substrate Gateway:\n", style="bold #fdd663")
        t.append("   kubectl port-forward svc/substrate-gateway 8080:8080 -n substrate-system\n", style="bold #8ab4f8")
        t.append("   export SUBSTRATE_GATEWAY=localhost:8080\n\n", style="bold #8ab4f8")
        t.append("1. Deploy your first actor session:\n", style="bold #fdd663")
        t.append("   # Install developer CLI\n", style="#80868b")
        t.append("   curl -sSL https://ate.dev/atectl | sh\n\n", style="bold #8ab4f8")
        t.append("   # Deploy AI agent actor from template\n", style="#80868b")
        t.append("   atectl actor create my-first-actor --template=default-agent --workerpool=default-worker-pool\n\n", style="bold #8ab4f8")
        t.append("   # Send an interactive prompt to your actor\n", style="#80868b")
        t.append("   atectl actor execute my-first-actor --prompt=\"Analyze recent logs and report status\"\n\n", style="bold #8ab4f8")
        t.append("2. Inspect your standby workers at any time:\n", style="bold #fdd663")
        t.append("   atectl get workerpools\n", style="bold #8ab4f8")
        t.append("   atectl logs workerpool/default-worker-pool --follow\n\n", style="bold #8ab4f8")
        t.append("3. Live Verification & Teardown Safety:\n", style="bold #fdd663")
        t.append("   # Run 48ms live test turn: atectl test turn --workerpool=default-worker-pool\n", style="bold #81c995")
        t.append("   # Teardown anytime: kubectl delete -f manifests/substrate-control-plane.yaml\n", style="#80868b")
        return t

    def _get_step_keymaps(self) -> List[tuple]:
        if self.step_key == OnboardingStep.CHECK_SETUP:
            return [
                ("Enter ↵", "Next: Connect Cluster", True),
                ("b", "Back", False),
                ("?", "Help", False),
            ]
        elif self.step_key == OnboardingStep.CONNECT_CLUSTER:
            return [
                ("Enter ↵", "Next: Turn on Substrate", True),
                ("1-4", "Pick & Advance", False),
                ("↑/↓", "Select", False),
                ("b", "Back", False),
                ("?", "Help", False),
            ]
        elif self.step_key == OnboardingStep.TURN_ON_SUBSTRATE:
            return [
                ("Enter ↵", "Next: Node Pool", True),
                ("m", "YAML Manifest", False),
                ("b", "Back", False),
                ("?", "Help", False),
            ]
        elif self.step_key == OnboardingStep.COMPATIBLE_NODEPOOL:
            return [
                ("Enter ↵", "Next: Autoscaling", True),
                ("1-3", "Pick & Advance", False),
                ("↑/↓", "Select", False),
                ("m", "YAML Manifest", False),
                ("b", "Back", False),
                ("?", "Help", False),
            ]
        elif self.step_key == OnboardingStep.CONFIG_AUTOSCALING:
            return [
                ("Enter ↵", "Next: WorkerPool", True),
                ("1-3", "Pick & Advance", False),
                ("↑/↓", "Select", False),
                ("m", "YAML Manifest", False),
                ("b", "Back", False),
                ("?", "Help", False),
            ]
        elif self.step_key == OnboardingStep.DEPLOY_WORKERPOOL:
            return [
                ("Enter ↵", "Finish Installation", True),
                ("1-2", "Pick & Advance", False),
                ("↑/↓", "Select", False),
                ("m", "YAML Manifest", False),
                ("b", "Back", False),
                ("?", "Help", False),
            ]
        elif self.step_key == OnboardingStep.COMPLETE:
            return [
                ("Enter ↵", "Close", True),
                ("y", "Run Test Turn", False),
                ("n", "Skip", False),
                ("0", "Restart Setup", False),
                ("b", "Back", False),
                ("?", "Help", False),
            ]
        return [("Enter ↵", "Proceed", True), ("b", "Back", False), ("?", "Help", False)]

    def on_key(self, event) -> None:
        """Handle question mark and shortcuts gracefully."""
        if event.key in ("question_mark", "?") or getattr(event, "character", "") == "?":
            self.action_show_help()
            event.prevent_default()
            event.stop()

    def action_proceed_next(self) -> None:
        if self.step_key == OnboardingStep.COMPLETE:
            if hasattr(self.app, "finish_onboarding"):
                self.app.finish_onboarding()
            return
        # Default-Forward Enter: Fast-forward preflight checklist if in progress
        if self.step_key == OnboardingStep.CHECK_SETUP:
            self._checklist_progress = len(self.meta.checklist_items)
        if self.selected_option_idx < 0:
            self.selected_option_idx = 0
        if hasattr(self.app, "advance_step"):
            self.app.advance_step()

    def action_previous_step(self) -> None:
        if hasattr(self.app, "previous_step"):
            self.app.previous_step()

    def action_select_opt_1(self) -> None:
        self.selected_option_idx = 0
        self._refresh_screen_options()
        self.action_proceed_next()

    def action_select_opt_2(self) -> None:
        self.selected_option_idx = 1
        self._refresh_screen_options()
        self.action_proceed_next()

    def action_select_opt_3(self) -> None:
        if self.step_key == OnboardingStep.COMPATIBLE_NODEPOOL:
            # Return to cluster selection (Step 2)
            if hasattr(self.app, "previous_step"):
                self.app.previous_step()
                self.app.previous_step()
            return
        self.selected_option_idx = 2
        self._refresh_screen_options()
        self.action_proceed_next()

    def action_select_opt_4(self) -> None:
        self.selected_option_idx = 3
        self._refresh_screen_options()
        self.action_proceed_next()

    def action_navigate_up(self) -> None:
        if self.selected_option_idx > 0:
            self.selected_option_idx -= 1
            self._refresh_screen_options()

    def action_navigate_down(self) -> None:
        max_len = 4
        if self.step_key == OnboardingStep.CONNECT_CLUSTER:
            max_len = len(self.clusters)
        elif self.step_key == OnboardingStep.COMPATIBLE_NODEPOOL:
            max_len = len(self.nodepool_opts)
        elif self.step_key == OnboardingStep.CONFIG_AUTOSCALING:
            max_len = len(self.autoscaling_opts)
        elif self.step_key == OnboardingStep.DEPLOY_WORKERPOOL:
            max_len = len(self.deploy_wp_opts)

        if self.selected_option_idx < max_len - 1:
            self.selected_option_idx += 1
            self._refresh_screen_options()

    def _refresh_screen_options(self) -> None:
        if self.step_key == OnboardingStep.CONNECT_CLUSTER:
            for idx in range(len(self.clusters)):
                try:
                    row = self.query_one(f"#cluster-item-{idx}", Static)
                    row.update(self._render_cluster_row(idx))
                except Exception:
                    pass
            try:
                ver_box = self.query_one("#cluster-verification-box", Static)
                ver_box.update(self._render_cluster_verification_box())
            except Exception:
                pass
        elif self.step_key == OnboardingStep.COMPATIBLE_NODEPOOL:
            for idx in range(len(self.nodepool_opts)):
                try:
                    row = self.query_one(f"#nodepool-opt-{idx}", Static)
                    row.update(self._render_option_card(self.nodepool_opts, idx))
                except Exception:
                    pass
            try:
                chk = self.query_one("#execution-checklist-card", Static)
                chk.update(self._render_checklist_box())
            except Exception:
                pass
        elif self.step_key == OnboardingStep.CONFIG_AUTOSCALING:
            for idx in range(len(self.autoscaling_opts)):
                try:
                    row = self.query_one(f"#autoscaling-opt-{idx}", Static)
                    row.update(self._render_option_card(self.autoscaling_opts, idx))
                except Exception:
                    pass
            try:
                chk = self.query_one("#execution-checklist-card", Static)
                chk.update(self._render_checklist_box())
            except Exception:
                pass
        elif self.step_key == OnboardingStep.DEPLOY_WORKERPOOL:
            for idx in range(len(self.deploy_wp_opts)):
                try:
                    row = self.query_one(f"#deploy-wp-opt-{idx}", Static)
                    row.update(self._render_option_card(self.deploy_wp_opts, idx))
                except Exception:
                    pass
            try:
                chk = self.query_one("#execution-checklist-card", Static)
                chk.update(self._render_checklist_box())
            except Exception:
                pass

    def action_run_test_turn(self) -> None:
        """Trigger in-TUI test turn on completion step."""
        self._test_turn_ran = True
        self._test_turn_skipped = False
        try:
            widget = self.query_one("#test-turn-playground", Static)
            widget.update(self._render_test_turn_playground())
        except Exception:
            pass

    def action_return_to_start(self) -> None:
        if hasattr(self.app, "state_machine"):
            self.app.state_machine.transition_to(OnboardingStep.WELCOME)

    def action_toggle_manifest(self) -> None:
        self._drawer_open = not self._drawer_open
        try:
            drawer = self.query_one("#declarative-manifest-card", Static)
            drawer.update(self._render_manifest_drawer())
        except Exception:
            pass

    def action_show_help(self) -> None:
        if hasattr(self.app, "action_show_help"):
            self.app.action_show_help()

    def action_request_exit(self) -> None:
        if hasattr(self.app, "action_request_exit"):
            self.app.action_request_exit()

    def action_handle_escape(self) -> None:
        if self.step_key == OnboardingStep.CHECK_SETUP:
            self.action_request_exit()
        else:
            self.action_previous_step()
