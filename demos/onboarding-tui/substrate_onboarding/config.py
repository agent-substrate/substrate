"""Configuration schemas, options, and state models for Agent Substrate Onboarding.

Features:
1. Splash Title: "Agent Substrate" with Google 4-color gradient.
2. Two Installation Choices: Quickstart and Advanced.
3. Step 2: Connect Your Cluster:
   - Includes Region information (e.g. us-central1, us-east-1, eastus, local).
   - Side-by-side Cluster Selection & Substrate Probe Verification.
   - Conditional GKE Private GA Agreement: Acknowledgment is displayed only when a GKE cluster is selected, and omitted for non-GKE clusters.
4. Post-Installation WorkerPool Configuration:
   - Step 4: Compatible Node Pool setup (CCC / Nested-Virt with YAML re-apply note)
   - Step 5: WorkerPool Autoscaling (OneHPA min=10 max=100 & CapacityBuffer=3 standby with YAML note)
   - Step 6: Confirm & Deploy Substrate WorkerPool (Yes vs. No, skip)
5. Step 7: Installation Complete & Next Steps:
   - Celebratory animation confirming Agent Substrate on GKE is complete & ready for agent workloads.
   - Actionable next steps: Deploy first actor session & Inspect standby workers.
"""

from __future__ import annotations
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional


class OnboardingStep(str, Enum):
    WELCOME = "welcome"                          # Step 0: Welcome & Setup Track Selection
    CHECK_SETUP = "check_setup"                  # Step 1: Check your environment
    CONNECT_CLUSTER = "connect_cluster"          # Step 2: Select cluster, Region, & Probe (with GKE GA agreement)
    TURN_ON_SUBSTRATE = "turn_on_sub"            # Step 3: Turn on Substrate Control Plane
    COMPATIBLE_NODEPOOL = "compatible_nodepool"  # Step 4: Compatible Node Pool (CCC / Nested-Virt)
    CONFIG_AUTOSCALING = "config_autoscaling"    # Step 5: WorkerPool Autoscaling (HPA & CapacityBuffer)
    DEPLOY_WORKERPOOL = "deploy_workerpool"      # Step 6: Confirm & Deploy Substrate WorkerPool
    COMPLETE = "complete"                        # Step 7: Installation Complete & Next Steps

    # Backward compatibility aliases
    PRIVATE_GA_AGREEMENT = "connect_cluster"
    CLUSTER = "connect_cluster"
    CREATE_CLUSTER = "connect_cluster"
    CONTROL_PLANE = "turn_on_sub"
    NODE_POOL = "compatible_nodepool"
    AUTOSCALING = "config_autoscaling"
    DEPLOY_WP = "deploy_workerpool"
    LAUNCHPAD = "complete"
    DOCTOR = "check_setup"
    QUESTIONNAIRE = "connect_cluster"
    AUTH = "connect_cluster"
    SUMMARY = "complete"
    INSTALL_CLI = "complete"
    FIRST_ACTOR = "complete"
    SEND_REQUEST = "complete"
    PAUSE_RESUME = "complete"
    SCALE_UP = "complete"


@dataclass
class OptionItem:
    id: str
    title: str
    description: str
    icon: str = "⚡"
    tip: str = ""
    shortcut_key: str = "1"
    provider: str = "Kubernetes"
    region: str = "us-central1"
    version: str = "v1.35"
    nodes: int = 12
    is_gke: bool = False
    control_plane_status: str = "Not Installed"


# Setup Tracks for Welcome Screen
SETUP_TRACKS: List[OptionItem] = [
    OptionItem(
        id="track_quickstart",
        title="Quickstart — Automatic cluster detection & default configuration (Recommended)",
        description="Automatically connects to your pre-configured cluster and applies sensible defaults in seconds.",
        icon="🚀",
        tip="Press [1] or [Enter] for 1-click automatic bootstrap.",
        shortcut_key="1",
    ),
    OptionItem(
        id="track_advanced",
        title="Advanced — Custom installation with kubectl",
        description="Customize YAML manifests, resource quotas, microVM isolation drivers, and eBPF routing rules.",
        icon="⚙️",
        tip="Press [2] for tailored manifest configuration.",
        shortcut_key="2",
    ),
]

# Available Clusters from Kubeconfig (with Region Details)
AVAILABLE_CLUSTERS: List[OptionItem] = [
    OptionItem(
        id="cluster_gke_prod",
        title="gke_enterprise_us-central1_prod",
        description="GKE Standard (v1.35+) • Region: us-central1 (Iowa) • 12 nodes • KVM Ready",
        icon="🌐",
        tip="Active context. Verified ready for Substrate installation.",
        shortcut_key="1",
        provider="Google Kubernetes Engine (GKE)",
        region="us-central1 (Iowa)",
        version="v1.35.1-gke.1520000",
        nodes=12,
        is_gke=True,
        control_plane_status="Not Installed (Clean cluster ready for Substrate)",
    ),
    OptionItem(
        id="cluster_aws_eks",
        title="aws-eks-production-us-east-1",
        description="AWS EKS (v1.35+) • Region: us-east-1 (N. Virginia) • 8 nodes • Nitro Enclaves",
        icon="☁️",
        tip="Multi-cloud enterprise cluster.",
        shortcut_key="2",
        provider="Amazon Elastic Kubernetes Service (EKS)",
        region="us-east-1 (N. Virginia)",
        version="v1.35.0-eks",
        nodes=8,
        is_gke=False,
        control_plane_status="Not Installed (Clean cluster ready for Substrate)",
    ),
    OptionItem(
        id="cluster_azure_aks",
        title="azure-aks-agent-fleet-eastus",
        description="Azure AKS (v1.35+) • Region: eastus (Virginia) • 6 nodes • Hyper-V Isolated",
        icon="🔷",
        tip="Enterprise Azure cluster.",
        shortcut_key="3",
        provider="Azure Kubernetes Service (AKS)",
        region="eastus (Virginia)",
        version="v1.35.0-aks",
        nodes=6,
        is_gke=False,
        control_plane_status="Not Installed (Clean cluster ready for Substrate)",
    ),
    OptionItem(
        id="cluster_local_kind",
        title="kind-substrate-sandbox",
        description="Local Kind Sandbox (v1.35+) • Region: local (localhost) • 3 nodes",
        icon="🧪",
        tip="Local development sandbox.",
        shortcut_key="4",
        provider="Kind (Local Kubernetes)",
        region="local (localhost)",
        version="v1.35.0",
        nodes=3,
        is_gke=False,
        control_plane_status="Not Installed (Clean cluster ready for Substrate)",
    ),
]

# Step 4: Compatible Node Pool Options (Scanning & Nested-Virt)
NODEPOOL_OPTIONS: List[OptionItem] = [
    OptionItem(
        id="ccc_auto",
        title="Automatically create a compatible node pool using Custom Compute Class (Recommended)",
        description="Applies Custom Compute Class manifest with n2-standard-48, Spot fallback, and nested virtualization enabled.",
        icon="⚡",
        tip="Applies manifests/workerpool-ccc.yaml. Modifiable anytime.",
        shortcut_key="1",
    ),
    OptionItem(
        id="ccc_manual_gcloud",
        title="Create a compatible node pool manually via gcloud",
        description="Generates gcloud container node-pools create command with --enable-nested-virtualization.",
        icon="🛠️",
        tip="For custom enterprise security policies.",
        shortcut_key="2",
    ),
    OptionItem(
        id="ccc_different_cluster",
        title="Choose a different cluster",
        description="Return to Step 2 to select another cluster context from your kubeconfig.",
        icon="🔄",
        tip="Switch cluster context.",
        shortcut_key="3",
    ),
]

# Step 5: WorkerPool Autoscaling Options (HPA & CapacityBuffer)
AUTOSCALING_OPTIONS: List[OptionItem] = [
    OptionItem(
        id="auto_hpa_buffer",
        title="Automatically configure HPA & CapacityBuffer with sensible defaults (Recommended)",
        description="Applies OneHPA (min=10, max=100) and fixed-replica-buffer (3 standby replicas) for instant <100ms agent session injection.",
        icon="⚡",
        tip="Applies manifests/workerpool-autoscaling.yaml. Modifiable anytime.",
        shortcut_key="1",
    ),
    OptionItem(
        id="manual_hpa",
        title="Configure autoscaling manually via kubectl",
        description="Export template manifests to customize scaling metrics, CPU/memory thresholds, and buffer headroom.",
        icon="🛠️",
        tip="Custom metrics & thresholds.",
        shortcut_key="2",
    ),
    OptionItem(
        id="skip_autoscaling",
        title="Skip autoscaling configuration",
        description="Keep fixed worker pool replica count without horizontal dynamic scaling.",
        icon="⏭️",
        tip="Fixed worker count.",
        shortcut_key="3",
    ),
]

# Step 6: Confirm & Deploy Substrate WorkerPool
DEPLOY_WP_OPTIONS: List[OptionItem] = [
    OptionItem(
        id="deploy_yes_default",
        title="Yes, deploy default Substrate WorkerPool [default-worker-pool] (Recommended)",
        description="Bootstraps 10 warm worker sandboxes with microVM isolation and instant actor attachment in [substrate-system].",
        icon="🚀",
        tip="Instant warm agent capacity.",
        shortcut_key="1",
    ),
    OptionItem(
        id="deploy_skip",
        title="No, skip default WorkerPool deployment",
        description="Skip initial worker pool provisioning. You can create custom worker pools at any time via kubectl or atectl.",
        icon="⏭️",
        tip="Skip default pool creation.",
        shortcut_key="2",
    ),
]

CLUSTER_OPTIONS = AVAILABLE_CLUSTERS
TRACK_OPTIONS = SETUP_TRACKS
DATAPLANE_OPTIONS = NODEPOOL_OPTIONS
SANDBOX_OPTIONS = AUTOSCALING_OPTIONS
EDITOR_OPTIONS = DEPLOY_WP_OPTIONS


@dataclass
class StepMetadata:
    step_num: int
    title: str
    heading: str
    description: str
    real_command: str
    checklist_title: str
    checklist_items: List[str]
    done_message: str
    next_action_label: str
    benchmark_text: Optional[str] = None
    is_cluster_step: bool = False
    is_option_step: bool = False
    yaml_notice: Optional[str] = None


STEP_CONFIGS: Dict[OnboardingStep, StepMetadata] = {
    OnboardingStep.WELCOME: StepMetadata(
        step_num=0,
        title="Welcome",
        heading="Agent Substrate Onboarding",
        description="High-density sandboxing and sub-100ms runtime for autonomous AI agents on pre-existing Kubernetes clusters.",
        real_command="atectl onboard",
        checklist_title="System Readiness Check",
        checklist_items=[
            "Pre-configured Kubernetes Cluster: Required & Portable",
            "Hardware Virtualization: KVM / microVM Compatible",
            "Substrate Control Plane: Ready for install",
        ],
        done_message="Ready to begin setup! Press [Enter] to start.",
        next_action_label="Get Started [Enter ↵] →",
    ),
    OnboardingStep.CHECK_SETUP: StepMetadata(
        step_num=1,
        title="Check your setup",
        heading="Check your environment",
        description="We'll check if you have everything needed to run Substrate — a container runtime, Python, and kubectl CLI.",
        real_command="which docker && which kubectl && which python3",
        checklist_title="Checking prerequisites...",
        checklist_items=[
            "Container runtime detected (Docker / Podman / Containerd)",
            "Python 3.10+ runtime available",
            "Kubectl command utility ready in PATH",
        ],
        done_message="Prerequisites verified. Let's select your target cluster next.",
        next_action_label="Connect your cluster [Enter ↵] →",
    ),
    OnboardingStep.CONNECT_CLUSTER: StepMetadata(
        step_num=2,
        title="Connect your cluster",
        heading="Select Cluster & Verify Substrate Control Plane",
        description="Choose a cluster from your active kubeconfig. We'll verify its provider type, region, version, and probe for existing Substrate components in real-time.",
        real_command="kubectl config get-contexts && kubectl get ns substrate-system",
        checklist_title="Verifying selected cluster & control plane...",
        checklist_items=[
            "Cluster API Reachability: Connected to active context",
            "Cluster Provider & Region: Verified in active kubeconfig",
            "Node Fleet Capacity: Ready nodes verified with hardware nested-virt support",
            "Control Plane Status: Checked [substrate-system] — Clean cluster ready for install",
        ],
        done_message="Cluster verified & ready! Next, let's turn on Substrate.",
        next_action_label="Turn on Substrate [Enter ↵] →",
        is_cluster_step=True,
    ),
    OnboardingStep.TURN_ON_SUBSTRATE: StepMetadata(
        step_num=3,
        title="Turn on Substrate",
        heading="Turn on Substrate Control Plane",
        description="Installing the Substrate core controllers, state registry, and high-speed networking onto your cluster in namespace [substrate-system].",
        real_command="kubectl apply -f manifests/substrate-control-plane.yaml",
        checklist_title="Installing Substrate components...",
        checklist_items=[
            "Applying CustomResourceDefinitions (WorkerPool, ActorTemplate, Actor)",
            "Deploying Valkey Metadata & State Registry",
            "Bootstrapping Substrate Gateway & API Server (listening on :8080)",
            "Initializing eBPF network routing controller in [substrate-system]",
        ],
        done_message="Substrate control plane is active! Next, let's configure the worker pool node fleet.",
        next_action_label="Set up WorkerPool [Enter ↵] →",
    ),
    OnboardingStep.COMPATIBLE_NODEPOOL: StepMetadata(
        step_num=4,
        title="Compatible Node Pool",
        heading="Set up Compatible WorkerPool Node Fleet",
        description="Scanning cluster node pools for hardware nested virtualization (KVM/microVM). If no compatible pool is found, configure one via Custom Compute Class (CCC).",
        real_command="kubectl apply -f manifests/workerpool-ccc.yaml",
        checklist_title="Configuring compatible node pool...",
        checklist_items=[
            "Scanning existing node pools: No hardware nested-virt pool detected",
            "Applying Custom Compute Class manifest [agent-spot-ccc] (n2-standard-48, KVM enabled)",
            "Configuring Spot fallback & capacity reservation",
            "Compatible node pool ready for high-density agent sandboxing",
        ],
        done_message="Compatible node pool configured! You can modify & re-apply manifests/workerpool-ccc.yaml anytime.",
        next_action_label="Configure Autoscaling [Enter ↵] →",
        is_option_step=True,
        yaml_notice="💡 Tip: You can modify and re-apply the Custom Compute Class YAML manifest later at any time (e.g. manifests/workerpool-ccc.yaml).",
    ),
    OnboardingStep.CONFIG_AUTOSCALING: StepMetadata(
        step_num=5,
        title="Configure Autoscaling",
        heading="Configure WorkerPool Autoscaling (HPA & CapacityBuffer)",
        description="Configure horizontal pod autoscaling and standby capacity buffers so your agent fleet can absorb sudden traffic surges with instant (<100ms) cold starts.",
        real_command="kubectl apply -f manifests/workerpool-autoscaling.yaml",
        checklist_title="Applying autoscaling & capacity buffer...",
        checklist_items=[
            "Applying HorizontalPodAutoscaler (OneHPA: minReplicas=10, maxReplicas=100)",
            "Applying CapacityBuffer (fixed-replica-buffer: 3 standby replicas via buffer.gke.io/standby-capacity)",
            "Standby buffer verified: Ready for instant (<100ms) agent session injection",
        ],
        done_message="Autoscaling active! You can modify & re-apply manifests/workerpool-autoscaling.yaml anytime.",
        next_action_label="Deploy WorkerPool [Enter ↵] →",
        is_option_step=True,
        yaml_notice="💡 Tip: You can modify and re-apply the HPA and CapacityBuffer YAML manifests later at any time (e.g. manifests/workerpool-autoscaling.yaml).",
    ),
    OnboardingStep.DEPLOY_WORKERPOOL: StepMetadata(
        step_num=6,
        title="Deploy WorkerPool",
        heading="Confirm & Deploy Substrate WorkerPool",
        description="Deploy the default Substrate WorkerPool into namespace [substrate-system] with pre-warmed agent sandboxes and microVM isolation.",
        real_command="kubectl apply -f manifests/default-workerpool.yaml",
        checklist_title="Deploying default Substrate WorkerPool...",
        checklist_items=[
            "Resolving worker sandbox image (gcr.io/ate-platform/worker:v1)",
            "Deploying WorkerPool CR [default-worker-pool] in namespace [substrate-system]",
            "Provisioning 10 warm worker sandboxes (3 standby buffer replicas active)",
            "WorkerPool is ready: 10/10 warm pods listening for agent execution turns",
        ],
        done_message="WorkerPool configured! Proceeding to installation summary.",
        next_action_label="Complete Installation [Enter ↵] →",
        is_option_step=True,
    ),
    OnboardingStep.COMPLETE: StepMetadata(
        step_num=7,
        title="Installation Complete",
        heading="Installation Complete 🎉",
        description="Your cluster is fully configured and ready to host low-latency AI agent workloads.",
        real_command="atectl get workerpools",
        checklist_title="Cluster Readiness Status",
        checklist_items=[
            "Substrate Control Plane: Healthy (Gateway :8080 active)",
            "Worker Fleet: 10 warm microVM sandboxes listening",
            "Standby CapacityBuffer: 3 standby pods pre-warmed (<100ms cold start)",
            "GKE Cluster: Ready for high-density AI agent turns",
        ],
        done_message="All systems go! Follow the next steps below to deploy your first agent.",
        next_action_label="🚀 Finish & Close [Enter ↵]",
    ),
}


@dataclass
class CheckResult:
    name: str
    category: str = "System"
    status: str = "pending"
    message: str = ""
    details: Optional[str] = None
    fix_command: Optional[str] = None
    doc_url: Optional[str] = "https://ate.dev/docs/prereqs"
    plain_description: Optional[str] = None
    duration_ms: int = 0
    is_fatal: bool = False
    is_critical: bool = False


@dataclass
class UserSetupState:
    current_step: OnboardingStep = OnboardingStep.WELCOME
    selected_track: str = "quickstart"
    selected_cluster_id: str = "cluster_gke_prod"
    cluster_region: str = "us-central1 (Iowa)"
    cluster_provider: str = "Google Kubernetes Engine (GKE)"
    is_gke_cluster: bool = True
    gke_agreement_accepted: bool = True
    gke_token: str = "ga-sub-8f92a-live-contract"
    selected_nodepool_mode: str = "ccc_auto"
    selected_autoscaling_mode: str = "auto_hpa_buffer"
    deploy_default_workerpool: bool = True
    installed_crds: bool = False
    installed_control_plane: bool = False
    installed_cli: bool = False
    first_actor_deployed: bool = False
    is_complete: bool = False
