#!/bin/bash
set -e

# --- Configuration ---
export PROJECT_ID="<YOUR_GCP_PROJECT_ID>"
export PROJECT_NUMBER="142966328212"
export GCE_REGION="us-central1"
export CLUSTER_LOCATION="us-central1-c"
export CLUSTER_NAME="substrate-poc"
export CLUSTER_VERSION="1.35.5-gke.1057000"
export BUCKET_NAME="snapshot-substrate-${PROJECT_ID}"
export GVISOR_NODE_MACHINE_TYPE="c3-standard-4"
export NETWORK="default"
export SUBNETWORK="default"
export NODE_POOL_NAME="gvisor-pool"
export NODE_POOL_VERSION="1.35.5-gke.1057000"

SUBSTRATE_REPO="${HOME}/repo/substrate"
OPENCLAW_REPO="$(pwd)"
IMAGE_TAG="gcr.io/${PROJECT_ID}/substrate-actor:latest"
export KO_DOCKER_REPO="gcr.io/${PROJECT_ID}/ate-images"

echo "🚀 Starting OpenClaw-on-Substrate PoC Deployment"
echo "Project: ${PROJECT_ID} (${PROJECT_NUMBER})"
echo "Cluster: ${CLUSTER_NAME} in ${CLUSTER_LOCATION}"

# 1. Locate Tools & Setup PATH
GCLOUD_PAR="/google/bin/releases/cloud-sdk-build/gcloud.par"
mkdir -p bin

# Create a "Smart" Credential Helper that uses the ADC token
cat <<EOF > bin/docker-credential-gcloud
#!/bin/bash
# Mock GCR credential helper using existing ADC token
if [ "\$1" = "get" ]; then
    TOKEN=\$("$GCLOUD_PAR" auth application-default print-access-token 2>/dev/null)
    if [ -n "\$TOKEN" ]; then
        echo "{\"Secret\": \"\$TOKEN\", \"Username\": \"oauth2accesstoken\"}"
        exit 0
    fi
fi
# Fallback to standard gcloud if ADC fails
exec "$GCLOUD_PAR" auth docker-helper "\$@"
EOF
chmod +x bin/docker-credential-gcloud

if [ -f "$GCLOUD_PAR" ]; then
    ln -sf "$GCLOUD_PAR" bin/gcloud
fi
export PATH="$(pwd)/bin:${PATH}"
GCLOUD=$(which gcloud || echo "gcloud")
echo "🔍 Using gcloud: $GCLOUD"

# Load NVM if present
if [ -s "$HOME/.nvm/nvm.sh" ]; then
    echo "📦 Loading NVM..."
    export NVM_DIR="$HOME/.nvm"
    [ -s "$NVM_DIR/nvm.sh" ] && \. "$NVM_DIR/nvm.sh"
fi

# Use local kubectl if available
if [ -f "./scripts/kubectl" ]; then
    ln -sf "$(pwd)/scripts/kubectl" bin/kubectl
fi
KUBECTL=$(which kubectl || echo "kubectl")

# Detect Docker command (use sudo if needed)
DOCKER="docker"
if ! docker ps &>/dev/null; then
    if sudo docker ps &>/dev/null; then
        echo "⚠️ Using 'sudo docker' for container operations."
        DOCKER="sudo docker"
    fi
fi

# 2. Build Substrate Tooling
echo "🛠️ Building Substrate CLI (kubectl-ate)..."
cd "${SUBSTRATE_REPO}"
make build-atectl
export PATH="${SUBSTRATE_REPO}/bin:${PATH}"
cd "${OPENCLAW_REPO}"

# 3. Pre-flight dependency checks
echo "🔍 Checking dependencies (docker, pnpm)..."
MISSING_DEPS=0
if ! $DOCKER ps &>/dev/null; then
    echo "❌ Error: 'docker' not working (even with sudo)."
    MISSING_DEPS=1
fi
if ! command -v pnpm &>/dev/null; then
    if [ -f "$HOME/.nvm/versions/node/$(nvm current)/bin/pnpm" ]; then
        export PATH="$HOME/.nvm/versions/node/$(nvm current)/bin:$PATH"
    else
        echo "❌ Error: 'pnpm' not found in PATH."
        MISSING_DEPS=1
    fi
fi
if [ $MISSING_DEPS -eq 1 ]; then
    exit 1
fi

# 4. Authenticate & Configure
echo "🔐 Checking GCP Authentication..."
"$GCLOUD" auth application-default set-quota-project "${PROJECT_ID}" --quiet || true

if ! "$GCLOUD" auth application-default print-access-token &>/dev/null; then
    echo "⚠️ Application Default Credentials (ADC) missing."
    echo "Please run: $GCLOUD auth application-default login"
    exit 1
fi

echo "🎯 Setting project to ${PROJECT_ID}..."
"$GCLOUD" config set project "${PROJECT_ID}" --quiet

echo "🐳 Configuring Docker authentication..."
"$GCLOUD" auth configure-docker --quiet

# 5. Provision GCP Resources
echo "🏗️ Provisioning GCP Resources (GKE, GCS, IAM)..."
cd "${SUBSTRATE_REPO}"
go run ./tools/setup-gcp --all
cd "${OPENCLAW_REPO}"

# 6. Connect to Cluster
echo "☸️ Connecting to GKE Cluster..."
export KUBECONFIG="${HOME}/.kube/config"
if ! "$GCLOUD" container clusters get-credentials "${CLUSTER_NAME}" --zone "${CLUSTER_LOCATION}" 2>/dev/null; then
    echo "⚠️ Standard 'get-credentials' failed (likely CAA policy)."
    echo "🔄 Attempting manual kubeconfig setup using ADC token..."
    bash scripts/setup-kubeconfig.sh
else
    echo "✅ Connected via standard gcloud credentials."
fi

# 7. Install Substrate System
echo "📦 Installing Agent Substrate system to GKE..."
cd "${SUBSTRATE_REPO}"
# To avoid the 'ko' command failing with --context, we unset KUBECTL_CONTEXT.
# We also unset PROJECT_ID temporarily to prevent the script from calling 'gcloud'.
# Our KUBECONFIG already has the correct default context.
(
  export KUBECTL_CONTEXT=""
  export PROJECT_ID=""
  ./hack/install-ate.sh --deploy-ate-system
)
cd "${OPENCLAW_REPO}"

# 8. Build and Push OpenClaw Actor Image
echo "🐳 Building OpenClaw Actor Image..."
# Ensure the Substrate CLI is in the build context
if [ -d "substrate" ]; then
  cp "${SUBSTRATE_REPO}/bin/kubectl-ate" demos/openclaw/kubectl-ate
  $DOCKER build -t "${IMAGE_TAG}" -f demos/openclaw/Dockerfile demos/openclaw/
else
  cp "${SUBSTRATE_REPO}/bin/kubectl-ate" ./kubectl-ate
  $DOCKER build -t "${IMAGE_TAG}" -f Dockerfile .
fi
echo "⬆️ Pushing image to GCR..."
$DOCKER push "${IMAGE_TAG}"

# 9. Deploy OpenClaw Actor Template
echo "🎭 Deploying OpenClaw ActorTemplate..."
export KUBECONFIG="${HOME}/.kube/config"
if [ -d "demos/openclaw/manifests" ]; then
  "${SUBSTRATE_REPO}/bin/kubectl-ate" apply -f demos/openclaw/manifests/actor-template.yaml
else
  "${SUBSTRATE_REPO}/bin/kubectl-ate" apply -f manifests/actor-template.yaml
fi

echo "✅ Deployment Complete!"
echo "You can now create actors using:"
echo "  ${SUBSTRATE_REPO}/bin/kubectl-ate create actor my-agent --template openclaw/openclaw-agent"
echo ""
echo "To run the density test:"
if [ -f "demos/openclaw/scripts/substrate-density-test.ts" ]; then
  echo "  ACTOR_COUNT=100 pnpm tsgo demos/openclaw/scripts/substrate-density-test.ts"
else
  echo "  ACTOR_COUNT=100 pnpm tsgo scripts/substrate-density-test.ts"
fi
