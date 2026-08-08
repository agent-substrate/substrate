#!/usr/bin/env bash
# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Brings up the dedicated cluster the atenet-router capacity run measures on,
# installs substrate, and pins every component to the node pool it belongs on.
# Idempotent: re-running repairs a half-provisioned cluster.
#
# Confirming the zone actually has capacity for the SUT machine type is an
# operator step taken beforehand — see the README. A stockout surfaces as the
# raw gcloud error: no probe, no retry, no silent fallback to a smaller node.

# shellcheck source=benchmarking/routercap/common.sh
source "$(git rev-parse --show-toplevel)/benchmarking/routercap/common.sh"

RC_WORKER_PODS="${ROUTERCAP_WORKER_PODS:-100}"
SKIP_INSTALL=false

usage() {
  cat <<EOF
Usage: $0 [options]

  --worker-pods N   Worker pod replicas (default: ${RC_WORKER_PODS}). One actor
                    per pod, so this is also the actor ceiling.
  --skip-install    Create/repair the cluster and pools, skip installing
                    substrate and the workloads.
  -h, --help        This.

Environment (all optional, all recorded in the run header):
  ROUTERCAP_CLUSTER              default ${RC_CLUSTER}
  ROUTERCAP_CLUSTER_LOCATION     default ${RC_LOCATION}
  ROUTERCAP_MACHINE_TYPE         default ${RC_MACHINE_TYPE}   (router, workers, loadgen)
  ROUTERCAP_SYSTEM_MACHINE_TYPE  default ${RC_SYSTEM_MACHINE_TYPE}
  ROUTERCAP_WORKER_NODES         default ${RC_WORKER_NODES}
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --worker-pods) shift; RC_WORKER_PODS="$1" ;;
    --worker-pods=*) RC_WORKER_PODS="${1#*=}" ;;
    --skip-install) SKIP_INSTALL=true ;;
    -h|--help) usage; exit 0 ;;
    *) rc::die "unknown option: $1" ;;
  esac
  shift
done

rc::need gcloud kubectl git go
rc::env

# Can the nodes actually pull substrate's images? Checked before anything is
# created, because the failure mode is every pod in ImagePullBackOff after the
# cluster is already paid for.
#
# tools/setup-gcp grants this; hack/teardown.sh revokes it. Neither is called
# from here, because IAM is shared with every other cluster in the project.
#
# Either scope counts: some projects' IAM automation silently strips broad
# project-level grants about a minute after they land, while a binding on the
# Artifact Registry repository itself survives. See RESULTS.md.
: "${PROJECT_NUMBER:=$(gcloud projects describe "${PROJECT_ID}" --format='value(projectNumber)')}"
node_sa="serviceAccount:${PROJECT_NUMBER}-compute@developer.gserviceaccount.com"

# Repo coordinates from KO_DOCKER_REPO's host: gcr.io and its regional aliases
# are Artifact Registry repositories named after the host, in the location the
# host prefix names; anything else is assumed to be <loc>-docker.pkg.dev/<proj>/<repo>.
ar_host="${KO_DOCKER_REPO%%/*}"
case "${ar_host}" in
  gcr.io)      ar_repo="gcr.io";      ar_loc="us" ;;
  us.gcr.io)   ar_repo="us.gcr.io";   ar_loc="us" ;;
  eu.gcr.io)   ar_repo="eu.gcr.io";   ar_loc="europe" ;;
  asia.gcr.io) ar_repo="asia.gcr.io"; ar_loc="asia" ;;
  *)           ar_repo="$(echo "${KO_DOCKER_REPO}" | cut -d/ -f3)"; ar_loc="${ar_host%%-docker.pkg.dev}" ;;
esac

has_pull_access() {
  local roles
  roles="$(gcloud projects get-iam-policy "${PROJECT_ID}" \
    --flatten='bindings[].members' --filter="bindings.members:${node_sa}" \
    --format='value(bindings.role)' 2>/dev/null || true)"
  case "${roles}" in *artifactregistry.reader*) return 0 ;; esac
  roles="$(gcloud artifacts repositories get-iam-policy "${ar_repo}" \
    --project="${PROJECT_ID}" --location="${ar_loc}" \
    --flatten='bindings[].members' --filter="bindings.members:${node_sa}" \
    --format='value(bindings.role)' 2>/dev/null || true)"
  case "${roles}" in *artifactregistry.reader*) return 0 ;; esac
  return 1
}

if ! has_pull_access; then
  rc::die "the GKE node service account (${PROJECT_NUMBER}-compute@developer.gserviceaccount.com) has no roles/artifactregistry.reader on ${PROJECT_ID} or on the ${ar_repo} repository, so nodes cannot pull substrate images and every pod would land in ImagePullBackOff. Grant it with either:

  go run ./tools/setup-gcp create iam --gke-nodes --atelet --bucket-bindings --bucket \"\${BUCKET_NAME}\"

or, if this project's IAM automation strips project-level bindings, the narrower repository-scoped form:

  gcloud artifacts repositories add-iam-policy-binding ${ar_repo} --location=${ar_loc} \\
    --member=${node_sa} --role=roles/artifactregistry.reader

This script reports rather than grants: IAM here is shared with every cluster in the project."
fi

# GKE caps a node at 110 pods and DaemonSets count toward that cap; asserted
# here rather than discovered with the pool already paid for. 10 is a
# generous allowance for GKE's own DaemonSets plus atelet.
per_node=$(( (RC_WORKER_PODS + RC_WORKER_NODES - 1) / RC_WORKER_NODES ))
if (( per_node + 10 > 110 )); then
  rc::die "${RC_WORKER_PODS} worker pods over ${RC_WORKER_NODES} nodes is ${per_node}/node, past GKE's 110-per-node cap once DaemonSets are counted; raise ROUTERCAP_WORKER_NODES"
fi

# --- cluster -----------------------------------------------------------------

cluster_exists() {
  gcloud container clusters describe "${RC_CLUSTER}" \
    --location="${RC_LOCATION}" --project="${PROJECT_ID}" >/dev/null 2>&1
}

# substrate needs certificates.k8s.io/v1beta1 for PodCertificateRequest and
# ClusterTrustBundle, which is 1.36+, and the zone's default can be behind
# that. The version is resolved rather than defaulted: newest valid release
# at or above the floor.
RC_MIN_MINOR=36
resolve_cluster_version() {
  if [[ -n "${ROUTERCAP_CLUSTER_VERSION:-}" ]]; then
    echo "${ROUTERCAP_CLUSTER_VERSION}"
    return
  fi
  # validMasterVersions comes back newest-first, so the first one clearing the
  # floor is the newest that does. Compared numerically rather than by regex:
  # a pattern over version strings is how you end up rejecting 1.40.
  gcloud container get-server-config \
    --location="${RC_LOCATION}" --project="${PROJECT_ID}" \
    --format="value(validMasterVersions)" 2>/dev/null \
    | tr ';' '\n' \
    | awk -F. -v floor="${RC_MIN_MINOR}" '$1 == 1 && $2 >= floor { print; exit }'
}

if cluster_exists; then
  rc::step "cluster ${RC_CLUSTER} already exists"
else
  version="$(resolve_cluster_version)"
  if [[ -z "${version}" ]]; then
    rc::die "no GKE version >= 1.${RC_MIN_MINOR} offered in ${RC_LOCATION}; substrate needs certificates.k8s.io/v1beta1. Pin one with ROUTERCAP_CLUSTER_VERSION if you know better."
  fi
  rc::step "creating cluster ${RC_CLUSTER} in ${RC_LOCATION} on ${version}"
  # The default pool stays small and untainted: GKE's own addons have to land
  # somewhere.
  gcloud container clusters create "${RC_CLUSTER}" \
    --project="${PROJECT_ID}" \
    --location="${RC_LOCATION}" \
    --cluster-version="${version}" \
    --num-nodes=1 \
    --machine-type=e2-standard-8 \
    --workload-pool="${PROJECT_ID}.svc.id.goog" \
    --enable-kubernetes-unstable-apis=certificates.k8s.io/v1beta1/podcertificaterequests,certificates.k8s.io/v1beta1/clustertrustbundles
fi

# Pools are created router-first: it is the one pool that genuinely needs the
# large machine type, so a zone shortage surfaces before the other three have
# been paid for.

# gcloud reports taint effects in the API's enum spelling, not the one you
# create them with, so a comparison has to translate.
taint_enum() {
  case "$1" in
    NoSchedule)       echo NO_SCHEDULE ;;
    PreferNoSchedule) echo PREFER_NO_SCHEDULE ;;
    NoExecute)        echo NO_EXECUTE ;;
    *)                rc::die "unknown taint effect: $1" ;;
  esac
}

ensure_pool() {
  local name="$1" machine="$2" nodes="$3" role="$4" effect="$5"
  if gcloud container node-pools describe "${name}" \
      --cluster="${RC_CLUSTER}" --location="${RC_LOCATION}" \
      --project="${PROJECT_ID}" >/dev/null 2>&1; then
    # Reconcile the taint rather than just reporting the pool present: a pool
    # created by an older revision of this script carries that revision's
    # taint, and the system pool's effect is the difference between an install
    # that converges and one that deadlocks.
    local have want
    have="$(gcloud container node-pools describe "${name}" \
      --cluster="${RC_CLUSTER}" --location="${RC_LOCATION}" --project="${PROJECT_ID}" \
      --format="value(config.taints[0].effect)" 2>/dev/null || true)"
    want="$(taint_enum "${effect}")"
    if [[ "${have}" != "${want}" ]]; then
      rc::step "node pool ${name} exists with taint effect ${have:-<none>}; updating to ${want}"
      # --quiet because the update prompts to confirm replacing the pool's
      # taints, and a provision run has to be unattended. Safe: the taints
      # being replaced are the ones this same function wrote.
      gcloud container node-pools update "${name}" --quiet \
        --cluster="${RC_CLUSTER}" --location="${RC_LOCATION}" --project="${PROJECT_ID}" \
        --node-taints="${RC_ROLE_KEY}=${role}:${effect}"
    else
      rc::step "node pool ${name} already exists"
    fi
    return
  fi
  rc::step "creating node pool ${name} (${nodes} x ${machine})"
  gcloud container node-pools create "${name}" \
    --cluster="${RC_CLUSTER}" \
    --location="${RC_LOCATION}" \
    --project="${PROJECT_ID}" \
    --machine-type="${machine}" \
    --num-nodes="${nodes}" \
    --node-labels="${RC_ROLE_KEY}=${role}" \
    --node-taints="${RC_ROLE_KEY}=${role}:${effect}"
}

ensure_pool "${RC_POOL_ROUTER}"  "${RC_MACHINE_TYPE}"        1                    "${RC_POOL_ROUTER}"  "${RC_TAINT_HARD}"
ensure_pool "${RC_POOL_SYSTEM}"  "${RC_SYSTEM_MACHINE_TYPE}" 1                    "${RC_POOL_SYSTEM}"  "${RC_TAINT_SYSTEM}"
ensure_pool "${RC_POOL_WORKERS}" "${RC_MACHINE_TYPE}"        "${RC_WORKER_NODES}" "${RC_POOL_WORKERS}" "${RC_TAINT_HARD}"
ensure_pool "${RC_POOL_LOADGEN}" "${RC_MACHINE_TYPE}"        1                    "${RC_POOL_LOADGEN}" "${RC_TAINT_HARD}"

rc::step "fetching credentials into ${RC_KUBECONFIG}"
mkdir -p "$(dirname "${RC_KUBECONFIG}")"
KUBECONFIG="${RC_KUBECONFIG}" gcloud container clusters get-credentials "${RC_CLUSTER}" \
  --location="${RC_LOCATION}" --project="${PROJECT_ID}"
rc::assert_cluster

# --- does the largest arm actually fit? ---------------------------------------
#
# If the biggest arm exceeds what the router node can allocate, the rollout
# does not fail loudly: the new pod sits Pending and the arm records the
# *previous* arm's CPU under the new arm's label. Caught here at provision
# time rather than forty minutes into a sweep.
#
# The DaemonSet allowance is a flat 3 cores: atelet requests 2, and GKE's own
# per-node DaemonSets come to well under 1. An allowance rather than a
# measurement, because atelet is not installed yet at this point.
rc::step "checking the largest arm fits the router node"
rc::kubectl wait --for=condition=Ready node \
  -l "${RC_ROLE_KEY}=${RC_POOL_ROUTER}" --timeout=10m >/dev/null

router_alloc="$(rc::kubectl get node -l "${RC_ROLE_KEY}=${RC_POOL_ROUTER}" \
  -o jsonpath='{.items[0].status.allocatable.cpu}')"
# Allocatable is either plain cores ("88") or millicores ("87630m").
if [[ "${router_alloc}" == *m ]]; then
  alloc_m="${router_alloc%m}"
else
  alloc_m=$(( router_alloc * 1000 ))
fi

max_arm=0
for arm in ${RC_ARMS_DEFAULT}; do
  (( arm > max_arm )) && max_arm="${arm}"
done
need_m=$(( (max_arm + RC_SIDECAR_CORES + 3) * 1000 ))

if (( need_m > alloc_m )); then
  rc::die "router node (${RC_MACHINE_TYPE}) allocates ${alloc_m}m CPU, but the largest arm needs ${need_m}m (${max_arm} envoy + ${RC_SIDECAR_CORES} sidecar + 3 for DaemonSets). Use a larger ROUTERCAP_MACHINE_TYPE, or drop the top arm."
fi
rc::step "router node allocates ${alloc_m}m; largest arm needs ${need_m}m — fits with $(( (alloc_m - need_m) / 1000 )) cores spare"

if [[ "${SKIP_INSTALL}" == "true" ]]; then
  rc::step "--skip-install: stopping after the cluster and pools"
  exit 0
fi

# --- substrate ---------------------------------------------------------------

rc::step "installing substrate (hack/install-ate.sh --deploy-ate-system)"
# NO_DEV_ENV=1 because install-ate.sh re-sources .ate-dev-env.sh, which would
# undo rc::env's CLUSTER_NAME override and install substrate into whatever
# cluster the developer's file names. rc::env has already exported everything
# the install needs.
NO_DEV_ENV=1 "${ROOT}/hack/install-ate.sh" --deploy-ate-system

# --- placement ---------------------------------------------------------------
#
# Node pinning is what isolates the measurement: a CPU limit is CFS quota, not
# core pinning, and does not partition run-queue delay, L3, memory bandwidth,
# the NIC or conntrack — all per node. Giving the router a node to itself
# partitions all of them at once.

rc::step "pinning ate-system to its pools"
rc::pin_workload deployment "${RC_ROUTER_NS}" atenet-router "${RC_POOL_ROUTER}"
rc::pin_workload deployment "${RC_ROUTER_NS}" ate-api-server "${RC_POOL_SYSTEM}"
rc::pin_workload deployment "${RC_ROUTER_NS}" ate-controller "${RC_POOL_SYSTEM}"
rc::pin_workload deployment "${RC_ROUTER_NS}" dns "${RC_POOL_SYSTEM}"
rc::pin_workload statefulset "${RC_ROUTER_NS}" valkey-cluster "${RC_POOL_SYSTEM}"
rc::pin_workload deployment podcertificate-controller-system podcertificate-controller "${RC_POOL_SYSTEM}"

# atelet is a DaemonSet and nothing under manifests/ate-install declares any
# tolerations, so without this it gets no worker node and no actor ever
# starts. It must run everywhere, so it tolerates all four taints.
rc::step "letting atelet onto every tainted pool"
rc::kubectl -n "${RC_ROUTER_NS}" patch daemonset atelet --type=strategic -p "$(cat <<EOF
{"spec":{"template":{"spec":{"tolerations":[
  {"key":"${RC_ROLE_KEY}","operator":"Equal","value":"${RC_POOL_ROUTER}","effect":"NoSchedule"},
  {"key":"${RC_ROLE_KEY}","operator":"Equal","value":"${RC_POOL_SYSTEM}","effect":"NoSchedule"},
  {"key":"${RC_ROLE_KEY}","operator":"Equal","value":"${RC_POOL_WORKERS}","effect":"NoSchedule"},
  {"key":"${RC_ROLE_KEY}","operator":"Equal","value":"${RC_POOL_LOADGEN}","effect":"NoSchedule"}
]}}}}
EOF
)" >/dev/null

# --- workloads ---------------------------------------------------------------

rc::step "deploying workloads (${RC_WORKER_PODS} worker pods)"
"${ROOT}/benchmarking/workloads/deploy.sh" --deploy --worker-count "${RC_WORKER_PODS}"

# The shared workloads template carries no placement, so the pool is patched
# here rather than forked. Same ActorTemplate, same ateom image, different
# scheduling.
rc::step "pinning the worker pool"
rc::kubectl -n "${RC_WORKER_NS}" patch workerpool "${RC_WORKER_POOL}" --type=merge -p "$(cat <<EOF
{"spec":{"replicas":${RC_WORKER_PODS},"template":{
  "nodeSelector":{"${RC_ROLE_KEY}":"${RC_POOL_WORKERS}"},
  "tolerations":$(rc::tolerations "${RC_POOL_WORKERS}")
}}}
EOF
)" >/dev/null

# --- the run's own namespace and RBAC ----------------------------------------

rc::step "applying the runner's namespace, ServiceAccount and RBAC"
rc::kubectl apply -f "${RC_DIR}/manifests/rbac.yaml"

# --- checks ------------------------------------------------------------------

rc::step "waiting for ate-system to settle"
rc::kubectl -n "${RC_ROUTER_NS}" rollout status deployment/atenet-router --timeout=10m
rc::kubectl -n "${RC_ROUTER_NS}" rollout status deployment/ate-api-server --timeout=10m

router_pod="$(rc::kubectl -n "${RC_ROUTER_NS}" get pod -l "${RC_ROUTER_SELECTOR}" -o jsonpath='{.items[0].metadata.name}')"
range="$(rc::router_port_range "${router_pod}" "${RC_ROUTER_NS}")"
if [[ -n "${range}" ]]; then
  low="${range%%[[:space:]]*}"; high="${range##*[[:space:]]}"
  rc::step "router ephemeral port range: ${low}-${high} ($((high - low + 1)) ports)"
else
  rc::warn "could not read the router's ip_local_port_range; runs will record the Linux default as assumed"
fi

# Derived rather than hardcoded, because the machine type is a variable and a
# stale dollar figure is worse than none. ~$0.05 per vCPU-hour is C3 on-demand
# in us-central1 with memory folded in — sizes the decision, not an invoice.
total_vcpu=$(( ${RC_MACHINE_TYPE##*-} * (2 + RC_WORKER_NODES) + ${RC_SYSTEM_MACHINE_TYPE##*-} ))

cat >&2 <<EOF

$(echo -e "${COLOR_CYAN}[routercap] cluster ready${COLOR_RESET}")

  cluster    ${RC_CLUSTER} (${RC_LOCATION})
  kubeconfig ${RC_KUBECONFIG}
  pools      router 1x${RC_MACHINE_TYPE} · system 1x${RC_SYSTEM_MACHINE_TYPE} · workers ${RC_WORKER_NODES}x${RC_MACHINE_TYPE} · loadgen 1x${RC_MACHINE_TYPE}
  workers    ${RC_WORKER_PODS} pods

This is real money — ${total_vcpu} vCPU, roughly \$$(( total_vcpu / 20 ))/hour. Tear it down when the run is done:

  gcloud container clusters delete ${RC_CLUSTER} --location=${RC_LOCATION} --project=${PROJECT_ID}

Next:  benchmarking/routercap/run.sh --smoke
EOF
