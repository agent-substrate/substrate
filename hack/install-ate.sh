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
#
# Installs Agent Substrate.
#
# This is a translation shim: cmd/ate-setup carries out every action, and this
# script only maps the flags and environment variables the installer has always
# accepted onto its commands, so existing command lines and CI jobs keep
# working unchanged. It deliberately holds no install logic of its own.
#
# New work belongs in cmd/ate-setup. See cmd/ate-setup/commands.md for the
# equivalent invocation of each flag below, and cmd/ate-setup/cli-diff.md for
# the behaviors that differ after the move.
#
# The environment variables the installer reads -- BUCKET_NAME, KO_DOCKER_REPO,
# PROJECT_ID, CLUSTER_NAME, CLUSTER_LOCATION, KUBECTL_CONTEXT, NO_DEV_ENV, the
# ATE_API_POSTGRES_* set, and the rest -- are read by ate-setup directly, as is
# .ate-dev-env.sh. They need no translation and are not repeated here.

set -o errexit -o nounset -o pipefail

ROOT="$(git rev-parse --show-toplevel)"
cd "${ROOT}"

# Demos, in the order --help lists them. Keeping the list here rather than
# asking ate-setup for it means --help answers without a build, and that a demo
# flag this script does not know is rejected outright rather than handed to
# ate-setup as an unknown subcommand. `go run ./cmd/ate-setup deploy demo
# --help` is the authoritative list; add new demos there and mirror them here.
ATE_DEMOS=(
  demo-counter
  demo-counter-microvm
  demo-egress
  demo-egress-microvm
  demo-egress-mitm
  demo-egress-microvm-mitm
  demo-jupyter
  demo-sandbox
  demo-claude-code-multiplex
  demo-multi-template
  demo-parking
  demo-autoscaled-workerpool
)

# Prerequisites and caveats worth repeating in --help, keyed by demo.
demo_usage() {
  case "$1" in
    demo-counter)
      echo "  --deploy-demo-counter-with-external-volume    Deploy demo-counter with external volume validation"
      echo "                                                (STORAGE_CLASS names the class; it otherwise follows --setup-csi)"
      ;;
    demo-counter-microvm|demo-egress-microvm)
      echo "  Needs hack/install-microvm-deps.sh --install to have run (cluster-wide microvm SandboxConfig)."
      ;;
    demo-egress-mitm)
      echo "  Needs an sdsmint install (--deploy-atenet --experimental-use-sdsmint): the actors"
      echo "  project the egress gateway trust bundle, which does not resolve otherwise."
      ;;
    demo-egress-microvm-mitm)
      echo "  Needs hack/install-microvm-deps.sh --install to have run (cluster-wide microvm SandboxConfig),"
      echo "  and an sdsmint install (--deploy-atenet --experimental-use-sdsmint) for the trust bundle."
      ;;
    demo-claude-code-multiplex)
      echo "  Required env: ANTHROPIC_API_KEY, BUCKET_NAME, KO_DOCKER_REPO"
      echo "  See demos/claude-code-multiplex/README.md for the walkthrough."
      ;;
    demo-autoscaled-workerpool)
      echo "  Kind only: it ships its own prometheus-adapter and a Kind-specific HPA."
      ;;
  esac
}

usage() {
  echo "Usage: $0 [options]"
  echo ""
  echo "Overall infrastructure (all infrastructure components):"
  echo ""
  echo "  --deploy-ate-system                    Deploy core system (CRDs, atelet, apiserver)"
  echo "  --setup-csi[=DRIVER]                   Setup CSI driver: nfs, hostpath, both, none (default: none;"
  echo "                                         a bare --setup-csi means nfs; hostpath is Kind only)"
  echo "  --delete-ate-system                    Delete core system"
  echo "  --delete-all                           Delete core system and all registered demos"
  echo "  --atenet-dataplane=envoy|agentgateway  Select the atenet ingress and egress dataplane (default: envoy)"
  echo "  --podcert-workers-per-signer N         Concurrent workers per podcertificate-controller signer (default: 1)"
  echo "  --rollout-timeout DURATION             Per-workload readiness wait timeout, kubectl-style Go duration (default: 60s)"
  echo "  --otlp-endpoint URL                    Send all control plane telemetry to URL, not to the cluster default (see benchmarking/telemetry/README.md)"
  echo ""
  echo "Experiments:"
  echo ""
  echo "  --experimental-use-sdsmint             Deploy the egress gateway with per-SNI certificate minting (experimental)"
  echo "  --experimental-additional-egress-extproc-service NS/SVC:PORT"
  echo "                                         Run an additional ext_proc authorization filter, served by that Service."
  echo "                                         Requires --experimental-use-sdsmint. (experimental)"
  echo "  --experimental-egress-credential-injection"
  echo "                                         Point the egress gateway's MITM-leg handler at a credential provider, so a"
  echo "                                         matching EgressPolicy rule injects its credential. A modifier applied when"
  echo "                                         the gateway is deployed (e.g. with --deploy-atenet); the credential provider"
  echo "                                         itself is deployed separately. Implies --experimental-use-sdsmint; requires"
  echo "                                         --atenet-dataplane=envoy. (experimental)"
  echo "  --credential-provider-name NAME        Provider the injector serves, as a ate-secret:// prefix"
  echo "                                         (default ate-secret://k8s.io). Only meaningful with"
  echo "                                         --experimental-egress-credential-injection. (experimental)"
  echo "  --credential-provider-address HOST:PORT"
  echo "                                         Address the egress gateway dials the credential provider at"
  echo "                                         (default k8s-credential-provider.ate-system.svc:50051). Only meaningful with"
  echo "                                         --experimental-egress-credential-injection. (experimental)"
  echo ""
  echo "Infrastructure components:"
  echo ""
  echo "  --deploy-atelet                        Deploy atelet only"
  echo "  --deploy-ate-apiserver                 Deploy ate-api-server only"
  echo "  --deploy-atenet                        Deploy atenet only"
  echo "  --delete-atenet                        Delete atenet only"
  echo ""
  echo "To create individual resources used by ate-system (Note: These are"
  echo "called automatically by --deploy-ate-system):"
  echo ""
  echo "  --create-jwt-authority-pool-secret     Create JWT authority pool secret"
  echo "  --create-actor-id-ca-pool-secret       Create actor ID CA pool secret"
  echo "  --create-actor-id-ca-certs-secret      Create actor ID CA certs secret"
  echo "  --create-egress-mitm-ca-pool-secret    Create egress MITM CA pool secret"
  echo "  --create-podcertificate-controller-cas Create podcertificate controller CAs"
  echo "  --create-api-server-env-vars           Create ate-api-server env vars"
  echo "  --create-api-authentication-config     Create the default ate-api-server authentication config"
  echo ""
  echo "PostgreSQL configuration (either of the first two selects an external"
  echo "database and skips the bundled instance):"
  echo ""
  echo "  ATE_API_POSTGRES_CONNECTION_STRING     DSN for any external PostgreSQL (stored in a Secret;"
  echo "                                         pair with ATE_API_POSTGRES_SERVER_CA_FILE for sslmode=verify-ca)"
  echo "  ATE_API_POSTGRES_CLOUDSQL_INSTANCE     Cloud SQL instance connection name (project:region:instance)."
  echo "                                         Deploys the Cloud SQL Auth Proxy sidecar: connector-managed TLS"
  echo "                                         and automatic IAM database auth, no passwords (see tools/setup-gcp/cloud-sql.md)."
  echo "                                         Unset = keep the cluster's current Cloud SQL config; set to \"\" to remove it"
  echo "  ATE_API_POSTGRES_CLOUDSQL_GSA          GSA email backing Workload Identity + the IAM database user"
  echo "  ATE_API_POSTGRES_CLOUDSQL_IP_TYPE      private (default) | public | psc"
  echo "  ATE_API_POSTGRES_CLOUDSQL_IAM_AUTH     true (default) | false (password-over-proxy escape hatch)"
  echo "  ATE_API_POSTGRES_POOL_MAX_CONNS        pgxpool max connections per ateapi replica (default: max(4, NumCPU))"
  echo "  ATE_API_POSTGRES_SERVER_CA_FILE        PEM file to mount for verify-ca DSNs (non-Cloud-SQL databases)"
  echo "  ATE_API_POSTGRES_SCHEMA                Select the Substrate schema (default: public)"
  echo ""
  echo "Authentication configuration:"
  echo ""
  echo "  EXPECTED_JWT_ISSUER                    Issuer URL ate-api-server requires in service account tokens, verbatim."
  echo "                                         Default: derived from PROJECT_ID/CLUSTER_LOCATION/CLUSTER_NAME"
  echo "                                         (https://container.googleapis.com/v1/projects/.../clusters/...),"
  echo "                                         else the cluster's OIDC discovery document"
  echo ""
  echo "Benchmarks (see benchmarking/README.md for details and customization):"
  echo ""
  echo "  --deploy-benchmarks                    Deploy workloads + locust load test stack"
  echo "  --delete-benchmarks                    Delete the locust stack and workloads"
  echo "  --benchmark-worker-count N             Number of WorkerPool replicas (default: 1)"
  echo "  --benchmark-sandbox-class CLASS        Sandbox runtime for the benchmark WorkerPool: gvisor | microvm (default: gvisor)."
  echo "                                         microvm requires hack/install-microvm-deps.sh --install to have run."
  echo "  --benchmark-actor-memory SIZE          Memory limit for the benchmark ActorTemplates (default: 256Mi,"
  echo "                                         the smallest size microvm admits)"
  echo ""
  local demo_name
  for demo_name in "${ATE_DEMOS[@]}"; do
    echo "Demo: ${demo_name}"
    echo ""
    echo "  --deploy-${demo_name}                         Deploy ${demo_name}"
    echo "  --delete-${demo_name}                         Delete ${demo_name}"
    demo_usage "${demo_name}"
    echo ""
  done
}

# ate_setup runs one ate-setup command. GLOBAL_FLAGS holds the pre-scanned
# flags that shape every action, which is how they behaved here: a single
# command line could ask for several actions and one --atenet-dataplane.
ate_setup() {
  go run ./cmd/ate-setup ${GLOBAL_FLAGS[@]+"${GLOBAL_FLAGS[@]}"} "$@"
}

# counter_storage_class names the StorageClass the counter demo's external
# volume is provisioned from. STORAGE_CLASS names it outright; without it, fall
# back to whatever --setup-csi just installed, and to ate-setup's own default
# when it installed nothing.
counter_storage_class() {
  if [[ -n "${STORAGE_CLASS:-}" ]]; then
    echo "${STORAGE_CLASS}"
    return
  fi
  case "${SETUP_CSI}" in
    hostpath) echo "csi-hostpath-sc" ;;
    nfs|both|true) echo "csi-nfs-sc" ;;
    *) echo "standard" ;;
  esac
}

# run_demo maps --deploy-demo-NAME / --delete-demo-NAME onto
# `ate-setup {deploy,delete} demo NAME`. A demo this installer does not
# register is rejected the way any other unknown flag is, rather than reaching
# ate-setup as an unknown subcommand.
run_demo() {
  local action="$1" flag="$2"
  local demo="${flag#--"${action}"-}"
  local known
  for known in "${ATE_DEMOS[@]}"; do
    if [[ "${demo}" == "${known}" ]]; then
      ate_setup "${action}" demo "${demo#demo-}"
      return
    fi
  done
  echo "Error: unknown option: ${flag}" >&2
  echo ""
  usage
  exit 1
}

if [ "$#" -eq 0 ]; then
  usage
  exit 1
fi

# If -h or --help appears anywhere in the command line, print the usage and exit.
for arg in "$@"; do
  case "$arg" in
    -h|--help)
      usage
      exit 0
      ;;
  esac
done

# Pre-scan value-bearing flags so they can appear before or after the action
# flag they configure (e.g. --benchmark-worker-count before/after
# --deploy-benchmarks). The dispatch loop below also accepts these flags but
# treats them as no-ops since the value is already captured here.
GLOBAL_FLAGS=()
# Valid values for SETUP_CSI: nfs, hostpath, both, none. Defaults to none: a
# CSI driver is an opt-in extra, and NFS needs kernel modules a plain
# workstation will not have loaded.
SETUP_CSI="${SETUP_CSI:-none}"
BENCHMARK_FLAGS=()
prescan_args=("$@")
for ((i = 0; i < ${#prescan_args[@]}; i++)); do
  case "${prescan_args[i]}" in
    --atenet-dataplane=*) GLOBAL_FLAGS+=("${prescan_args[i]}") ;;
    --atenet-dataplane)
      if (( i + 1 >= ${#prescan_args[@]} )); then
        echo "Error: --atenet-dataplane requires envoy or agentgateway" >&2
        exit 1
      fi
      GLOBAL_FLAGS+=("--atenet-dataplane=${prescan_args[$((i + 1))]}")
      ;;
    --experimental-use-sdsmint) GLOBAL_FLAGS+=(--experimental-use-sdsmint) ;;
    --experimental-additional-egress-extproc-service=*)
      GLOBAL_FLAGS+=("${prescan_args[i]}")
      ;;
    --experimental-additional-egress-extproc-service)
      if (( i + 1 >= ${#prescan_args[@]} )); then
        echo "Error: --experimental-additional-egress-extproc-service requires <namespace>/<service>:<port>" >&2
        exit 1
      fi
      GLOBAL_FLAGS+=("--experimental-additional-egress-extproc-service=${prescan_args[$((i + 1))]}")
      ;;
    # Enabling credential injection implies sdsmint in the shell installer, so
    # forward both flags to ate-setup.
    --experimental-egress-credential-injection)
      GLOBAL_FLAGS+=(--experimental-use-sdsmint --experimental-egress-credential-injection)
      ;;
    --credential-provider-name=*) GLOBAL_FLAGS+=("${prescan_args[i]}") ;;
    --credential-provider-name)
      if (( i + 1 >= ${#prescan_args[@]} )); then
        echo "Error: --credential-provider-name requires a value" >&2
        exit 1
      fi
      GLOBAL_FLAGS+=("--credential-provider-name=${prescan_args[$((i + 1))]}")
      ;;
    --credential-provider-address=*) GLOBAL_FLAGS+=("${prescan_args[i]}") ;;
    --credential-provider-address)
      if (( i + 1 >= ${#prescan_args[@]} )); then
        echo "Error: --credential-provider-address requires <host>:<port>" >&2
        exit 1
      fi
      GLOBAL_FLAGS+=("--credential-provider-address=${prescan_args[$((i + 1))]}")
      ;;
    --podcert-workers-per-signer=*) GLOBAL_FLAGS+=("${prescan_args[i]}") ;;
    --podcert-workers-per-signer)
      if (( i + 1 >= ${#prescan_args[@]} )); then
        echo "Error: --podcert-workers-per-signer requires a positive integer" >&2
        exit 1
      fi
      GLOBAL_FLAGS+=("--podcert-workers-per-signer=${prescan_args[$((i + 1))]}")
      ;;
    --rollout-timeout=*) GLOBAL_FLAGS+=("${prescan_args[i]}") ;;
    --rollout-timeout)
      if (( i + 1 >= ${#prescan_args[@]} )); then
        echo "Error: --rollout-timeout requires a Go duration (e.g. 300s, 10m)" >&2
        exit 1
      fi
      GLOBAL_FLAGS+=("--rollout-timeout=${prescan_args[$((i + 1))]}")
      ;;
    --otlp-endpoint=*) GLOBAL_FLAGS+=("${prescan_args[i]}") ;;
    --otlp-endpoint)
      if (( i + 1 >= ${#prescan_args[@]} )); then
        echo "Error: --otlp-endpoint requires a URL" >&2
        exit 1
      fi
      GLOBAL_FLAGS+=("--otlp-endpoint=${prescan_args[$((i + 1))]}")
      ;;
    --benchmark-worker-count=*) BENCHMARK_FLAGS+=("--worker-count=${prescan_args[i]#*=}") ;;
    --benchmark-worker-count)
      BENCHMARK_FLAGS+=("--worker-count=${prescan_args[i+1]:-1}")
      ;;
    --benchmark-sandbox-class=*) BENCHMARK_FLAGS+=("--sandbox-class=${prescan_args[i]#*=}") ;;
    --benchmark-sandbox-class)
      if (( i + 1 >= ${#prescan_args[@]} )); then
        echo "Error: --benchmark-sandbox-class requires gvisor or microvm" >&2
        exit 1
      fi
      BENCHMARK_FLAGS+=("--sandbox-class=${prescan_args[$((i + 1))]}")
      ;;
    # The benchmark actor memory has no ate-setup flag; it is read from the
    # environment, which is also how the benchmark scripts have always taken it.
    --benchmark-actor-memory=*) export BENCHMARK_ACTOR_MEMORY="${prescan_args[i]#*=}" ;;
    --benchmark-actor-memory)
      if (( i + 1 >= ${#prescan_args[@]} )); then
        echo "Error: --benchmark-actor-memory requires a size (e.g. 256Mi)" >&2
        exit 1
      fi
      export BENCHMARK_ACTOR_MEMORY="${prescan_args[$((i + 1))]}"
      ;;
    --setup-csi=*) SETUP_CSI="${prescan_args[i]#*=}" ;;
    --setup-csi)
      if (( i + 1 < ${#prescan_args[@]} )) && [[ "${prescan_args[$((i + 1))]}" != --* ]]; then
        SETUP_CSI="${prescan_args[$((i + 1))]}"
      else
        SETUP_CSI="nfs"
      fi
      ;;
  esac
done

# Actions run in command line order, one ate-setup invocation each, so a single
# line can still ask for several of them.
while [[ "$#" -gt 0 ]]; do
  case $1 in
    # Captured in the pre-scan above; matched here only so the `*)` branch does
    # not reject them, and so a separated value is consumed with its flag.
    --atenet-dataplane|--podcert-workers-per-signer|--rollout-timeout|--otlp-endpoint) shift ;;
    --experimental-additional-egress-extproc-service) shift ;;
    --credential-provider-name|--credential-provider-address) shift ;;
    --benchmark-worker-count|--benchmark-sandbox-class|--benchmark-actor-memory) shift ;;
    --atenet-dataplane=*|--podcert-workers-per-signer=*|--rollout-timeout=*|--otlp-endpoint=*) ;;
    --experimental-use-sdsmint|--experimental-additional-egress-extproc-service=*) ;;
    --experimental-egress-credential-injection|--credential-provider-name=*|--credential-provider-address=*) ;;
    --benchmark-worker-count=*|--benchmark-sandbox-class=*|--benchmark-actor-memory=*) ;;

    --deploy-ate-system) ate_setup deploy ate-system "--setup-csi=${SETUP_CSI}" ;;
    --setup-csi=*) ate_setup setup csi "${SETUP_CSI}" ;;
    --setup-csi)
      if [[ "$#" -gt 1 && "$2" != --* ]]; then
        shift
      fi
      ate_setup setup csi "${SETUP_CSI}"
      ;;
    --delete-ate-system) ate_setup delete ate-system ;;
    --delete-all) ate_setup delete all ;;

    --deploy-atelet) ate_setup deploy atelet ;;
    --deploy-ate-apiserver) ate_setup deploy apiserver ;;

    --deploy-atenet) ate_setup deploy atenet ;;
    --delete-atenet) ate_setup delete atenet ;;

    --deploy-benchmarks) ate_setup deploy benchmarks ${BENCHMARK_FLAGS[@]+"${BENCHMARK_FLAGS[@]}"} ;;
    --delete-benchmarks) ate_setup delete benchmarks ${BENCHMARK_FLAGS[@]+"${BENCHMARK_FLAGS[@]}"} ;;

    --create-jwt-authority-pool-secret) ate_setup create jwt-authority-pool ;;
    --create-actor-id-ca-pool-secret) ate_setup create actor-id-ca-pool ;;
    --create-actor-id-ca-certs-secret) ate_setup create actor-id-ca-certs ;;
    --create-egress-mitm-ca-pool-secret) ate_setup create egress-mitm-ca-pool ;;
    --create-podcertificate-controller-cas) ate_setup create podcertificate-controller-cas ;;
    --create-api-server-env-vars) ate_setup create api-server-env-vars ;;
    --create-api-authentication-config) ate_setup create api-authentication-config ;;

    # The one demo flag that is not just a demo name.
    --deploy-demo-counter-with-external-volume)
      ate_setup deploy demo counter --with-external-volume \
        "--storage-class=$(counter_storage_class)"
      ;;
    --deploy-demo-*) run_demo deploy "$1" ;;
    --delete-demo-*) run_demo delete "$1" ;;

    *)
      # Invalid option, should usage and exit with an error.
      echo "Error: unknown option: $1" >&2
      echo ""
      usage
      exit 1
      ;;
  esac
  shift
done
