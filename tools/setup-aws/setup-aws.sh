#!/usr/bin/env bash

# Copyright 2026 Ant Weiss
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
# Bash analog of tools/setup-gcp for AWS EKS. Provisions every AWS resource
# Agent Substrate needs to install onto an EKS cluster, in one invocation.
# See tools/setup-aws/README.md for the walkthrough.
#
# Running with no subcommand (./setup-aws.sh) does the full install:
# cluster -> s3 -> ecr -> rds -> rds-grant -> bootstrap-postgres -> summary.
# Any missing secrets (BUCKET_NAME, RDS passwords) are auto-generated and
# persisted to bin/aws-env.sh so re-runs reuse them deterministically.
# Individual phases can be invoked by name for debugging.

set -o errexit -o nounset -o pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
RENDERED_CONFIG="${ROOT}/bin/aws-cluster.yaml"
CREDENTIALS_FILE="${ROOT}/bin/aws-env.sh"

usage() {
  cat <<EOF
Usage: $(basename "$0") [phase]

Running with no phase does the full install end-to-end:
  cluster -> s3 -> ecr -> rds -> rds-grant -> bootstrap-postgres -> summary

Individual phases (for debugging or partial reruns):
  cluster            Create VPC, EKS cluster, node groups, IRSA roles, add-ons
  s3                 Create S3 snapshot bucket (private, encrypted, versioned)
  ecr                Create ECR repository
  rds                Create RDS PostgreSQL instance with IAM auth enabled
  rds-grant          Attach rds-db:connect to the ate-api-server IRSA role
  bootstrap-postgres Create substrate schema and roles inside the RDS instance
                     via an in-cluster psql pod
  summary            Print the env block to source for hack/install-ate.sh
  all                All of the above in order (same as no argument)

Required env:
  AWS_REGION           AWS region (e.g. us-west-2)

Auto-generated and persisted to ${CREDENTIALS_FILE} on first run:
  BUCKET_NAME               Globally unique S3 bucket name
  RDS_MASTER_PASSWORD       Postgres master user password (32 hex chars)
  RDS_READWRITE_PASSWORD    substrate_readwrite password (32 hex chars)

Env with defaults:
  CLUSTER_NAME                   (default: substrate-poc)
  K8S_VERSION                    (default: 1.37 — serves PodCertificateRequest v1)
  ECR_REPO                       (default: substrate)
  RDS_INSTANCE_ID                (default: substrate-poc)
  RDS_INSTANCE_CLASS             (default: db.t4g.medium)
  RDS_DB_NAME                    (default: substrate)
  RDS_MASTER_USERNAME            (default: substrate_admin)
  RDS_ENGINE_VERSION             (default: 16.15)
  RDS_ALLOCATED_STORAGE          (default: 100 GB)
  CONTROL_PLANE_INSTANCE_TYPE    (default: m6i.xlarge)
  CONTROL_PLANE_DESIRED/MIN/MAX  (default: 2/2/3)
  WORKER_INSTANCE_TYPE           (default: m6i.xlarge)
  WORKER_DESIRED/MIN/MAX         (default: 2/1/10)

Teardown:
  ${SCRIPT_DIR}/teardown-aws.sh
EOF
}

: "${AWS_REGION:?AWS_REGION must be set}"

# Load persisted credentials if present (so re-runs reuse what we generated).
[[ -f "${CREDENTIALS_FILE}" ]] && source "${CREDENTIALS_FILE}"

CLUSTER_NAME="${CLUSTER_NAME:-substrate-poc}"
K8S_VERSION="${K8S_VERSION:-1.37}"
ECR_REPO="${ECR_REPO:-substrate}"

CONTROL_PLANE_INSTANCE_TYPE="${CONTROL_PLANE_INSTANCE_TYPE:-m6i.xlarge}"
CONTROL_PLANE_DESIRED="${CONTROL_PLANE_DESIRED:-2}"
CONTROL_PLANE_MIN="${CONTROL_PLANE_MIN:-2}"
CONTROL_PLANE_MAX="${CONTROL_PLANE_MAX:-3}"
WORKER_INSTANCE_TYPE="${WORKER_INSTANCE_TYPE:-m6i.xlarge}"
WORKER_DESIRED="${WORKER_DESIRED:-2}"
WORKER_MIN="${WORKER_MIN:-1}"
WORKER_MAX="${WORKER_MAX:-10}"

RDS_INSTANCE_ID="${RDS_INSTANCE_ID:-substrate-poc}"
RDS_INSTANCE_CLASS="${RDS_INSTANCE_CLASS:-db.t4g.medium}"
RDS_DB_NAME="${RDS_DB_NAME:-substrate}"
RDS_MASTER_USERNAME="${RDS_MASTER_USERNAME:-substrate_admin}"
RDS_ENGINE_VERSION="${RDS_ENGINE_VERSION:-16.15}"
RDS_ALLOCATED_STORAGE="${RDS_ALLOCATED_STORAGE:-100}"

check_tools() {
  local missing=()
  for t in aws eksctl kubectl jq envsubst openssl; do
    command -v "$t" >/dev/null 2>&1 || missing+=("$t")
  done
  if [[ ${#missing[@]} -gt 0 ]]; then
    echo "error: missing tools: ${missing[*]}" >&2
    echo "       install eksctl: https://eksctl.io/" >&2
    exit 1
  fi
}

# Auto-generate secrets and persist to bin/aws-env.sh. Reruns of setup-aws.sh
# re-source the file at the top of the script, so values stay stable.
ensure_credentials() {
  mkdir -p "${ROOT}/bin"
  local account_id region_short changed=false
  account_id=$(aws sts get-caller-identity --query 'Account' --output text)
  region_short=$(echo "${AWS_REGION}" | tr -d '-' | sed 's/\(.\{6\}\).*/\1/')

  if [[ -z "${BUCKET_NAME:-}" ]]; then
    BUCKET_NAME="substrate-snaps-${account_id}-${region_short}"
    echo "generated BUCKET_NAME=${BUCKET_NAME}"
    changed=true
  fi
  if [[ -z "${RDS_MASTER_PASSWORD:-}" ]]; then
    RDS_MASTER_PASSWORD=$(openssl rand -hex 16)
    echo "generated RDS_MASTER_PASSWORD (32 hex)"
    changed=true
  fi
  if [[ -z "${RDS_READWRITE_PASSWORD:-}" ]]; then
    RDS_READWRITE_PASSWORD=$(openssl rand -hex 16)
    echo "generated RDS_READWRITE_PASSWORD (32 hex)"
    changed=true
  fi

  if [[ "${changed}" == "true" ]]; then
    cat > "${CREDENTIALS_FILE}" <<EOF
# Generated by tools/setup-aws/setup-aws.sh. Do not commit (bin/ is gitignored).
# Re-sourced by setup-aws.sh and teardown-aws.sh so values stay stable.
export AWS_REGION='${AWS_REGION}'
export CLUSTER_NAME='${CLUSTER_NAME}'
export K8S_VERSION='${K8S_VERSION}'
export BUCKET_NAME='${BUCKET_NAME}'
export ECR_REPO='${ECR_REPO}'
export RDS_INSTANCE_ID='${RDS_INSTANCE_ID}'
export RDS_DB_NAME='${RDS_DB_NAME}'
export RDS_MASTER_USERNAME='${RDS_MASTER_USERNAME}'
export RDS_MASTER_PASSWORD='${RDS_MASTER_PASSWORD}'
export RDS_READWRITE_PASSWORD='${RDS_READWRITE_PASSWORD}'
export RDS_ENGINE_VERSION='${RDS_ENGINE_VERSION}'
EOF
    chmod 600 "${CREDENTIALS_FILE}"
    echo "wrote ${CREDENTIALS_FILE} (mode 0600)"
  fi
}

phase_cluster() {
  ensure_credentials
  mkdir -p "${ROOT}/bin"
  echo "==> Rendering eksctl ClusterConfig to ${RENDERED_CONFIG}"
  export AWS_REGION CLUSTER_NAME K8S_VERSION BUCKET_NAME
  export CONTROL_PLANE_INSTANCE_TYPE CONTROL_PLANE_DESIRED CONTROL_PLANE_MIN CONTROL_PLANE_MAX
  export WORKER_INSTANCE_TYPE WORKER_DESIRED WORKER_MIN WORKER_MAX
  envsubst < "${SCRIPT_DIR}/cluster.yaml.tmpl" > "${RENDERED_CONFIG}"

  if eksctl get cluster --name "${CLUSTER_NAME}" --region "${AWS_REGION}" >/dev/null 2>&1; then
    echo "==> Cluster ${CLUSTER_NAME} already exists; reconciling addons + IAM"
    eksctl create addon -f "${RENDERED_CONFIG}" --wait || true
    eksctl create iamserviceaccount -f "${RENDERED_CONFIG}" --approve || true
  else
    echo "==> Creating EKS cluster ${CLUSTER_NAME} in ${AWS_REGION} (15-20 minutes)"
    eksctl create cluster -f "${RENDERED_CONFIG}"
  fi

  echo "==> Updating local kubeconfig"
  aws eks update-kubeconfig --region "${AWS_REGION}" --name "${CLUSTER_NAME}"

  echo "==> Setting gp3 as default StorageClass"
  kubectl annotate storageclass gp2 storageclass.kubernetes.io/is-default-class- --overwrite >/dev/null 2>&1 || true
  if ! kubectl get storageclass gp3 >/dev/null 2>&1; then
    kubectl apply -f - <<'EOF'
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: gp3
  annotations:
    storageclass.kubernetes.io/is-default-class: "true"
provisioner: ebs.csi.aws.com
volumeBindingMode: WaitForFirstConsumer
reclaimPolicy: Delete
parameters:
  type: gp3
  encrypted: "true"
EOF
  else
    kubectl annotate storageclass gp3 storageclass.kubernetes.io/is-default-class=true --overwrite >/dev/null
  fi
}

phase_s3() {
  ensure_credentials
  echo "==> Creating S3 bucket ${BUCKET_NAME} in ${AWS_REGION}"
  if aws s3api head-bucket --bucket "${BUCKET_NAME}" --region "${AWS_REGION}" 2>/dev/null; then
    echo "    already exists"
  elif [[ "${AWS_REGION}" == "us-east-1" ]]; then
    aws s3api create-bucket --bucket "${BUCKET_NAME}" --region "${AWS_REGION}" >/dev/null
  else
    aws s3api create-bucket --bucket "${BUCKET_NAME}" --region "${AWS_REGION}" \
      --create-bucket-configuration "LocationConstraint=${AWS_REGION}" >/dev/null
  fi

  echo "    blocking all public access"
  aws s3api put-public-access-block --bucket "${BUCKET_NAME}" \
    --public-access-block-configuration \
      "BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true"

  echo "    enabling default encryption (SSE-S3)"
  aws s3api put-bucket-encryption --bucket "${BUCKET_NAME}" \
    --server-side-encryption-configuration \
      '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"}}]}'

  echo "    enabling versioning (protects against accidental snapshot delete)"
  aws s3api put-bucket-versioning --bucket "${BUCKET_NAME}" \
    --versioning-configuration Status=Enabled
}

phase_ecr() {
  echo "==> Creating ECR repository ${ECR_REPO} in ${AWS_REGION}"
  if aws ecr describe-repositories --repository-names "${ECR_REPO}" \
       --region "${AWS_REGION}" >/dev/null 2>&1; then
    echo "    already exists"
  else
    aws ecr create-repository --repository-name "${ECR_REPO}" \
      --region "${AWS_REGION}" \
      --image-scanning-configuration scanOnPush=true \
      --image-tag-mutability MUTABLE >/dev/null
  fi
}

phase_rds() {
  ensure_credentials
  echo "==> Discovering cluster VPC and private subnets"
  local vpc_id
  vpc_id=$(aws eks describe-cluster --name "${CLUSTER_NAME}" --region "${AWS_REGION}" \
    --query 'cluster.resourcesVpcConfig.vpcId' --output text)
  [[ "${vpc_id}" != "None" && -n "${vpc_id}" ]] || {
    echo "error: could not resolve VPC for cluster ${CLUSTER_NAME}; run the 'cluster' phase first" >&2
    exit 1
  }

  local private_subnets
  private_subnets=$(aws ec2 describe-subnets --region "${AWS_REGION}" \
    --filters "Name=vpc-id,Values=${vpc_id}" "Name=tag:kubernetes.io/role/internal-elb,Values=1" \
    --query 'Subnets[].SubnetId' --output text)
  [[ -n "${private_subnets}" ]] || {
    echo "error: no private subnets tagged kubernetes.io/role/internal-elb=1 in ${vpc_id}" >&2
    exit 1
  }

  echo "==> Creating RDS DB subnet group ${CLUSTER_NAME}-db"
  if ! aws rds describe-db-subnet-groups --db-subnet-group-name "${CLUSTER_NAME}-db" \
       --region "${AWS_REGION}" >/dev/null 2>&1; then
    # shellcheck disable=SC2086 # private_subnets is a space-separated list
    aws rds create-db-subnet-group --region "${AWS_REGION}" \
      --db-subnet-group-name "${CLUSTER_NAME}-db" \
      --db-subnet-group-description "Substrate RDS subnet group" \
      --subnet-ids ${private_subnets} >/dev/null
  else
    echo "    already exists"
  fi

  echo "==> Creating RDS security group ${CLUSTER_NAME}-rds"
  local rds_sg
  rds_sg=$(aws ec2 describe-security-groups --region "${AWS_REGION}" \
    --filters "Name=vpc-id,Values=${vpc_id}" "Name=group-name,Values=${CLUSTER_NAME}-rds" \
    --query 'SecurityGroups[0].GroupId' --output text 2>/dev/null || true)
  if [[ -z "${rds_sg}" || "${rds_sg}" == "None" ]]; then
    rds_sg=$(aws ec2 create-security-group --region "${AWS_REGION}" \
      --vpc-id "${vpc_id}" --group-name "${CLUSTER_NAME}-rds" \
      --description "Substrate RDS access from EKS nodes" \
      --query 'GroupId' --output text)
    local node_sg
    node_sg=$(aws eks describe-cluster --name "${CLUSTER_NAME}" --region "${AWS_REGION}" \
      --query 'cluster.resourcesVpcConfig.clusterSecurityGroupId' --output text)
    aws ec2 authorize-security-group-ingress --region "${AWS_REGION}" \
      --group-id "${rds_sg}" --protocol tcp --port 5432 --source-group "${node_sg}" >/dev/null
    echo "    created ${rds_sg}, allow 5432 from node SG ${node_sg}"
  else
    echo "    already exists (${rds_sg})"
  fi

  echo "==> Creating RDS PostgreSQL instance ${RDS_INSTANCE_ID}"
  if aws rds describe-db-instances --db-instance-identifier "${RDS_INSTANCE_ID}" \
       --region "${AWS_REGION}" >/dev/null 2>&1; then
    echo "    already exists"
  else
    aws rds create-db-instance --region "${AWS_REGION}" \
      --db-instance-identifier "${RDS_INSTANCE_ID}" \
      --db-instance-class "${RDS_INSTANCE_CLASS}" \
      --engine postgres \
      --engine-version "${RDS_ENGINE_VERSION}" \
      --master-username "${RDS_MASTER_USERNAME}" \
      --master-user-password "${RDS_MASTER_PASSWORD}" \
      --db-name "${RDS_DB_NAME}" \
      --allocated-storage "${RDS_ALLOCATED_STORAGE}" \
      --storage-type gp3 \
      --storage-encrypted \
      --db-subnet-group-name "${CLUSTER_NAME}-db" \
      --vpc-security-group-ids "${rds_sg}" \
      --no-publicly-accessible \
      --enable-iam-database-authentication \
      --backup-retention-period 7 \
      --multi-az \
      --deletion-protection >/dev/null
    echo "    waiting for instance to be available (10-15 minutes)"
    aws rds wait db-instance-available \
      --db-instance-identifier "${RDS_INSTANCE_ID}" --region "${AWS_REGION}"
  fi
}

phase_rds_grant() {
  echo "==> Granting rds-db:connect to the ate-api-server IRSA role"
  local dbi_resource_id account_id
  dbi_resource_id=$(aws rds describe-db-instances \
    --db-instance-identifier "${RDS_INSTANCE_ID}" --region "${AWS_REGION}" \
    --query 'DBInstances[0].DbiResourceId' --output text)
  account_id=$(aws sts get-caller-identity --query 'Account' --output text)

  local policy_doc
  policy_doc=$(cat <<EOF
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": ["rds-db:connect"],
    "Resource": "arn:aws:rds-db:${AWS_REGION}:${account_id}:dbuser:${dbi_resource_id}/ate_api_server"
  }]
}
EOF
)
  aws iam put-role-policy \
    --role-name "${CLUSTER_NAME}-ate-api-server" \
    --policy-name rds-db-connect \
    --policy-document "${policy_doc}"
  echo "    policy attached (used only if you switch to IAM-auth DSN)"
}

# phase_bootstrap_postgres applies postgres-bootstrap.sql against the RDS
# instance from an in-cluster psql pod. RDS is in private subnets so this
# can't run from the operator's laptop. The script templates the SQL with
# the generated substrate_readwrite password, cps it into a short-lived
# postgres:16-alpine pod, exec's psql, deletes the pod.
phase_bootstrap_postgres() {
  ensure_credentials
  echo "==> Bootstrapping Postgres schema and roles"

  local rds_endpoint
  rds_endpoint=$(aws rds describe-db-instances --db-instance-identifier "${RDS_INSTANCE_ID}" \
    --region "${AWS_REGION}" --query 'DBInstances[0].Endpoint.Address' --output text)

  # Make sure we're talking to the right cluster.
  local ctx_want="arn:aws:eks:${AWS_REGION}:$(aws sts get-caller-identity --query Account --output text):cluster/${CLUSTER_NAME}"
  kubectl config use-context "${ctx_want}" >/dev/null 2>&1 || {
    aws eks update-kubeconfig --region "${AWS_REGION}" --name "${CLUSTER_NAME}" >/dev/null
    kubectl config use-context "${ctx_want}" >/dev/null
  }

  # Idempotency: skip if the substrate schema already exists with the right
  # readwrite password that we can log in with.
  local tmp_pod="setup-aws-psql-$$"
  trap 'kubectl delete pod '"${tmp_pod}"' --ignore-not-found --wait=false >/dev/null 2>&1' RETURN

  kubectl delete pod "${tmp_pod}" --ignore-not-found >/dev/null 2>&1
  kubectl run "${tmp_pod}" --image=postgres:16-alpine --restart=Never \
    --command -- sleep 300 >/dev/null
  kubectl wait --for=condition=Ready "pod/${tmp_pod}" --timeout=120s >/dev/null

  # Template the SQL with the real readwrite password.
  local rendered
  rendered=$(sed "s/'CHANGE_ME'/'${RDS_READWRITE_PASSWORD}'/g" "${SCRIPT_DIR}/postgres-bootstrap.sql")

  # Skip if substrate schema already exists.
  local existing
  existing=$(kubectl exec "${tmp_pod}" -- sh -c "PGPASSWORD='${RDS_MASTER_PASSWORD}' psql 'host=${rds_endpoint} user=${RDS_MASTER_USERNAME} dbname=${RDS_DB_NAME} port=5432 sslmode=require' -tAc \"SELECT 1 FROM pg_namespace WHERE nspname='substrate'\"" 2>/dev/null || true)
  if [[ "${existing}" == "1" ]]; then
    echo "    substrate schema already exists; skipping bootstrap"
    return 0
  fi

  echo "${rendered}" | kubectl exec -i "${tmp_pod}" -- sh -c "PGPASSWORD='${RDS_MASTER_PASSWORD}' psql 'host=${rds_endpoint} user=${RDS_MASTER_USERNAME} dbname=${RDS_DB_NAME} port=5432 sslmode=require'"
  echo "    schema and roles created"
}

phase_summary() {
  ensure_credentials
  local account_id oidc_issuer="" rds_endpoint=""
  account_id=$(aws sts get-caller-identity --query 'Account' --output text)
  oidc_issuer=$(aws eks describe-cluster --name "${CLUSTER_NAME}" --region "${AWS_REGION}" \
    --query 'cluster.identity.oidc.issuer' --output text 2>/dev/null || true)
  rds_endpoint=$(aws rds describe-db-instances --db-instance-identifier "${RDS_INSTANCE_ID}" \
    --region "${AWS_REGION}" --query 'DBInstances[0].Endpoint.Address' --output text 2>/dev/null || true)

  cat <<EOF

=================================================================
AWS infrastructure ready.
Source these before running hack/install-ate.sh:
=================================================================

source ${CREDENTIALS_FILE}

export KUBECTL_CONTEXT="$(kubectl config current-context 2>/dev/null || echo "<run: aws eks update-kubeconfig>")"
export NO_DEV_ENV=true
export ATE_INSTALL_AWS=true
export KO_DOCKER_REPO="${account_id}.dkr.ecr.${AWS_REGION}.amazonaws.com/${ECR_REPO}"
export EXPECTED_JWT_ISSUER="${oidc_issuer:-<unknown — cluster phase not run>}"
export ATE_CREDENTIAL_PROVIDER='{"name":"k8s.io"}'
export ATE_API_SERVER_ROLE_ARN="arn:aws:iam::${account_id}:role/${CLUSTER_NAME}-ate-api-server"
export ATELET_ROLE_ARN="arn:aws:iam::${account_id}:role/${CLUSTER_NAME}-atelet"
EOF

  if [[ -n "${rds_endpoint}" && "${rds_endpoint}" != "None" ]]; then
    cat <<EOF
export ATE_API_POSTGRES_READ_WRITE_CONNECTION_STRING="postgres://substrate_readwrite:\${RDS_READWRITE_PASSWORD}@${rds_endpoint}:5432/${RDS_DB_NAME}?sslmode=require"
export ATE_API_POSTGRES_OWNER_CONNECTION_STRING="postgres://${RDS_MASTER_USERNAME}:\${RDS_MASTER_PASSWORD}@${rds_endpoint}:5432/${RDS_DB_NAME}?sslmode=require"
EOF
  fi

  cat <<EOF

# Unset GCP vars so they can't leak into the installer:
unset PROJECT_ID GCE_REGION CLUSTER_LOCATION NETWORK SUBNETWORK MEMORYSTORE_INSTANCE

EOF
}

phase_all() {
  phase_cluster
  phase_s3
  phase_ecr
  phase_rds
  phase_rds_grant
  phase_bootstrap_postgres
  phase_summary
}

main() {
  check_tools
  local phase="${1:-all}"
  case "${phase}" in
    cluster)            phase_cluster ;;
    s3)                 phase_s3 ;;
    ecr)                phase_ecr ;;
    rds)                phase_rds ;;
    rds-grant)          phase_rds_grant ;;
    bootstrap-postgres) phase_bootstrap_postgres ;;
    summary)            phase_summary ;;
    all)                phase_all ;;
    -h|--help)          usage ;;
    *)                  echo "error: unknown phase '${phase}'" >&2; usage; exit 1 ;;
  esac
}

main "$@"
