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
# Dangerous counterpart to setup-aws.sh. Destroys the AWS infrastructure that
# setup-aws.sh provisioned, in reverse order. Honors the same env vars as
# setup-aws.sh (AWS_REGION, CLUSTER_NAME, BUCKET_NAME, ECR_REPO, RDS_INSTANCE_ID)
# so you can `source bin/aws-env.sh` and run this directly. See
# tools/setup-aws/README.md for usage.

set -o errexit -o nounset -o pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

usage() {
  cat <<EOF
Usage: $(basename "$0") [--yes|-y] <phase>

Phases (reverse order of setup-aws.sh):
  rds           Disable deletion protection, delete RDS instance, DB subnet
                group, RDS security group
  s3            Empty all object versions + delete markers, then delete the
                S3 snapshot bucket
  ecr           Delete ECR repository (force; removes all images)
  cluster       eksctl delete cluster (VPC, node groups, IRSA, add-ons, OIDC)
  iam-cleanup   Belt-and-braces: verify the IRSA roles are gone; detach the
                rds-db-connect inline policy and delete leftover roles
  all           rds + s3 + ecr + cluster + iam-cleanup

Flags:
  --yes, -y     Skip the interactive confirmation prompt (for CI)

Required env (same as setup-aws.sh):
  AWS_REGION    AWS region (e.g. us-west-2)

Env with defaults (must match what setup-aws.sh used):
  CLUSTER_NAME       (default: substrate-poc)
  BUCKET_NAME        (no default — required for 's3' and 'all')
  ECR_REPO           (default: substrate)
  RDS_INSTANCE_ID    (default: substrate-poc)

Safety:
  This script is destructive and irreversible. It will NOT touch:
    - bin/aws-env.sh or any credentials file
    - any resource not matching the expected names above

Examples:
  source bin/aws-env.sh
  AWS_REGION=us-west-2 BUCKET_NAME=substrate-snaps-acme-dev $(basename "$0") all
  AWS_REGION=us-west-2 $(basename "$0") -y cluster
EOF
}

ASSUME_YES=false
while [[ $# -gt 0 && "$1" =~ ^- ]]; do
  case "$1" in
    -y|--yes)   ASSUME_YES=true; shift ;;
    -h|--help)  usage; exit 0 ;;
    *)          echo "error: unknown flag '$1'" >&2; usage; exit 1 ;;
  esac
done

: "${AWS_REGION:?AWS_REGION must be set}"
CLUSTER_NAME="${CLUSTER_NAME:-substrate-poc}"
BUCKET_NAME="${BUCKET_NAME:-}"
ECR_REPO="${ECR_REPO:-substrate}"
RDS_INSTANCE_ID="${RDS_INSTANCE_ID:-substrate-poc}"

check_tools() {
  local missing=()
  for t in aws eksctl jq; do
    command -v "$t" >/dev/null 2>&1 || missing+=("$t")
  done
  if [[ ${#missing[@]} -gt 0 ]]; then
    echo "error: missing tools: ${missing[*]}" >&2
    echo "       install eksctl: https://eksctl.io/" >&2
    exit 1
  fi
}

require_bucket_name() {
  [[ -n "${BUCKET_NAME}" ]] || { echo "error: BUCKET_NAME must be set" >&2; exit 1; }
}

confirm() {
  if [[ "${ASSUME_YES}" == "true" ]]; then
    return 0
  fi
  cat <<EOF

================================================================
  DANGER: this will PERMANENTLY DELETE the following AWS resources
================================================================

  Region:            ${AWS_REGION}
  EKS cluster:       ${CLUSTER_NAME}  (plus its VPC, node groups, IRSA roles)
  S3 bucket:         ${BUCKET_NAME:-<unset — s3 phase will error>}
                     (all object versions and delete markers will be purged)
  ECR repository:    ${ECR_REPO}  (all images)
  RDS instance:      ${RDS_INSTANCE_ID}  (deletion protection will be disabled)
  RDS subnet group:  ${CLUSTER_NAME}-db
  RDS security grp:  ${CLUSTER_NAME}-rds

Snapshots and automated backups will NOT be retained.
This is irreversible. There is no undo.

To proceed, type the cluster name exactly: ${CLUSTER_NAME}
EOF
  local answer
  read -r -p "> " answer
  if [[ "${answer}" != "${CLUSTER_NAME}" ]]; then
    echo "aborted: input did not match cluster name" >&2
    exit 1
  fi
}

# phase_rds: disable deletion protection, delete the instance, wait for it to
# be gone, then drop the DB subnet group and the dedicated RDS security group.
# Both the subnet group and the SG live outside the eksctl CloudFormation stack
# (setup-aws.sh:phase_rds created them directly), so eksctl delete cluster
# won't touch them.
phase_rds() {
  echo "==> Tearing down RDS instance ${RDS_INSTANCE_ID}"
  if aws rds describe-db-instances --db-instance-identifier "${RDS_INSTANCE_ID}" \
       --region "${AWS_REGION}" >/dev/null 2>&1; then
    local protection
    protection=$(aws rds describe-db-instances \
      --db-instance-identifier "${RDS_INSTANCE_ID}" --region "${AWS_REGION}" \
      --query 'DBInstances[0].DeletionProtection' --output text)
    if [[ "${protection}" == "True" || "${protection}" == "true" ]]; then
      echo "    disabling deletion protection"
      aws rds modify-db-instance --region "${AWS_REGION}" \
        --db-instance-identifier "${RDS_INSTANCE_ID}" \
        --no-deletion-protection --apply-immediately >/dev/null
      echo "    waiting for modify-db-instance to settle"
      aws rds wait db-instance-available \
        --db-instance-identifier "${RDS_INSTANCE_ID}" --region "${AWS_REGION}"
    fi

    echo "    deleting instance (skip final snapshot, drop automated backups)"
    aws rds delete-db-instance --region "${AWS_REGION}" \
      --db-instance-identifier "${RDS_INSTANCE_ID}" \
      --skip-final-snapshot --delete-automated-backups >/dev/null
    echo "    waiting for instance to be fully deleted (5-10 minutes)"
    aws rds wait db-instance-deleted \
      --db-instance-identifier "${RDS_INSTANCE_ID}" --region "${AWS_REGION}"
  else
    echo "    not found; skipping"
  fi

  echo "==> Deleting RDS DB subnet group ${CLUSTER_NAME}-db"
  if aws rds describe-db-subnet-groups --db-subnet-group-name "${CLUSTER_NAME}-db" \
       --region "${AWS_REGION}" >/dev/null 2>&1; then
    aws rds delete-db-subnet-group --region "${AWS_REGION}" \
      --db-subnet-group-name "${CLUSTER_NAME}-db" >/dev/null
  else
    echo "    not found; skipping"
  fi

  echo "==> Deleting RDS security group ${CLUSTER_NAME}-rds"
  # The SG lives in the cluster VPC. Scope the lookup to that VPC so a
  # same-named SG in another VPC (e.g. a parallel test cluster in the same
  # account) can't be matched by accident. If the cluster is already gone the
  # VPC is gone with it, and both describes below return None.
  local cluster_vpc rds_sg
  cluster_vpc=$(aws eks describe-cluster --name "${CLUSTER_NAME}" --region "${AWS_REGION}" \
    --query 'cluster.resourcesVpcConfig.vpcId' --output text 2>/dev/null || true)
  if [[ -z "${cluster_vpc}" || "${cluster_vpc}" == "None" ]]; then
    echo "    cluster VPC not found; skipping (SG went with the VPC)"
    return 0
  fi
  rds_sg=$(aws ec2 describe-security-groups --region "${AWS_REGION}" \
    --filters "Name=vpc-id,Values=${cluster_vpc}" "Name=group-name,Values=${CLUSTER_NAME}-rds" \
    --query 'SecurityGroups[0].GroupId' --output text 2>/dev/null || true)
  if [[ -n "${rds_sg}" && "${rds_sg}" != "None" ]]; then
    aws ec2 delete-security-group --region "${AWS_REGION}" --group-id "${rds_sg}" \
      || echo "    warning: delete-security-group failed; may still be in-use"
  else
    echo "    not found; skipping"
  fi
}

# phase_s3: the bucket has versioning enabled, so a bare `aws s3 rb --force`
# leaves object versions and delete markers behind and the bucket delete
# itself fails. We page through all versions + delete markers in batches of
# up to 1000 and feed them to delete-objects, then rb.
phase_s3() {
  require_bucket_name
  echo "==> Emptying versioned S3 bucket ${BUCKET_NAME}"
  if ! aws s3api head-bucket --bucket "${BUCKET_NAME}" --region "${AWS_REGION}" 2>/dev/null; then
    echo "    not found; skipping"
    return 0
  fi

  while :; do
    local payload
    payload=$(aws s3api list-object-versions --bucket "${BUCKET_NAME}" \
      --region "${AWS_REGION}" --max-items 1000 \
      --query '{Objects: (Versions || `[]`)[].{Key: Key, VersionId: VersionId} + (DeleteMarkers || `[]`)[].{Key: Key, VersionId: VersionId}}' \
      --output json)
    local count
    count=$(echo "${payload}" | jq -r '.Objects | length')
    if [[ "${count}" == "0" ]]; then
      break
    fi
    echo "    deleting ${count} object version(s) / delete marker(s)"
    aws s3api delete-objects --bucket "${BUCKET_NAME}" --region "${AWS_REGION}" \
      --delete "$(echo "${payload}" | jq -c '. + {Quiet: true}')" >/dev/null
  done

  echo "==> Deleting bucket ${BUCKET_NAME}"
  aws s3api delete-bucket --bucket "${BUCKET_NAME}" --region "${AWS_REGION}"
}

phase_ecr() {
  echo "==> Deleting ECR repository ${ECR_REPO}"
  if aws ecr describe-repositories --repository-names "${ECR_REPO}" \
       --region "${AWS_REGION}" >/dev/null 2>&1; then
    aws ecr delete-repository --repository-name "${ECR_REPO}" \
      --region "${AWS_REGION}" --force >/dev/null
  else
    echo "    not found; skipping"
  fi
}

# phase_cluster: eksctl delete cluster tears down the VPC, both managed node
# groups, the OIDC provider, IRSA roles, and the core add-ons. ~10-15 min.
phase_cluster() {
  echo "==> Deleting EKS cluster ${CLUSTER_NAME} in ${AWS_REGION} (10-15 minutes)"
  if eksctl get cluster --name "${CLUSTER_NAME}" --region "${AWS_REGION}" >/dev/null 2>&1; then
    eksctl delete cluster --name "${CLUSTER_NAME}" --region "${AWS_REGION}" \
      --disable-nodegroup-eviction --wait
  else
    echo "    not found; skipping"
  fi
}

# phase_iam_cleanup: eksctl should have removed the IRSA roles as part of
# delete cluster, but if a reconcile left orphans (or the rds-grant inline
# policy on ate-api-server blocked the implicit delete), clean them up here.
# The rds-db-connect inline policy must be removed before delete-role.
phase_iam_cleanup() {
  echo "==> Verifying IRSA role cleanup"
  for role in "${CLUSTER_NAME}-atelet" "${CLUSTER_NAME}-ate-api-server"; do
    if aws iam get-role --role-name "${role}" >/dev/null 2>&1; then
      echo "    found leftover role ${role}; detaching policies and deleting"
      local inline_policies managed_policies
      inline_policies=$(aws iam list-role-policies --role-name "${role}" \
        --query 'PolicyNames[]' --output text 2>/dev/null || true)
      for p in ${inline_policies}; do
        [[ -z "${p}" ]] && continue
        echo "      delete-role-policy ${role}/${p}"
        aws iam delete-role-policy --role-name "${role}" --policy-name "${p}" || true
      done
      managed_policies=$(aws iam list-attached-role-policies --role-name "${role}" \
        --query 'AttachedPolicies[].PolicyArn' --output text 2>/dev/null || true)
      for arn in ${managed_policies}; do
        [[ -z "${arn}" ]] && continue
        echo "      detach-role-policy ${role} ${arn}"
        aws iam detach-role-policy --role-name "${role}" --policy-arn "${arn}" || true
      done
      aws iam delete-role --role-name "${role}" || true
    else
      echo "    ${role}: gone (good)"
    fi
  done
}

phase_all() {
  phase_rds
  phase_s3
  phase_ecr
  phase_cluster
  phase_iam_cleanup
  echo
  echo "==> Teardown complete."
  echo "    Note: bin/aws-env.sh and bin/aws-cluster.yaml were left in place."
}

main() {
  [[ $# -ge 1 ]] || { usage; exit 1; }
  check_tools
  # Validate phase before prompting for confirmation.
  case "$1" in
    rds|s3|ecr|cluster|iam-cleanup|all) ;;
    -h|--help) usage; exit 0 ;;
    *) echo "error: unknown phase '$1'" >&2; usage; exit 1 ;;
  esac
  confirm
  case "$1" in
    rds)         phase_rds ;;
    s3)          phase_s3 ;;
    ecr)         phase_ecr ;;
    cluster)     phase_cluster ;;
    iam-cleanup) phase_iam_cleanup ;;
    all)         phase_all ;;
  esac
}

main "$@"
