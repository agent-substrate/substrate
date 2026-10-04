# setup-aws

Bash analog of `tools/setup-gcp` for AWS EKS. Provisions the AWS
infrastructure that `hack/install-ate.sh` installs Agent Substrate onto.

See `AWS_INSTALL.md` for the full porting plan and the GCP → AWS mapping. This
directory is Phase 4 (one-time infrastructure provisioning). A future Go port
could live under `tools/setup-aws/`.

## Prerequisites

- `aws` CLI configured with credentials that can create VPC, EKS, EC2, IAM,
  S3, ECR, and RDS resources.
- `eksctl` ≥ 0.195 (handles VPC + EKS + OIDC + IRSA + core add-ons from one
  ClusterConfig).
- `kubectl`, `jq`, `envsubst` (from `gettext`).

## Usage

```bash
export AWS_REGION=us-west-2
export BUCKET_NAME=substrate-snapshots-<unique>   # globally unique
export CLUSTER_NAME=substrate-poc                 # default
export RDS_MASTER_PASSWORD=<strong-password>      # only for 'rds' phase

./setup-aws.sh all
```

Or step through phases:

```bash
./setup-aws.sh cluster      # ~15-20 minutes
./setup-aws.sh s3
./setup-aws.sh ecr
./setup-aws.sh rds          # ~10-15 minutes
./setup-aws.sh rds-grant    # attach rds-db:connect to the IRSA role
./setup-aws.sh summary      # prints the env block for hack/install-ate.sh
```

`./setup-aws.sh --help` lists every env var and default.

### Teardown

To destroy everything `setup-aws.sh` created, use the companion script:

```bash
source bin/aws-env.sh
./teardown-aws.sh all       # prompts for confirmation; type the cluster name
./teardown-aws.sh -y all    # skip the prompt (CI)
```

Individual phases run in reverse order of create: `rds`, `s3`, `ecr`,
`cluster`, `iam-cleanup`. See `./teardown-aws.sh --help`. The script
handles the two gotchas:

- RDS deletion protection is disabled first (then the modify is awaited)
  before `delete-db-instance`, and the DB subnet group and dedicated RDS
  security group are removed afterwards (they live outside the eksctl
  CloudFormation stack).
- The S3 bucket is versioned, so all object versions and delete markers are
  purged in batches before `delete-bucket`.

## What gets created

- **VPC** across 3 AZs, public + private subnets, one NAT per AZ, subnet tags
  for the AWS Load Balancer Controller.
- **EKS cluster** at `K8S_VERSION` (default `1.33` — must serve
  `certificates.k8s.io/v1beta1`; see `AWS_INSTALL.md` §3) with the OIDC
  provider enabled.
- **Two managed node groups** (`control-plane`, `workers`), both labeled
  `ate.dev/role=*`, both pre-creating `/var/lib/ate` on the node. See
  `cluster.yaml.tmpl` for a commented example of a bare-metal `workers-microvm`
  node group with the `ate.dev/sandboxClass=microvm:NoSchedule` taint.
- **Core add-ons**: `vpc-cni` with NetworkPolicy, `coredns`, `kube-proxy`,
  `aws-ebs-csi-driver`, `eks-pod-identity-agent`.
- **IRSA roles** for `ate-system/atelet` and `ate-system/ate-api-server` with
  S3 R/W on the snapshot bucket. eksctl annotates the KSAs with
  `eks.amazonaws.com/role-arn=...` at install time.
- **gp3 StorageClass** marked default (replaces gp2); the bundled Postgres
  StatefulSet requests a 500Gi PVC through it.
- **S3 bucket** with BlockPublicAccess (all four), SSE-S3 default encryption,
  versioning enabled.
- **ECR repository** with image scanning on push.
- **RDS Postgres** instance: Multi-AZ, private subnets, IAM auth enabled,
  encrypted gp3 storage, 7-day automated backups, deletion protection on.
  A dedicated security group allows `5432` from the EKS node security group.
- **`rds-db:connect`** policy attached to the `ate-api-server` IRSA role,
  scoped to `dbuser:<DbiResourceId>/ate_api_server` (if you choose the IAM
  login path in `postgres-bootstrap.sql`).

## What this script does *not* do

Deliberately out of scope — install these separately:

- **AWS Load Balancer Controller** — needed to expose `atenet-router`
  externally. Install via Helm after the cluster is up.
- **ADOT (OpenTelemetry) Collector** — point `ate-otel-config` at your
  collector endpoint via the `aws` kustomize overlay (`AWS_INSTALL.md` §6c),
  or pass `--otlp-endpoint=` to the installer to skip the OTel patch.
- **EFS CSI driver** — needed only if you'll register an EFS-backed
  `CSIDriverConfig` for actor RWX volumes.
- **gVisor / microVM assets** — mirror them from `gs://gvisor/...` into S3
  per `AWS_INSTALL.md` §6e. The `WORKER_INSTANCE_TYPE` default (`m6i.xlarge`)
  does NOT expose `/dev/kvm`; switch to `*.metal` for microVM workloads.
- **Postgres schema and roles** — after `rds`, run
  `postgres-bootstrap.sql` as the master user.

## Files

| File | Purpose |
|---|---|
| `setup-aws.sh` | Orchestrator with per-phase subcommands |
| `cluster.yaml.tmpl` | eksctl ClusterConfig template (envsubst) |
| `postgres-bootstrap.sql` | One-time schema and role setup for RDS |
| `README.md` | This file |

Rendered output lives in `bin/aws-cluster.yaml` (gitignored).

## Teardown

See `teardown-aws.sh` (documented under [Usage → Teardown](#teardown) above).
