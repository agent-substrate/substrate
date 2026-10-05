# setup-aws

One-shot provisioner for the AWS infrastructure Agent Substrate needs to run
on EKS. Analog of `tools/setup-gcp`, written as bash around the `aws` CLI and
`eksctl` instead of Go.

Running it stands up a complete, correctly-configured EKS environment in
~25-30 minutes; after it finishes you can go straight to
`hack/install-ate.sh --deploy-ate-system` to install the substrate control
plane.

## Prerequisites

- `aws` CLI, configured with credentials that can create VPC, EKS, EC2, IAM,
  S3, ECR, and RDS resources.
- `eksctl` ≥ 0.195.
- `kubectl`, `jq`, `envsubst` (ships with GNU `gettext`), `openssl`.

## Quick start

```bash
AWS_REGION=us-west-2 ./tools/setup-aws/setup-aws.sh
```

That's it. The script auto-generates every secret it needs (S3 bucket name,
RDS master password, substrate_readwrite password), persists them to
`bin/aws-env.sh`, runs every phase in sequence, and prints the environment
block you need to feed into `hack/install-ate.sh` at the end.

Re-runs are idempotent: each phase skips what already exists, and the
generated secrets are reused from `bin/aws-env.sh` so Postgres doesn't get
locked out if you re-run.

## What it provisions

| Resource | Notes |
|---|---|
| **VPC** | 3 AZs, public + private subnets, one NAT per AZ, subnet-tagged for the AWS Load Balancer Controller |
| **EKS cluster** | `K8S_VERSION` (default `1.37` — serves `certificates.k8s.io/v1 PodCertificateRequest`, which substrate's podcertificate-controller needs) with the OIDC provider enabled |
| **Managed node groups** | `control-plane` and `workers`, both `m6i.xlarge` by default, both pre-creating `/var/lib/ate` on the node via user-data. See `cluster.yaml.tmpl` for a commented bare-metal `workers-microvm` example. |
| **Core add-ons** | `vpc-cni` with NetworkPolicy enabled, `coredns`, `kube-proxy`, `aws-ebs-csi-driver`, `eks-pod-identity-agent` |
| **IRSA roles** | `ate-system/atelet` and `ate-system/ate-api-server` with S3 read/write on the snapshot bucket. The atelet role also carries `AmazonEC2ContainerRegistryReadOnly` — the kubelet image-credential-provider runs as atelet's subprocess, so actor image pulls use atelet's IRSA role, not the node role. |
| **Default StorageClass** | `gp3` (replaces `gp2`), encrypted |
| **S3 bucket** | BlockPublicAccess on all four, SSE-S3 default encryption, versioning enabled |
| **ECR repository** | Image scanning on push |
| **RDS PostgreSQL** | Multi-AZ, private subnets only, IAM auth enabled, encrypted gp3 storage, 7-day automated backups, deletion protection on. Dedicated security group allows `5432` from the EKS node security group. |
| **Postgres bootstrap** | `substrate_owner` and `substrate_readwrite` roles created, `substrate` schema owned by `substrate_owner`, default privileges set up so new DDL from the app is visible to the read-write role. Applied through an in-cluster psql pod. |
| **`rds-db:connect` IAM policy** | Attached to the `ate-api-server` IRSA role, scoped to `dbuser:<DbiResourceId>/ate_api_server`. Only exercised if you flip the DSN to IAM-auth; by default the generated password is what the installer embeds. |

## Phased invocation (debugging)

The default is to run all phases in order. For partial re-runs you can
invoke any single phase by name:

```bash
./tools/setup-aws/setup-aws.sh cluster              # ~15-20 min
./tools/setup-aws/setup-aws.sh s3
./tools/setup-aws/setup-aws.sh ecr
./tools/setup-aws/setup-aws.sh rds                  # ~10-15 min
./tools/setup-aws/setup-aws.sh rds-grant            # attach IAM-auth binding
./tools/setup-aws/setup-aws.sh bootstrap-postgres   # schema + roles
./tools/setup-aws/setup-aws.sh summary              # env block for hack/install-ate.sh
```

Each phase is idempotent and reads its secrets from the shared
`bin/aws-env.sh` written on the first `setup-aws.sh` run.

`./tools/setup-aws/setup-aws.sh --help` lists every env var and default.

## Teardown

```bash
./tools/setup-aws/teardown-aws.sh         # typed-cluster-name confirmation
./tools/setup-aws/teardown-aws.sh -y all  # skip the prompt (CI)
```

Phases run in reverse of create order: `rds`, `s3`, `ecr`, `cluster`,
`iam-cleanup`. It handles the two awkward bits that `aws` CLI one-liners
miss:

- RDS has deletion protection on by default. Teardown turns it off, waits
  for the modify to settle, then issues the delete with
  `--skip-final-snapshot --delete-automated-backups`. The DB subnet group
  and dedicated RDS security group are dropped after the instance is gone
  (they live outside the eksctl CloudFormation stack).
- The S3 bucket is versioned, so a plain `aws s3 rb --force` leaves object
  versions and delete markers behind and the bucket delete fails. Teardown
  pages through `list-object-versions` and `delete-objects` in batches of
  1000 before `delete-bucket`.

Teardown does **not** touch `bin/aws-env.sh` or any resources that don't
match the expected cluster/bucket/repo names.

## What this script does *not* do

Deliberately out of scope — install these separately when you need them:

- **AWS Load Balancer Controller** — needed to expose `atenet-router`
  externally. Install via Helm after the cluster is up.
- **ADOT (OpenTelemetry) Collector** — point `ate-otel-config` at your
  collector via `--otlp-endpoint=` at install time, or install ADOT and
  patch the ConfigMap.
- **EFS CSI driver** — needed only if you'll register an EFS-backed
  `CSIDriverConfig` for actor RWX volumes.
- **gVisor / microVM assets** — mirror them from `gs://gvisor/...` into S3
  and update `manifests/ate-install/sandboxconfig-gvisor.yaml` to point at
  the mirror. The default `WORKER_INSTANCE_TYPE` (`m6i.xlarge`) does NOT
  expose `/dev/kvm`; switch to a `*.metal` type for microVM workloads.

## Files

| File | Purpose |
|---|---|
| `setup-aws.sh` | One-shot orchestrator (also supports per-phase invocation) |
| `teardown-aws.sh` | Dangerous counterpart |
| `cluster.yaml.tmpl` | `eksctl` ClusterConfig template, rendered by envsubst |
| `postgres-bootstrap.sql` | Schema + role setup, applied by the `bootstrap-postgres` phase |
| `README.md` | This file |

Rendered + generated artifacts land under `bin/` at the repo root
(gitignored):

- `bin/aws-cluster.yaml` — envsubst'd `ClusterConfig` for `eksctl`
- `bin/aws-env.sh` — generated secrets + reusable env for subsequent runs
  and for sourcing into shells that invoke `hack/install-ate.sh`

## After the infrastructure is up

See `AWS_INSTALL.md` at the repo root for the full install walkthrough —
what the `aws` kustomize overlay (committed) does to adapt the base
manifests to EKS, what env the installer needs, and the known gotchas (ECR
auth for the atelet IRSA role, llama.cpp needing `--no-mmap` so the model
survives gVisor memory snapshots, and so on).
