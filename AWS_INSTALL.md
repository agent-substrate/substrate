# Agent Substrate on AWS EKS — Installation Plan

> **Status: planning document.** The upstream installer today supports two
> targets: local `kind` (`hack/install-ate-kind.sh`) and GKE via
> `tools/setup-gcp` + `hack/install-ate.sh`. There is no AWS target. This
> document is a step-by-step plan for porting the install to EKS, derived from
> an analysis of `hack/create-kind-cluster.sh`, `hack/install-ate.sh`, the
> `cmd/ate-setup` Go installer, and `tools/setup-gcp`.
>
> **Load-bearing manifest/code edits are called out inline** so the reader can
> stop reading at any point and know what still needs to change.

---

## 1. What we are installing

The substrate control plane consists of these components (all in namespace
`ate-system` unless noted):

| Component | Kind | Purpose |
|---|---|---|
| CRDs: `workerpools.ate.dev`, `sandboxconfigs.ate.dev`, `csidriverconfigs.ate.dev` | Cluster | Substrate API surface |
| `podcertificate-controller` (ns `podcertificate-controller-system`) | Deployment | Signs `PodCertificateRequest`s; publishes `ClusterTrustBundle`s used for all mTLS |
| `ate-api-server` | Deployment (2 replicas) | Public gRPC API; owns Postgres and S3/GCS state |
| `ate-controller` | Deployment | Reconciles `WorkerPool` CRs → Jobs, NetworkPolicies |
| `atelet` | DaemonSet | Per-node agent; manages actor filesystems + snapshots + CSI mounts |
| `atenet-router` | Deployment (+ envoy or agentgateway) | North–south ingress to actor pods |
| `atenet-egress` | Deployment (envoy + sdsmint + ext-proc) | mTLS-interception egress gateway for actor outbound traffic |
| `k8s-credential-provider` | Deployment | Serves Secret-backed credentials to `atenet-egress` |
| `SandboxConfig/gvisor-default` + `ValidatingAdmissionPolicy` | Cluster | Admission policy over Pod sandbox classes |
| Postgres (bundled StatefulSet) *or* external DSN *or* Cloud SQL/RDS | — | API state |
| OpenTelemetry collector | Deployment | Metrics/traces/logs forwarding |

Everything else — the actor pods — is spawned by `ate-controller` from
`WorkerPool` CRs at runtime.

---

## 2. GCP → AWS infrastructure mapping

| GCP concept (what the current installer assumes) | AWS equivalent |
|---|---|
| GKE cluster with WorkloadIdentityConfig `${PROJECT_ID}.svc.id.goog` | EKS cluster with OIDC provider + **IRSA** or **EKS Pod Identity** |
| GKE beta API enablement (`certificates.k8s.io/v1beta1`) at create time | EKS 1.37+ (serves `PodCertificateRequest` and `ClusterTrustBundle` v1 natively). 1.33 is alpha-only and EKS doesn't let you flip the feature gate — podcertificate-controller CrashLoops. |
| GKE Dataplane V2 (eBPF/Cilium) | VPC CNI (default) with NetworkPolicy enabled, or Cilium add-on |
| GKE Managed OpenTelemetry | AWS Distro for OpenTelemetry (ADOT) add-on → AMP + CloudWatch |
| GCS bucket (snapshots) | S3 bucket in cluster region, BlockPublicAccess on all four, bucket policy only |
| Artifact Registry | ECR (one repo per image, or one repo with tags) |
| Workload Identity: `principal://.../ns/<ns>/sa/<ksa>` → `roles/storage.objectAdmin` | IRSA role trust policy conditioning `sub = system:serviceaccount:<ns>:<ksa>` → inline S3 policy |
| GKE kubelet image-credential-provider at `/home/kubernetes/bin` + `/etc/srv/kubernetes/cri_auth_config.yaml` | EKS kubelet image-credential-provider at `/etc/eks/image-credential-provider/` (bin + `config.json`). **Load-bearing — atelet host-mounts these paths; see §6b.** |
| `AdvancedMachineFeatures.EnableNestedVirtualization=true` on `c3-standard-4` | Bare-metal EC2 (`*.metal`) or a Nitro type that passes through `/dev/kvm`. **Required for microVM worker pods**; gVisor workers do not need it. |
| Cloud SQL for Postgres (private IP, IAM auth, Auth Proxy sidecar) | RDS for Postgres in private subnet, `EnableIAMDatabaseAuthentication=true`, IAM auth token minted via `aws rds generate-db-auth-token`, optional RDS Proxy |
| Cloud Monitoring dashboards | CloudWatch dashboards or Grafana (AMG). Dashboard JSON schema differs — port manually. |

---

## 3. Prerequisites

**Tools (local):** `aws` CLI, `kubectl`, `eksctl` (or Terraform), `helm`, `go`
(for the installer), `docker` + `buildx` (for the envoy data-plane image),
`ko` (vendored via `hack/run-tool.sh`), `jq`.

**AWS account:** permission to create VPC, EKS, EC2, IAM, S3, ECR, RDS,
CloudWatch, and (optionally) Secrets Manager + AMP + AMG.

**Pick a region** that supports the EC2 families you need for microVM
workers (bare-metal availability varies by region).

**Pick an EKS version:** target **1.37 or later**. The certificate APIs
substrate needs (`certificates.k8s.io/v1 PodCertificateRequest` and
`ClusterTrustBundle`) are stable from 1.37, beta from 1.34, and alpha only
in 1.33. EKS doesn't let you flip the alpha feature gate, so on 1.33 the
podcertificate-controller CrashLoops with "neither v1 nor v1beta1
PodCertificateRequest is served" and the installer times out waiting for
the Deployment to come ready. 1.34–1.36 may serve v1beta1 by default
(unverified against EKS); 1.37 is the known-good choice.

---

## 4. Phase 1 — AWS infrastructure (one-time per environment)

This is the moral equivalent of `go run ./tools/setup-gcp bootstrap` plus
`create cloudsql`. There is no Go tool for this yet; write it as Terraform /
CDK / `eksctl` config, or run the `aws` CLI steps below by hand.

### 4.1 VPC

- 3 AZs, public + private subnets per AZ.
- 1 NAT Gateway per AZ (private subnets need egress for image pulls and OTel export).
- Tag private subnets `kubernetes.io/role/internal-elb=1`, public subnets `kubernetes.io/role/elb=1` so the AWS Load Balancer Controller can discover them later.

### 4.2 EKS cluster

```bash
eksctl create cluster \
  --name substrate-poc \
  --version 1.37 \
  --region us-west-2 \
  --vpc-private-subnets=... --vpc-public-subnets=... \
  --with-oidc \
  --without-nodegroup
```

- `--with-oidc` is **mandatory** — this is what IRSA binds against.
- Install the EKS Pod Identity Agent add-on (or stick with IRSA; pick one and
  be consistent — the installer only cares about the ServiceAccount
  annotations).

### 4.3 Node groups

Create **two** managed node groups (three if you cordon the control plane —
see §4.4):

**(a) Control-plane / general workloads**
- Instance type: `m6i.xlarge` (adjust for your workload).
- Labels: `ate.dev/substrate-version=<version>` *(see §6c — this label is
  required on every node that will run `atelet`)*.
- Optionally: `ate.dev/workloadType=ate-control-plane:NoSchedule` taint
  and label, if `--cordon-control-plane` is set.
- Attach `AmazonEC2ContainerRegistryReadOnly` to the node role (via
  managed node group default).
- User-data: ensure `/var/lib/ate` is pre-created (the atelet DaemonSet
  bind-mounts it) and the EKS kubelet image-credential-provider is present
  at `/etc/eks/image-credential-provider/` (default on the EKS AMI).

**(b) Actor-worker pool (optionally bare-metal for microVM)**
- For gVisor-only workloads: standard Nitro instances work. For microVM:
  `*.metal` (e.g. `m5.metal`, `m5zn.metal`).
- Taint: `ate.dev/sandboxClass=<class>:NoSchedule` so only matching
  WorkerPools land here (`atelet` tolerates `ate.dev/sandboxClass=*`).
- Same `ate.dev/substrate-version` label.

### 4.4 Optional: Postgres node pool

If you plan to run the **bundled** Postgres StatefulSet (not RDS), make a
single-node group with taint `ate.dev/workloadType=ate-postgres:NoSchedule`
and the matching label, and pass `--cordon-control-plane` to the installer.
Otherwise skip this and use RDS (recommended for anything beyond a POC).

### 4.5 Cluster add-ons

- **AWS EBS CSI driver** (`aws-ebs-csi-driver` add-on) — the bundled Postgres
  StatefulSet asks for a 500Gi PVC (`manifests/ate-install/postgres/postgres.yaml:242-253`).
  Make `gp3` the default StorageClass.
- **AWS VPC CNI** with `ENABLE_NETWORK_POLICY=true` — the installer applies
  NetworkPolicies restricting `k8s-credential-provider` to the egress pod.
- **ADOT Operator** (if you want metrics/traces) and an `OpenTelemetryCollector`
  CR; otherwise pass `--otlp-endpoint=` at install time to skip the OTel
  patch.
- **AWS Load Balancer Controller** — needed later to expose `atenet-router`.
- Optional: **EFS CSI driver** if actor workloads will use RWX volumes
  through `CSIDriverConfig`.

### 4.6 S3 snapshot bucket

```bash
aws s3api create-bucket --bucket substrate-snapshots-<unique> --region us-west-2 \
  --create-bucket-configuration LocationConstraint=us-west-2
aws s3api put-public-access-block --bucket substrate-snapshots-<unique> \
  --public-access-block-configuration BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
```

Equivalent of `tools/setup-gcp/cmd/bucket.go` with UniformBucketLevelAccess.

### 4.7 ECR repos

One per substrate image (`ateapi`, `atecontroller`, `atelet`, `atenet`,
`podcertcontroller`, `envoy-dataplane`, …), or one shared repo. `ko` will
push via `KO_DOCKER_REPO=<acct>.dkr.ecr.<region>.amazonaws.com/substrate`.

### 4.8 IAM — IRSA roles

Create two IAM roles, one per Kubernetes ServiceAccount. Trust policy on
each role conditions `sub` on the KSA.

**Role for `ate-system/atelet`:**

- S3 on snapshot bucket: `s3:ListBucket`, `s3:GetObject`, `s3:PutObject`,
  `s3:DeleteObject`, `s3:AbortMultipartUpload`.
- `AmazonEC2ContainerRegistryReadOnly` (managed policy). The kubelet
  image-credential-provider runs as a *subprocess* of atelet, so when
  atelet pulls an actor image (e.g. `ateom-gvisor`), the plugin inherits
  atelet's IRSA credentials, not the node role's. Without this, image
  pulls fail with 401 Unauthorized and the golden snapshot never completes.

**Role for `ate-system/ate-api-server`:**

- S3 on snapshot bucket: same five permissions as `atelet` (needed for
  external-snapshot copy, tag updates, orphan deletion — see the GCP
  equivalent `tools/setup-gcp/README.md:208-221`).
- If using RDS IAM auth: `rds-db:connect` on
  `arn:aws:rds-db:<region>:<acct>:dbuser:<db-resource-id>/<db-user>`.

The installer discovers these via the KSA annotations
`eks.amazonaws.com/role-arn=...`. There is no Go code to change here —
IRSA is wire-compatible with the installer's existing
`atelet` / `ate-api-server` manifests, which already consume AWS SDK env
vars when `ATE_STORAGE_BACKEND=s3` (the kind path sets this; see §6a).

### 4.9 RDS for PostgreSQL (recommended)

- Engine: Postgres 15+ (upstream pins 18 for Cloud SQL — any supported
  version works for the schema).
- Multi-AZ, private subnets only, DB subnet group across your AZs.
- **`EnableIAMDatabaseAuthentication=true`** — this is the AWS analogue of
  Cloud SQL's `cloudsql.iam_authentication=on`.
- Create role `substrate_owner` and `substrate_readwrite`, create database
  user mapped to IAM (follow the pattern in `tools/setup-gcp/cloud-sql.md`
  §2 and run the equivalent SQL against RDS).
- Store the DSN in Secrets Manager if you prefer; the installer will take it
  as `ATE_API_POSTGRES_READ_WRITE_CONNECTION_STRING`.

**Simplest path:** connect ate-api-server directly to RDS using IAM auth
tokens (minted by a tiny sidecar or an initContainer that writes the token
into a shared volume). This sidesteps the Cloud SQL Auth Proxy branch in
the installer entirely (`cmd/ate-setup/internal/steps/cloudsql.go:37-39`
hardcodes the GKE annotation `iam.gke.io/gcp-service-account` and will try
to add the Cloud SQL Auth Proxy image if `ATE_API_POSTGRES_CLOUDSQL_INSTANCE`
is set — leave that env var unset on AWS).

---

## 5. Phase 2 — Build and push images

With AWS infra up:

```bash
aws ecr get-login-password --region us-west-2 \
  | docker login --username AWS --password-stdin <acct>.dkr.ecr.us-west-2.amazonaws.com

export KO_DOCKER_REPO=<acct>.dkr.ecr.us-west-2.amazonaws.com/substrate
export KO_DEFAULTPLATFORMS=linux/amd64,linux/arm64
```

`ko` is invoked internally by `cmd/ate-setup/internal/ko/` to resolve every
`ko://github.com/agent-substrate/substrate/cmd/...` reference against
`KO_DOCKER_REPO` and push.

**envoy-dataplane** is built via `docker buildx` (not `ko`) from
`cmd/dataplane/envoy/` because it embeds a Rust-built dynamic egress-policy
module. Ensure `buildx` is installed; the install script will build + push
it to `KO_DOCKER_REPO` too.

Alternative: use `--image-repo` + `--image-tag` to pull pre-published images
instead of building locally.

---

## 6. Phase 3 — Create a kustomize overlay for AWS

The installer today picks between four kustomize overlays —
`base | kind | agentgateway | kind-agentgateway` — selected in
`cmd/ate-setup/internal/steps/overlay.go` by `IsKind()` and the
`--atenet-dataplane` flag. The clean port is to add a fifth: `aws` (and
`aws-agentgateway`). It parallels the existing `kind/` overlay but points at
real S3 + IRSA instead of in-cluster rustfs + static credentials.

### 6a. Overlay layout

```
manifests/ate-install/aws/
├── kustomization.yaml          # patches ate-api-server + ate-otel-config + KSAs
└── atelet/
    └── kustomization.yaml      # patches atelet DaemonSet (env, hostPaths, KSA)
```

### 6b. Top-level `aws/kustomization.yaml`

Mirrors `manifests/ate-install/kind/kustomization.yaml:49-76` but strips the
rustfs-specific pieces:

- Patch `ate-api-server` env:
  - `ATE_STORAGE_BACKEND=s3`
  - `AWS_REGION=${AWS_REGION}`
  - **Drop** `AWS_ENDPOINT_URL`, `AWS_S3_USE_PATH_STYLE`, `AWS_ACCESS_KEY_ID`,
    `AWS_SECRET_ACCESS_KEY` — IRSA injects `AWS_ROLE_ARN` +
    `AWS_WEB_IDENTITY_TOKEN_FILE` automatically and the SDK picks them up.
- Patch the `ate-api-server` ServiceAccount:
  `eks.amazonaws.com/role-arn: ${ATE_API_SERVER_ROLE_ARN}`.
- Patch `ate-otel-config` ConfigMap: replace the GKE Managed OTel endpoint
  (`opentelemetry-collector.gke-managed-otel.svc.cluster.local:4317`,
  `ate-otel-config.yaml:35`) with the ADOT collector service.

### 6c. `aws/atelet/kustomization.yaml`

Mirrors `manifests/ate-install/kind/atelet/kustomization.yaml:45-57` plus
one additional strategic-merge patch:

- Env: `ATE_STORAGE_BACKEND=s3`, `AWS_REGION=${AWS_REGION}`.
- `atelet` ServiceAccount: `eks.amazonaws.com/role-arn: ${ATELET_ROLE_ARN}`.
- **Replace the kubelet image-credential-provider hostPath mounts**
  (`atelet.yaml:286-297`):
  - `/home/kubernetes/bin` → `/etc/eks/image-credential-provider/`
  - `/etc/srv/kubernetes/cri_auth_config.yaml` → `/etc/eks/image-credential-provider/config.json`

  Verify paths against your EKS AMI (`/etc/eks/image-credential-provider/`
  is the default on Amazon Linux 2 and AL2023 EKS AMIs). Without this,
  atelet cannot pull from ECR using the node's instance identity.

### 6d. Installer-side change — teach the overlay selector about AWS

In `cmd/ate-setup/internal/steps/overlay.go`:

- Add `aws` and `aws-agentgateway` to the overlay set.
- Add a selector: `--aws` flag or `ATE_INSTALL_AWS=true` env (parallel to
  `--kind` / `ATE_INSTALL_KIND`). On AWS, `--aws` and `--agentgateway`
  compose into `aws-agentgateway`, same as the kind pair.
- Extend the existing `${SUBSTRATE_VERSION}` substitution step to also expand
  `${AWS_REGION}`, `${ATE_API_SERVER_ROLE_ARN}`, `${ATELET_ROLE_ARN}` before
  `kubectl apply`. This is one `strings.NewReplacer` entry — the mechanism
  already exists.

### 6e. Assets that resist overlays — edit in place

gVisor and microVM asset URLs sit inside CRD spec string fields and don't
patch cleanly with kustomize. Mirror the assets to S3 and update the URLs
directly (or templatize these two files):

```
manifests/ate-install/sandboxconfig-gvisor.yaml:34,38
   gs://gvisor/releases/nightly/<date>/<arch>/gvisor.tar.zstd
   → s3://<assets-bucket>/gvisor/<date>/<arch>/gvisor.tar.zstd

manifests/microvm/sandboxconfig-microvm.yaml.tmpl
   all four gs:// URLs → s3://

hack/install-microvm-deps.sh
   add a stage-to-s3.sh branch alongside stage-to-gcs.sh
```

atelet already supports `s3://` URLs — this is purely a URL swap and a
one-time asset mirror.

### 6f. Cloud SQL Auth Proxy — leave unused

The Cloud SQL proxy sidecar is only added when
`ATE_API_POSTGRES_CLOUDSQL_INSTANCE` is set. Keep it unset on AWS; use
`ATE_API_POSTGRES_READ_WRITE_CONNECTION_STRING` to point at RDS directly.
This avoids the hardcoded GKE Workload Identity annotation at
`cmd/ate-setup/internal/steps/cloudsql.go:37-39`.

---

## 7. Phase 4 — Run the installer

### Environment

```bash
export KUBECTL_CONTEXT=$(kubectl config current-context)
export NO_DEV_ENV=true                                   # skip .ate-dev-env.sh
export BUCKET_NAME=substrate-snapshots-<unique>
export KO_DOCKER_REPO=<acct>.dkr.ecr.us-west-2.amazonaws.com/substrate
export EXPECTED_JWT_ISSUER="https://oidc.eks.us-west-2.amazonaws.com/id/<cluster-oidc-id>"
export ATE_CREDENTIAL_PROVIDER='{"name":"k8s.io"}'
export ATE_API_POSTGRES_READ_WRITE_CONNECTION_STRING="postgres://...@rds-endpoint:5432/substrate?sslmode=verify-full"
export ATE_API_POSTGRES_SERVER_CA_FILE=/path/to/rds-combined-ca-bundle.pem

# Explicitly unset GCP env, mirroring what install-ate-kind.sh does:
unset GCE_REGION CLUSTER_LOCATION NETWORK SUBNETWORK MEMORYSTORE_INSTANCE PROJECT_ID
```

Why `EXPECTED_JWT_ISSUER`? The installer defaults to the GKE issuer URL
(`cmd/ate-setup/internal/steps/create.go:149-162`). On EKS, use the OIDC
provider URL from `aws eks describe-cluster --name substrate-poc
--query cluster.identity.oidc.issuer --output text`. No code change needed —
the fallback already honors this env var.

### Run

```bash
./hack/install-ate.sh --deploy-ate-system \
  --credential-provider='{"name":"k8s.io"}' \
  --rollout-timeout=10m
```

This invokes `go run ./cmd/ate-setup deploy ate-system`, which:

1. Validates env, applies CRDs and RBAC
2. Labels every existing node with `ate.dev/substrate-version=<version>`
3. Creates CA pool Secrets (JWT, actor-id, egress-MITM, podcert signers)
4. Deploys `podcertificate-controller` and waits for `ClusterTrustBundle`s
5. Deploys `SandboxConfig` validation + default gVisor sandbox
6. Resolves Postgres (external DSN wins → RDS)
7. Builds/pushes images via ko, renders the kustomize overlay, applies the
   system bundle (`ate-api-server`, `ate-controller`, `atelet`,
   `atenet-router`, `atenet-router-monitoring`, `ate-otel-config`)
8. Reconciles the atenet-egress Deployment (envoy or agentgateway)
9. Waits for every Deployment / DaemonSet rollout

Full step list: see analysis of `cmd/ate-setup/internal/steps/deploy.go:DeployAteSystem`.

Optional: `--setup-csi=nfs` only works on kind today — on EKS you would
instead deploy the EFS CSI driver via its add-on, then write a
`CSIDriverConfig` CR for `efs.csi.aws.com` following `docs/csi-deployment.md`
(expose the controller gRPC over a Service with the socat sidecar pattern
and mTLS).

---

## 8. Phase 5 — Expose and verify

The base install creates no `LoadBalancer` or `Ingress`. `atenet-router` is
`ClusterIP` on ports 80, 443, 8081, 8444, 4040.

**Expose externally:** create an `Ingress` (AWS Load Balancer Controller
provisions an ALB) or a `Service type=LoadBalancer` (NLB). For gRPC
workloads use NLB; for HTTP use ALB.

```yaml
apiVersion: v1
kind: Service
metadata:
  name: atenet-router-lb
  namespace: ate-system
  annotations:
    service.beta.kubernetes.io/aws-load-balancer-type: external
    service.beta.kubernetes.io/aws-load-balancer-nlb-target-type: ip
    service.beta.kubernetes.io/aws-load-balancer-scheme: internet-facing
spec:
  type: LoadBalancer
  selector:
    app: atenet-router
  ports:
    - name: https
      port: 443
      targetPort: 443
```

**Smoke test:** `kubectl -n ate-system get pods` should show all control-plane
components `Ready`. Then follow the demo flow in the main `README.md`
(e.g. `go run ./cmd/ate-setup deploy demo counter`) to confirm the
`WorkerPool` CRD plumbing works end-to-end.

---

## 9. Known gaps that require upstream code changes

After the `aws` overlay in §6 lands, the remaining gaps are small.

| Gap | Where | Resolution |
|---|---|---|
| Overlay selector only knows `kind` / `base` / `*-agentgateway` | `cmd/ate-setup/internal/steps/overlay.go` | §6d — add `aws` + `aws-agentgateway` plus `--aws` flag / `ATE_INSTALL_AWS` env |
| Env substitution covers only `${SUBSTRATE_VERSION}` | same file (substitution step) | §6d — extend to `${AWS_REGION}`, `${ATE_API_SERVER_ROLE_ARN}`, `${ATELET_ROLE_ARN}` |
| Credentials shim calls `gcloud container clusters get-credentials` when `PROJECT_ID` set | `cmd/ate-setup/internal/config/credentials.go:43-56` | Keep `PROJECT_ID` unset; pass `--context` instead. (Future: add an `aws eks update-kubeconfig` branch keyed on `AWS_REGION` + `CLUSTER_NAME`.) |
| JWT issuer defaulted to GKE URL from `PROJECT_ID`/`CLUSTER_LOCATION`/`CLUSTER_NAME` | `cmd/ate-setup/internal/steps/create.go:149-162` | Already works — the fallback honors `EXPECTED_JWT_ISSUER=https://oidc.eks....`. No code change needed. |
| Workload Identity annotation is hardcoded to `iam.gke.io/gcp-service-account` for Cloud SQL | `cmd/ate-setup/internal/steps/cloudsql.go:37-39` | Do not use the Cloud SQL branch; set DSN directly to RDS. |
| gVisor + microVM asset URLs are `gs://` inside CRD spec strings — not overlay-friendly | `sandboxconfig-gvisor.yaml`, `microvm/*.tmpl`, `hack/install-microvm-deps.sh` | §6e — mirror assets to S3, update URLs in place; add `stage-to-s3.sh` alongside `stage-to-gcs.sh` |
| No AWS equivalent of `tools/setup-gcp` | `tools/setup-gcp/` | Hand-rolled Terraform / `eksctl` / `aws` CLI as in §4. Future: `tools/setup-aws/` Go CLI mirroring the GCP one. |

---

## 10. Appendix A — Installer env var reference (relevant subset)

| Var | Required on AWS? | Purpose |
|---|---|---|
| `KUBECTL_CONTEXT` / `--context` | yes | Target cluster |
| `NO_DEV_ENV=true` | yes | Don't source `.ate-dev-env.sh` |
| `ATE_INSTALL_AWS=true` / `--aws` | yes | Select the `aws` kustomize overlay (new; §6d) |
| `AWS_REGION` | yes | Substituted into the `aws` overlay (`ate-api-server` + `atelet` env) |
| `ATE_API_SERVER_ROLE_ARN` | yes | Substituted into the `aws` overlay (`ate-api-server` KSA IRSA annotation) |
| `ATELET_ROLE_ARN` | yes | Substituted into the `aws` overlay (`atelet` KSA IRSA annotation) |
| `KO_DOCKER_REPO` | yes (if building images) | ECR push target |
| `BUCKET_NAME` | yes | S3 snapshot bucket name |
| `EXPECTED_JWT_ISSUER` | yes | EKS OIDC provider URL |
| `ATE_CREDENTIAL_PROVIDER` | yes | `{"name":"k8s.io"}` for in-cluster Secret-backed credentials |
| `ATE_API_POSTGRES_READ_WRITE_CONNECTION_STRING` | yes if using RDS | External DSN |
| `ATE_API_POSTGRES_SERVER_CA_FILE` | yes if using RDS with sslmode=verify-* | Path to RDS CA bundle |
| `ATE_API_POSTGRES_SCHEMA` | no (default `substrate`) | Schema name |
| `ATE_OTLP_ENDPOINT` / `--otlp-endpoint` | no | Override OTel endpoint post-install |
| `ATE_INSTALL_CORDON_CONTROL_PLANE` / `--cordon-control-plane` | no | Pin control plane to dedicated node pool |
| `ATE_INSTALL_CLUSTER_SIZE` / `--cluster-size` | no | `size0` (default) or `size10` for 10k-pod scale |
| `ATE_ATENET_DATAPLANE` | no | `envoy` (default) or `agentgateway` |
| `ATE_IMAGE_REPO` + `ATE_IMAGE_TAG` | no | Use pre-built images instead of ko |
| `ATE_API_POSTGRES_CLOUDSQL_INSTANCE` | **leave unset** | Triggers the GKE-specific Cloud SQL proxy branch |
| `PROJECT_ID` / `GCE_REGION` / `CLUSTER_LOCATION` / `NETWORK` / `SUBNETWORK` | **leave unset** | GCP-only |

---

## 11. Appendix B — Summary of required one-time AWS actions

1. VPC, 3 AZs, private + public subnets, NAT per AZ, subnet tags for LBC.
2. EKS cluster ≥ 1.37 with OIDC provider.
3. Two or three managed node groups (control-plane / workers / optional postgres),
   all labeled `ate.dev/substrate-version=<v>`, with user-data ensuring
   `/var/lib/ate` exists.
4. Add-ons: EBS CSI (default gp3 StorageClass), VPC CNI with NetworkPolicy,
   AWS Load Balancer Controller, ADOT Operator, optional EFS CSI.
5. S3 bucket for snapshots (private).
6. ECR repos.
7. Two IRSA roles for `ate-system/{atelet,ate-api-server}` with the S3 and
   (if applicable) `rds-db:connect` permissions in §4.8.
8. RDS Postgres with IAM auth enabled; create `substrate_owner` /
   `substrate_readwrite` roles and database.
9. Mirror gVisor (and microVM, if applicable) assets to S3.
10. Land the `aws` kustomize overlay (§6a–6c) and the installer-side
    overlay-selector change (§6d). Edit the two asset-URL files in place (§6e).
11. `./hack/install-ate.sh --deploy-ate-system --aws ...` with the env in §7
    (also export `AWS_REGION`, `ATE_API_SERVER_ROLE_ARN`, `ATELET_ROLE_ARN`
    for the overlay substitution).
12. Create a `Service type=LoadBalancer` or `Ingress` to expose
    `atenet-router` externally.
