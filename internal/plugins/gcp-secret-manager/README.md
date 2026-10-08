# gcp-secret-manager credential provider

> [!NOTE]
> This provider is hosted in the substrate repository temporarily. It will move
> to a dedicated repository in the near future.

A [substrate](https://github.com/agent-substrate/substrate) credential provider
backed by Google Cloud Secret Manager. It implements substrate's
`CredentialProvider` gRPC API (`pkg/proto/credproviderpb`) for egress
credential injection: given an `ate-secret://secretmanager.googleapis.com/...`
URI, it reads the secret version from Secret Manager and returns its payload,
so the secret stays in Secret Manager and substrate never stores it.

This directory is a Go module of its own, so that it can move out as it is.

## Endpoint

The manifests in `config/` deploy the provider into substrate's `ate-system`
namespace:

| | |
|---|---|
| Provider name | `secretmanager.googleapis.com`, the host of its `ate-secret://` credential URIs |
| Address | `gsm-credential-provider.ate-system.svc:50051` |
| API | `credprovider.CredentialProvider/FetchSecret` over mutual TLS |

The provider's serving certificate names the Service's DNS name, so callers
must use exactly this address.

## Credential URIs

The URI path is the resource name of a secret version, optionally followed by a
key into the version's JSON payload. A regional secret names its location after
the project, as its resource name does:

```
ate-secret://secretmanager.googleapis.com/projects/<project>/secrets/<secret>/versions/<version>[/keys/<key>]
ate-secret://secretmanager.googleapis.com/projects/<project>/locations/<location>/secrets/<secret>/versions/<version>[/keys/<key>]
```

- `<project>` is a project ID or number.
- `<location>` is a region, such as `us-central1`. The provider reads a
  regional secret from its region's endpoint,
  `secretmanager.<location>.rep.googleapis.com`. The URI host stays
  `secretmanager.googleapis.com`: it names the provider, not the endpoint.
- `<secret>` is 1-255 letters, digits, underscores or dashes.
- `<version>` is required: a positive integer or `latest`. A URI without a
  version is refused rather than read as `latest`.
- `<key>` names a top-level key of a payload holding a JSON object, such as
  `{"api_key": "..."}`. It is a single path segment other than `.` or `..`, so
  a key containing `/` or a character that needs percent-encoding cannot be
  named.

User information, queries, fragments, percent-encoding and empty path segments
(a doubled or trailing slash) are refused.

## Responses

Without a key, `FetchSecret` returns the payload verbatim in `opaque_bytes`.
With a key, it returns the key's value, which must be a JSON string, with its
JSON escapes decoded. When Secret Manager supplies a CRC32C checksum, the
provider verifies the payload against it first.

The egress gateway trims trailing CR and LF from the value and refuses an empty
value or one containing any other control character, since the value becomes
part of an HTTP header. A multi-line credential, such as a PEM-encoded key,
cannot be injected.

Each Secret Manager read may take up to `--fetch-timeout`, 3s by default. The
gateway sets no deadline of its own and Envoy abandons the request after its 5s
ext_proc message timeout, so keep the flag below that.

The gateway answers `Unavailable` and `DeadlineExceeded` with a retryable 503
and every other code with a 403:

| Condition | Code |
|---|---|
| The URI is malformed, lacks a version, or names another provider | `InvalidArgument` |
| Secret Manager rejects the resource name | `InvalidArgument` |
| The actor's atespace is not granted the URI's project, or the actor identity is unusable | `PermissionDenied` |
| The provider's Google identity may not access the secret, or its credentials are refused | `PermissionDenied` |
| The secret or version does not exist or has no payload, or the payload's JSON object has no such key | `NotFound` |
| The version is disabled or destroyed, including a `latest` whose newest version is disabled | `FailedPrecondition` |
| With a key, the payload is not a JSON object or the key's value is not a string | `FailedPrecondition` |
| Secret Manager does not answer in time | `DeadlineExceeded` |
| The payload fails its CRC32C check, or any other failure, such as an outage or exhausted quota | `Unavailable` |

Error messages name the key but never quote the payload.

## Security model

- **Only the egress gateway may call.** The provider serves mutual TLS with a
  certificate from substrate's servicedns signer, and admits a caller only if
  its certificate chains to substrate's podidentity trust bundle and carries
  the egress gateway's SPIFFE ID. `--injector-identity` names that ID (the
  gateway is what injects credentials); `config/` sets it to the gateway in
  `ate-system`.
- **Each atespace resolves secrets only in the projects its policy grants.**
  The provider takes the atespace from the actor SPIFFE ID the gateway sends
  and, before calling Secret Manager, refuses with `PermissionDenied` a URI
  whose project the `--project-policy-file` policy does not list for it. The
  policy is default-deny, like the Kubernetes Secrets provider's namespace
  policy. A project is matched as the URI writes it, so list
  it by the ID or number your URIs use.
- **The provider's Google identity bounds the rest.** Within a granted project,
  an atespace reaches whatever that identity can read, so grant it access only
  to the secrets some atespace needs.

## Install

Prerequisites:

- A GKE cluster with Workload Identity Federation, running substrate v0.2.0 or
  later. The provider gets its Google credentials from GKE's metadata server,
  and its serving certificate and its callers' trust bundle from substrate's
  pod certificate controller.
- A registry for `KO_DOCKER_REPO`. The Makefile runs [ko](https://ko.build)
  with `go run`.

**1. Grant Secret Manager access.** The provider reads secrets through Workload
Identity Federation for GKE, as its Kubernetes ServiceAccount
`gsm-credential-provider` in `ate-system`. Grant it access to each secret it
serves in one of two ways. `<cluster-project>` is the project that holds the GKE
cluster, which need not be the secret's `<project>`.

*Grant the Kubernetes ServiceAccount directly.* Nothing else needs configuring:

```bash
gcloud secrets add-iam-policy-binding <secret> --project <project> \
  --role roles/secretmanager.secretAccessor \
  --member "principal://iam.googleapis.com/projects/<cluster-project-number>/locations/global/workloadIdentityPools/<cluster-project>.svc.id.goog/subject/ns/ate-system/sa/gsm-credential-provider"
```

*Or act as a Google service account*, for example one that already holds the
access. Grant the Google service account access to the secret, and let the
Kubernetes ServiceAccount impersonate it:

```bash
GSA=gsm-credential-provider@<cluster-project>.iam.gserviceaccount.com
gcloud iam service-accounts create gsm-credential-provider --project <cluster-project>
gcloud secrets add-iam-policy-binding <secret> --project <project> \
  --role roles/secretmanager.secretAccessor --member "serviceAccount:$GSA"
gcloud iam service-accounts add-iam-policy-binding "$GSA" --project <cluster-project> \
  --role roles/iam.workloadIdentityUser \
  --member "serviceAccount:<cluster-project>.svc.id.goog[ate-system/gsm-credential-provider]"
```

Then uncomment the `iam.gke.io/gcp-service-account` annotation in
`config/serviceaccount.yaml`, set to that email, before deploying the provider.

For a regional secret, see step 5.

**2. Apply the project policy.** Edit
[`config/project-policy.yaml`](config/project-policy.yaml), the
`gsm-credential-provider-project-policy` ConfigMap, to list the projects each
atespace may resolve secrets in, then apply it from this directory. The
provider's pod mounts it, so it must exist first. `make deploy` leaves it out,
so a redeploy never overwrites it.

```bash
kubectl apply -f config/project-policy.yaml
```

The provider reads the policy once, at startup, and exits if it is malformed.
After editing it, restart the provider:

```bash
kubectl -n ate-system rollout restart deployment/gsm-credential-provider
```

**3. Deploy the provider.** From this directory:

```bash
KO_DOCKER_REPO=<registry> make deploy
kubectl -n ate-system rollout status deployment/gsm-credential-provider
```

The provider exits at startup, logging `could not find default credentials`,
if it finds no Application Default Credentials. Off GKE, on a kind cluster say,
no metadata server supplies them; mount a service account key into the pod and
point `GOOGLE_APPLICATION_CREDENTIALS` at it. A missing IAM grant does not stop
the provider: it fails each fetch with `PermissionDenied`.

**4. Point the egress gateway at the provider.** From the repository root:

```bash
hack/install-ate.sh --deploy-atenet \
  --credential-provider='{"name":"secretmanager.googleapis.com","address":"gsm-credential-provider.ate-system.svc:50051"}'
```

This sets the `--credential-provider-*` flags on the `ext-proc` container of
the `atenet-egress` Deployment and restarts it. It also redeploys the atenet
router, building its images from the checkout with ko; for a published release,
put `ATE_IMAGE_REPO=<repo> ATE_IMAGE_TAG=<tag>` in front of the command. The
gateway serves one provider at a time, so this replaces any previous one. To
confirm:

```bash
kubectl -n ate-system get deployment atenet-egress -o yaml | grep credential-provider
```

**5. Store a secret.** The payload is the raw value:

```bash
printf '%s' "$TOKEN" | gcloud secrets create example-api-token --project <project> --data-file=-
```

Its URI is
`ate-secret://secretmanager.googleapis.com/projects/<project>/secrets/example-api-token/versions/1`,
or `.../versions/latest` to follow new versions.

To keep several credentials in one secret, store a JSON object and name the key:

```bash
jq -cn --arg v "$TOKEN" '{api_key: $v}' | gcloud secrets create example-api --project <project> --data-file=-
```

`.../secrets/example-api/versions/1/keys/api_key` then resolves to `$TOKEN`.

For a regional secret, add `--location <location>` to the `gcloud secrets`
commands, here and in step 1, and point gcloud at the regional endpoint:
`--location` alone still sends the request to the global endpoint, which fails.

```bash
printf '%s' "$TOKEN" | \
  CLOUDSDK_API_ENDPOINT_OVERRIDES_SECRETMANAGER="https://secretmanager.<location>.rep.googleapis.com/" \
  gcloud secrets create example-api-token --project <project> --location <location> --data-file=-
```

Its URI names the location:
`ate-secret://secretmanager.googleapis.com/projects/<project>/locations/<location>/secrets/example-api-token/versions/1`.
A project's policy grant covers its regional secrets too.

**Uninstall.** Redeploy the gateway without the provider first: removing the
provider while the gateway still points at it makes every credential fetch
fail.

```bash
hack/install-ate.sh --deploy-atenet --credential-provider='{"enabled":false}'   # from the repository root
make undeploy                                                        # from this directory
```

`make undeploy` removes the provider's ServiceAccount, Deployment and Service,
and leaves the project policy in place.

## Flags

| Flag | Default | Purpose |
|---|---|---|
| `--listen-address` | `:50051` | gRPC listen address |
| `--health-address` | `:9090` | HTTP address for `/healthz` and `/readyz` |
| `--server-cred-bundle` | required | Serving credential bundle: PKCS#8 key and certificate chain |
| `--client-ca-file` | required | Trust bundle the caller's certificate must chain to |
| `--injector-identity` | required | SPIFFE ID of substrate's egress gateway, the only caller the provider accepts; `spiffe://cluster.local/ns/ate-system/sa/atenet-egress` in a default install |
| `--project-policy-file` | required | Atespace→project policy YAML |
| `--fetch-timeout` | `3s` | Time limit for one Secret Manager read; keep it under the gateway's 5s ext_proc message timeout |
| `--log-level` | `info` | `debug`, `info`, `warn` or `error` |
| `--drain-grace` | `5s` | How long in-flight calls may finish on shutdown |

## Development

```bash
make test               # go test -race ./...
make verify             # gofmt, go vet, go mod tidy check
make fmt                # gofmt -w .
make release-manifest   # manifests with the image pinned by digest
```

`go.mod` replaces the substrate module with the enclosing checkout, so the
provider builds and tests against the current `credproviderpb` API.

The module imports nothing of substrate's but its public `pkg/` packages, so
it can be copied out as it is; `TestImportsOnlySubstratePublicPackages` fails
on any other substrate import. For that reason, two packages are copies kept in
step by hand: `internal/credbundle` of substrate's `internal/credbundle`, and
`internal/mtls` of the server credentials in
`cmd/credential-provider/kubernetes-secrets`.

To move the module to its own repository, rename the module path in `go.mod`,
in the Go imports, and in the `ko://` image of `config/deployment.yaml`, then
drop the `replace` directive so the `require` of a released substrate version
applies.
