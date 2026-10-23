# Pulling actor images from private registries

atelet pulls actor images itself — not the kubelet — so a Pod's `imagePullSecrets` do not apply to actors. atelet
authenticates image pulls through **kubelet credential-provider exec plugins**: `--image-credential-provider-config`
names a `CredentialProviderConfig` (`kubelet.config.k8s.io/v1`) and `--image-credential-provider-bin-dir` the
directory holding the plugin binaries. Every provider whose `matchImages` matches an image is tried, in order;
without a match the pull is anonymous.

## GKE

The base manifest (`manifests/ate-install/atelet.yaml`) mounts the node's own provider configuration
(`/etc/srv/kubernetes/cri_auth_config.yaml`) and plugins (`/home/kubernetes/bin`), so atelet authenticates to
Artifact Registry the same way the node's kubelet does.

## Any cluster: credentials from a Secret

Clusters without those node files (EKS with a private registry outside ECR, kind, bare metal) can use the
`image-credential-secret` component. It runs `cmd/dockerconfig-credential-provider`, a plugin that answers from a
`kubernetes.io/dockerconfigjson` Secret — the same shape as an `imagePullSecret` — and needs no host paths:

- the plugin binary is installed from its image into an `emptyDir` by an init container;
- the `CredentialProviderConfig` comes from a ConfigMap
  (`manifests/ate-install/components/image-credential-secret/credential-provider-config.yaml`);
- the Secret `ate-system/atelet-registry-auth` is mounted read-only. The plugin reads it on every call, so a rotated
  Secret applies without restarting atelet. It is optional: atelet starts, pulling anonymously, before it exists.

Create the Secret before installing (a Secret created later reaches atelet only on the kubelet's next volume sync):

```sh
kubectl -n ate-system create secret docker-registry atelet-registry-auth \
  --docker-server=<registry> --docker-username=<user> --docker-password=<token>
```

Then include the component in the kustomization that applies atelet:

```yaml
components:
  - <path to manifests/ate-install>/components/image-credential-secret
```

`manifests/ate-install/kind-image-credential-secret` is kind's atelet with the component applied.

Narrow `matchImages` in `credential-provider-config.yaml` to the registries the Secret serves; the default matches
any registry host, and the plugin answers with credentials only for registries the Secret has an entry for (an
empty answer otherwise, so the pull falls through to the next provider or goes anonymous).

## Scope

The credentials are node-level: every actor on a node can pull what the Secret can read — the same boundary as a
node's own credential provider. Per-tenant pull credentials are tracked in #868 and #2080.

## Testing

`internal/e2e/suites/imagepullsecret` pulls a fresh-digest copy of the probe from a private registry. It needs a
registry the cluster reaches over TLS (atelet allows plain HTTP only for localhost registries) and credentials that
can push and pull there, and skips otherwise:

```sh
export E2E_PRIVATE_REPOSITORY=<registry>/<repo>/e2e-private-probe   # a PRIVATE repository
export E2E_PRIVATE_REGISTRY_USERNAME=<user> E2E_PRIVATE_REGISTRY_PASSWORD=<token>
# denied: atelet installed WITHOUT the component — the golden build must fail with a registry refusal
E2E_PRIVATE_PULL_EXPECT=denied hack/run-e2e-kind.sh ./internal/e2e/suites/imagepullsecret -v -args --no-color --preview=""
# allowed: create the Secret, re-apply atelet with the component, then
hack/run-e2e-kind.sh ./internal/e2e/suites/imagepullsecret -v -args --no-color --preview=""
```
