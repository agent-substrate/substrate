# ate-api Authentication

`ate-api` accepts mTLS client certificates and bearer JWTs. JWT providers are
configured with the file passed to `--authentication-config`:

```yaml
jwtProviders:
- name: kubernetes
  issuer: https://kubernetes.default.svc.cluster.local
  audiences:
  - api.ate-system.svc
  certificateAuthorityFile: /var/run/secrets/kubernetes.io/serviceaccount/ca.crt
  discoveryTokenFile: /var/run/secrets/kubernetes.io/serviceaccount/token
- name: google
  issuer: https://accounts.google.com
  audiences:
  - 32555940559.apps.googleusercontent.com
```

Provider names and issuers must be unique. `issuer` must be an HTTPS URL and
`audiences` must be non-empty; a token is accepted when any configured audience
matches. `certificateAuthorityFile` and `discoveryTokenFile` are optional and
are needed for OIDC discovery against some private Kubernetes API servers.

What an authenticated caller may do is decided by authorization; see
[authorization.md](authorization.md). Its principal ID does not record which
provider issued the token, so only configure providers you trust not to issue
a token whose subject is another principal's ID.

## Client credentials

`kubectl-ate` gets its credential from the kubectl credential plugin that
`--credential-plugin` or `$ATE_CREDENTIAL_PLUGIN` names; see the
[kubectl-ate README](../cmd/kubectl-ate/README.md#authentication). It caches
the plugin's token until its `expirationTimestamp`, and runs the plugin again
when `ate-api` returns `UNAUTHENTICATED`. If the plugin returns the
rejected token again, the client runs it once more, and on every later run,
with `ATE_REFRESH_CREDENTIAL=1`, which tells a plugin that caches tokens to
return a new one.

`ate-api` checks only a JWT's issuer, audience, signature, and expiry. A
credential source is therefore safe only when its issuer limits who can get a
token for the configured audience. Kubernetes ServiceAccount tokens qualify,
because RBAC decides who may create them. Google ID tokens for the Cloud SDK's
shared client ID do not, since any Google account can get one. Neither do
Google service account ID tokens or GitHub Actions OIDC tokens: any service
account or workflow can request one for any audience.

`kubectl ate credential` is the built-in plugin. It mints a ServiceAccount
token through your kubeconfig and caches it under `~/.ate/cache`.
`kubectl ate credential wrap -- COMMAND` adapts a command that prints a JWT.
`kubectl ate whoami` shows the principal `ate-api` authenticated you as.

### Per-user ServiceAccounts

Without a credential, every user acts as the shared `ate-client`
ServiceAccount. To give a user their own identity, let them create tokens for
a ServiceAccount of their own:

```sh
kubectl create namespace ate-users
kubectl -n ate-users create serviceaccount alice
kubectl -n ate-users create role alice-token --verb=create --resource=serviceaccounts/token --resource-name=alice
kubectl -n ate-users create rolebinding alice-token --role=alice-token --user=alice@example.com
```

Alice then sets
`ATE_CREDENTIAL_PLUGIN="kubectl-ate credential --service-account=ate-users/alice"`,
and `kubectl ate whoami` reports `system:serviceaccount:ate-users:alice`.
Access policies grant to that ID. The token carries the ServiceAccount's
groups, not Alice's.

A user who is not a cluster admin also needs to port-forward to `ate-api` and
read its trust bundle. Create these roles once:

```sh
kubectl apply -f - <<'EOF'
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: ate-api-client
  namespace: ate-system
rules:
- apiGroups: [""]
  resources: [services]
  resourceNames: [api]
  verbs: [get]
- apiGroups: [""]
  resources: [pods]
  verbs: [list]
- apiGroups: [""]
  resources: [pods/portforward]
  verbs: [get, create]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: ate-api-client
rules:
- apiGroups: [certificates.k8s.io]
  resources: [clustertrustbundles]
  verbs: [list]
EOF
```

and bind them to each user:

```sh
kubectl -n ate-system create rolebinding alice-ate-api-client --role=ate-api-client --user=alice@example.com
kubectl create clusterrolebinding alice-ate-api-client --clusterrole=ate-api-client --user=alice@example.com
```

`ate-api` verifies a token's signature and expiry without asking Kubernetes
whether its ServiceAccount still exists. Deleting Alice's ServiceAccount stops
her getting new tokens, but one she already holds keeps working until it
expires, plus `ate-api`'s five-minute allowance for clock skew.
`kubectl ate credential` requests one-hour tokens; Alice can request longer
ones herself, up to the cluster's maximum token lifetime. To cut off her access
at once, remove the access policy grants to her ID. A ServiceAccount recreated
with the same name gets the old one's grants.

### In-cluster workloads

A pod can act as its own ServiceAccount with
`--endpoint=api.ate-system.svc:443` and a `--token-file` pointing at a
projected ServiceAccount token with audience `api.ate-system.svc`. The client
rereads the file on every RPC, so it picks up the kubelet's rotations. The
pod's ServiceAccount must be able to list ClusterTrustBundles, which verify
`ate-api`. The install's NetworkPolicy admits only pods in namespaces labeled
`ate.dev/api-access=true`.

## Google Cloud CLI tokens

The Google Cloud CLI currently issues user identity tokens with issuer
`https://accounts.google.com` and audience
`32555940559.apps.googleusercontent.com`, the Cloud SDK's shared client ID.
These values are examples rather than built-in defaults; verify the claims
issued by your identity provider and configure them explicitly. Any Google
account can obtain such a token, so this provider admits every Google account.

With the provider configured, wrap the token as a credential plugin:

```sh
export ATE_CREDENTIAL_PLUGIN="kubectl-ate credential wrap -- gcloud auth print-identity-token"
```

or pass it for one command with `--token-file`, which accepts a file path or
`-` for stdin:

```sh
gcloud auth print-identity-token | kubectl ate --token-file=- get actors
```

Either way, `kubectl-ate` still uses kubeconfig access to establish its
port-forward and obtain the server trust bundle.

For a manifest-based installation, replace the authentication ConfigMap and
restart the deployment:

```sh
kubectl -n ate-system create configmap ate-api-authentication \
  --from-file=authentication.yaml \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl -n ate-system rollout restart deployment/ate-api-server
```

Configuration is read at process startup. Restart `ate-api` pods after changing
the ConfigMap. OIDC signing keys are cached and refreshed when an unknown key ID
is encountered.
