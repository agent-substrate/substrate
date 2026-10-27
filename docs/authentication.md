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

## ServiceAccount tokens

`ate-api` accepts a token whose `sub` is
`system:serviceaccount:<namespace>:<name>` only while that ServiceAccount
exists in its own cluster, is not being deleted, and has the UID in the token's
`kubernetes.io` claim. This applies to every provider, so ServiceAccount tokens
from another cluster are rejected. A ServiceAccount recreated with the same
name gets new tokens that work, and access policies that name it still apply
to it.

`ate-api` watches ServiceAccounts, so a token keeps working until the deletion
reaches its cache. When the cache does not have the ServiceAccount with the
token's UID, `ate-api` reads it from the Kubernetes API server, at a limited
rate. A call over that rate fails with `Unavailable` and can be retried.

## Google Cloud CLI tokens

The Google Cloud CLI currently issues user identity tokens with issuer
`https://accounts.google.com` and audience
`32555940559.apps.googleusercontent.com`, the Cloud SDK's shared client ID.
These values are examples rather than built-in defaults; verify the claims
issued by your identity provider and configure them explicitly.

With the provider configured, pipe the token to `kubectl-ate`:

```sh
gcloud auth print-identity-token | kubectl ate --token-file=- get actors
```

`--token-file` accepts either a file path or `-` for stdin and only replaces the
credential sent to `ate-api`. `kubectl-ate`
still uses kubeconfig access to establish its port-forward and obtain the
server trust bundle.

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
