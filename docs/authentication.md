# ate-api Authentication

`ate-api` accepts mTLS client certificates and bearer JWTs. JWT providers are
configured with the file passed to `--authentication-config`:

```yaml
actorIdentityJWTProvider: kubernetes
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

`actorIdentityJWTProvider` identifies the provider allowed to call
`ActorIdentity.MintJWT`. Other authenticated providers can call every RPC.
Authorization and RBAC are not implemented yet, so only configure providers
whose users should have full control of the entire control plane: every
atespace, actor, actor template, egress policy, snapshot and worker in the
cluster.

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
