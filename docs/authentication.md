# ate-api Authentication

Every call to `ate-api-server` must carry a credential, which authentication
turns into a principal ID. [Authorization](authorization.md) then decides what
that principal may do.

- **Client certificate.** If the client presents a certificate issued by the
  `podidentity.podcert.ate.dev/identity` signer, the principal is its first
  URI SAN, a SPIFFE ID. A bearer token sent alongside it is ignored.
- **Bearer JWT.** Otherwise the call needs an `authorization: Bearer <JWT>`
  header with a token from a [trusted issuer](#configuring-jwt-providers). The
  principal is the token's `sub`.
- Anything else gets `Unauthenticated`.

## Which credential to use

| Caller | Credential | Principal |
|---|---|---|
| [A person running `kubectl ate`](#people-running-kubectl-ate) | `--token-file` with a token for your own ServiceAccount, or from your identity provider | `system:serviceaccount:<namespace>:<name>`, or the token's `sub` |
| [A pod in the Substrate cluster](#pods-in-the-substrate-cluster) | A projected ServiceAccount token, or a pod certificate | `system:serviceaccount:<namespace>:<name>`, or `spiffe://cluster.local/ns/<namespace>/sa/<name>` |
| [A pod in another cluster](#pods-in-another-cluster) | A projected ServiceAccount token from that cluster | `system:serviceaccount:<namespace>:<name>` |
| [A CI job](#ci-jobs) | The CI system's OIDC token | The token's `sub` |

Then grant the principal a role in an
[access policy](authorization.md#managing-access).

## People running `kubectl ate`

`kubectl ate` uses your kubeconfig for the Substrate cluster whatever
credential it sends to `ate-api-server`. It:

- lists ClusterTrustBundles to verify `ate-api-server`'s serving certificate;
- without `--endpoint`, gets the `api` Service, lists pods and port-forwards in
  `ate-system`;
- without `--token-file`, creates a token for the shared `ate-client`
  ServiceAccount and warns that it is acting as a shared identity.

Everyone relying on that last step is the same principal, and `ate-setup`
makes it a bootstrap owner by default (see the warning in
[authorization.md](authorization.md)). That suits a cluster only you use. On a
shared cluster, give each person their own identity in one of the two ways
below.

`kubectl ate` rereads `--token-file` on every call but never refreshes it, so
get a new token when it expires. With `--token-file=-` it reads the token once
from stdin.

### Your own ServiceAccount

Create a ServiceAccount per person and let each mint tokens only for their own:

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata: {name: mint-alice-token, namespace: team-a}
rules:
- apiGroups: [""]
  resources: ["serviceaccounts/token"]
  resourceNames: ["alice"]
  verbs: ["create"]
```

```sh
kubectl create token -n team-a alice --audience api.ate-system.svc > alice.token
kubectl ate --token-file alice.token get actors -a team-a
```

The principal is `system:serviceaccount:team-a:alice`. The audience must be
one the `kubernetes` provider lists; the config `ate-setup` writes lists
`api.ate-system.svc`.

### A token from your identity provider

If your identity provider issues OIDC ID tokens,
[add it as a JWT provider](#configuring-jwt-providers) and pass its token.
With the Google provider from that section:

```sh
gcloud auth print-identity-token | kubectl ate --token-file=- get actors
```

The principal is the token's `sub`. For Google that is the numeric account ID,
not the email; [bootstrap owners](authorization.md#bootstrap-owners) shows how
to find it.

## Pods in the Substrate cluster

Dial `api.ate-system.svc:443` and verify the serving certificate against the
`servicedns.podcert.ate.dev/identity` trust bundle, with server name
`api.ate-system.svc`. The Service is headless, so DNS returns one address per
replica; use round-robin load balancing to spread calls across them. Go
clients can use the generated stubs in `pkg/proto/ateapipb`.

Authenticate with a ServiceAccount token or a pod certificate. The kubelet
rotates both, so read them from the volume instead of caching them:

```yaml
spec:
  serviceAccountName: my-app
  containers:
  - name: app
    volumeMounts:
    - {name: ate-api, mountPath: /var/run/ate, readOnly: true}
  volumes:
  - name: ate-api
    projected:
      sources:
      - clusterTrustBundle:
          signerName: servicedns.podcert.ate.dev/identity
          labelSelector:
            matchLabels:
              podcert.ate.dev/canarying: live
          path: ca.crt
      # Either a ServiceAccount token, sent as a bearer token:
      - serviceAccountToken:
          audience: api.ate-system.svc
          path: token
      # or a pod certificate, presented as the TLS client certificate:
      # - podCertificate:
      #     signerName: podidentity.podcert.ate.dev/identity
      #     keyType: ECDSAP256
      #     credentialBundlePath: credential-bundle.pem
```

- **ServiceAccount token.** The principal is
  `system:serviceaccount:<namespace>:<name>`. The config `ate-setup` writes
  already trusts the cluster's issuer and this audience. To use `kubectl ate`
  from the pod:

  ```sh
  kubectl ate --endpoint=api.ate-system.svc:443 --token-file=/var/run/ate/token get actors
  ```

  It still reads the trust bundle through the Kubernetes API, so the pod's
  ServiceAccount needs permission to list `clustertrustbundles`.
- **Pod certificate.** The principal is
  `spiffe://cluster.local/ns/<namespace>/sa/<name>`. The credential bundle is
  one PEM file holding the private key followed by the certificate chain;
  reread it on each TLS handshake. `kubectl ate` can't present a client
  certificate.

## Pods in another cluster

A pod in another cluster sends a ServiceAccount token from its own cluster,
mounted as in the [previous section](#pods-in-the-substrate-cluster).

1. **Trust the other cluster's issuer.** Add a
   [JWT provider](#configuring-jwt-providers) whose `audiences` include the
   audience the pod requests. Run
   `kubectl get --raw /.well-known/openid-configuration | jq -r .issuer`
   against the other cluster to print its issuer; on GKE it is
   `https://container.googleapis.com/v1/projects/<project>/locations/<location>/clusters/<cluster>`.
   `ate-api-server` fetches the issuer's discovery document and keys over
   HTTPS, so they must be reachable from the Substrate cluster. GKE serves
   them publicly.
2. **Pick a ServiceAccount name no one else can mint.** The principal doesn't
   name the cluster ([principals are not qualified by
   issuer](authorization.md#known-limitations)), so
   `system:serviceaccount:team-a:app` is the same principal in both clusters,
   and anyone who can create a token for `team-a/app` in either one gets its
   grants. If the other cluster also runs Substrate, its `ate-client` is the
   same principal as yours.
3. **Reach and verify `ate-api-server`.** The install doesn't expose it
   outside its cluster; exposing it is up to you. Its serving certificate
   names only in-cluster Service names such as `api.ate-system.svc`, so the
   client must use that as the TLS server name whatever address it dials,
   and trust a copy of the serving CA. Copy it again when the CA rotates:

   ```sh
   kubectl get clustertrustbundles -l podcert.ate.dev/canarying=live -o json \
     | jq -r '.items[] | select(.spec.signerName == "servicedns.podcert.ate.dev/identity") | .spec.trustBundle' \
     > ate-api-ca.crt
   ```

`kubectl ate` works there only with a kubeconfig for the Substrate cluster,
which it needs to read the trust bundle.

## CI jobs

Trust the CI system's OIDC issuer and send its token. For GitHub Actions, add
this provider:

```yaml
- name: github-actions
  issuer: https://token.actions.githubusercontent.com
  audiences:
  - api.ate-system.svc
```

and request a token in the job:

```yaml
permissions:
  id-token: write
steps:
- run: |
    curl -sSf -H "Authorization: bearer $ACTIONS_ID_TOKEN_REQUEST_TOKEN" \
      "$ACTIONS_ID_TOKEN_REQUEST_URL&audience=api.ate-system.svc" \
      | jq -r .value > "$RUNNER_TEMP/ate.token"
    kubectl ate --token-file "$RUNNER_TEMP/ate.token" get actors -a ci
```

The principal is the token's `sub`, which names the repository and the branch,
environment or pull request, such as `repo:my-org/my-repo:environment:prod`.
Its exact form depends on the repository (see GitHub's
[OIDC reference](https://docs.github.com/en/actions/reference/security/oidc)),
so decode a token before granting it. Any workflow on GitHub can request a
token for any audience, so grant only exact `sub` values. The job still needs
Kubernetes access for `kubectl ate`, as described
[above](#people-running-kubectl-ate).

## Configuring JWT providers

`ate-api-server` reads the issuers it trusts from the file passed to
`--authentication-config`, which the install mounts from the
`ate-api-authentication` ConfigMap:

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

- Provider names and issuers must be unique. `issuer` must be an HTTPS URL and
  `audiences` must be non-empty.
- `ate-api-server` checks a token with the provider whose `issuer` equals its
  `iss`. The signature must verify against keys from the issuer's OpenID
  discovery document, and `aud` must contain one of the provider's audiences.
- `certificateAuthorityFile` and `discoveryTokenFile` are optional; some
  private Kubernetes API servers need them for discovery.
- The Google values are what the Google Cloud CLI issues today: issuer
  `https://accounts.google.com`, and the Cloud SDK's shared client ID as the
  audience. They are examples, not built-in defaults; check the claims your
  identity provider issues.

The principal ID doesn't record which provider issued the token, so only
configure providers you trust not to issue a token whose `sub` is another
principal's ID.

`ate-setup` creates the ConfigMap on first install with one provider,
`kubernetes`, for the cluster's ServiceAccount issuer and the audience
`api.ate-system.svc`. Set `ateapi.expectedJWTIssuer` if it detects the wrong
issuer. Later runs leave an existing ConfigMap alone, so providers you add
survive them; `ate-setup create api-authentication-config` overwrites it.

To add a provider, edit the ConfigMap and restart `ate-api-server`, which reads
it only at startup:

```sh
kubectl -n ate-system get configmap ate-api-authentication \
  -o jsonpath='{.data.authentication\.yaml}' > authentication.yaml
# Add the provider to authentication.yaml, then:
kubectl -n ate-system create configmap ate-api-authentication \
  --from-file=authentication.yaml \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl -n ate-system rollout restart deployment/ate-api-server
```

`ate-api-server` caches signing keys and refreshes them periodically, and when
a token names an unknown key ID.

## Not yet supported

- **Signing in.** `kubectl ate` can't run a command to fetch or refresh a
  token; people obtain tokens themselves
  ([#1464](https://github.com/agent-substrate/substrate/issues/1464)).
- **`kubectl ate` without Kubernetes access** to the Substrate cluster.
- **Reaching `ate-api-server` from outside its cluster.** The install doesn't
  expose it.
- **JWT providers at install time.** `ate-setup` configures only the cluster's
  own ServiceAccount issuer; add others to the ConfigMap afterwards.
