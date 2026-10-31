---
title: "Authentication"
linkTitle: "Authentication"
weight: 3
description: >
  How clients authenticate to ate-api-server over SPIFFE mTLS or OIDC JWTs.
---

# ate-api Authentication

Every call to `ate-api-server` must carry a credential, which authentication
turns into a principal ID. [Authorization](authorization.md) then decides what
that principal may do.

- **Client certificate.** A client certificate must chain to the
  `podidentity.podcert.ate.dev/identity` trust bundle, or the TLS handshake
  fails. The principal is its first URI SAN, a SPIFFE ID. A bearer token sent
  alongside it is ignored.
- **Bearer JWT.** A client that sends no certificate needs an
  `authorization: Bearer <JWT>` header with a token from a
  [trusted issuer](#configuring-jwt-providers). The principal is the token's
  `sub`.
- A call with no certificate and no valid token gets `Unauthenticated`.

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

`kubectl ate` needs a kubeconfig for the Substrate cluster even when you pass
`--token-file`. It uses it to:

- list ClusterTrustBundles, to verify `ate-api-server`'s serving certificate;
- without `--endpoint`, get the `api` Service, list pods and port-forward in
  `ate-system`;
- without `--token-file`, create a token for the shared `ate-client`
  ServiceAccount, warning that it is acting as a shared identity;
- for `logs actor` and `top worker`, read worker pods' logs and metrics, and
  for `admin`, read and write the Secrets it manages.

A person who isn't a cluster admin needs these roles for the first two:

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
kubectl -n ate-system create rolebinding alice-ate-api-client --role=ate-api-client --user=alice@example.com
kubectl create clusterrolebinding alice-ate-api-client --clusterrole=ate-api-client --user=alice@example.com
```

`--user` is the person's Kubernetes user name, which on GKE is their Google
account email.

RBAC can limit port-forwarding only by pod name, and `ate-api-server`'s pod
names change on every rollout, so in practice the grant covers every pod and
port in `ate-system`. NetworkPolicy doesn't filter it. It reaches ports that
listen only on loopback, including the router's `/statusz`, which lists recent
requests to every atespace, and the unauthenticated Envoy admin APIs of the
router and egress gateway, which can drain or stop them. Grant it only to
people you'd trust to operate Substrate.

Without `--token-file`, everyone is the same principal, and `ate-setup` makes
it a bootstrap owner by default (see the warning in
[authorization.md](authorization.md)). That suits a cluster only you use. On a
shared cluster, give each person their own identity in one of the two ways
below.

`kubectl ate` rereads `--token-file` on every call but never refreshes it, so
get a new token when it expires. With `--token-file=-` it reads the token once
from stdin.

### Your own ServiceAccount

Create each person's ServiceAccount in a namespace set aside for them, such as
`ate-users`, and let each person create tokens only for their own
ServiceAccount:

```sh
kubectl create namespace ate-users
kubectl -n ate-users create serviceaccount alice
kubectl -n ate-users create role alice-token --verb=create --resource=serviceaccounts/token --resource-name=alice
kubectl -n ate-users create rolebinding alice-token --role=alice-token --user=alice@example.com
```

Alice then mints a token and passes it:

```sh
kubectl create token -n ate-users alice --audience api.ate-system.svc > alice.token
kubectl ate --token-file alice.token get actors -a team-a
```

The principal is `system:serviceaccount:ate-users:alice`. The audience must be
one the `kubernetes` provider lists; the config `ate-setup` writes lists
`api.ate-system.svc`.

RBAC grants only add up, so these roles don't stop anyone else from acting as
alice. Anyone who can create pods or tokens in `ate-users` can, and the
built-in `edit` and `admin` roles allow both. The same rights in `ate-system`
get the `ate-client` token. On GKE a call is allowed if either IAM or RBAC
allows it, and an IAM grant covers every namespace: Kubernetes Engine Developer
can create a token for any ServiceAccount, and the basic Editor role can create
pods anywhere.
Give these people only Kubernetes Engine Cluster Viewer in IAM, and grant the
rest through RBAC.

`ate-api-server` checks a token's signature and expiry without asking
Kubernetes whether its ServiceAccount still exists. Deleting alice's
ServiceAccount doesn't revoke tokens she already holds: they work until they
expire, plus a five-minute allowance for clock skew. To cut her off at once,
remove her from access policies.

### A token from your identity provider

If your identity provider issues OIDC ID tokens,
[add it as a JWT provider](#configuring-jwt-providers) and pass its token.
With the Google provider from that section:

```sh
gcloud auth print-identity-token | kubectl ate --token-file=- get actors -a team-a
```

The principal is the token's `sub`. For Google that is the numeric account ID,
not the email; [bootstrap owners](authorization.md#bootstrap-owners) shows how
to find it. Any Google account can get a token for the Cloud SDK's client ID,
so this provider authenticates every Google account, and access policies
decide what each may do. Any service you send one of these tokens to can
replay it here until it expires, so prefer a provider that issues tokens for
an audience of your own.

## Pods in the Substrate cluster

Apart from Substrate's own components, the install's NetworkPolicy admits
connections to `ate-api-server` only from pods in namespaces labeled
`ate.dev/api-access=true`, and not from worker pods. Label the client's
namespace:

```sh
kubectl label namespace <namespace> ate.dev/api-access=true
```

Then dial `api.ate-system.svc:443` and verify the serving certificate against
the `servicedns.podcert.ate.dev/identity` trust bundle, with server name
`api.ate-system.svc`. The Service is headless, so DNS returns one address per
replica; use round-robin load balancing to spread calls across them. Go
clients can use the generated stubs in `api/ateapi/v1`.

Authenticate with a ServiceAccount token or a pod certificate. The kubelet
updates all of these files, so read them from the volume instead of caching
them:

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
  kubectl ate --endpoint=api.ate-system.svc:443 --token-file=/var/run/ate/token get actors -a <atespace>
  ```

  It still reads the trust bundle through the Kubernetes API, which default
  RBAC lets every ServiceAccount do.
- **Pod certificate.** The principal is
  `spiffe://cluster.local/ns/<namespace>/sa/<name>`. The credential bundle is
  one PEM file holding the private key followed by the certificate chain;
  reread it on each TLS handshake. `kubectl ate` can't present a client
  certificate.

## Pods in another cluster

A pod in another cluster sends a ServiceAccount token from its own cluster.
That cluster's pod certificates aren't trusted, and it has no copy of the
Substrate cluster's `servicedns.podcert.ate.dev/identity` trust bundle, so the
pod's projected volume holds the token and a copy of the serving CA from
step 3:

```yaml
  volumes:
  - name: ate-api
    projected:
      sources:
      - configMap:
          name: ate-api-ca
      - serviceAccountToken:
          audience: api.ate-system.svc
          path: token
```

1. **Trust only a cluster whose admins you'd trust as owners here.**
   Principals don't name the cluster ([principals are not qualified by
   issuer](authorization.md#known-limitations)). Anyone who can get a token
   for `<namespace>/<name>` in the other cluster acts as
   `system:serviceaccount:<namespace>:<name>` here, whether or not that
   ServiceAccount exists here. That includes `ate-system:ate-client`, the
   default [bootstrap owner](authorization.md#bootstrap-owners), so replace it
   first, and run the caller as a ServiceAccount that no one else in either
   cluster can get a token for.
2. **Trust the other cluster's issuer.** Add a
   [JWT provider](#configuring-jwt-providers) whose `audiences` include the
   audience the pod requests. As a cluster admin of the other cluster, run
   `kubectl --context <other-cluster> get --raw /.well-known/openid-configuration | jq -r .issuer`
   to print its issuer; on GKE it is
   `https://container.googleapis.com/v1/projects/<project>/locations/<location>/clusters/<cluster>`.
   `ate-api-server` fetches the issuer's discovery document and keys over
   HTTPS, so they must be reachable from the Substrate cluster. GKE serves
   them publicly.
3. **Reach and verify `ate-api-server`.** The install doesn't expose it
   outside its cluster, and its NetworkPolicy admits only the in-cluster peers
   described [above](#pods-in-the-substrate-cluster). Exposing it and admitting
   that traffic is up to you; the [threat model](../security/threat-model.md) (T-09) says
   not to expose it directly to the internet. Its serving certificate names
   only in-cluster Service names such as `api.ate-system.svc`, so use that as
   the TLS server name whatever address you dial. Copy the serving CA into the
   other cluster, and copy it again when the CA rotates:

   ```sh
   kubectl --context <substrate-cluster> get clustertrustbundles -l podcert.ate.dev/canarying=live -o json \
     | jq -r '.items[] | select(.spec.signerName == "servicedns.podcert.ate.dev/identity") | .spec.trustBundle' \
     > ca.crt
   kubectl --context <other-cluster> -n <namespace> create configmap ate-api-ca --from-file=ca.crt \
     --dry-run=client -o yaml | kubectl --context <other-cluster> apply -f -
   ```

`kubectl ate` works there only with a kubeconfig for the Substrate cluster,
which it needs to read the trust bundle and, for `logs actor` and
`top worker`, worker pods' logs and metrics.

## CI jobs

Trust the CI system's OIDC issuer and send its token. Any workflow on GitHub
can request a GitHub Actions token for any audience, so this provider
authenticates every workflow on GitHub, and access policies decide which of
them may do anything. Use an audience unique to this install, so that a token
minted for it can't be replayed against another install that trusts GitHub:

```yaml
- name: github-actions
  issuer: https://token.actions.githubusercontent.com
  audiences:
  - ate-api.prod.example.com
```

and request a token in the job:

```yaml
permissions:
  contents: read
  id-token: write
steps:
- run: |
    curl -sSf -H "Authorization: bearer $ACTIONS_ID_TOKEN_REQUEST_TOKEN" \
      "$ACTIONS_ID_TOKEN_REQUEST_URL&audience=ate-api.prod.example.com" \
      | jq -r .value > "$RUNNER_TEMP/ate.token"
    kubectl ate --token-file "$RUNNER_TEMP/ate.token" get actors -a ci
```

The principal is the token's `sub`. Grant only a `sub` that names the
repository by ID and either a protected branch or an environment that only
protected branches can deploy to, such as
`repo:my-org@123456/my-repo@456789:ref:refs/heads/main`. Repositories created
before July 15, 2026 use names instead of IDs until they opt in or are renamed
or transferred, and a name can pass to a new owner. Don't grant a `sub` ending
in `:pull_request`, which every pull request job without an environment
shares. GitHub's
[OIDC reference](https://docs.github.com/en/actions/reference/security/oidc)
lists the forms; decode a token before granting it.

The job also needs a kubeconfig for the Substrate cluster. On GKE, get one with
`google-github-actions/auth` and `google-github-actions/get-gke-credentials`
using a service account. As for people [above](#your-own-serviceaccount), give
it only Kubernetes Engine Cluster Viewer in IAM, and bind the `ate-api-client`
roles [above](#people-running-kubectl-ate) to it; its `--user` is the service
account's email.

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
- `certificateAuthorityFile` and `discoveryTokenFile` are for an in-cluster
  issuer such as `https://kubernetes.default.svc`, and `ate-setup` sets them
  for one. `certificateAuthorityFile` replaces the system roots for that
  provider. `discoveryTokenFile` requires it, and sends the token in that file
  to the issuer and to any `/openid/v1/jwks` URL. The example points it at
  `ate-api-server`'s own ServiceAccount token, so set it only for the
  Substrate cluster's own issuer.
- The Google values are what the Google Cloud CLI issues today: issuer
  `https://accounts.google.com`, and the Cloud SDK's shared client ID as the
  audience. They are examples, not built-in defaults; check the claims your
  identity provider issues.

The principal ID doesn't record which provider issued the token, so only
configure providers you trust not to issue a token whose `sub` is another
principal's ID.

`ate-setup` creates the ConfigMap on first install with one provider,
`kubernetes`, for the cluster's ServiceAccount issuer and the audience
`api.ate-system.svc`. Later runs leave it alone, so providers you add survive
them. If it detected the wrong issuer, set `ateapi.expectedJWTIssuer`, run
`ate-setup create api-authentication-config`, which rewrites the file and
drops any providers you added, then restart `ate-api-server` as below.

To add a provider, edit the ConfigMap and restart `ate-api-server`, which reads
it only at startup:

```sh
kubectl -n ate-system get configmap ate-api-authentication \
  -o jsonpath='{.data.authentication\.yaml}' > authentication.yaml
# Add the provider to authentication.yaml, then:
kubectl -n ate-system create configmap ate-api-authentication \
  --from-file=authentication.yaml \
  --dry-run=client -o yaml | kubectl replace -f -
kubectl -n ate-system rollout restart deployment/ate-api-server
kubectl -n ate-system rollout status deployment/ate-api-server
```

If the rollout doesn't finish, the new pod (the one restarting in
`kubectl -n ate-system get pods -l app=ate-api-server`) logs what is wrong with
the config. Fix the ConfigMap promptly: the old pods mount it too and read the
bad file if they restart.

`ate-api-server` caches signing keys and refreshes them periodically, and when
a token names an unknown key ID.

## Not yet supported

- **Signing in.** `kubectl ate` can't run a command to fetch or refresh a
  token; people obtain tokens themselves.
- **`kubectl ate` without Kubernetes access** to the Substrate cluster, or,
  from outside the cluster, without port-forward rights in `ate-system`.
- **Reaching `ate-api-server` from outside its cluster.** The install doesn't
  expose it.
- **JWT providers at install time.** `ate-setup` configures only the cluster's
  own ServiceAccount issuer; add others to the ConfigMap afterwards.
