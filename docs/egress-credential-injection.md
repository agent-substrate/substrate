# Egress credential injection

Egress credential injection puts a secret into an actor's outbound HTTPS
requests without the actor ever holding it. It works on HTTPS the egress
gateway intercepts: an `https` rule in the actor's `EgressPolicy` has the
gateway terminate the actor's TLS for the names it lists and re-originate it
(see [egress-trust-bundle.md](egress-trust-bundle.md)). When that rule carries
a `replace_headers` effect and the actor's request includes a header it names,
the gateway resolves the referenced credential from a **credential provider**
and replaces the header's value with it (for example
`Authorization: Bearer <token>`) before the request leaves the cluster.

The actor never holds the secret. It sends the header with a placeholder
value, which the gateway discards, so it cannot read, snapshot, or exfiltrate
the secret, nor choose the value that leaves the cluster.


## When you need this

Actors call APIs that need bearer tokens or API keys, and those secrets must
stay out of the actor's filesystem, environment, and snapshots.

Injection is supported on HTTP and HTTPS requests allowed by an `http` or
`https` rule with credential replacement effects. For HTTPS, the actor must
trust the gateway CA — see [egress-trust-bundle.md](egress-trust-bundle.md).

## How it works

```mermaid
flowchart LR
    actor["Actor"]
    gateway["Egress gateway<br/>applies the EgressPolicy"]
    provider["Credential provider"]
    store[("Secret store")]
    upstream["Upstream API"]

    actor -->|"HTTPS request,<br/>placeholder header"| gateway
    gateway -->|"fetch credential<br/>for this actor"| provider
    provider -->|"authorize, then read"| store
    gateway -->|"request with<br/>the credential"| upstream
```

1. The actor makes an HTTPS request, sending the header the policy names with
   a placeholder value. For a name an `https` rule lists, the gateway
   terminates the actor's TLS — the actor trusts the gateway CA through a
   projected trust bundle — and decides the decrypted request against the
   actor's `EgressPolicy`.
2. If the deciding rule has `replace_headers` and the request carries one of
   those headers, the gateway asks the credential provider for the credential
   that header names.
3. The provider decides whether that actor may have that credential, reads it
   from its secret store, and returns it. The provider is the **only**
   component in the path with access to secret storage — the gateway never
   reads secrets at rest.
4. The gateway replaces the placeholder with the credential and sends the
   request on to the origin over a new TLS connection.

The reference k8s secret provider, `cmd/credential-provider/kubernetes-secrets`, resolves
Kubernetes Secrets and is the one [For cluster admins](#for-cluster-admins)
deploys.

## The policy

Injection is declared in the `effects` of an `http` or `https` rule. This policy allows
HTTPS to `api.example.com` on port 443, the default, and replaces the actor's
`Authorization` header with a credential:

```bash
rules:
- https:
    hostnames: ["api.example.com"]
    effects:
      replaceHeaders:
      - header: Authorization
        prefix: "Bearer "
        credentialUri: ate-secret://k8s.io/default/ns1/example-api/token
```

The actor then sends the header with any placeholder value:

```bash
curl -H 'Authorization: placeholder' https://api.example.com/v1/items
```

This URI names the sample Secret deployed under [Enable it](#enable-it). The
sample namespace policy lets only actors in atespace `team-a` resolve it; an
actor in any other atespace gets a 403 until a cluster admin grants its
atespace access.

* `header` names the request header to replace.
* `prefix` is prepended verbatim to the credential — include the separator,
  e.g. `"Bearer "` with the trailing space.
* `credentialUri` names the credential; see [Credential URIs](#credential-uris).

### Credential URIs

A credential URI has the form `ate-secret://<provider-class>/<path>`: the host
names the provider that resolves it, and the path is that provider's to
interpret. A URI the provider refuses denies the request with 403.

* **Kubernetes Secrets** (provider class `k8s.io`) — see
  [The reference provider](#the-reference-provider).
* **Google Cloud Secret Manager** (provider class
  `secretmanager.googleapis.com`) — see its
  [README](../internal/plugins/gcp-secret-manager/README.md#credential-uris).

## What the gateway does

| Situation | Outcome |
|---|---|
| Request decided by an `http` or `https` rule, carries the header, provider configured, credential resolves | Header value replaced with the credential; request re-originated upstream |
| Request decided by an `http` or `https` rule, does not carry the header | Forwarded unchanged; no credential is fetched |
| Request decided by an `http` or `https` rule, carries the header, no provider configured (`--credential-provider='{"enabled":false}'`) | **500** (Envoy) or **403** (agentgateway), fail closed |
| Secret missing, or namespace not authorized for the atespace | **403**, fail closed |
| Provider unreachable or timed out | **503**, fail closed but retryable |
| Provider returns an empty credential, or one containing control characters | **503**, fail closed |
| URI names a provider class this gateway does not serve; unusable header name; unparseable URI | **500** (Envoy) or **403** (agentgateway), fail closed |

A request that does not carry the header skips injection. On an HTTP or HTTPS
request that carries it, any failure to produce the credential, including
having no provider, denies the request rather than letting it out without
the credential.

## For cluster admins

### Enable it

**1. The gateway.** The provider is selected when the egress gateway is
deployed, with `--deploy-ate-system` or `--deploy-atenet`, for either the Envoy
or agentgateway dataplane. The selection is required: an install that wants
no injection says so with `{"enabled":false}`.

```bash
hack/install-ate.sh --deploy-atenet --credential-provider='{"name":"k8s.io"}'
```

`--credential-provider` (or `ATE_CREDENTIAL_PROVIDER` when the flag is
absent) is a JSON object with these keys; any other key is an error:

| Key | Value |
|---|---|
| `enabled` | Defaults to `true`. `false` turns injection off, and then no other key is allowed |
| `name` | Required unless `enabled` is `false`. A DNS name: `k8s.io` for the bundled Kubernetes Secrets provider, or the name of a provider you deploy, which is the host of the `ate-secret://` URIs it serves, e.g. `vault.example.com` for `ate-secret://vault.example.com/...`. A policy URI naming any other provider fails closed |
| `address` | The `host:port` the gateway dials the provider at. Required for a provider you deploy; optional with `k8s.io`, where it defaults to the bundled provider's Service, `k8s-credential-provider.ate-system.svc:50051` |

For example:

```bash
--credential-provider='{"name":"k8s.io"}'
--credential-provider='{"enabled":false}'
--credential-provider='{"name":"vault.example.com","address":"vault-provider.ate-system.svc:50051"}'
```

**2. The provider.** With `k8s.io` the install deploys the Kubernetes Secrets
provider itself, from `manifests/egress-credential-injection/`, and waits for
it alongside the gateway. The install applies:

* the provider's ServiceAccount, its Deployment and its Service in
  `ate-system`;
* a ClusterRole and ClusterRoleBinding that let it read Secrets in every
  namespace;
* the `k8s-credential-provider-namespace-policy` ConfigMap, only when it is
  absent (see below); and
* a NetworkPolicy that admits only the egress gateway's pods
  (`app: atenet-egress`), on the provider's gRPC port. Every other connection
  to the provider pods is dropped. The NetworkPolicy takes effect only on a
  cluster whose network plugin enforces NetworkPolicy, such as GKE Dataplane
  V2, which `tools/setup-gcp` turns on by default. Elsewhere the provider's
  check of the gateway's client certificate is the only restriction.

Selecting any other provider, or `{"enabled":false}`, removes a bundled
provider an earlier install left behind, NetworkPolicy included. A provider of
your own is yours to deploy, and until something serves the configured address
every matching injection rule fails closed with 503.

The provider enforces an atespace→namespace authorization policy, the
`k8s-credential-provider-namespace-policy` ConfigMap in `ate-system`. The install creates
it empty, granting nothing, and only when it is absent, so a redeploy keeps
edits. Grant each atespace the namespaces whose Secrets it may resolve:

```bash
kubectl -n ate-system edit configmap k8s-credential-provider-namespace-policy
```

```yaml
  namespace-policy.yaml: |
    policies:
    - atespace: team-a
      allowedNamespaces:
      - ns1
```

The provider loads the policy **once at startup** and does not yet reload it,
so after editing, restart the provider:

```bash
kubectl -n ate-system rollout restart deployment/k8s-credential-provider
```

For a first smoke test, grant your actor's atespace `ns1` as above and apply a
sample secret that the URI in the sample policy resolves to:

```bash
kubectl apply -f manifests/egress-credential-injection/sample-secret.yaml
```

**3. The actors.** Actors can now add `replaceHeaders` effects to their
`https` rules, as described under [The policy](#the-policy). Each such actor
also needs the projected egress trust bundle to do TLS through the gateway at
all — see [egress-trust-bundle.md](egress-trust-bundle.md).

### Verify

Confirm the provider is ready and its NetworkPolicy is in place:

```bash
kubectl -n ate-system rollout status deployment/k8s-credential-provider
kubectl -n ate-system get networkpolicy k8s-credential-provider
```

Then give an actor an `https` rule for `httpbin.org` that replaces a header,
have it request `https://httpbin.org/headers` with that header set to a
placeholder, and look for the credential in the echoed response.

If the request is denied instead, the gateway logs the reason:

```bash
kubectl -n ate-system logs deployment/atenet-egress -c ext-proc | grep 'egress denied'
```

### Operational notes

**Transient 503s right after (re)deploying the provider.** The gateway keeps a
long-lived gRPC channel to the provider. If the provider's Service was deleted
and recreated, the channel can sit in connect backoff for up to a couple of
minutes before re-resolving; injection fails closed with 503 (deliberately
retryable) until it reconnects.

**Scope of the reference provider's RBAC.** The sample deployment grants read
on Secrets cluster-wide so the policy may name any namespace; a production
deployment should scope this to the namespaces the provider is allowed to
serve (see the note in `k8s-credential-provider.yaml`).

## For credential provider developers

### The plugin API

The provider is a plugin: any gRPC service that implements
`CredentialProvider.FetchSecret` (`pkg/proto/credproviderpb/credprovider.proto`)
can back injection. To bring your own — reading HashiCorp Vault, Google Secret
Manager, or any other secret store — implement the API under your own provider
class (the URI host, e.g. `ate-secret://vault.example.com/...`), then have a
cluster admin point the gateway at it with
`--credential-provider='{"name":"vault.example.com","address":"<host>:<port>"}'`.
A gateway currently fronts **one** provider:
a policy URI naming any other class fails closed rather than being sent to the
wrong provider.

The gateway reads only the URI host, to confirm the URI targets the provider it
serves. Everything after the host is the provider's to interpret.

`internal/plugins/gcp-secret-manager` is such a provider for Google Cloud Secret Manager.

**Trust model.** The gateway dials the provider over mTLS with its own pod
identity, `spiffe://cluster.local/ns/ate-system/sa/atenet-egress`, and every
`FetchSecretRequest` carries the SPIFFE ID of the actor the gateway verified.
A provider should:

* accept only callers presenting the gateway's identity — the reference
  provider requires a client certificate chaining to the pod-identity trust
  bundle and pins that URI SAN on every connection; and
* enforce an actor-scoped authorization on the asserted actor SPIFFE ID — the
  gateway attests *which* actor is asking, but what that actor may resolve is
  the provider's decision.

**Status codes.** The gateway maps a `FetchSecret` failure onto the actor's
request: `NotFound` and `PermissionDenied` deny with 403 (retrying cannot
succeed); `Unavailable` and `DeadlineExceeded` fail closed with a retryable
503; any other code denies with 403.

### The reference provider

`cmd/credential-provider/kubernetes-secrets` is a working example to start
from. It resolves URIs of the `k8s.io` class,
`ate-secret://k8s.io/default/<namespace>/<secret>/<key>`, to Kubernetes Secret
values, and enforces a **default-deny atespace→namespace policy**, read from
the `k8s-credential-provider-namespace-policy` ConfigMap: an actor's atespace
may only resolve Secrets in namespaces explicitly granted to it.

## See also

* [egress-trust-bundle.md](egress-trust-bundle.md) — the actor-side trust
  projection needed for HTTPS injection.
* `demos/egress/README.md` — how tunneled egress, actor identity, and policy
  authorization fit together.
* `pkg/proto/credproviderpb/credprovider.proto` — the provider plugin API and
  its trust model.
* `cmd/credential-provider/kubernetes-secrets` — the reference provider.
* `internal/e2e/suites/egresscredinject` — the e2e suite that proves the
  behavior table above on both sandbox classes, against a cluster installed
  with `--credential-provider='{"name":"k8s.io"}'`.
