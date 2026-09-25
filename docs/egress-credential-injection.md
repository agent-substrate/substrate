# Egress credential injection

Under an sdsmint install, the egress gateway terminates every TLS connection an
actor opens and re-originates it (see
[egress-trust-bundle.md](egress-trust-bundle.md)). Egress credential injection
runs on that decrypted leg: when an actor's `EgressPolicy` rule matches a
request and carries an `inject_static_headers` effect, the gateway resolves the
referenced credential from a **credential provider** and sets it as a request
header (for example `Authorization: Bearer <token>`) before the request leaves
the cluster.

The actor never holds the secret. It cannot read it, snapshot it, or exfiltrate
it — and a header the actor sets itself is overwritten, so it cannot smuggle a
credential of its own choosing past the policy.

## When you need this

* Actors call APIs that need bearer tokens or API keys, and those secrets must
  stay out of actor filesystems, environments, and snapshots.
* The cluster runs the sdsmint egress gateway with the envoy dataplane —
  injection happens on the TLS-terminated MITM leg, so there is no injection
  without interception.

## How it works

The injector is part of the egress gateway's existing `ext_proc` handler — the
one that already fetches, caches, and evaluates each actor's `EgressPolicy` on
the MITM leg.

When a matched rule carries injections, the handler calls the **credential
provider** over mTLS: a gRPC service implementing
`CredentialProvider.FetchSecret` (`pkg/proto/credproviderpb`). 

The provider is the **only** component in the path with
access to secret storage — the gateway and injector never read secrets at rest.

The provider is a plugin: any gRPC service that implements
`CredentialProvider.FetchSecret` and authorizes callers the same way can back
injection. To bring your own — reading HashiCorp Vault, Google Secret Manager,
or any other secret store — implement the API under your own provider class
(the URI host, e.g. `ate-secret://vault.example.com/...`), then point the
gateway at it with `--credential-provider-name` and
`--credential-provider-address`. A gateway currently fronts **one** provider:
a policy URI naming any other class fails closed rather than being sent to the
wrong provider.

The reference implementation, `cmd/credential-provider/kubernetes-secrets`,
resolves URIs of the `k8s.io` class to Kubernetes Secret values and enforces a
**default-deny atespace→namespace policy**: an actor's atespace may only
resolve Secrets in namespaces explicitly granted to it. A custom provider
should enforce an equivalent actor-scoped authorization on the asserted
SPIFFE identity — the gateway attests *which* actor is asking, but what that
actor may resolve is the provider's decision.

## The policy

Injection is declared per hostname rule. Here is an example of the rule shape, in protojson:

```json
{
  "hostnames": {
    "patterns": ["api.example.com"],
    "effects": {
      "injectStaticHeaders": [{
        "header": "Authorization",
        "prefix": "Bearer ",
        "credentialUri": "ate-secret://k8s.io/default/team-a-secrets/example-api/token"
      }]
    }
  }
}
```

* `header` is set on the request; a value the actor sent is overwritten.
* `prefix` is prepended verbatim to the credential — include the separator,
  e.g. `"Bearer "` with the trailing space.
* `credentialUri` is a source-agnostic reference,
  `ate-secret://<provider-class>/<provider-name>/<provider-specific-tail>`,
  interpreted by the provider. For the Kubernetes Secrets provider:
  `ate-secret://k8s.io/default/<namespace>/<secret>/<key>`.

Effects never authorize traffic: they apply only when the rule is the first
match for a request the policy allows anyway.

## What the gateway does

| Situation | Outcome |
|---|---|
| TLS MITM leg, provider configured, credential resolves | Header injected, overwriting any actor-set value; request re-originated upstream |
| Cleartext (plain HTTP) leg | Injection **skipped**, request passes through without the credential — a secret is never put on a cleartext wire |
| No provider configured (injection not enabled at install) | Injection **skipped**, request passes through — a policy that asks for injection does not break egress on a gateway that cannot perform it |
| Secret missing, or namespace not authorized for the atespace | **403**, fail closed |
| Provider unreachable or timed out | **503**, fail closed but retryable |
| URI names a provider class this gateway does not serve; unusable header or URI; empty or malformed secret | **500** (or 503 for the unusable secret), fail closed |

The dividing line: skipping is only for a gateway that was never asked to
inject on this leg. Once injection is *attempted* — TLS leg, provider
configured — any failure to produce the credential the policy promised denies
the request rather than letting it out without it.

## Enable it

**1. The gateway.** Injection is an install-time modifier on the egress
gateway (requires `--experimental-use-sdsmint` and the envoy dataplane):

```bash
hack/install-ate.sh --deploy-atenet \
  --experimental-use-sdsmint \
  --experimental-egress-credential-injection
```

| Flag | Purpose | Default |
|---|---|---|
| `--credential-provider-name` | Provider class the gateway serves, as an `ate-secret://` prefix; a policy URI of any other class fails closed | `ate-secret://k8s.io` |
| `--credential-provider-address` | Where the gateway dials the provider | `k8s-credential-provider.ate-system.svc:50051` |

**2. The provider.** A separate component — the flag above only configures the
gateway's client side. Until something serves the configured address, every
matching injection rule fails closed with 503. For the Kubernetes Secrets
provider, deploy the manifests under `manifests/egress-credential-injection/`:

```bash
# The atespace→namespace authorization policy (edit for your atespaces first;
# default-deny, so an atespace absent from it resolves nothing):
kubectl apply -f manifests/egress-credential-injection/namespace-policy.yaml

# A sample secret matching the sample policy:
kubectl apply -f manifests/egress-credential-injection/sample-secret.yaml

# The provider itself (ko builds its image):
hack/run-tool.sh ko apply -f manifests/egress-credential-injection/k8s-credential-provider.yaml
```

The provider loads the namespace policy **once at startup**. After editing the
ConfigMap, restart it:

```bash
kubectl -n ate-system rollout restart deployment/k8s-credential-provider
```

**3. The actor's policy.** Add the injection effect to the hostname rule, as
above. The actor also needs the projected egress trust bundle to do TLS
through the MITM gateway at all — see
[egress-trust-bundle.md](egress-trust-bundle.md).

## Verify

The e2e suite `internal/e2e/suites/egresscredinject` is the executable form of
the behavior table above: it proves the injected header on the wire (a
headers-echo origin reports what it received), the overwrite of an actor-set
header, the cleartext skip, and each fail-closed status. Against a kind
cluster:

```bash
hack/install-ate-kind.sh --deploy-atenet --experimental-use-sdsmint --experimental-egress-credential-injection
E2E_EGRESS_CREDINJECT=1 hack/run-e2e-kind.sh ./internal/e2e/suites/egresscredinject -v -args --no-color
```

For a quick manual check, point an actor whose policy injects at
`https://httpbin.org/headers` and look for the injected header in the echoed
response.

## Operational notes

**Transient 503s right after (re)deploying the provider.** The gateway keeps a
long-lived gRPC channel to the provider. If the provider's Service was deleted
and recreated, the channel can sit in connect backoff for up to a couple of
minutes before re-resolving; injection fails closed with 503 (deliberately
retryable) until it reconnects.

**Policy ConfigMap edits need a provider restart** (startup-only load; dynamic
reload is a known TODO in `manifests/egress-credential-injection/namespace-policy.yaml`).

**URIs reject percent-encoding.** Namespace, secret, and key segments are used
literally; a URI with `%`-escapes is refused at policy write time.

**Scope of the reference provider's RBAC.** The sample deployment grants read
on Secrets cluster-wide so the policy may name any namespace; a production
deployment should scope this to the namespaces the provider is allowed to
serve (see the note in `k8s-credential-provider.yaml`).

## See also

* [egress-trust-bundle.md](egress-trust-bundle.md) — the MITM leg this feature
  runs on, and the actor-side trust projection it presupposes.
* `demos/egress/README.md` — how tunneled egress, actor identity, and policy
  authorization fit together.
* `pkg/proto/credproviderpb/credprovider.proto` — the provider plugin API and
  its trust model.
* `cmd/credential-provider/kubernetes-secrets` — the reference provider.
