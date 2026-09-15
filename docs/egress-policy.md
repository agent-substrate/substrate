# Actor egress policy

Every outbound TCP connection an actor makes leaves through the egress gateway, and
the gateway authorizes it against the actor's `EgressPolicy`. This document
describes the three rule types a policy is built from, the order they are
evaluated in, and how the policy composes with an additional external
processor configured through
`--experimental-additional-egress-extproc-service`.

## The resource

`EgressPolicy` is a resource nested under an Actor. An Actor has at most one,
its `metadata.name` is always `default`, and its `metadata.atespace` must match
the parent Actor. It is managed through the control API:
`CreateActorEgressPolicy`, `GetActorEgressPolicy`, `UpdateActorEgressPolicy`,
and `DeleteActorEgressPolicy`. Updates are full replacements.

A policy holds an ordered list of up to 256 rules. Rules only ever *allow*;
there is no deny rule and no priority field. Everything not allowed by a rule
is denied, so:

* An actor with **no** `EgressPolicy` reaches nothing.
* An actor whose policy has **no rules** reaches nothing.

Create the policy before resuming the actor, so the actor's first outbound
connection already finds it.

## The three rule types

An `EgressRule` carries exactly one destination matcher. The three are
`hostnames`, `cidrs`, and `all`.

### `hostnames` — match the destination name

```yaml
hostnames:
  patterns:
  - api.example.com
  - "*.googleapis.com"
```

Matches when the hostname the request named equals a pattern. Patterns are
lowercase DNS names conforming to RFC 1034 and RFC 1123, with no trailing dot,
up to 256 per rule.

A pattern may carry a `*` that stands for the **complete leftmost label**:
`*.example.com` matches `api.example.com`, but neither `example.com` nor
`nested.api.example.com`. No other wildcard syntax is accepted. A pattern
without a wildcard matches only the whole normalized name.

Internationalized names must be written in their IDNA A-label (punycode) form;
no Unicode conversion happens. URLs, IP literals, names with ports, and
malformed names such as `foo..example.com` are rejected at write time.

A hostname rule is the only rule type that can carry `effects` (see
[Effects](#effects)). It is also the only one that changes *where* the request
goes: a request allowed by name is resolved and dialed by that name.

A hostname rule can only match where a hostname is known. A connection the
gateway sees only as an address — including the tunnel-open decision described
below — never matches one.

### `cidrs` — match the destination address

```yaml
cidrs:
  cidrs:
  - 192.0.2.0/24
  - 2001:db8::/32
```

Matches when the address the actor originally dialed falls inside any prefix,
up to 256 per rule. Prefixes must be canonical: IPv4 in dotted-decimal, IPv6 in
lowercase compressed RFC 5952 form, every bit after the prefix length zero, and
no IPv4-mapped IPv6 prefixes.

CIDR rules match the dialed address, never the `Host` header. A request to
`https://example.com/` is matched by a CIDR rule only if the address the actor's
kernel connected to is in the prefix.

### `all` — match everything

```yaml
all: {}
```

Matches every destination, by name or by address. This is the blanket allow;
use it for development and for actors whose egress is not being restricted.

## Evaluation order

Rules are evaluated **in list order, first match wins**:

1. Walk the rules from index 0.
2. The first rule that matches the destination authorizes it. Evaluation stops
   there, even if a later rule would also match.
3. Only the matching rule's effects are applied. Effects on later rules that
   would also have matched are not.
4. If no rule matches, the request is denied.

Order therefore matters, and the list is atomic — a server or client that
reorders it changes the policy. Put the specific rules that carry effects
before any broader rule that would swallow them. In this policy the second rule
is dead, because `all` already matched everything:

```yaml
rules:
- all: {}
- hostnames:                  # never reached
    patterns: ["api.example.com"]
    effects: {...}
```

### Where each decision is made

The policy is evaluated at two points, and which rule types can match differs
between them.

**When the connection opens**, the gateway knows only the address the actor
dialed, so only `cidrs` and `all` rules can match. There are three outcomes:

* A `cidrs` or `all` rule matches: the connection is allowed and that address
  is the one it may reach.
* No rule matches, but the policy contains at least one hostname rule: the
  connection opens with nothing authorized yet, and each request inside it is
  decided on its own.
* Neither: the connection is refused immediately.

**When a request the gateway can read goes through it** — cleartext HTTP, or
HTTPS under a MITM-intercepting install — the full destination is known: the
hostname the request named plus the dialed address. All three rule types can
match, and the rules are walked again from the top for every request, because
the `Host` may change between requests on one connection.

One consequence worth noting: a policy of only hostname rules still lets a
connection open to any address, but nothing flows unless a request inside it
names an allowed host.

Matching requires the request to name one destination unambiguously. A request
whose `:authority` and `Host` name different destinations, or whose authority is
not a DNS name or IP literal, is denied without being evaluated.

Header-injecting effects need the gateway to read the request, which means an
HTTPS destination needs MITM interception and an actor configured to trust the
gateway's CA — see
[Enabling MITM interception for Actor Egress policy](egress-trust-bundle.md).

## Effects

`effects` hang off a hostname rule and are applied once, when that rule is the
first match. They do not authorize anything on their own.

The only effect today is `injectStaticHeaders`: a list of up to 16
`CredentialHeaderInjection` entries, each naming a case-insensitively unique
HTTP request header, an optional literal `prefix`, and a `credentialUri` of the
form `substrate-secret://<provider-class>/<provider-name>/<tail>`.

```yaml
hostnames:
  patterns: ["api.example.com"]
  effects:
    injectStaticHeaders:
    - header: Authorization
      prefix: "Bearer "               # include the separator yourself
      credentialUri: substrate-secret://kubernetes/my-provider/my-secret
```

**Credential injection is not implemented yet.** A request that matches a rule
promising an injection is denied with `501`, rather than being forwarded without
the credential the policy author expects it to carry.

## Writing a policy

There is no CLI surface yet; use the control API. The e2e helpers in
`internal/e2e/egresspolicy.go` are the shortest working example:

```go
_, err := client.CreateActorEgressPolicy(ctx, &ateapipb.CreateActorEgressPolicyRequest{
    Actor: actorRef,
    EgressPolicy: &ateapipb.EgressPolicy{
        Metadata: &ateapipb.ResourceMetadata{
            Atespace: actorRef.GetAtespace(),
            Name:     "default",
        },
        Rules: []*ateapipb.EgressRule{
            // Specific first: this rule's effects would be lost behind a
            // broader rule that also matched.
            {Hostnames: &ateapipb.HostnameRule{Patterns: []string{"*.googleapis.com"}}},
            {Cidrs: &ateapipb.CIDRRule{Cidrs: []string{"10.0.0.0/8"}}},
        },
    },
})
```

`UpdateActorEgressPolicy` replaces the rule list wholesale and requires the
metadata UID and version as preconditions; `atespace` and `name` are immutable.

A changed policy is not picked up instantly: the gateway caches each actor's
compiled policy for 10 seconds, so a decision can be that stale. A policy
lookup that fails against the control plane is a `503` and is not cached; an
actor with no policy is cached as such, so a flood of denied requests does not
hammer the control plane.

## Using it with `--experimental-additional-egress-extproc-service`

`--experimental-additional-egress-extproc-service NS/SVC:PORT` (on both
`ate-setup` and `hack/install-ate.sh`) runs an additional external processor on
the gateway's decrypted request path. It is an authorization hook for policy
that `EgressPolicy` cannot express — method, path, headers, or anything else
that needs a service to decide — layered on top of the policy rather than
replacing it.

### Substrate's policy is evaluated first

The additional processor runs **after** Substrate's own egress-policy check, on
each request the gateway can read. That ordering is the contract:

* A request the actor's `EgressPolicy` denies **never reaches** the additional
  processor. It cannot observe, override, or re-allow such a request.
* A request the policy allows arrives with the matching rule's effects already
  applied, so the processor sees the request as it will be sent, headers the
  policy added included.
* The additional processor is a second, independent gate: it can still reject a
  request the policy allowed. Both must allow for the request to go out.

### Enabling it

```bash
hack/install-ate.sh --deploy-atenet \
  --experimental-use-sdsmint \
  --experimental-additional-egress-extproc-service my-namespace/my-authz:50051
```

or with `ate-setup`, which takes the same flags, and also reads
`ATE_ADDITIONAL_EGRESS_EXTPROC_SERVICE` when the flag is absent.

Constraints the installer enforces:

* The value must be exactly `<namespace>/<service>:<port>`, with a valid port
  in 1–65535.
* It requires `--experimental-use-sdsmint`. Without MITM interception there is
  no decrypted request for the processor to see.
* It requires the default `--atenet-router=envoy`; the agentgateway dataplane
  does not support it.

### What the service must implement

The gateway dials `<service>.<namespace>.svc.cluster.local:<port>` and speaks
the Envoy `ext_proc` v3 `ExternalProcessor` gRPC service over HTTP/2.

* **TLS is mandatory and mutual.** The connection is TLS 1.3 only. The gateway
  presents its pod-identity client certificate, and the service's certificate
  must carry the DNS SAN `<service>.<namespace>.svc`, which is also the SNI
  sent.
* **Request headers only.** The processor receives the request headers; request
  bodies, trailers, and the whole response direction are skipped. Deployments
  should not expect to inspect payloads.
* **The actor identity comes as an attribute**, not a header: the
  `dev.ate.actor.identity` filter state, whose value is the actor's SPIFFE ID
  (`spiffe://substrate-actor.local/atespace/<atespace>/actor/<name>`),
  authenticated from the client certificate. Never trust an actor identity read
  out of a request header.
* **It must be fast and available.** The call has a 2-second timeout, and the
  filter fails closed: if the service is down, slow, or errors, the request is
  denied.
* **Header mutations are constrained.** The processor may set ordinary request
  headers but not system (`:`-prefixed) ones, and an attempt to mutate a
  disallowed header fails the request rather than being silently dropped.

## See also

* [Enabling man-in-the-middle (MITM) interception for Actor Egress policy](egress-trust-bundle.md)
  — how an actor is configured to trust the gateway so HTTPS egress works.
