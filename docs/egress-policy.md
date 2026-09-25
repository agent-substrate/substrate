# Actor egress policy

All actor egress traffic (with the exception of DNS) is proxied through the
Substrate Egress Gateway. The Egress Gateway uses an Actor's `EgressPolicy` to
determine whether the traffic is allowed or denied. This document
describes the rule types a policy is built from and the order they are
evaluated in.

## The resource

A `EgressPolicy` holds an ordered list of *allow* rules.
there is no deny rule and no priority field. Everything not allowed by a rule
is denied, so:

* An actor with **no** `EgressPolicy` reaches nothing.
* An actor whose policy has **no rules** reaches nothing.

Create the policy before resuming the actor, so the actor's first outbound
connection already finds it.

## The rule types

An `EgressRule` carries exactly one destination matcher. The three are
`hostnames`, `cidrs`, and `all`.

### `hostnames` — match the destination name

```yaml
hostnames:
  patterns:
  - api.example.com
  - "*.googleapis.com"
```

Matches when the hostname the request named equals a pattern.

A hostname rule is the only rule type that can carry `effects`.
The only effect today is `injectStaticHeaders`: a list of
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

### `cidrs` — match the destination address

```yaml
cidrs:
  cidrs:
  - 192.0.2.0/24
  - 2001:db8::/32
```

Matches when the address the actor originally dialed falls inside any prefix.

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

Put the specific rules that carry effects
before any broader rule that would swallow them. In this policy the second rule
is dead, because `all` already matched everything:

```yaml
rules:
- all: {}
- hostnames:                  # never reached
    patterns: ["api.example.com"]
    effects: {...}
```
