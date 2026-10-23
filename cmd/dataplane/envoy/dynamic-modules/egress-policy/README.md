# Envoy Substrate Egress Policy Implementation

This Envoy dynamic module implements two extensions that work together to minimize
memory usage by Substrate egress policies and to improve latency and QPS by minimizing
external callouts for policy enforcement.

To minimize memory usage egress policies are hashed using [Rapidhash](https://docs.rs/rapidhash/latest/rapidhash/)
which produces 64 bit hashes with low probability of collision. Policies are then stored in the worker local
store using computed hash and the hash is exchanged between outer CONNECT request and inner
listener that terminates TCP connection encapsulated in the CONNECT request.

To minimize callouts the pep_http_filter enforces a subset of policies locally on Envoy. Presently only https rules
without effects are enforced. Enforcement of http rules and header injection will be implemented as a follow up.

pep_http_filter enforces the policy for the duration of time specified in its cache_ttl configuration, with the
default 5s value. After this policy became stale and ext_proc callout is invoked that also refreshes the egress policy.
If the policy did not change, the local policy timestamp is updated and local enforcement continues for another cache_tll
interval. If policy did change, the enforcement will be defered to ext_proc. In the future the connection will be gracefully
drained after policy has changed (requires update to Envoy dynamic module's SDK).

## Details

In the outer CONNECT filter chain the policy store HTTP filter is instantiated after
the ext_proc filter. After ext_proc successfully looks up egress policy for the given actor and
destination port, the subsequent policy store filter:

1. Creates EgressPolicy object by parsing JSON from metadata sent by the ext_proc server and computes its hash.
1. Checks if the policy with this hash already exists in the worker local policy store.
1. If it does it adds a reference to the existing policy.
1. Otherwise it stores the new policy in the store.
1. The policy hash is written to filter state, which the chain's set_filter_state filter copies into filter state shared with the upstream.
1. The policy creation timestamp is written into the filter state.

In the inner listener filter chain, the policy enforcement listener filter:

1. reads policy hash from the filter state, shared by the outer CONNECT filter chain.
1. Looks up the policy object by hash in the worker local policy store.
1. Evaluates and enforces egress policy.

With this approach, egress policy objects are shared by many CONNECT requests. Per request state is
a 64 bit hash. For example if all actors have the same policy, Envoy will keep only one policy object in memory
and all requests will share the same object.

In the inner HTTP MITM filter chain
1. the policy enforcement HTTP filter:
   1. reads policy timestamp from the filter state and checks it agains its cache_ttl configuration.
   1. if the policy is fresh, enforce the policy.
   1. if the policy has no effects, skip the ext_proc
   1. otherwise continue to the ext_proc callout.
1. the ext_proc filter enforces the policy on the request and sends the egress policy in the response metadata.
1. the policy_store filter computes policy cache and (if policy is new) writes it into the store. The new policy cahce is
   written into the filter state.
1. the policy_refrsh filter compares the new and old policy hash
   1. if hashes are the same, it updates the policy timestamp to current time, so the policy can be used for the next
      freshness interval.
   1. if hashes are different it increments the counter. Presently, connection will stay up and policy will be enforced
      by ext_proc. After SDK update, connection will be gracefully drained, so it can be re-established with the new
      policy.

## Policy Lifetime

Ownership of EgressPolicy objects is shared by policy store HTTP filters and the worker local policy store. As such
policy object is guaranteed to outlive all CONNECT requests that share the same policy. Lifetime of the CONNECT request
is at least as long as the lifetime of the inner request, so it is guaranteed that inner request will always
find a policy with hash it received from the outer CONNECT request.

To avoid leaking policy object in the local policy store, policy store HTTP filter, during its cleanup, checks the
reference count value of the EgressPolicy. If this count value is 2, then only this filter and policy store have outstanding
references, and at this point it is safe to remove policy object from the store. This guarantees that policy is destroyed,
when no more CONNECT requests are referencing it.

## Policy Hashing

[Rapidhash](https://docs.rs/rapidhash/latest/rapidhash/) is considered one of the best non-cryptographic hashing functions
with near [ideal collision probability](https://chromium.googlesource.com/chromium/src/+/HEAD/third_party/rapidhash/README.md).
Other options are xxhash which can produce 128 bit hashes or cryptographic hash such as SHA-256. 64 bit Rapidhash offers the
best balance between quality and performance, with approximately 1 in 10^19 probability of collision.

## Envoy stats

The HTTP filter (`envoy_substrate_egress_policy_store`) defines the following
metrics:

| Metric | Type | Description |
|---|---|---|
| `ate_egress.policy_store_size` | Gauge | Current number of unique `EgressPolicy` entries stored in the worker thread's local store (summed across workers by Envoy). |
| `ate_egress.parse_errors` | Counter | Number of times `EgressPolicy::new_from_metadata` returned `None` due to missing or malformed policy metadata. |
| `ate_egress.policy_shared` | Counter | Number of times a `CONNECT` request reused an `EgressPolicy` already present in the worker thread's local store. |

The policy enforcement HTTP filter (`envoy_substrate_egress_pep`) defines the following
metrics:

| Metric | Type | Description |
|---|---|---|
| `ate_egress.cache_hit` | Counter | Number of times cached policy was used for local enforcement. |
| `ate_egress.cache_miss` | Counter | Number of times cached policy was stale. |
| `ate_egress.allowed` | Counter | Number of requests allowed by local enforcement. |
| `ate_egress.rejected` | Counter | Number of requests rejected by local enforcement. |
| `ate_egress.has_effects` | Counter | Number of requests that had effects configured by policy. |

The policy refresh HTTP filter (`envoy_substrate_egress_policy_refresh`) defines the following
metrics:

| Metric | Type | Description |
|---|---|---|
| `ate_egress.policy_change_for_active_connection` | Counter | Number of times policy has changed for an active connection. |
