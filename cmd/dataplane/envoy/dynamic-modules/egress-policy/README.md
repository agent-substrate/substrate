# Envoy Substrate Egress Policy Implementation

This Envoy dynamic module implements two extensions that work together to minimize
memory usage by Substrate egress policies.

To minimize memory usage egress policies are hashed using [Rapidhash](https://docs.rs/rapidhash/latest/rapidhash/)
which produces 64 bit hashes with low probability of collision. Policies are then stored in the worker local
store using computed hash and the hash is exchanged between outer CONNECT request and inner
listener that terminates TCP connection encapsulated in the CONNECT request.

## Details

In the outer CONNECT filter chain the policy store HTTP filter is instantiated after
the ext_proc filter. After ext_proc successfully looks up egress policy for the given actor and
destinaion port, the subsequent policy store filter:

1. Creates EngressPolicy object by parsing JSON from metadata sent by the ext_proc server and computes its hash.
1. Checks if the policy with this hash already exists in the worker local policy store.
1. If it does it adds a reference to the existing policy.
1. Otherwise it stores the new policy in the store.
1. The policy hash is written to the filter state shared with the upstream.

In the inner listener filter chain, the policy enforcement listener filter:

1. reads policy hash from the filter state, shared by the outer CONNECT filter chain.
1. Looks up the policy object by hash in the worker local policy store.
1. Evaluates and enforces egress policy.

With this approch, egress policy objects are shared by many CONNECT requests. Per request state is
a 64 bit hash. For example if all actors have the same policy, Envoy will keep only one policy object in memory
and all requests will share the same object.

## Policy Lifetime

Ownership of EgressPolicy objects is shared by policy store HTTP filters and the worker local policy store. As such
policy object is guaranteed to outlive all CONNECT requests that share the same policy. Lifetime of the CONNECT request
is at least as long as the lifetime of the inner request, so it is guaratedd that inner request will always
find a policy with hash it received from the outer CONNECT request.

To avoid leaking policy object in the local policy store, policy store HTTP filter, during its cleanup, checks the
reference count value of the EgressPolicy. If this cound vaoue is 2, then only this filter and policy store have outstanding
references, and it this point it is safe to remove policy object from the store. This guaratees that policy is detroyed,
when no more CONNECT requests are referencing it.

## Policy Hashing

[Rapidhash](https://docs.rs/rapidhash/latest/rapidhash/) is considered one of the best noon-cryptographic hashing funections
wth near [ideal collision probability](https://chromium.googlesource.com/chromium/src/+/HEAD/third_party/rapidhash/README.md).
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
