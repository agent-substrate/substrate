This document specifies the supported egress traffic for [GA](https://github.com/agent-substrate/substrate/milestone/3).

Actor TCP egress (except DNS to the sandbox's gateway) is redirected to atunnel,
which opens a CONNECT tunnel to the egress gateway. Egress gateway applies policy.

DNS over UDP and TCP goes to the sandbox's DNS relay, which forwards a query
to the worker pod's resolvers only when the queried hostname matches a host or
host wildcard in the actor's egress policy. Queries from an actor without a
policy, or for a hostname not allowed by any rule, are answered `NXDOMAIN`.
A policy created, updated, or deleted while the actor runs applies to its DNS
within about 5 seconds.

Other UDP and other traffic is filtered by nftables and never reaches the gateway.

This applies to an actor's own telemetry too: an OpenTelemetry SDK inside the actor
cannot reach the collector until the actor has an `http` egress rule for it. See
[Actor telemetry needs an egress policy](observability.md#actor-telemetry-needs-an-egress-policy).

## TCP

| Port | Traffic | Behavior | Path | What the actor sees when refused |
| :---- | :---- | :---- | :---- | :---- |
| any | HTTP(S) 1.1 / 2 | Supported with policy controls | atunnel -> egress gateway -> origin | `403 Forbidden` |
| any | WebSocket | Blocked | n/a | `403 Forbidden` |
| any | Standard HTTP(S) CONNECT (forward-proxy tunnel) | Blocked | n/a | `403 Forbidden` |
| 53 | DNS to the sandbox's gateway | Allowed for hostnames matching the actor's egress policy | sandbox DNS relay -> worker pod's resolvers | `NXDOMAIN` |
| any | Any other TCP | Blocked | n/a | The connection is accepted and then closed with no bytes returned. There is no status code. atunnel logs the failure. |

## UDP


| Port | Traffic | Behavior | Path | What the actor sees when refused |
| :---- | :---- | :---- | :---- | :---- |
| 53 | DNS to the sandbox's gateway | Allowed for hostnames matching the actor's egress policy | sandbox DNS relay -> worker pod's resolvers | `NXDOMAIN` |
| any other | Any other UDP | Blocked | n/a | Packets are dropped, not rejected: no ICMP port-unreachable is sent, so the client hangs until its own timeout. |

## Other protocols

Everything that is neither TCP nor UDP is blocked. Packets are dropped, not rejected: no ICMP port-unreachable is sent, so the client hangs until its own timeout.

## Requesting support

If you would like Substrate to support egress traffic that is blocked above, please
[file an issue](https://github.com/agent-substrate/substrate/issues/new)
describing your use case.
## Gateway assignment

A global Substrate owner chooses the egress gateway when creating an Atespace.
All actors in that Atespace use it, including actors created from templates in
another Atespace. Actors cannot override the assignment.

A Kubernetes administrator defines the gateway:

```yaml
apiVersion: ate.dev/v1alpha1
kind: EgressGateway
metadata:
  name: tenant-default
spec:
  address: agentgateway.egress.svc:443
  identity: spiffe://cluster.local/ns/egress/sa/agentgateway
```

Then a global Substrate owner creates the Atespace:

```sh
kubectl ate create atespace tenant-a --egress-gateway=tenant-default
```

The binding is immutable. Omitting `--egress-gateway` selects the installed
`default` EgressGateway. Edit that resource to change the installation's default
address; the `--default-egress-gateway-address` API server flag has been removed.
The gateway definition can be updated by Kubernetes administrators. Address
changes take effect on the next actor activation, including restores from local
and external snapshots. Running actors retain their current tunnel until then.
Identity changes affect authorization as soon as the API server's Kubernetes
cache observes them; coordinate identity rotation with actor reactivation.

Missing, deleting, or invalid gateways fail activation closed; there is no
fallback to another gateway. Provision the gateway before activating actors.
The binding selects a gateway independently of an actor's EgressPolicy rules.

A custom gateway must support Substrate's mTLS CONNECT tunnel and trust the
actor identity CA. Its serving certificate must match the configured address's
host and chain to the workers' egress trust bundle. `identity` is its client
SPIFFE identity when calling ateapi, not a substitute for tunnel server TLS
verification. It permits actor reads, policy reads, and credential minting only
for Atespaces assigned to that gateway. Distinct tenant gateways should use
distinct identities. Custom gateways may list actors within their assigned
Atespace; the built-in gateway retains its global actor-list health check.

For a separate `atenet --mode=egress` deployment, set
`--health-check-atespace=tenant-a` to scope its API health probe to a bound
Atespace. Otherwise its global actor-list probe is denied.
