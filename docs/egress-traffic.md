This document specifies the supported egress traffic for [GA](https://github.com/agent-substrate/substrate/milestone/3).

Actor TCP egress (except DNS to the sandbox's gateway) is redirected to atunnel,
which opens a CONNECT tunnel to the egress gateway. Egress gateway applies policy.

DNS over UDP and TCP goes to the sandbox's DNS relay, which forwards a query
to the worker pod's resolvers only when the queried hostname matches a host or
host wildcard in the actor's egress policy. Queries from an actor without a
policy, or for a hostname not allowed by any rule, are answered `REFUSED`.
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
| 53 | DNS to the sandbox's gateway | Allowed for hostnames matching the actor's egress policy | sandbox DNS relay -> worker pod's resolvers | `REFUSED` |
| any | Any other TCP | Blocked | n/a | The connection is accepted and then closed with no bytes returned. There is no status code. atunnel logs the failure. |

## UDP


| Port | Traffic | Behavior | Path | What the actor sees when refused |
| :---- | :---- | :---- | :---- | :---- |
| 53 | DNS to the sandbox's gateway | Allowed for hostnames matching the actor's egress policy | sandbox DNS relay -> worker pod's resolvers | `REFUSED` |
| any other | Any other UDP | Blocked | n/a | Packets are dropped, not rejected: no ICMP port-unreachable is sent, so the client hangs until its own timeout. |

## Other protocols

Everything that is neither TCP nor UDP is blocked. Packets are dropped, not rejected: no ICMP port-unreachable is sent, so the client hangs until its own timeout.

## Requesting support

If you would like Substrate to support egress traffic that is blocked above, please
[file an issue](https://github.com/agent-substrate/substrate/issues/new)
describing your use case.