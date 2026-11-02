---
title: "Egress Traffic"
linkTitle: "Egress Traffic"
weight: 7
aliases:
  - "/docs/guides/egress/"
description: >
  Supported outbound actor protocols, DNS relay filtering, and what actors see when egress is refused.
---

# Egress Traffic

This document specifies the supported egress traffic for [GA](https://github.com/agent-substrate/substrate/milestone/3).

Actor TCP egress (except DNS to the sandbox's gateway) is redirected to atunnel,
which opens a CONNECT tunnel to the egress gateway. Egress gateway applies policy.

DNS over UDP and TCP goes to the sandbox's DNS relay, which forwards a query
to the worker pod's resolvers only when the queried hostname matches a host or
host wildcard in the actor's egress policy. Queries from an actor without a
policy, or for a hostname not allowed by any rule, are answered `NXDOMAIN`.
A policy created, updated, or deleted while the actor runs applies to its DNS
within about 5 seconds.

> [!NOTE]
> The actor sees a denied name as an ordinary resolver failure, such as
> `wget: bad address 'api.github.com'` or `curl: (6) Could not resolve host`.
> Nothing logs the denied name. If an external name does not resolve, check that
> the actor's egress policy has a rule for it with
> `kubectl ate get egress-policy <actor-name> -a <atespace> -o yaml`.

Other UDP and other traffic is rejected by nftables and never reaches the egress
gateway.

This applies to an actor's own telemetry too: an OpenTelemetry SDK inside the actor
cannot reach the collector until the actor has an `http` egress rule for it. See
[Actor telemetry needs an egress policy](observability.md#actor-telemetry-needs-an-egress-policy).

## TCP

| Port | Traffic | Behavior | Path | What the actor sees when refused |
| :---- | :---- | :---- | :---- | :---- |
| any | HTTP(S) 1.1 / 2 | Supported with policy controls | atunnel -> egress gateway -> origin | `403 Forbidden`. For HTTPS whose SNI matches no `https` or `tls_passthrough` rule, the gateway closes the connection during the TLS handshake, without an HTTP response. |
| any | TLS whose SNI matches a `tls_passthrough` rule | Relayed to the SNI hostname without inspection, so WebSocket, CONNECT, and other TCP inside it get through | atunnel -> egress gateway -> origin | Closed during the TLS handshake. |
| any | WebSocket | Blocked | atunnel -> egress gateway | `403 Forbidden` |
| any | Standard HTTP(S) CONNECT (forward-proxy tunnel) | Blocked | atunnel -> egress gateway | `403 Forbidden` |
| 53 | DNS to the sandbox's gateway | Allowed for hostnames matching the actor's egress policy | sandbox DNS relay -> worker pod's resolvers | `NXDOMAIN` |
| any | Any other TCP | Blocked | atunnel -> egress gateway | A client that sends first gets `400 Bad Request` before the connection closes. If the client waits for the server to speak first, the connection closes after about 1 second with no data. Only the gateway logs the refusal. |

See the [Network Egress Contract](network-egress.md#refusal-responses-and-policy-changes)
for refusal status codes, policy-refresh semantics, and the distinction between DNS
refusals and HTTP policy denials.

## UDP


| Port | Traffic | Behavior | Path | What the actor sees when refused |
| :---- | :---- | :---- | :---- | :---- |
| 53 | DNS to the sandbox's gateway | Allowed for hostnames matching the actor's egress policy | sandbox DNS relay -> worker pod's resolvers | `NXDOMAIN` |
| any other | Any other UDP | Blocked | n/a | Packets are dropped, not rejected: no ICMP port-unreachable is sent, so the client hangs until its own timeout. |

## Other protocols

Everything that is neither TCP nor UDP is rejected with ICMP network
unreachable.

## Requesting support

If you would like Substrate to support egress traffic that is blocked above, please
[file an issue](https://github.com/agent-substrate/substrate/issues/new)
describing your use case.