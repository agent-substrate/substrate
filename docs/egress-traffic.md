This document specifies the supported egress traffic for [GA](https://github.com/agent-substrate/substrate/milestone/3).

Actor TCP egress (except DNS traffic on port 53) is redirected to atunnel,
which opens a CONNECT tunnel to the egress gateway. Egress gateway applies policy.

DNS-over-TCP, UDP and other traffic is filtered by nftables and never reaches the gateway.

## TCP

| Port | Traffic | Behavior | Path | What the actor sees when refused |
| :---- | :---- | :---- | :---- | :---- |
| any | HTTP(S) 1.1 / 2 | Supported with policy controls | atunnel -> egress gateway -> origin | `403 Forbidden` |
| any | WebSocket | Blocked | n/a | `403 Forbidden` |
| any | Standard HTTP(S) CONNECT (forward-proxy tunnel) | Blocked | n/a | `403 Forbidden` |
| 53 | DNS | Allowed via netfilter rules | nftables -> node-configured DNS | n/a |
| any | Any other TCP | Blocked | n/a | The connection is accepted and then closed with no bytes returned. There is no status code. atunnel logs the failure. |

## UDP


| Port | Traffic | Behavior | Path | What the actor sees when refused |
| :---- | :---- | :---- | :---- | :---- |
| 53 | DNS | Allowed via netfilter rules | nftables -> node-configured DNS | n/a |
| any other | Any other UDP | Blocked | n/a | Packets are dropped, not rejected: no ICMP port-unreachable is sent, so the client hangs until its own timeout. |

## Other protocols

Everything that is neither TCP nor UDP is blocked. Packets are dropped, not rejected: no ICMP port-unreachable is sent, so the client hangs until its own timeout.

## Requesting support

If you would like Substrate to support egress traffic that is blocked above, please
[file an issue](https://github.com/agent-substrate/substrate/issues/new)
describing your use case.