---
title: "Egress Traffic"
linkTitle: "Egress Traffic"
weight: 7
aliases:
  - "/docs/guides/egress/"
  - "/docs/guides/network-egress/"
description: >
  Supported actor traffic and the contract between atunnel and the egress policy enforcement point.
---

# Egress Traffic

This document specifies supported egress traffic and the contract between
Substrate and its egress policy enforcement point (PEP).

> [!NOTE]
> Substrate supports IPv4-only, IPv6-only, and dual-stack Kubernetes clusters.
> The cluster family does not determine the private actor network family.

Every actor gets the private IPv4 network `169.254.17.0/30`, including actors on
IPv6-only clusters. Actor networks are either IPv4-only or dual-stack; they are
never IPv6-only. Actor IPv6 networking is a preview feature. With
`ATE_PREVIEW=ActorIPv6`, an actor whose worker Pod has an IPv6 address also gets
`fd00:169:254::/126`. Actor ingress, DNS, and wakeup probes use the IPv4 network.
Applications must listen on IPv4 or on a dual-stack socket. IPv6-only listeners,
such as Go's `tcp6`, are not supported.

Before traffic reaches the PEP, ateworker rejects packets destined for the
IPv4 link-local range `169.254.0.0/16` or the IPv6 link-local range `fe80::/10`.
The internal actor networks `169.254.17.0/30` and `fd00:169:254::/126` are exempt
because they carry traffic to the actor gateway. The IPv4 network also carries
DNS, ingress, and wakeup probes. Localhost traffic remains inside the actor
sandbox.

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
gateway. The actor receives an ICMP destination-unreachable response.

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

See [Refusal responses and policy changes](#refusal-responses-and-policy-changes)
for policy-refresh semantics and the distinction between DNS refusals and HTTP
policy denials.

## UDP


| Port | Traffic | Behavior | Path | What the actor sees when refused |
| :---- | :---- | :---- | :---- | :---- |
| 53 | DNS to the sandbox's gateway | Allowed for hostnames matching the actor's egress policy | sandbox DNS relay -> worker pod's resolvers | `NXDOMAIN` |
| any other | Any other UDP | Blocked | n/a | ICMP destination unreachable. |

## Other protocols

Everything that is neither TCP nor UDP is rejected with ICMP network
unreachable.

## Egress PEP contract

`atunnel` opens an mTLS HTTP/1.1 or HTTP/2 CONNECT tunnel to the PEP, as
negotiated by ALPN. The CONNECT request target and HTTP/1.1 `Host` header
(HTTP/2 `:authority`) contain the original destination IP and port derived from
the actor's TCP connection.

### Refusal responses and policy changes

For cleartext HTTP requests and HTTP requests on an authorized intercepted TLS
connection, a request denied by policy MUST receive HTTP 403. WebSocket upgrades,
other HTTP upgrades, and actor-originated CONNECT requests inside the tunnel MUST
also receive HTTP 403, regardless of whether the destination is otherwise allowed.
This applies to cleartext HTTP and intercepted HTTPS, including mixed-case HTTP
upgrade tokens, and does not affect atunnel's outer CONNECT. The PEP MUST NOT open
an upstream connection or apply credential effects for these refused requests.
Response bodies are implementation-specific.

A TLS passthrough authorization lasts for that connection. A policy update MUST
apply to new connections after cached policy decisions expire; it need not
terminate an already authorized connection. If a new ClientHello no longer
matches a passthrough rule, the PEP MUST NOT pass the TLS connection through.
If an HTTPS rule matches the SNI and original destination port, the PEP MAY
intercept the connection and authorize each HTTP request under that rule.
Otherwise, the PEP MUST close the connection at ClientHello, without completing
the TLS handshake, returning an HTTP response, or opening an upstream connection.
An HTTP rule alone MUST NOT authorize TLS. Missing or invalid SNI MUST also close
the connection without completing TLS. Unsupported protocols MAY close the
connection without an HTTP response.

DNS authorization happens before the PEP and is independent of the protocol and
port being dialed. Removing every rule for a hostname can prevent resolution
before the connection reaches the PEP; that refusal does not promise an HTTP
status or a particular resolver error string.

### Trust boundary

The PEP MUST NOT trust any decision or attestation from the actor. It MUST trust
only information carried through Substrate-controlled channels, such as the actor
certificate and the CONNECT request produced by atunnel. In particular, an
actor-provided HTTP hostname or TLS SNI does not prove that the original
destination has that name. When authorization uses that hostname, the PEP MUST
route to the authorized hostname rather than use it to authorize an arbitrary IP
address from the CONNECT request.

The PEP MUST NOT connect to a loopback, link-local, or unspecified address under
any rule, including `*`. It MUST check the address after resolving the hostname,
because an actor can point a public name at any address.

### Server-side TLS

The PEP MUST serve TLS using a certificate valid for its configured hostname and
chained to a CA trusted by `atunnel`. `atunnel` uses that hostname as the TLS
server name and MUST reject a certificate it cannot verify.

### Client authorization

`atunnel` MUST present an actor-specific client certificate when connecting to
the PEP. Before the actor's workload starts, `atunnel` generates a private key
and requests a short-lived certificate through the node-local atelet. Egress is
active from then on because a workload may need the network to become ready; it
does not wait for the wakeup probe as ingress does. The private key remains in
`atunnel`. The certificate identifies the actor by atespace, name, and UID and is
scoped to the `atunnel` purpose.

The PEP MUST verify the certificate chain and lifetime against the Actor Identity
CA, require the client authentication usage, and require exactly one valid
`ActorIdentity` extension whose atespace, actor name, actor UID, and purpose are
present. The current extension OID is `1.3.6.1.4.1.11129.2.12.2`; it is allocated
under Google's enterprise number and will change after the CNCF donation. The
purpose MUST be `atunnel`, and the certificate's actor URI SAN MUST identify the
same actor as the extension. The PEP MUST then confirm with ate-api-server that
the actor still exists, its current UID matches the certificate, and it is placed
on a worker in `RESUMING` or `RUNNING`. The PEP MUST reject the CONNECT request if
any check fails.

```text
CURRENT CONNECT EGRESS PATH (one tunnel per actor TCP connection except destination port 53)

  Actor sandbox                 Worker pod / ateworker                  Egress PEP                     Authorized upstream
  +------------------+          +--------------------------+        +-------------------------+    +------------------+
  | Actor process    |          | nftables                 |        | mTLS listener           |    | Target selected  |
  | (untrusted)      |          | TCP REDIRECT             |        | + CONNECT terminator    |    | under policy     |
  +--------+---------+          +------------+-------------+        +------------+------------+    +--------+---------+
           | connect(IP:port)                |                                   |                          |
           +-------------------------------->|                                   |                          |
                                             | SO_ORIGINAL_DST                   |                          |
                                             v                                   |                          |
                                +------------+-------------+                     |                          |
                                | atunnel                  |                     |                          |
                                | (trusted tunnel client)  |                     |                          |
                                +------------+-------------+                     |                          |
                                             |                                   |                          |
                                             | TLS 1.2+ mutual authentication    |                          |
                                             | actor-specific client certificate |                          |
                                             +---------------------------------->| verify certificate and   |
                                             |                                   | authorize current actor  |
                                             |                                   |                          |
                                             | HTTP/1.1 CONNECT IP:port          |                          |
                                             | Host: IP:port                     |                          |
                                             +---------------------------------->| validate destination;    |
                                             |                                   | evaluate egress policy   |
                                             |                                   |                          |
                                             | HTTP/1.1 2xx                      |                          |
                                             |<----------------------------------+                          |
                                             |                                   | CIDR/all policy:         |
                                             |                                   | dial now;                |
                                             |                                   | hostname policy:         |
                                             |                                   | inspect inner request    |
                                             |                                   | before dialing           |
                                             |                                   +------------------------->|
                                             |                                   |                          |
           |<================ raw bidirectional bytes inside CONNECT ===========>|<========================>|
           |             (PEP may pass through, inspect, redirect, or MITM according to policy)             |
```

## Requesting support

If you would like Substrate to support egress traffic that is blocked above, please
[file an issue](https://github.com/agent-substrate/substrate/issues/new)
describing your use case.
