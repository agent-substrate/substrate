---
title: "Network Egress"
linkTitle: "Network Egress"
weight: 8
description: >
  How atunnel and the egress gateway tunnel, authenticate, and apply policy to outbound actor traffic.
---

# Network Egress Contract

Last updated: 10/20/2026

## Overview

Agent Substrate enforces that all outbound traffic from an actor, except traffic sent to TCP or UDP destination port 53, MUST go through an egress policy enforcement point (PEP). That PEP can (optionally) inspect, filter, or redirect the traffic according to substrate-native egress policies. This document describes the contract between substrate and egress PEPs.

## Transport

Each actor has one private Linux network namespace. For gVisor, both ends of
the `eth0`–`atside` veth pair live there. During sandbox setup, `eth0` has
`169.254.17.2/30`, while `atside` stays addressless with IPv6 disabled.
After runsc imports `eth0` into its network stack, ateworker assigns
`169.254.17.1/30` to `atside`. This happens before application containers
start or restore. The actor address then lives in gVisor's network stack;
Linux retains the gateway address, DNS relay, and redirected egress sockets.
MicroVMs use a TAP device in their actor namespace.

At present, actor TCP traffic exits the sandbox through [`atunnel`](../reference/glossary.md#components) and is forwarded on to the PEP. The exception is DNS: TCP and UDP traffic to port 53 on the sandbox's gateway bypasses the PEP and is answered by the sandbox's DNS relay, which forwards a query to the worker pod's resolvers only when the queried hostname matches a host or host wildcard in the actor's egress policy; queries from an actor without a policy, or for a hostname not matched by any rule, are answered `NXDOMAIN`. All other protocols are blocked. `atunnel` is configured with the PEP's network address and port at startup and uses that address to open an mTLS HTTP/1.1 or HTTP/2 CONNECT tunnel to the PEP, as negotiated by ALPN. The CONNECT request target and HTTP/1.1 `Host` header (HTTP/2 `:authority`) are set to the original destination IP and port that `atunnel` derived from the actor's TCP connection. Actor networking is currently IPv4-only.

Before traffic reaches the egress PEP, ateworker rejects packets destined for the IPv4 link-local range `169.254.0.0/16` or the IPv6 link-local range `fe80::/10`. The internal actor network `169.254.17.0/30` is exempt because it carries traffic for the actor gateway, DNS relay, ingress, and wakeup probes. Localhost traffic remains inside the actor sandbox and does not traverse the PEP. Access to non-link-local destinations remains controlled by substrate egress policy and, at the `WorkerPool` boundary, Kubernetes NetworkPolicy.

## Refusal responses and policy changes

The [supported traffic table](egress-traffic.md) defines the permitted protocols.
For cleartext HTTP requests and HTTP requests on an authorized intercepted TLS
connection, a request denied by policy MUST receive HTTP 403. WebSocket upgrades,
other HTTP upgrades, and actor-originated CONNECT requests inside the tunnel MUST
also receive HTTP 403, regardless of whether the destination is otherwise allowed.
This applies to cleartext HTTP and intercepted HTTPS, including mixed-case HTTP
upgrade tokens, and does not affect atunnel's outer CONNECT. The PEP MUST NOT open an upstream connection
or apply credential effects for these refused requests. Response bodies are
implementation-specific.

A TLS passthrough authorization lasts for that connection. A policy update MUST
apply to new connections after cached policy decisions expire; it need not
terminate an already authorized connection. If a new ClientHello no longer
matches a passthrough rule, the PEP MUST NOT pass the TLS connection through.
If an HTTPS rule matches the SNI and original destination port, the PEP MAY
intercept the connection and authorize each HTTP request under that rule.
Otherwise, the PEP MUST close the connection at ClientHello, without completing
the TLS handshake, returning an HTTP response, or opening an upstream connection.
An HTTP rule alone MUST NOT authorize TLS.
Missing or invalid SNI MUST also close the connection without completing TLS.
Unsupported protocols MAY close the connection without an HTTP response.

DNS authorization happens before the PEP and is independent of the protocol and
port being dialed. Removing every rule for a hostname can prevent resolution
before the connection reaches the PEP; that refusal does not promise an HTTP
status or a particular resolver error string.

## Trust Boundary

In substrate, we consider everything originating from the actor to be untrusted. The egress PEP MUST NOT trust any decision or attestation coming from the actor itself; it MUST instead trust only information carried through substrate-controlled channels (e.g. the actor certificate and the CONNECT request produced by atunnel). For example, the PEP MUST NOT treat an actor-provided HTTP hostname or TLS SNI as proof that the original destination has that name. The PEP MAY use the hostname as a policy input, but when authorization is based on that hostname, it MUST route the traffic to the authorized hostname rather than use the hostname to authorize an arbitrary IP address from the CONNECT request.

## Server-side TLS

The egress PEP MUST serve TLS using a certificate that is valid for the configured PEP hostname and chains to a CA trusted by `atunnel`. `atunnel` uses the configured PEP hostname as the TLS server name and MUST reject the connection if it cannot verify the PEP's certificate.

## Client Authorization

`atunnel` MUST present an actor-specific client certificate when connecting to the PEP. Before the actor's workload starts, `atunnel` generates a private key and requests a short-lived certificate from substrate through the node-local atelet, and egress is active from then on: a workload may need the network to become ready, so egress does not wait for the wakeup probe the way ingress does. The private key remains in `atunnel`. The certificate identifies the actor by its atespace, name, and UID and is scoped to the `atunnel` purpose.

The PEP MUST require and verify the client certificate before accepting a CONNECT request. It MUST verify the certificate chain and lifetime against the Actor Identity CA, require the client authentication usage, and require exactly one valid `ActorIdentity` extension whose atespace, actor name, actor UID, and purpose are present. The current extension OID is `1.3.6.1.4.1.11129.2.12.2`; it is allocated under Google's enterprise number and will change after the CNCF donation is complete. The purpose MUST be `atunnel`, and the certificate's actor URI SAN MUST identify the same actor as the extension. The PEP MUST then confirm with ate-api-server that the actor still exists, that its current UID matches the certificate, and that it is placed on a worker: `RESUMING` (booting or restoring there) or `RUNNING`. The PEP MUST reject the CONNECT request if any of these checks fail.

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
