---
title: "DNS Correlation: What to Expect and How to Debug"
linkTitle: "DNS Correlation Debugging"
weight: 7
status: preview
status_note: >
  DNS correlation is disabled by default behind the `DnsCorrelationInWorker`
  preview gate, and its behavior may change.
description: >
  Symptoms of DNS correlation, what causes them, and how to tell them apart.
---

# DNS Correlation: What to Expect and How to Debug

How DNS correlation works and what it guarantees is in
[DNS Correlation](dns-correlation.md). This page is for when a connection does
not reach the gateway by name.

## Where to look

* **What the actor resolved.** Look the name up from inside the actor. An
  address in `198.18.0.0/15` is a fake address. Anything else is the real
  answer, which the relay passed through.
* **What the gateway received.** The egress gateway logs the CONNECT
  authority: `authority` on the Envoy `[egress]` line, and
  `substrate.connect.authority` on agentgateway.
  * A name means the connection was tunneled by name.
  * A `198.18.x.y` address means the actor dialed a fake address the table has
    no name for.
  * Any other address means the actor dialed a real address.
* **What the worker did.** The actor's worker pod logs these. The first two
  carry the actor's `ate.actor.uid` (see
  [Actor-Attributed Component Logs](observability.md#actor-attributed-component-logs)).
  The last carries only the `destination` and the error.

  | Level | Message | Meaning |
  |---|---|---|
  | WARN | `The actor has used up its fake DNS addresses; ...` | New names now get their real addresses. |
    | ERROR | `Could not save the actor's fake addresses` | The snapshot has no table. The next activation starts a new one. |
  | WARN | `atunnel failed to open egress tunnel` | The CONNECT failed. `destination` shows what was sent. |

* **Which names the policy refused.** `ate.dns.queries` counts lookups by
  `ate.dns.decision`, `allowed` or `denied`.

## Symptoms

| Symptom | Likely cause | How to tell |
|---|---|---|
| A name resolves to its real address | The gate is off on this worker. | The worker pod's `--preview` flag does not list `DnsCorrelationInWorker`. |
| | The actor used up its addresses. | The "used up" warning. |
| | The name resolves to a loopback, link-local, or unspecified address. | This is by design: those connections never leave the sandbox. |
| | The address came from an `AAAA` answer, an `HTTPS` record hint, DoH/DoT, or `/etc/hosts`. | The relay rewrites only `A` answers it sees. |
| A restore fails with `loading the actor's fake addresses: ...` | The table in the snapshot is corrupt, has an unknown format version, or a bug in `Save` wrote it. | The restore error. Turning the gate off doesn't help, because every worker loads the table. The fix is to recreate the actor, which loses its memory and rootfs, or to repair the snapshot in object storage by hand. |
| A lookup returns `NXDOMAIN` | The policy does not allow the name. | `ate.dns.queries` with `denied`. |
| | The name does not exist upstream. | The same lookup fails from the worker pod. |
| | It is an `SRV` name, such as `_mongodb._tcp.example.com`. | Underscore labels fail the policy check today, with or without DNS correlation. |
| The gateway logs `CONNECT <real IP>:port` | The actor got the IP without a lookup through the relay: a hardcoded IP, `/etc/hosts`, DoH/DoT, or an `AAAA` or `HTTPS` record. | The app's configuration and how it resolves names. |
| | The server handed out IPs, like Redis Cluster `MOVED` replies, MongoDB replica-set members configured by IP, Kafka advertised listeners, or FTP passive mode. | The first connection went by name and the follow-up ones by IP. |
| The gateway logs `CONNECT 198.18.x.y:port` | The actor kept an address whose name the table no longer has. | See [Kept addresses](#kept-addresses). |
| | The gate is off on this worker. | As above. |
| The gateway logs a longer name than the app used, like `api.example.com.svc.cluster.local` | A name with a search domain appended resolved, so it got the fake address. | Only under a policy that allows that name, like `*`. CoreDNS `autopath` and wildcard records in a search domain do this. |
| Connections to fake addresses fail right after the gate is turned on | `atenet` is older than this release and refuses a name in the CONNECT. | The "failed to open egress tunnel" warning shows a 403. Upgrade `atenet` first. |
| The client refuses the address or the answer | An SSRF guard that blocks private ranges, or a client that validates DNSSEC. | The client's own error. Python's `ipaddress` counts `198.18.0.0/15` as private. |
| Lookups hang for about 5 seconds | More than 64 queries from one actor were in flight, and the relay dropped the rest. | Debug log `dns relay dropped query; too many in flight`. |

HTTP and TLS reach the right host in every case above, because the gateway
decides on the SNI or Host, not on the CONNECT authority. Plain TCP to an
address the table has no name for has no name to decide on.

## Kept addresses

An actor can hold a fake address in memory or in a file after its table stops
knowing it. Its connections to that address go out as `CONNECT 198.18.x.y:port`.

* **Eviction.** A table holds the 4096 names used most recently. A client that
  cached an address, did not use it while the actor used 4096 other names,
  and then reconnects, gets this. Every lookup and connection counts as use,
  so an address in steady use is not evicted.
* **A failed save.** The snapshot has no table, so the next activation starts
  a new one.
* **A crash.** The restore uses the last snapshot, so addresses given out
  since then are unknown. Only an external volume can still hold one.
* **A rollback to a release without DNS correlation, then an upgrade.** That
  release does not keep the table in snapshots.

After a failed save, a crash, or a rollback, the new or rewound table can
later give a kept address to another name the actor resolved. The connection
then goes out as `CONNECT that-name:port`.

## Running out of addresses

Each actor can be given 130,048 addresses in its life, which lasts until the
actor is deleted. The count carries across every suspend and resume. A name
that was evicted takes a new address on its next lookup, so an actor that
looks up many different names, like random names under a wildcard zone, can
use them up quickly. After that, names it has not looked up before get their
real addresses. Deleting and recreating the actor starts a new table.
