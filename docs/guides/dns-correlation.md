---
title: "DNS Correlation"
linkTitle: "DNS Correlation"
weight: 6
status: preview
status_note: >
  DNS correlation is disabled by default behind the `DnsCorrelationInWorker` preview
  gate, and its behavior may change.
description: >
  Tunnel an actor's connections to the egress gateway by name.
---

# DNS Correlation

An actor connects to addresses, and the egress gateway decides on names. For
HTTP and TLS the gateway reads the name from the request. For anything else,
such as SSH or a database, the name exists only in the actor's DNS lookup.

DNS correlation carries the name from the lookup to the gateway:

1. The sandbox's DNS relay answers an actor's `A` query with a fake address
   from `198.18.0.0/15`, and remembers which name got which address.
2. The actor connects to the fake address.
3. `atunnel` finds the name the address was given to and opens the tunnel with
   `CONNECT name:port` instead of `CONNECT IP:port`.

This is the worker's half of allowing plain TCP by hostname
([#3873](https://github.com/agent-substrate/substrate/issues/3873)). The
gateway does not decide on the name in the CONNECT yet, so no policy changes
with this preview.

## Enabling

Run Substrate with `--preview=DnsCorrelationInWorker` (ate-setup's `preview`
setting). Every worker then answers its actors' lookups with fake addresses.

The egress gateway has to accept a name in the CONNECT, which `atenet` does
from the release that added this preview. An older gateway refuses one, and
the actor's connections to fake addresses fail. So turn the gate on only after
`atenet` is upgraded, and turn it off before rolling `atenet` back.

## What the actor sees

* An allowed name that resolves gets one `A` record with its fake address and
  a TTL of 30 seconds. A name keeps its address, see [Limits](#limits).
* Everything else is unchanged: a name outside the policy is still `NXDOMAIN`,
  a name that does not exist upstream still fails, and other record types
  (`AAAA`, `TXT`, `MX`, ...) are still forwarded. Names with an underscore
  label, such as the `_service._tcp` names `SRV` lookups use, fail the policy
  check, with or without DNS correlation.
* The resolver's search list works as before. A name with a search domain
  appended that does not exist gets `NXDOMAIN`, and the resolver tries the
  next one. Under a policy that allows such a name, like `*`, one that does
  resolve keeps the fake address under that longer name. CoreDNS `autopath`
  and a wildcard record in a search domain both do that.
* A name that resolves to a loopback or link-local address keeps its real
  answer. Connections to those addresses never leave the sandbox.
* An `A` answer no longer holds the name's real addresses or the `CNAME`
  records that led to them. Other answers still can: `AAAA` records, and the
  address hints in `HTTPS` records.

## Guarantees

* **Per actor.** Each actor has its own table in the worker. No actor can read
  or change another actor's addresses.
* **An address means one name.** An address is never given to a second name,
  so a client that keeps an address for hours, or resolves once at startup,
  cannot end up at a different name. The exceptions are a crash, a failed
  save, and a rollback, see [Limits](#limits).
* **Suspend and resume.** The table is saved in every snapshot and loaded
  before the actor's network serves again, on any worker. A worker without
  DNS correlation does not use the table, but keeps it in the actor's
  snapshots.
* **Never worse than without it.** A connection to an address `atunnel` has no
  name for is tunneled as that address, exactly as without DNS correlation.

## Limits

For symptoms and how to tell them apart, see
[What to Expect and How to Debug](dns-correlation-debugging.md).

* `198.18.0.0/15` must not hold real destinations of the actors. A connection
  to an address in it that a name was given is tunneled to that name.
* A table holds the 4096 names an actor used most recently. A name pushed out
  gets a new address on its next lookup. A client still holding the old
  address keeps working for HTTP and TLS, which the gateway decides on the
  request.
* An actor can be given 130,048 addresses in its life. After that, names it
  has not looked up before resolve to their real addresses, and the worker
  logs a warning. An actor that looks up many different names, such as random
  names under a wildcard zone, can use them up quickly.
* Clients that refuse non-public addresses, such as SSRF guards, refuse the
  fake addresses.
* Clients that validate DNSSEC themselves reject the replaced answers.
* After the gate is turned off, the fake addresses an actor kept are tunneled
  as addresses.
* A crash loses the addresses an actor was given since its last snapshot, with
  the rest of its state. Only an external volume can still hold one, and that
  address can later be given to another name.
* A table that cannot be saved is missing from that snapshot, so the actor's
  next activation starts a new table. That table can give an address the
  actor kept to another name.
* A table that cannot be loaded fails the restore, so a kept address cannot
  end up at another name. Only a corrupt file, an unknown format version, or
  a bug causes it.
* A worker of a release without DNS correlation does not keep the table in the
  actor's snapshots. After a rollback to such a release and a later upgrade,
  an address an actor kept can be given to another name.
* The answer the relay builds carries no EDNS `OPT` record, like the
  `NXDOMAIN` it builds for a name outside the policy.
