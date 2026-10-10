# Actor DNS

Actor DNS relays DNS requests from the actor to the external world.

## Code structure

### Relay

- `Relay` -- one per Ateom, holds upstream resolvers, and is used to create the per-Actor `Server`.
- `Relay.Serve()` -- creates a per-Actor `Server` that manages DNS requests for the Actor. Returns a `*Server`.

### Server

`Server` is the per-Actor state for the DNS traffic, including per-Actor concurrency limits (`maxInFlightDNS` and `maxDNSConnections`).

- `Server.pendingRequests` map (UDP only):
  - Allocates a rewritten `upstreamID` (`uint16`) per in-flight UDP query to avoid transaction ID collisions across different actor source ports on the shared egress socket (and prevent predictable upstream IDs).
  - Maps `upstreamID` -> pending request entry:
    - `clientRequestID` (`uint16`) and `clientAddr` (`net.Addr`) to restore the original ID and route the response back.
    - Question metadata (`QNAME`, `QTYPE`, `QCLASS`) to validate that the response matches the query (RFC 5452).
    - `upstreamIdx` and `deferredResp` to track multi-upstream failover (`SERVFAIL`, `NOTIMP`, `REFUSED`, or timeout) and return the last failure response if all upstreams fail.
    - `expiry` timestamp for timing out stale entries and triggering failover via a periodic sweep ticker.
- `Server.Stop()` -- cancels in-flight work, closes sockets, and stops serving DNS requests.

### Implementation details

#### UDP handling

- `udphandler.go`: `struct udpHandler`
- Two goroutines per `Server` (plus a sweep ticker for timeouts/failover):
  - **Ingress reader**: reads datagrams from the actor-facing gateway `net.PacketConn`, runs `onUDPRequest()` (which validates the query, rewrites the transaction ID to `upstreamID`, and records the entry in `pendingRequests`), and sends the query on the egress socket.
  - **Egress reader**: reads datagrams from an unconnected worker-namespace `net.PacketConn`, runs `onUDPResponse()` to match and validate against `pendingRequests`, either fails over to the next upstream or restores `clientRequestID` and writes the answer back to `clientAddr`.

#### TCP handling

- `tcphandler.go`: `struct tcpHandler`
- Framing: each DNS message on a TCP stream is prefixed with a 2-byte big-endian length (`uint16`, RFC 1035 §4.2.2).
- Dedicated upstream stream per connection (with pipelining and out-of-order response support per RFC 7766 §6.2.1.1):
  - Two goroutines per accepted TCP connection sharing a per-connection downstream write mutex:
    - **Downstream reader**: loops reading length-prefixed frames from the actor connection and calls `onTCPRequest()`.
      - If forwarded: writes the unmodified frame directly to the dedicated upstream TCP connection.
      - If rejected with a synthesized reply (`FORMERR`, `NOTIMP`, `REFUSED`): writes the framed error response directly to the actor connection under the write mutex without contacting upstream.
      - On client `EOF`: half-closes the upstream write side (`CloseWrite()`) so in-flight pipelined responses can finish draining.
    - **Upstream reader**: loops reading length-prefixed frames from the upstream TCP connection, validates each frame via `onTCPResponse()`, and writes valid responses back to the actor connection under the write mutex.

#### DNS protocol layer

`dnshandler.go`: `struct dnsHandler`

UDP and TCP call up to the DNS protocol layer (`golang.org/x/net/dns/dnsmessage`), which inspects packets and returns transport-agnostic decisions:

- Shared request validation (`validateRequest`):
  - **Drop**: packet `< 12` bytes or `QR == 1` (response bit set on a query).
  - **Reply** (synthesized response without upstream round-trip):
    - `FORMERR` (`RCode = 1`): malformed header/question or `QDCOUNT != 1`.
    - `NOTIMP` (`RCode = 4`): `OpCode != 0` (non-standard query) or zone transfer `QTYPE` (`AXFR`, `IXFR`).
    - `REFUSED` (`RCode = 5`): disallowed by domain or record-type policy.
- `onUDPRequest(raw []byte, clientAddr net.Addr, upstreams []string)` -> `action`:
  - Acquires an `inFlight` slot, runs `validateRequest()`, and on `actionForward` records the query in `pendingRequests` with a rewritten `upstreamID`.
- `onUDPResponse(raw []byte, from net.Addr)` -> `action`:
  - Validates `QR == 1`, parses header + question, and matches against `pendingRequests` (verifying `upstreamID`, expected upstream source, and matching `QNAME`/`QTYPE`/`QCLASS`).
  - Returns **Drop**, **Failover** (on `SERVFAIL`, `NOTIMP`, or `REFUSED` when fallback upstreams remain), or **Deliver** (restoring `clientRequestID` and returning `clientAddr`).
- `onTCPRequest(raw []byte)` -> `action`:
  - Runs `validateRequest()` and returns **Drop**, **Reply**, or **Forward** (unmodified frame).
- `onTCPResponse(raw []byte)` -> `action`:
  - Validates `QR == 1` and parses header + question, returning **Drop** or **Deliver**.
