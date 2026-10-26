# Envoy socket memory benchmark

Measures how much memory piles up in an Envoy configured like atenet-router
(ingress) or the egress gateway (egress) when a reader stops reading, and how
fast data flows. It compares socket options such as `TCP_NOTSENT_LOWAT`. The memory that piles up
is in the kernel's socket buffers: it counts toward the container's memory
limit, but Envoy's own statistics don't show it. See
[docs/network-socket-memory.md](../../docs/network-socket-memory.md) for the
results.

```mermaid
flowchart LR
  D[downstream] --> E[Envoy] --> U[upstream]
```

The downstream side opens connections through Envoy to the upstream side.
Either side stops reading, and the benchmark measures Envoy's memory. Each
measurement starts a fresh Envoy.

## Running

Linux containers only; on macOS, Docker Desktop provides the Linux.

### Commands

```sh
# Memory with slow readers, through the egress Envoy, options off:
benchmarking/envoy-sockmem/run-docker.sh --kind egress --conns 100,400

# The same with TCP_NOTSENT_LOWAT on every socket:
benchmarking/envoy-sockmem/run-docker.sh --kind egress --conns 100,400 \
  --tcp-notsent-lowat 16384

# Memory with slow readers, through the ingress Envoy:
benchmarking/envoy-sockmem/run-docker.sh --kind ingress --conns 100 \
  --upstream-stream-window 1048576

# Speed through the ingress Envoy, with 10 ms of delay added:
benchmarking/envoy-sockmem/run-docker.sh --kind ingress --modes download,upload \
  --conns 4 --hold 15s --netem-delay 10ms
```

Each run prints a table with one row per measurement, and keeps it in
`runs/<timestamp>/results.txt`. Each row starts with the settings it ran
with, such as `off`, `lowat=16384` or `lowat=16384 +10ms`. To put several runs
in one table, give them the same `--out DIR`.

### Options

| Option | Default | What it does |
|---|---|---|
| `--kind K` | `egress` | which Envoy to run: `ingress`, like atenet-router, or `egress`, like the egress gateway; see [Why two kinds](#why-two-kinds) |
| `--modes M,...` | `slow-downstream,slow-upstream` | what to measure; see [Modes](#modes) |
| `--conns N,...` | 100 | connections per measurement; one measurement per value |
| `--hold D` | long enough to open and fill | how long to hold the connections, or to move data |
| `--tcp-notsent-lowat N` | 0 | `TCP_NOTSENT_LOWAT` in bytes on every Envoy socket; 0 leaves the kernel default |
| `--tcp-send-buffer N` | 0 | `SO_SNDBUF` in bytes on every Envoy socket |
| `--tcp-receive-buffer N` | 0 | `SO_RCVBUF` in bytes on every Envoy socket |
| `--netem-delay D` | | add `D` of delay, e.g. `10ms`, to everything Envoy sends |
| `--upstream-stream-window N` | Envoy's default | ingress only: Envoy's HTTP/2 window toward upstream; set `1048576` for slow-reader runs (see [How close it is to the real thing](#how-close-it-is-to-the-real-thing)) |
| `--out DIR` | a new `runs/<timestamp>` | add results to `DIR` instead |

Envoy runs with 2 CPUs and 2 GiB of memory. Every container gets a GKE node's
TCP buffer limits (`tcp_rmem` up to 6 MiB, `tcp_wmem` up to 4 MiB); Docker
Desktop's are larger.

## Why two kinds

atenet-router (ingress) and the egress gateway (egress) relay traffic in
different shapes, so memory piles up on different sockets:

| | Ingress | Egress |
|---|---|---|
| Downstream | one connection per client | atunnel's tunnels share HTTP/2 connections, up to 100 each |
| Upstream | one shared HTTP/2 connection to the worker | one connection per tunnel, to its destination |
| Buffer limits | Envoy's defaults | 64 KiB per connection, 1 MiB HTTP/2 windows |

So the socket that belongs to one flow, and fills when its reader stops, is
downstream on ingress and upstream on egress. One setup can't show both.

## Modes

The same four modes work for both kinds:

| Mode | What happens | Ingress: where it piles up in Envoy | Egress: where it piles up in Envoy |
|---|---|---|---|
| `slow-downstream` | upstream sends without end; downstream reads 4 MiB, then stops reading | the send queue downstream | the receive queue from upstream |
| `slow-upstream` | downstream sends without end; upstream reads 4 MiB, then stops reading | the receive queue from downstream | the send queue upstream |
| `download` | upstream sends; downstream reads as fast as it can | measures MB/s | measures MB/s |
| `upload` | downstream sends; upstream reads as fast as it can | measures MB/s | measures MB/s |

In the two slow modes the benchmark reads Envoy's memory every second until
the hold ends and keeps the highest value: the container's memory, its kernel
socket buffers, and Envoy's heap. The table shows how much each grew, in
total and per connection. The speed modes report MB/s and Envoy's CPU per GB,
over `--conns` connections.

## How close it is to the real thing

- The egress Envoy copies the egress gateway's front door from
  `manifests/ate-install/atenet-egress.yaml`: mTLS, HTTP/2 with 100 streams
  per connection and 1 MiB windows, a 64 KiB buffer limit, and CONNECT. The
  downstream side uses atunnel's own egress client, so tunnels arrive as
  atunnel sends them. The real gateway's internal TLS interception hop is
  left out.
- The ingress Envoy copies the router's listener for clients from
  `cmd/atenet/internal/router/xds.go`, and proxies over one HTTP/2 connection
  upstream, which stands in for a worker. The real router's ext_proc and
  CONNECT tunnel are left out.
- With Envoy's default HTTP/2 window toward upstream, which atenet-router
  uses, stalled streams starve the others and most slow-reader connections
  never open. `--upstream-stream-window 1048576` avoids that, so the kernel
  memory can be measured.

## Caveats

- The kernel limits TCP memory across the whole machine. Past about 6% of
  RAM, buffers stop growing everywhere, so per-connection numbers shrink with
  many connections. In an 8 GB Docker VM that starts somewhere between 200
  and 400 connections.
- Buffer sizes above 208 KiB take full effect here but not on GKE nodes,
  whose `rmem_max` and `wmem_max` cap them.
- The downstream and upstream sides hold kernel memory too, charged to their
  own containers, not Envoy's.
