# atunnel benchmark

Measures what atunnel, the per-worker ingress and egress tunnel, costs in CPU
and memory as load rises. atunnel is started and hosts its actors with the
worker's own code, in a process of its own, so the numbers are atunnel's alone.

```mermaid
flowchart LR
    A["A: load generator"] -- traffic --> B["B: atunnel"]
    B -- traffic --> C["C: ingress actors<br/>and fake gateway"]
    A -. "reads B's CPU and memory" .-> B
```

The generator (A) sends traffic through atunnel (B) to fakes of what atunnel
talks to in a cluster (C). A reads B's CPU and memory as it goes and prints a
table per scenario.

## Running

Linux containers only: atunnel creates real network namespaces and nftables
rules, so its container is privileged. On macOS, Docker Desktop provides the
Linux.

### Commands

```sh
# Every regular scenario, with atunnel at 1 CPU and 1 GiB:
benchmarking/atunnel/run-docker.sh

# More CPU and memory, at higher rates:
benchmarking/atunnel/run-docker.sh --tunnel-cpus 2 --tunnel-memory 2g -- --rungs 2000,4000,8000

# One scenario, with atunnel and the generator pinned to different CPUs:
benchmarking/atunnel/run-docker.sh --cpuset-worker 0-3 --cpuset-rig 4-9 -- --scenarios Connect

# Memory behind slow readers:
benchmarking/atunnel/run-docker.sh --tunnel-memory 3g -- --scenarios slow --rungs 100,400

# The same with atunnel's TCP_NOTSENT_LOWAT off, to compare:
benchmarking/atunnel/run-docker.sh --tunnel-memory 3g \
  --tunnel-flag=--atunnel-tcp-notsent-lowat=-1 -- --scenarios slow --rungs 100,400

# Throughput with 2 ms of network delay:
benchmarking/atunnel/run-docker.sh --network-delay 2ms -- \
  --scenarios EgressTCP,EgressTCPDownload,ConnectTCP,ConnectTCPUpload
```

By default the containers share the Docker VM's CPUs: atunnel may use at most
`--tunnel-cpus` worth of CPU time, but the generator and the fake gateway can
run on the same cores and compete with it. For capacity numbers, pin them to
different CPUs with `--cpuset-worker` and `--cpuset-rig`. `docker info
--format '{{.NCPU}}'` prints how many CPUs Docker has, numbered from 0; on
Docker Desktop, set the count in Settings, Resources.

Every container and every actor sandbox gets a GKE node's TCP buffer limits
(`tcp_rmem` up to 6 MiB, `tcp_wmem` up to 4 MiB), so kernel socket buffers grow
as they would on a node. Docker Desktop's own limits are larger.

### Options

`run-docker.sh [container options] -- [load options]`

Container options, before `--`:

| Option | Default | What it does |
|---|---|---|
| `--tunnel-cpus N` | 1 | atunnel's CPU limit |
| `--tunnel-memory M` | 1g | atunnel's memory limit, no swap |
| `--actors N` | 10 | actors atunnel hosts |
| `--cpuset-worker S` | | pin the worker container, which runs atunnel, to these CPUs, e.g. `0-3` |
| `--cpuset-rig S` | | pin the generator and the fake gateway to other CPUs, e.g. `4-9` |
| `--network-delay D` | | add `D` of delay, e.g. `2ms`, to atunnel's connections with the router and the gateway; its connections with actors stay local, as on a worker |
| `--tunnel-flag F` | | pass flag `F` to atunnel, e.g. `--tunnel-flag=--atunnel-tcp-notsent-lowat=-1`; repeat for more |

Load options, after `--`:

| Option | Default | What it does |
|---|---|---|
| `--scenarios S` | `all` | scenarios to run, comma-separated (see [Scenarios](#scenarios)); `all` runs the regular ones, `slow` the four slow-reader ones |
| `--rungs R` | `1000,2000,4000,8000` | rates per second to step through; for the connection-holding scenarios, connection counts |
| `--step D` | 10s | how long each rung runs |
| `--warmup D` | 2s | first part of each rung left out of the numbers |
| `--workers N` | 256 | how many requests the generator sends at once at most; each of its workers sends one at a time |

Each run writes to `benchmarking/atunnel/runs/<timestamp>/`:
`results.txt`, `results.json`, `profiles/` (a CPU and a heap profile of atunnel
per rung), and `logs/`. Only `results.txt`, and a `report.md` if you write
one, are kept in git. Open a profile with
`go tool pprof -http=: runs/.../profiles/IngressH1-4000.cpu.pb.gz`.

## How it works

The benchmark is one program, `atunnel-bench`, with a subcommand per process.
`run-docker.sh` runs it in three containers on one Docker network:

```mermaid
flowchart LR
    subgraph L["atunnel-load"]
        A["A: generator"]
    end
    subgraph W["atunnel-worker (privileged)"]
        B["B: atunnel<br/>own cgroup"]
        S["actor sandboxes:<br/>ingress actors,<br/>egress actor"]
    end
    subgraph K["atunnel-sink"]
        G["fake egress gateway"]
    end
    A -- "ingress: HTTPS :443, CONNECT :8443" --> B
    B -- "into each sandbox" --> S
    S -- "egress, redirected by nftables" --> B
    B -- "egress CONNECT" --> G
```

Which subcommand runs where:

| Subcommand | Container | Runs |
|---|---|---|
| `worker` | `atunnel-worker` | the rest of the worker pod: the fake atelet, `tunnel` in its own cgroup, and the actors in the sandboxes |
| `tunnel` | `atunnel-worker`, started by `worker` | atunnel, hosting actors with the worker's own code: the process measured |
| `ingress-actor`, `egress-actor` | `atunnel-worker`, inside the sandboxes | the actors' stand-ins, started by `worker` |
| `sink` | `atunnel-sink` | the fake egress gateway |
| `load` | `atunnel-load` | the load generator |
| `pki` | run once, before the containers | the throwaway certificates the others share |

### What is real

`atunnel-bench tunnel` makes the calls `cmd/ateworker-gvisor` makes, in the
same order:
- at startup: `tunnel.Start` with the worker's own flags, through
  `tunnel.RegisterFlags`;
- per actor: `tunnel.PrepareEgress`, `actornet.ServeSandbox`,
  `tunnel.ActivateEgress`, and `tunnel.ActivateIngress`;
- at teardown: `tunnel.Deactivate`, then closing the sandbox network.

Each actor gets real network namespaces, the nftables redirect, an egress
listener and DNS relay in its sandbox, and a certificate minted through
atunnel's own atelet client.

### What is faked

| In a worker | Stand-in |
|---|---|
| atelet | `worker` serves the two atelet calls atunnel makes, certificate minting and egress policy |
| the actor's program receiving requests | an ingress actor: a small HTTP server in each sandbox |
| the actor's program making outgoing connections | an egress actor in each sandbox; EgressIdleConns splits its connections across them, the other egress scenarios use actor-0's |
| the router | the generator, presenting the router's identity |
| the egress gateway | `sink`, with the real gateway's Envoy HTTP/2 settings; it never dials out |
| Pod certificates | files from a throwaway CA |

### How atunnel is measured

- atunnel runs in its own cgroup with the CPU and memory limits and no swap,
  so going over the limit is an OOM kill, as in a pod. The actors run outside
  it. If atunnel dies, `worker` logs it and starts a fresh one.
- The generator is open loop: work is scheduled at a fixed rate whatever
  earlier requests take, and latency is measured from when each unit was due.
- For each rung, the generator reads atunnel's `/stats` (CPU, memory,
  goroutines) when the warmup ends, every 500 ms after, and when the rung ends.
- atunnel's process also holds the other subcommands' code, which it never
  runs. That costs about 3 MB: atunnel idles at about 29 MB hosting 10 actors,
  against 26 MB built on its own.

## Scenarios

Each scenario runs at every rate in `--rungs` (default `1000,2000,4000,8000`
per second), for `--step` (10s) each, leaving the first `--warmup` (2s) out.

The router reaches atunnel in one of two ways, set by its `--upstream-connect`
flag: through CONNECT tunnels (the default), or over HTTPS (`false`).

| Scenario | Router mode | What is sent | Tells you |
|---|---|---|---|
| IngressH1 | `--upstream-connect=false` | HTTPS GETs over HTTP/1.1 keep-alive to `:443`, the `ate-target-actor` header cycling over the actors | CPU per request through atunnel's reverse proxy |
| IngressH2 | `--upstream-connect=false` | the same over HTTP/2 | the same, multiplexed |
| Connect | default | per unit: new TLS connection to `:8443`, `CONNECT actor:80`, one GET inside | cost of setting up a tunnel, mostly the TLS handshake |
| EgressHTTP | either | plain HTTP keep-alive from inside a sandbox | CPU per egress request on reused connections |
| EgressNewConnection | either | the same with a new connection per request | cost of a new egress connection |
| EgressIdleConns | either | rungs are connection counts: open at 200/s, one request each, hold | memory per open tunnel, read after a GC |
| EgressTCP | either | 4 connections write 64 KiB chunks as fast as they can; first rung only | throughput and CPU per byte |
| EgressTCPDownload | either | 4 connections read as fast as they can from a gateway writing without end; first rung only | the same, downloading |
| ConnectTCP | default | 4 CONNECT tunnels read as fast as they can from actors writing without end; first rung only | the same through ingress |
| ConnectTCPUpload | default | 4 CONNECT tunnels write 64 KiB chunks as fast as they can to actors that drop them; first rung only | the same, uploading |

### Slow-peer scenarios

These measure how much memory piles up in atunnel when a reader stops
reading. Run them with `--scenarios slow`; `all` doesn't include them.

Each rung is a number of connections. For each connection:

1. One end sends data without end.
2. The other end reads 4 MiB, then stops reading.
3. The data backs up inside atunnel.

The generator opens 200 connections a second until it has the rung's count,
then holds them. Three seconds later it reads atunnel's memory, including
the kernel socket buffers that no heap profile shows, and prints both per
connection.

| Scenario | Who stops reading | Where it piles up in atunnel |
|---|---|---|
| EgressSlowReader | the actor | the send queue to the actor, and the HTTP/2 stream's receive buffer in the heap |
| EgressSlowUpload | the gateway | the receive queue from the actor; the gateway's HTTP/2 window stops the rest |
| ConnectSlowReader | the router side | the send queue to the router and the receive queue from the actor |
| ConnectSlowUpload | the actor | the send queue to the actor and the receive queue from the router |

The CONNECT scenarios use one connection per tunnel, like an HTTP/1.1 client
of the router. The router's own CONNECT path shares one HTTP/2 connection
instead.

## Caveats

- **Not covered:** gVisor itself, the real router and gateway, and actors being
  activated and deactivated during load.
- **Shared machine.** The containers share the Docker host's CPUs. Pin them
  apart with `--cpuset-worker` and `--cpuset-rig` for clean capacity numbers.
- **Ephemeral ports.** Connect and EgressNewConnection leave tens of thousands
  of sockets in TIME_WAIT. `run-docker.sh` widens the port range and enables
  `tcp_tw_reuse` in its containers, and the egress actor does the same in its
  sandbox.
- **Heap figures** are Go's `HeapInuse`; for what is live, see the
  EgressIdleConns line, which collects garbage first.
