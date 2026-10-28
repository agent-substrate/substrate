# atunnel benchmark: 100,000 idle egress connections

Run: `20261020-163000`, EgressIdleConns only, with the idle-buffer change
(`streamCopy`) in atunnel. Raw numbers are in `results.txt`.

## Setup

- Docker Desktop on an Apple M-series Mac, with the VM given 10 CPUs and
  24 GB of memory.
- atunnel limited to 4 CPUs and 16 GiB without swap, far more than it used,
  so neither limit shapes the result.
- `run-docker.sh --tunnel-cpus 4 --tunnel-memory 16g -- --scenarios
  EgressIdleConns --rungs 25000,50000,100000`
- Connections open at 200/s and are split across the egress actors in all 10
  sandboxes. Each completes one request and is then held idle.

## Results

| Idle connections | Per connection, after GC | Goroutines per connection | atunnel heap, after GC | atunnel cgroup memory | atunnel CPU opening 200 conns/s | Errors |
|---|---|---|---|---|---|---|
| 25,000 | 26.9 KiB | 4.0 | 658 MiB | 1.3 GiB | 0.15 cores | 0 |
| 50,000 | 27.1 KiB | 4.0 | 1.3 GiB | 2.6 GiB | 0.14 cores | 0 |
| 100,000 | 26.8 KiB | 4.0 | 2.6 GiB | 5.2 GiB | 0.19 cores | 0 |

- Memory per connection stays flat at about 27 KiB, so it grows linearly with
  the number of connections. Before the change it was about 157 KiB, which
  would be about 15 GiB of heap at 100,000.
- The cgroup memory also counts the kernel's socket buffers, about 53 KiB per
  connection in total.
- The CPU column is what opening new connections costs, about 1 ms each, so
  200/s takes about 0.2 cores. Holding the idle connections costs almost none.
- The 25k and 50k rungs show SHORTFALL because some opens waited up to ~11 ms
  in the generator's queue. Every connection still opened.

## Nothing else was the limit

Sampled every 30 seconds with `docker stats` during the run:

| | Peak |
|---|---|
| atunnel | 0.19 of 4 cores, 5.2 of 16 GiB, no restarts |
| worker container (atunnel and the 20 actor processes) | 1.2 cores, 6.1 GiB |
| fake gateway | 0.13 cores, 1.6 GiB |
| generator container | 0.12 cores, 0.5 GiB |
| Docker VM | 8.5 of 24 GB used, swap unused |

## One sandbox holds at most ~64k egress connections

With every connection coming from one sandbox, the run stopped at 64,512
connections and later ones timed out. A sandbox has one address, and atunnel's
redirect sends every connection to the same listener, so only the source port
tells the connections apart: 1024–65535 is 64,512 ports. This is a limit per
actor, not per worker. It is why the harness now runs an egress actor in every
sandbox.
