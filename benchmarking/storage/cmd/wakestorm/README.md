# wakestorm

`wakestorm` times batches of actor lifecycle calls against a running Agent
Substrate cluster. It sends one call per actor at a set concurrency, times
every call and every batch, and writes two files to `-out`:

* `calls.csv`: one row per call, with its phase, actor, node, start and end
  offsets, latency, result and retries.
* `summary.json`: for each batch, the call count, failures by code, P50, P90,
  P99, max, and the time until 10% through 100% of the calls succeeded.

It also prints the results as a Markdown table.

## Phases

| `-phase` | What it does |
|---|---|
| `create` | Creates `-n` actors from `-template` and resumes each once. Substrate creates external volumes on that first resume. Actors that are not `RUNNING` afterwards get `-repair-rounds` rounds of slow retries. |
| `lifecycle` | Runs `-repeat` cycles of CreateActor, first ResumeActor, SuspendActor, ResumeActor and DeleteActor, each cycle on new actors. Prints P50, P90 and max for each call across all cycles. |
| `delete` | Deletes every actor whose name starts with `-prefix`, then waits until ListActors no longer shows them. |
| `script` | Runs the `-script` steps on `-n` actors at once. Each actor is created, gets an egress policy that allows every `-allow-http` host, is resumed, runs the steps and is deleted. Prints P50, P90 and max for each step. |

SuspendActor uses `DURABILITY_MODE_ASYNCHRONOUS` in `lifecycle`. It returns
once the node has the snapshot, and the upload to durable storage continues in
the background. `script` uses `DURABILITY_MODE_SYNCHRONOUS`, which returns once
the snapshot is in durable storage.

`-settle` pauses between steps. Call times exclude the pause.

`wakestorm` exits 1 if any call failed, and 130 if it was interrupted. It
saves the results it has in both cases.

## Examples

One actor, 30 lifecycle cycles:

```bash
go run ./benchmarking/storage/cmd/wakestorm -phase lifecycle -n 1 -repeat 30 \
  -template glutton-storage -retry-contention -out /tmp/latency
```

Add 20 actors named `d-0040` through `d-0059` and keep them running, then
delete every `d-` actor:

```bash
go run ./benchmarking/storage/cmd/wakestorm -phase create -prefix d -start 40 -n 20 \
  -create-concurrency 20 -repair-rounds 0 -out /tmp/step-60
go run ./benchmarking/storage/cmd/wakestorm -phase delete -prefix d -out /tmp/cleanup
```

## Scripts

A `-script` file has one step per line. Blank lines and lines that start with
`#` are skipped.

* `run NAME COMMAND` runs `COMMAND` with `sh -c` in the actor and records its
  run time as phase `NAME`. A nonzero exit fails the step, and the end of
  its stderr goes in the `err` column of `calls.csv`.
* `cycle` suspends the actor, waits `-idle`, and resumes it.

An actor stops at its first failed step and is still deleted. Every command
gets each `-env KEY=VALUE`. `wakestorm` checks a running command every 5 s.
Steps with the same name share one row in the summary, and so do all suspends
and all resumes.

Commands run through [ate-env-guest](https://github.com/agent-substrate/env),
which the actor image must serve on port 80. `wakestorm` reaches it through
`atenet-router`, the way `kubectl ate proxy actor` does, so the router's
`--ingress-allowed-spiffe-ids` must include
`spiffe://cluster.local/ns/ate-system/sa/atenet-client`. See the
[router README](../../../../cmd/atenet/internal/router/README.md#common-setups).
The step time comes from the process start and exit times in the actor, so the
connection adds nothing to it.

`build.steps` and `gitrepos.steps` in [scenarios](../../scenarios) are
examples.

## Connecting

Like `kubectl ate`, `wakestorm` reaches `ate-api-server` through a port
forward with the current kubeconfig. `-kubeconfig`, `-context`, `-endpoint`
and `-token-file` change that. The caller needs permission to create, resume,
suspend, delete and list actors in `-atespace`, and to read the template in
`-template-atespace`. See [authorization](../../../../docs/guides/authorization.md).
The ate-client token that `wakestorm` mints lasts 1 hour. For longer runs,
pass `-token-file`, as `scenarios/build.sh` does.
`-phase script` also needs permission to create actor egress policies, and the
Kubernetes permissions of `kubectl ate proxy actor`: create, get and delete
PodCertificateRequests and port forward to `atenet-router` in `ate-system`,
and read ClusterTrustBundles.

External volumes are a preview feature, so every call sends the
`Ate-Preview-Allowed` header.

## Retries

* `Aborted`, from a concurrent update, is retried on every call up to
  `-max-conflict-retries` times.
* `-retry-contention` retries ResumeActor when a volume claim fails with
  "please retry". A volume pool returns that when many first resumes claim
  volumes at once.
* `-retry-no-room` retries ResumeActor every second while no worker has room.

Retries count toward the call's latency and appear in the `retries` column.
