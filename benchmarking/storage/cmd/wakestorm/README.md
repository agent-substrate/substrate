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

SuspendActor uses `DURABILITY_MODE_ASYNCHRONOUS`. It returns once the node has
the snapshot, and the upload to durable storage continues in the background.

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

## Connecting

Like `kubectl ate`, `wakestorm` reaches `ate-api-server` through a port
forward with the current kubeconfig. `-kubeconfig`, `-context`, `-endpoint`
and `-token-file` change that. The caller needs permission to create, resume,
suspend, delete and list actors in `-atespace`, and to read the template in
`-template-atespace`. See [authorization](../../../../docs/guides/authorization.md).

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
