# Counter demo: manual end-to-end testing guide

This guide walks through manually testing the counter template on a fresh GCP
environment, end to end: provision a cluster, deploy the two-version counter
demo, drive an actor through suspend/resume, upgrade it to a new
`ActorTemplateVersion`, roll it back, and clean everything up. Use it to
verify a build by hand, or as a checklist when reproducing issues.

The counter server ([`demos/counter/counter.go`](../../demos/counter/counter.go))
keeps two counters — one in process memory, one in a file on a durable
volume — and echoes both plus its `VERSION` env var on every request. That
makes each state-preservation property directly observable in curl output.

## 1. Prerequisites

- A GCP project you can create clusters and buckets in.
- `gcloud` with application-default credentials, `kubectl`, and Go.
- Images are built automatically with `ko` (fetched via `hack/run-tool.sh`);
  no manual image builds are needed.

> [!NOTE]
> macOS ships bash 3.2, which is too old for `install-ate.sh`. Install a
> current bash (`brew install bash`) and prefix the install commands with
> `/opt/homebrew/bin/bash`.

## 2. Provision a cluster from scratch

Skip to step 3 if you already have a cluster with Agent Substrate deployed —
you still need the env file sourced (set `KUBECTL_CONTEXT` there to target an
existing cluster).

```bash
# Configure and source the dev environment (PROJECT_ID, CLUSTER_NAME,
# BUCKET_NAME, KO_DOCKER_REPO are the ones to review).
cp hack/ate-dev-env.sh.example .ate-dev-env.sh
$EDITOR .ate-dev-env.sh
source .ate-dev-env.sh

gcloud auth application-default login --project=${PROJECT_ID}

# Idempotent: enables APIs, creates the GKE cluster and snapshot bucket,
# and sets up IAM bindings. See tools/setup-gcp/README.md for the pieces.
go run ./tools/setup-gcp bootstrap

# Build images with ko and deploy the control plane; waits for rollouts.
./hack/install-ate.sh --deploy-ate-system
```

## 3. Deploy the two-version counter demo

```bash
./hack/install-ate.sh --deploy-demo-counter-atv
```

This deploys the CRD counter demo first (the AT/ATV variant reuses its
`WorkerPool` and warm node), then applies
[`demos/counter/counter-atv.yaml.tmpl`](../../demos/counter/counter-atv.yaml.tmpl):
one `ActorTemplate` (`counter-atv`) and two `ActorTemplateVersion`s
(`counter-atv-v1`, `counter-atv-v2`) that differ only in the `VERSION` env
var. The script blocks until both versions' golden snapshots are `STATE_READY`
— on a cold cluster the first build pays one-time costs (runsc download,
image pulls), so expect a few minutes.

Install the CLI and confirm what was created:

```bash
go install ./cmd/kubectl-ate

kubectl ate get actor-templates
kubectl ate get actor-template-versions --template counter-atv
```

> [!NOTE]
> After pulling proto changes, reinstall `kubectl-ate`. A stale binary can
> fail with confusing wire-level errors (e.g. "invalid page token") against a
> newer API server.

## 4. Create an actor and drive traffic

```bash
kubectl ate create atespace demo
kubectl ate create actor my-counter-1 -a demo --template counter-atv
```

The actor is pinned to the template's `defaultVersionOnCreate`
(`counter-atv-v1`). Traffic goes through the atenet router, which is a
`ClusterIP` service — port-forward it (and keep this running in a separate
terminal):

```bash
kubectl port-forward -n ate-system svc/atenet-router 8000:80
```

This is unrelated to `kubectl ate` itself, which auto-port-forwards to the
API server (or use `--endpoint` to target it directly).

```bash
curl -X POST -H "Host: my-counter-1.demo.actors.resources.substrate.ate.dev" http://localhost:8000
# hello from: <pod-ip> | version: v1 | preserved memory count: 1 | preserved file counter: 1
```

The first request activates the actor on demand: it boots from the v1 golden
snapshot onto an available worker. Confirm:

```bash
kubectl ate get actor my-counter-1 -a demo -o json
# "status": "STATUS_RUNNING", "actorTemplateVersion": "counter-atv-v1",
# "workerAssignment": {...}
```

## 5. Suspend, resume, and what survives

```bash
kubectl ate suspend actor my-counter-1 -a demo
kubectl ate get actor my-counter-1 -a demo -o json
# "status": "STATUS_SUSPENDED", "latestSnapshot": {...}, no workerAssignment
```

Either resume explicitly (`kubectl ate resume actor my-counter-1 -a demo`) or
just send traffic — the router resumes suspended actors on demand:

```bash
curl -X POST -H "Host: my-counter-1.demo.actors.resources.substrate.ate.dev" http://localhost:8000
# hello from: <pod-ip> | version: v1 | preserved memory count: 1 | preserved file counter: 2
```

What survived is determined by the snapshot scope. This demo's versions set
`onCommit: SNAPSHOT_CONTENT_SCOPE_DATA`, so a suspend captures only the
durable volume: the file counter carries over, the memory count restarts. To
see process memory survive instead, use `pause` — the demo sets `onPause:
FULL`, which takes a full local snapshot on the worker:

```bash
kubectl ate pause actor my-counter-1 -a demo
curl -X POST -H "Host: my-counter-1.demo.actors.resources.substrate.ate.dev" http://localhost:8000
# ... | preserved memory count: 2 | preserved file counter: 3   (memory continued)
```

## 6. Upgrade to v2

An upgrade is a resume with a new version pin. It requires the actor to be
`SUSPENDED` (or `CRASHED`) — a paused actor's local snapshot is bound to its
node and image, so it cannot be re-pinned. As a quick self-check, the request
is rejected while the actor is running:

```bash
kubectl ate resume actor my-counter-1 -a demo --template-version counter-atv-v2
# Error: ... FailedPrecondition ... requires STATUS_SUSPENDED or STATUS_CRASHED

kubectl ate suspend actor my-counter-1 -a demo
kubectl ate resume actor my-counter-1 -a demo --template-version counter-atv-v2

curl -X POST -H "Host: my-counter-1.demo.actors.resources.substrate.ate.dev" http://localhost:8000
# hello from: <pod-ip> | version: v2 | preserved memory count: 1 | preserved file counter: 4
```

The response shows exactly what an upgrade preserves: the durable file
counter carried over onto the new version, while the memory count restarted —
a memory snapshot cannot run on a different version's images, so the actor
cold-boots on v2 and unpacks its durable data. The pin is persisted before the
resume starts, so a retried or bare resume after a mid-flight failure still
lands on v2:

```bash
kubectl ate get actor my-counter-1 -a demo -o json
# "actorTemplateVersion": "counter-atv-v2"
```

Re-pins are validated: the target version must exist under the same template
and be `STATE_READY`, with the same sandbox class and unchanged volumes.

## 7. Roll back to v1

Rolling back is the same operation with the previous version:

```bash
kubectl ate suspend actor my-counter-1 -a demo
kubectl ate resume actor my-counter-1 -a demo --template-version counter-atv-v1

curl -X POST -H "Host: my-counter-1.demo.actors.resources.substrate.ate.dev" http://localhost:8000
# hello from: <pod-ip> | version: v1 | preserved memory count: 1 | preserved file counter: 5
```

(The API also exposes the pin via `UpdateActor` with an
`actor_template_version` field mask, followed by a plain resume; the CLI flag
does both in one step.)

## 8. Observing and troubleshooting

```bash
kubectl ate logs actors my-counter-1 -a demo -f   # stream actor logs
kubectl ate get workers -n ate-demo-counter       # worker pool state
kubectl ate top workers                           # per-worker utilization
```

| Symptom | What to check |
|---|---|
| Deploy hangs "Waiting for ActorTemplateVersion ... READY" | `kubectl ate get actor-template-versions counter-atv-v1 -o json` — a `STATE_FAILED` build includes a message; `STATE_BUILDING` on a cold cluster just needs time. |
| Actor is `STATUS_CRASHED` | Crashed actors keep their last committed snapshot and are directly resumable — `kubectl ate resume actor ...` (with or without `--template-version`). |
| curl: connection refused | The router port-forward dropped; restart it (step 4). |
| `kubectl ate` wire/parse errors | Stale binary — `go install ./cmd/kubectl-ate` again, or run `go run ./cmd/kubectl-ate ...` from the branch under test. |
| Deeper state inspection | [Observability guide](../observability.md) for logs/metrics/traces; [valkey direct access](valkey-direct-access.md) to inspect raw actor records. |

## 9. Micro-VM variant

The same counter also runs on the micro-VM sandbox class, which is the
easiest way to test guest-memory snapshots surviving suspend/resume across
workers:

```bash
./hack/run-microvm-demo.sh        # GKE; kind: hack/run-microvm-demo-kind.sh
```

The script composes `--deploy-ate-system`, the micro-VM asset staging, and
the `counter-microvm` manifest; then repeat the create/curl/suspend/resume
loop from steps 4–5 against that template. See the
[micro-VM section of the demo README](../../demos/counter/README.md#micro-vm-variant).

> [!NOTE]
> The micro-VM demo still uses the CRD `ActorTemplate`, so the
> `--template-version` upgrade flow in steps 6–7 does not apply to it.

## 10. Cleanup

```bash
kubectl ate delete actor my-counter-1 -a demo   # requires SUSPENDED/CRASHED; suspend first
kubectl ate delete atespace demo                # must be empty

# Suspends and deletes any remaining counter-atv actors, then removes
# v2, v1 (clearing the template's default), and the template.
./hack/install-ate.sh --delete-demo-counter-atv
./hack/install-ate.sh --delete-demo-counter

# Optional: tear down the GCP resources entirely.
./hack/teardown.sh --all
```
