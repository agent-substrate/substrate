# Upgrade runbook

This runbook upgrades a running Agent Substrate install to a newer release in the same release window, meaning the same `v0.x`: for example, from v0.2.0 to v0.2.1. Between windows, for example from v0.1.x to v0.2.x, reinstall instead. 

> [!NOTE]
> TODO: Depends on the policy doc. Link its release policy (which versions this runbook upgrades between) and its version skew policy (which component versions can run together during the upgrade).


| Step | What changes | What running actors see | How long |
|---|---|---|---|
| 1 | podcertificate-controller, CRDs and ate-controller | Usually nothing; see step 1 | A minute, or as long as step 3 if it replaces workers |
| 2 | atelet, one node at a time | Nothing | Seconds to about 6 minutes per node |
| 3 | workers | `SIGTERM`, then a 30-minute window to be suspended before `SIGKILL` | Minutes to hours per pool |
| 4 | ate-api-server, atenet, then SandboxConfig | API calls and long HTTP requests can be cut once | A few minutes |

podcertificate-controller signs the certificates the other components use, and ate-controller manages the WorkerPools, so they go first, together with the CRDs. atelet and the workers go before ate-api-server and atenet, so that when the API server changes, every node already understands requests from either version. The SandboxConfig admission policy and the default SandboxConfig go last, once every worker can run what they allow. Rollback is the same list in reverse, from the old release. An actor that crashes along the way goes back to its last snapshot with one revert call.

The system upgrade does not change the database engine, the CSI drivers `--setup-csi` installs, the Kubernetes version or the node OS version.

## Before you start

Check out both releases. `ate-setup` runs from the new checkout. The old checkout is kept for rollback.

```bash
OLD_RELEASE=<installed release, for example v0.2.0>
NEW_RELEASE=<new release, for example v0.2.1>
mkdir ~/ate-upgrade && cd ~/ate-upgrade
git clone --branch $OLD_RELEASE https://github.com/agent-substrate/substrate.git old
git clone --branch $NEW_RELEASE https://github.com/agent-substrate/substrate.git new
(cd old && go install ./cmd/kubectl-ate)
```

Every `ate-setup` run writes the settings it applied to a local settings file, in the format `--config` reads. Make two copies of the one from your install or your last `ate-setup` run:

> [!NOTE]
> TODO: Depends on #1887, which sets where `ate-setup` writes the settings file and its format.

```bash
cp <settings file> ~/ate-upgrade/old.yaml
cp <settings file> ~/ate-upgrade/new.yaml
```

Keep `old.yaml` as it is, for rollback. If you build from source, keep `new.yaml` the same: the checkout you run from decides the release. If you install prebuilt images, set the image tag in `new.yaml` to the new release.

You will run the steps from the new checkout:

```bash
cd ~/ate-upgrade/new
```

> [!CAUTION]
> Do not run `ate-setup deploy ate-system`, `ate-setup setup csi` or `ate-setup delete` during the upgrade. They change components out of upgrade order.

## Upgrade

### Step 1. Upgrade podcertificate-controller, the CRDs and ate-controller

```bash
go run ./cmd/ate-setup deploy podcertificate-controller --config ~/ate-upgrade/new.yaml
go run ./cmd/ate-setup deploy ate-controller --config ~/ate-upgrade/new.yaml
```

The first command deploys the new podcertificate-controller and waits until it publishes its trust bundles. The second applies the new CRDs (`WorkerPool`, `SandboxConfig` and `CSIDriverConfig`) and ate-controller's RBAC, waits until the CRDs are established, then deploys the new `ate-controller` and waits for it. Rerunning is safe.

- **Done:** both commands exit 0, and a minute later `kubectl -n ate-system get pods -l app=ate-controller` still shows one pod, Running, with RESTARTS 0.
- **Stuck:** a command fails, or the pod keeps restarting. [Roll back step 1](#roll-back-step-1).

A minute after the new pod starts, check whether it is replacing workers:

```bash
kubectl get deploy -A -l ate.dev/worker-pool
```

A pool is done rolling when READY shows `n/n`, and UP-TO-DATE and AVAILABLE both show `n`. Wait until every pool is done, then go to step 2. This can happen when this release changes the worker pod template.

> [!NOTE]
> TODO: Depends on the API compatibility policy in the policy doc: whether ate-controller may change the worker pod template it renders within a release window. Proposal: it does not, so this step replaces no workers.

### Step 2. Upgrade atelet

```bash
go run ./cmd/ate-setup deploy atelet --config ~/ate-upgrade/new.yaml --rollout-timeout 1h
kubectl -n ate-system rollout status ds/atelet
```

- **Progress:** `Waiting for daemon set "atelet" rollout to finish: 2 out of 5 new pods have been updated...`, then `... 4 of 5 updated pods are available...`.
- **Done:** `daemon set "atelet" successfully rolled out`.
- **Stuck:** the `N out of M` count does not move for more than about 6 minutes. A new atelet pod that cannot start stops the roll and leaves its node without atelet; if you cannot fix it quickly, [roll back step 2](#roll-back-step-2). A pod stuck in ContainerCreating usually means podcertificate-controller is not running.
- **Actors:** running actors keep running. A node's atelet is down for a few seconds while it restarts.

### Step 3. Move each WorkerPool to the new worker image

> [!NOTE]
> Do not start step 3 until step 2 is done: new workers need the new atelet.
>
> Change the WorkerPool, not its Deployment: `ate-controller` overwrites edits to the Deployment.

Before you patch any pool, list the pools and save the list for rollback. Each pool needs the new image for its sandbox CLASS:

```bash
kubectl get workerpools -A -o custom-columns='NAMESPACE:.metadata.namespace,NAME:.metadata.name,CLASS:.spec.sandboxClass,IMAGE:.spec.workerImage' | tee ~/ate-upgrade/before-workerpools.txt
```

If you build from source, build and push both worker images once. This touches nothing in the cluster and ends by printing `ateom-gvisor: <ref>` and `ateom-microvm: <ref>`:

```bash
go run ./cmd/ate-setup publish worker-images --config ~/ate-upgrade/new.yaml | tee ~/ate-upgrade/worker-images.txt
```

> [!NOTE]
> TODO: Depends on the release policy in the policy doc: whether open source releases publish prebuilt images, where, and under which tag.

Then move the pools. For each pool:

1. Set the pool:

   ```bash
   NS=<workerpool namespace>
   WORKERPOOL=<workerpool name>
   CLASS=$(kubectl -n $NS get workerpool $WORKERPOOL -o jsonpath='{.spec.sandboxClass}')
   ```

2. Set its new image. From source, copy the ref after `ateom-$CLASS: ` in `~/ate-upgrade/worker-images.txt`:

   ```bash
   NEW_IMAGE=<ref>
   ```

   With prebuilt images, set `NEW_IMAGE` to `ateom-$CLASS` from your image repo at the new tag, pinned by digest (`crane digest <image>` prints it).

3. Check that `echo "$NS/$WORKERPOOL $CLASS $NEW_IMAGE"` shows `ateom-` plus the pool's CLASS and a full `@sha256:` digest. A bad image still removes the pool's first old worker.
4. Patch the pool, wait until ate-controller moves its Deployment to the new image, and watch the roll:

   ```bash
   kubectl -n $NS patch workerpool $WORKERPOOL --type merge -p "{\"spec\":{\"workerImage\":\"$NEW_IMAGE\"}}"
   kubectl -n $NS wait deployment/$WORKERPOOL --for=jsonpath='{.spec.template.spec.containers[0].image}'="$NEW_IMAGE" --timeout=2m
   kubectl -n $NS rollout status deployment/$WORKERPOOL
   ```

5. Once the first pool of a CLASS reports `1 of N updated replicas are available`, patch the other pools of that CLASS without waiting. Then repeat on other CLASS.

What to expect:

- **Progress:** `kubectl rollout status` prints how many new workers are updated and available. Ctrl-C to safely stop the watch.
- **Done:** Step 3 is done when every pool is done rolling with `kubectl get deploy -A -l ate.dev/worker-pool`. Go to Step 4 once done.
- **Pace:** a WorkerPool replaces 10% of its workers per batch. New workers start wherever the scheduler finds room. With spare capacity, every batch has its `SIGTERM` within minutes and the old workers drain in parallel; without it, new workers stay Pending until old ones exit, up to 30 minutes per batch, so a large pool can take hours. 
- **Stuck:** rollout status fails with `exceeded its progress deadline` (the roll goes on), or a worker pod stays Terminating for more than 60 minutes. Check `kubectl -n $NS get pods -l ate.dev/worker-pool=$WORKERPOOL`. A bad image stops the roll after the first batch: [roll back that pool](#roll-back-step-3).
- **Actors:** each gets `SIGTERM` and 30 minutes to be suspended, as described under eviction in the [API guide](api-guide.md).

### Step 4. Upgrade the Substrate control plane

First the Substrate API server. The first new API server pod migrates the database schema.

```bash
cd ~/ate-upgrade/new
go run ./cmd/ate-setup deploy apiserver --config ~/ate-upgrade/new.yaml
kubectl -n ate-system rollout status deploy/ate-api-server
kubectl ate get actors -A
```

Then `atenet`:

```bash
go run ./cmd/ate-setup deploy atenet --config ~/ate-upgrade/new.yaml
kubectl -n ate-system rollout status deploy/atenet-router
kubectl -n ate-system rollout status deploy/atenet-egress
```

Then the `SandboxConfig` admission policy and the default `SandboxConfig`:

```bash
go run ./cmd/ate-setup deploy sandboxconfig --config ~/ate-upgrade/new.yaml
```

> [!NOTE]
> TODO: Depends on the shape of the default SandboxConfig. Until each release ships it under its own name, this overwrites `gvisor-default` in place.

- **Done:** all rollout status print `successfully rolled out`, and `deploy sandboxconfig` exits 0.
- **Stuck:** the old pod keeps serving. Roll back step 4 if you cannot fix it.
- **API clients:** an API call can fail once with a retriable error while an API server pod restarts. Retry it.
- **Actors:** when the router restarts, an HTTP request that takes longer than about 10 seconds can be cut once.

Then install the new `kubectl ate` with `go install ./cmd/kubectl-ate`. Actor owners can now use API fields and commands new in this release.

The upgrade is done when step 4 is done. Keep `~/ate-upgrade` while you might still roll back, then delete it.

## Something's wrong, or I need to roll back

Stopping between steps is safe.

### Pause a pool's roll

If a pool's new workers look wrong but you are not ready to roll it back, or you want to stop interrupting actors for a while, pause its Deployment. ate-controller leaves this one Deployment change in place.

```bash
kubectl -n $NS rollout pause deployment/$WORKERPOOL
kubectl -n $NS rollout resume deployment/$WORKERPOOL
```

Workers already removed still get their 30 minutes. Do not leave a pool paused, and do not start step 4 while one is: a paused pool ignores every later change to its workers.

### Roll back

To go back to the old release, roll back every step you began, finished or not, starting with the last. To fix one pool, roll back only that pool. Rollback runs from the old checkout, with `old.yaml`:

```bash
cd ~/ate-upgrade/old
```

#### Roll back step 4

Tell actor owners to stop using API fields new in this release, because the old API server rejects them. Then:

```bash
go run ./cmd/ate-setup deploy sandboxconfig --config ~/ate-upgrade/old.yaml
go run ./cmd/ate-setup deploy atenet --config ~/ate-upgrade/old.yaml
go run ./cmd/ate-setup deploy apiserver --config ~/ate-upgrade/old.yaml
go install ./cmd/kubectl-ate
```

The database schema stays new; the old API server drops fields it does not know when it rewrites a record.

#### Roll back step 3

Set each patched pool back to its IMAGE in `~/ate-upgrade/before-workerpools.txt`, with the exact string.
```bash
NS=<workerpool namespace>
WORKERPOOL=<workerpool name>
OLD_IMAGE=<IMAGE for this pool in before-workerpools.txt>
kubectl -n $NS patch workerpool $WORKERPOOL --type merge -p "{\"spec\":{\"workerImage\":\"$OLD_IMAGE\"}}"
```

If you paused a WorkerPool, resume it now with `kubectl -n $NS rollout resume deployment/$WORKERPOOL`. Then watch:

```bash
kubectl -n $NS rollout status deployment/$WORKERPOOL
```

If a WorkerPool was partway through a roll, only workers already on the new image are replaced. Every new version worker is replaced again, with another round of `SIGTERM`. Wait as in [step 3](#step-3-move-each-workerpool-to-the-new-worker-image).


#### Roll back step 2

**DO NOT** roll back step 2 before every pool is back on its old image and `kubectl get pods -A -l ate.dev/worker-pool` shows no Terminating pod. A new worker with an old atelet is not supported.

```bash
go run ./cmd/ate-setup deploy atelet --config ~/ate-upgrade/old.yaml --rollout-timeout 1h
```

#### Roll back step 1

```bash
go run ./cmd/ate-setup deploy ate-controller --config ~/ate-upgrade/old.yaml
go run ./cmd/ate-setup deploy podcertificate-controller --config ~/ate-upgrade/old.yaml
```

These commands restore the old CRDs, RBAC, `ate-controller` and `podcertificate-controller`. If the release changed the worker pod template, every pool rolls again: wait as in step 3. Fields the new release added to WorkerPools, 

SandboxConfigs or CSIDriverConfigs are dropped from those objects. Remove them from your YAML files too, or `kubectl apply` rejects the files.
