# Upgrade runbook

This runbook upgrades a running Agent Substrate install to a newer release in the same release window, meaning the same `v0.x`: for example, from v0.2.0 to v0.2.1. Between windows, for example from v0.1.x to v0.2.x, reinstall instead. 

> [!NOTE]
> TODO: Depends on two policy docs that are not in the repository yet. Link the compatibility policy, which says which releases this runbook upgrades between, and the version skew policy, which says which component releases may run together during the upgrade and so sets the step order below.


| Step | What changes | What running actors see | How long |
|---|---|---|---|
| 1 | podcertificate-controller | Nothing | A minute |
| 2 | atelet, one node at a time | Nothing | Seconds to about 6 minutes per node |
| 3 | workers | `SIGTERM`, then a 30-minute window to be suspended before `SIGKILL` | Seconds to hours per pool |
| 4 | ate-api-server, the CRDs and ate-controller, atenet, then SandboxConfig | API calls and long HTTP requests can be cut once; see step 4 | A few minutes, or as long as step 3 if workers roll again |

podcertificate-controller signs the certificates the other components use, so it goes first. atelet and the workers go before ate-api-server and atenet, so that when the API server changes, every node already understands requests from either version. ate-controller and the CRDs follow the API server: ate-controller is a client of the API server, and new workers start from the pod template the old ate-controller renders. The SandboxConfig admission policy and the default SandboxConfig go last, once every worker can run what they allow. Rollback is the same list in reverse, from the old release. An actor that crashes along the way goes back to its last snapshot with one revert call.

The system upgrade does not change the database engine, the CSI drivers `--setup-csi` installs, the Kubernetes version or the node OS version.

## Before you start

Check out both releases. `ate-setup` runs from the new checkout. The old checkout is kept for rollback. The installed release is `cluster.substrateVersion` in this cluster's install record, described below.

```bash
OLD_RELEASE=<installed release, for example v0.2.0>
NEW_RELEASE=<new release, for example v0.2.1>
mkdir ~/ate-upgrade && cd ~/ate-upgrade
git clone --branch $OLD_RELEASE https://github.com/agent-substrate/substrate.git old
git clone --branch $NEW_RELEASE https://github.com/agent-substrate/substrate.git new
(cd old && go install ./cmd/kubectl-ate)
```

Point kubectl at the cluster you upgrade, and keep it there until the upgrade is done:

```bash
kubectl config use-context <context of the cluster>
```

Every `ate-setup deploy` run records the settings it was given in a file under `installs/` in the record directory `record.dir` names, and prints the path as `recorded this configuration in <path>`. Make two copies of this cluster's record:

```bash
cp <record> ~/ate-upgrade/old.yaml
cp <record> ~/ate-upgrade/new.yaml
```

Check that the copy holds the settings you installed with. If it is missing or wrong, write the file from those settings; [operator-install.md](operator-install.md) lists them.

A record holds no database connection strings. If you use an external database, export `ATE_API_POSTGRES_READ_WRITE_CONNECTION_STRING`, and `ATE_API_POSTGRES_OWNER_CONNECTION_STRING` if you set it, in the shell you run the steps from.

Keep `old.yaml` as it is, for rollback. If you build from source, keep `new.yaml` the same: the checkout you run from decides the release. If you install prebuilt images, set `images.tag` in `new.yaml` to the new release.

You will run the steps from the new checkout:

```bash
cd ~/ate-upgrade/new
```

> [!CAUTION]
> Do not run `ate-setup deploy ate-system`, `ate-setup setup csi` or `ate-setup delete` during the upgrade. They change components out of upgrade order.

## Upgrade

### Step 1. Upgrade podcertificate-controller

```bash
go run ./cmd/ate-setup deploy podcertificate-controller --config ~/ate-upgrade/new.yaml
```

This deploys the new podcertificate-controller and waits until it publishes its trust bundles. Rerunning is safe.

- **Done:** the command exits 0.
- **Stuck:** the command fails. [Roll back step 1](#roll-back-step-1).

### Step 2. Upgrade atelet

```bash
go run ./cmd/ate-setup deploy atelet --config ~/ate-upgrade/new.yaml --rollout-timeout 1h
```

`ate-setup` prints nothing until the roll is done. To watch it, open a second terminal and run:

```bash
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

Then move the pools. For each pool:

1. Set the pool:

   ```bash
   NS=<workerpool namespace>
   WORKERPOOL=<workerpool name>
   CLASS=$(kubectl -n $NS get workerpool $WORKERPOOL -o jsonpath='{.spec.sandboxClass}')
   ```

2. Set its new image. From source, take the ref after `ateom-$CLASS: ` in `~/ate-upgrade/worker-images.txt`:

   ```bash
   NEW_IMAGE=$(sed -n "s/^ateom-$CLASS: //p" ~/ate-upgrade/worker-images.txt)
   ```

   With prebuilt images, set `NEW_IMAGE` to `ateom-$CLASS` from your image repo at the new tag, pinned by digest (`crane digest <image>` prints it).

3. Check that `echo "$NS/$WORKERPOOL $CLASS $NEW_IMAGE"` shows `ateom-` plus the pool's CLASS and a full `@sha256:` digest. A bad image still removes the pool's first old worker.
4. Patch the pool, wait until ate-controller moves its Deployment to the new image, and watch the roll:

   ```bash
   kubectl -n $NS patch workerpool $WORKERPOOL --type merge -p "{\"spec\":{\"workerImage\":\"$NEW_IMAGE\"}}"
   kubectl -n $NS wait deployment/$WORKERPOOL --for=jsonpath='{.spec.template.spec.containers[0].image}'="$NEW_IMAGE" --timeout=2m
   kubectl -n $NS rollout status deployment/$WORKERPOOL
   ```

5. Once the first pool of a CLASS has one new worker running, patch the other pools of that CLASS without waiting for the roll to finish. A new worker is running when the newest ReplicaSet of the pool shows READY 1 or more:

   ```bash
   kubectl -n $NS get rs -l ate.dev/worker-pool=$WORKERPOOL
   ```

   Then repeat on the other CLASS.

What to expect:

- **Progress:** `kubectl rollout status` prints how many new workers are updated and available. Ctrl-C to safely stop the watch.
- **Done:** Step 3 is done when every pool is done rolling with `kubectl get deploy -A -l ate.dev/worker-pool`. Go to Step 4 once done.
- **Pace:** a WorkerPool replaces 10% of its workers per batch. New workers start wherever the scheduler finds room. With spare capacity, every batch has its `SIGTERM` within minutes and the old workers drain in parallel; without it, new workers stay Pending until old ones exit, up to 30 minutes per batch, so a large pool can take hours. 
- **Stuck:** rollout status fails with `exceeded its progress deadline` (the roll goes on), or a worker pod stays Terminating for more than 60 minutes. Check `kubectl -n $NS get pods -l ate.dev/worker-pool=$WORKERPOOL`. A bad image stops the roll after the first batch: [roll back that pool](#roll-back-step-3).
- **Actors:** each gets `SIGTERM` and 30 minutes to be suspended. Substrate does not suspend them. An actor that is not suspended by then becomes `CRASHED` and loses its work since its last snapshot.

> [!NOTE]
> TODO: link to the eviction section in the [API guide](api-guide.md), which will say how an actor is expected to get suspended within the 30 minutes.

### Step 4. Upgrade the Substrate control plane

First the Substrate API server. The first new API server pod migrates the database schema.

```bash
cd ~/ate-upgrade/new
go run ./cmd/ate-setup deploy apiserver --config ~/ate-upgrade/new.yaml
kubectl -n ate-system rollout status deploy/ate-api-server
kubectl ate get actors -A
```

Then the CRDs and `ate-controller`:

```bash
go run ./cmd/ate-setup deploy ate-controller --config ~/ate-upgrade/new.yaml
```

This applies the new CRDs (`WorkerPool`, `SandboxConfig` and `CSIDriverConfig`) and ate-controller's RBAC, waits until the CRDs are established, then deploys the new `ate-controller` and waits for it. Rerunning is safe. A minute after the new pod starts, check whether it is replacing workers:

```bash
kubectl get deploy -A -l ate.dev/worker-pool
```

This happens when this release changes the worker pod template. A pool is done rolling when READY shows `n/n`, and UP-TO-DATE and AVAILABLE both show `n`. Wait until every pool is done before you go on.

> [!NOTE]
> TODO: Depends on the version skew policy: whether a compatible release may change the worker pod template. If it may not, the pools never roll in this step, and this check and the second roll in the rollback go away.

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
> TODO: Depends on per-release SandboxConfig names. Today this overwrites `gvisor-default` in place, so a release with new sandbox binaries breaks the restore of actors suspended under the old ones. Once each release ships its default SandboxConfig under its own name, this step adds the new one and leaves the old one in place.

- **Done:** all rollout status print `successfully rolled out`, `deploy ate-controller` and `deploy sandboxconfig` exit 0, and `kubectl -n ate-system get pods -l app=ate-controller` shows one pod, Running, with RESTARTS 0.
- **Stuck:** an old pod keeps serving, or the new ate-controller pod keeps restarting. Roll back step 4 if you cannot fix it.
- **API clients:** an API call can fail once with a retriable error while an API server pod restarts. Retry it.
- **Actors:** when the router restarts, an HTTP request that takes longer than about 10 seconds can be cut once.

Then install the new `kubectl ate` with `go install ./cmd/kubectl-ate`. Actor owners can now use API fields and commands new in this release, and you can set WorkerPool, SandboxConfig and CSIDriverConfig fields new in this release.

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
go run ./cmd/ate-setup deploy ate-controller --config ~/ate-upgrade/old.yaml
go run ./cmd/ate-setup deploy apiserver --config ~/ate-upgrade/old.yaml
go install ./cmd/kubectl-ate
```

`deploy ate-controller` restores the old CRDs, RBAC and `ate-controller`. If the release changed the worker pod template, every pool rolls again: wait as in step 3. Fields the new release added to WorkerPools, SandboxConfigs or CSIDriverConfigs are dropped from those objects. Remove them from your YAML files too, or `kubectl apply` rejects the files.

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
go run ./cmd/ate-setup deploy podcertificate-controller --config ~/ate-upgrade/old.yaml
```
