# Upgrade runbook

This runbook upgrades a running Agent Substrate install to a newer release.

| From → to | Supported |
|---|---|
| A later patch release of the same minor, for example 1.2.1 → 1.2.4 | Yes. Patch releases can be skipped |
| The next minor release, for example 1.2.x → 1.3.y | Yes |
| Skipping a minor release, for example 1.2.x → 1.4.y | No. Upgrade to 1.3 first |
| A new major release, for example 1.x → 2.x | No. Reinstall |
| A minor release before 1.0, for example 0.3.x → 0.4.y | No. Reinstall. Before 1.0, breakage still exists for minor versions |

If an upgrade goes wrong, you can roll back to the release you upgraded from, and no further.

These rules and the step order below come from the [version skew policy](version-skew-policy.md).

| Step | What changes | What running actors see | How long |
|---|---|---|---|
| 1 | podcertificate-controller | Nothing | A minute |
| 2 | atelet, one node at a time | Nothing | Seconds to about 6 minutes per node |
| 3 | workers | `SIGTERM`, then a 30-minute window to be suspended before `SIGKILL` | Seconds to hours per pool |
| 4 | ate-api-server and ate-idp-server, the CRDs and ate-controller, atenet, SandboxConfig, then the rest of the system bundle | API calls, long requests and outbound connections can be cut once. See step 4 | A few minutes |

Rollback is the same list in reverse, from the old release. An actor that crashes along the way goes back to its last snapshot with `kubectl ate revert actor <name> -a <atespace>`.

The system upgrade does not change the database engine, the CSI drivers `--setup-csi` installs, the Kubernetes version or the node OS version.

## Where am I?

Come back here after a break or a handoff. Each time a Deployment moves to a new image it gets a new ReplicaSet, and the atelet DaemonSet a new ControllerRevision, so their `AGE` tells what has moved. A component has moved when the `AGE` of its ReplicaSet with pods (`DESIRED` above 0), or of atelet's ControllerRevision with the highest `REVISION`, is shorter than the time since the upgrade began. Two ReplicaSets with pods mean a roll is under way. `podcertificate-controller` moves in step 1, `atelet` in step 2, the rest in step 4:

```bash
kubectl -n podcertificate-controller-system get rs
kubectl -n ate-system get rs,controllerrevision
```

The worker pools move in step 3, and their ReplicaSets read the same way. A pool is done rolling when `UP-TO-DATE` and `READY` equal `WANTED`:

```bash
kubectl get rs -A -l ate.dev/worker-pool
kubectl get deploy -A -l ate.dev/worker-pool -o custom-columns='NAMESPACE:.metadata.namespace,NAME:.metadata.name,UP-TO-DATE:.status.updatedReplicas,READY:.status.readyReplicas,WANTED:.spec.replicas,PAUSED:.spec.paused'
```

## Before you start

You need git, go and kubectl, and crane for prebuilt images. Agree on a time with actor owners: their running actors get `SIGTERM` in step 3.

Every `ate-setup deploy` run records the settings it was given in `installs/` under the record directory: `ATE_RECORD_DIR` if set, by default `~/.cache/ate-setup` on Linux and `~/Library/Caches/ate-setup` on macOS. It prints the path as `recorded this configuration in <path>`.

The record's `cluster.substrateVersion` is the installed release. If this machine has no record of the cluster, write one from the settings you installed with ([operator-install.md](operator-install.md) lists them), and set `OLD_RELEASE` to the installed release before you run the block below.

If `~/ate-upgrade` is left from an earlier upgrade, delete it first. Fill in the first three lines, then run the rest. `ate-setup` runs from the new checkout. The old checkout is kept for rollback.

```bash
NEW_RELEASE=v0.4.2                                   # change me
RECORD=~/.cache/ate-setup/installs/my-cluster.yaml   # change me: this cluster's record
kubectl config use-context my-cluster                # change me: keep it until the upgrade is done
# export ATE_API_POSTGRES_READ_WRITE_CONNECTION_STRING=...   # external database only

OLD_RELEASE=${OLD_RELEASE:-$(sed -n 's/^ *substrateVersion: //p' $RECORD)}
echo "Upgrading '$OLD_RELEASE' -> '$NEW_RELEASE'"    # stop if either is empty or not a release tag
mkdir ~/ate-upgrade && cd ~/ate-upgrade
cp $RECORD old.yaml && cp $RECORD new.yaml
sed -i.bak "s/^  tag: .*/  tag: $NEW_RELEASE/" new.yaml   # prebuilt images: the new tag
grep 'tag:' old.yaml new.yaml                        # prebuilt images: new.yaml has the new tag
git clone --branch $OLD_RELEASE https://github.com/agent-substrate/substrate.git old
git clone --branch $NEW_RELEASE https://github.com/agent-substrate/substrate.git new
(cd old && go install ./cmd/kubectl-ate)
cd new
```

Run every step from this shell. A record holds no database connection strings: with an external database, uncomment the `export` line, and export `ATE_API_POSTGRES_OWNER_CONNECTION_STRING` too if you set it.

Keep `old.yaml` as it is, for rollback. With prebuilt images, `new.yaml` now names the new tag. From source, the checkout you run from decides the release.

> [!CAUTION]
> Do not run `ate-setup setup csi` or `ate-setup delete` during the upgrade, or `ate-setup deploy ate-system` before step 4e. They change components out of upgrade order.

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
- **Stuck:** the `N out of M` count does not move for more than about 6 minutes. A new atelet pod that cannot start stops the roll and leaves its node without atelet; if you cannot fix it quickly, [roll back step 2](#roll-back-step-2). A pod stuck in ContainerCreating usually means podcertificate-controller is not running. If `deploy atelet` times out, the roll still goes on; keep watching.
- **Actors:** running actors keep running. A node's atelet is down for a few seconds while it restarts.

### Step 3. Move each WorkerPool to the new worker image

> [!IMPORTANT]
> Do not start step 3 until step 2 is done: new workers need the new atelet.

Change the WorkerPool, not its Deployment: `ate-controller` overwrites edits to the Deployment. If your WorkerPools live in Git or are applied by CI, change `workerImage` there as well, or the next sync puts the old image back and rolls every worker again.

Before you patch any pool, list the pools and save the list for rollback. Each pool needs the new image for its sandbox CLASS:

```bash
kubectl get workerpools -A -o custom-columns='NAMESPACE:.metadata.namespace,NAME:.metadata.name,CLASS:.spec.sandboxClasses[0].name,IMAGE:.spec.workerImage' | tee ~/ate-upgrade/before-workerpools.txt
```

If you build from source, build and push both worker images once. This touches nothing in the cluster and ends by printing `ateworker-gvisor: <ref>` and `ateworker-microvm: <ref>`:

```bash
go run ./cmd/ate-setup publish worker-images --config ~/ate-upgrade/new.yaml | tee ~/ate-upgrade/worker-images.txt
```

Then move the pools, starting with a small pool of each CLASS that you can afford to disrupt. For each pool:

1. Set the pool:

   ```bash
   NS=my-namespace          # change me
   WORKERPOOL=my-pool       # change me
   CLASS=$(kubectl -n $NS get workerpool $WORKERPOOL -o jsonpath='{.spec.sandboxClasses[0].name}')
   ```

2. Set its new image. From source, take the ref after `ateworker-$CLASS: ` in `~/ate-upgrade/worker-images.txt`:

   ```bash
   NEW_IMAGE=$(sed -n "s/^ateworker-$CLASS: //p" ~/ate-upgrade/worker-images.txt)
   ```

   With prebuilt images, take `ateworker-$CLASS` from your image repo at the new tag, pinned by digest:

   ```bash
   NEW_IMAGE=$(sed -n 's/^  repo: //p' ~/ate-upgrade/new.yaml)/ateworker-$CLASS:$NEW_RELEASE
   NEW_IMAGE=$NEW_IMAGE@$(crane digest $NEW_IMAGE)
   ```

3. Check that `echo "$NS/$WORKERPOOL $CLASS $NEW_IMAGE"` shows `ateworker-` plus the pool's CLASS and a full `@sha256:` digest. A bad image still removes the pool's first old worker.
4. Patch the worker pool and wait until ate-controller moves its Deployment to the new image:

   ```bash
   kubectl -n $NS patch workerpool $WORKERPOOL --type merge -p "{\"spec\":{\"workerImage\":\"$NEW_IMAGE\"}}"
   kubectl -n $NS wait deployment/$WORKERPOOL --for=jsonpath='{.spec.template.spec.containers[0].image}'="$NEW_IMAGE" --timeout=2m
   ```

5. Once the first pool of a CLASS has one new worker running, patch the other pools of that CLASS without waiting for the roll to finish. A new worker is running when the newest ReplicaSet of the pool shows READY 1 or more:

   ```bash
   kubectl -n $NS get rs -l ate.dev/worker-pool=$WORKERPOOL
   ```

   Then repeat on the other CLASS.

What to expect:

- **Progress:** `kubectl -n $NS rollout status deployment/$WORKERPOOL` prints how many new workers are updated and available. Ctrl-C stops only the watch.
- **Done:** Step 3 is done when every pool is done rolling with `kubectl get deploy -A -l ate.dev/worker-pool`. Go to Step 4 once done.
- **Pace:** a WorkerPool replaces 10% of its workers per batch, rounded down, at least one. With spare capacity, every batch has its `SIGTERM` within minutes and the old workers drain in parallel. Without it, each batch waits up to 30 minutes for old workers to exit.
- **Stuck:** rollout status fails with `exceeded its progress deadline` (the roll goes on), or a worker pod stays Terminating for more than 60 minutes, its grace period (a worker normally exits within about 30). Check `kubectl -n $NS get pods -l ate.dev/worker-pool=$WORKERPOOL`. A bad image stops the roll after the first batch: [roll back that pool](#roll-back-step-3).
- **Actors:** each gets `SIGTERM` and 30 minutes to be suspended. Substrate does not suspend them. An actor that is not suspended by then becomes `CRASHED` and loses its work since its last snapshot. [Eviction](api-guide.md#eviction) in the API guide says what an actor needs to keep its state.

### Step 4. Upgrade the Substrate control plane

#### 4a. ate-api-server and ate-idp-server

The first new API server pod migrates the database schema, and rolling back does not undo that. Back up the database first: with `pg_dump` for the bundled one, or an on-demand backup for Cloud SQL. With an external database, check that `ATE_API_POSTGRES_READ_WRITE_CONNECTION_STRING` is set in this shell: without it, `deploy apiserver` points the API server at the bundled database.

```bash
go run ./cmd/ate-setup deploy apiserver --config ~/ate-upgrade/new.yaml
kubectl -n ate-system rollout status deploy/ate-api-server
kubectl -n ate-system rollout status deploy/ate-idp-server
kubectl ate get actors -A
```

- **Done:** both print `successfully rolled out` and `kubectl ate get actors -A` lists your actors.
- **Stuck:** rollout status does not finish, or a new pod keeps restarting. [Roll back step 4](#roll-back-step-4).
- **API clients:** an API call can fail once with a retriable error while an API server pod restarts. Retry it.

#### 4b. CRDs and ate-controller

```bash
go run ./cmd/ate-setup deploy ate-controller --config ~/ate-upgrade/new.yaml
```

This applies the new CRDs and ate-controller's RBAC, then deploys the new `ate-controller`. Rerunning is safe. Within a major the worker pod template stays the same, so no pool rolls.

- **Done:** the command exits 0, `kubectl -n ate-system get pods -l app=ate-controller` shows one pod, Running, with RESTARTS 0, and a minute later `kubectl get pods -A -l ate.dev/worker-pool` shows no worker pod younger than this step.
- **Stuck:** the new ate-controller pod keeps restarting. [Roll back step 4](#roll-back-step-4).

#### 4c. atenet

```bash
go run ./cmd/ate-setup deploy atenet --config ~/ate-upgrade/new.yaml
kubectl -n ate-system rollout status deploy/atenet-router
kubectl -n ate-system rollout status deploy/atenet-egress
```

- **Done:** both print `successfully rolled out`.
- **Stuck:** rollout status does not finish, or a new pod keeps restarting. [Roll back step 4](#roll-back-step-4).
- **Actors:** a request to an actor, or an outbound connection, still open about 15 seconds after its `atenet` pod starts draining can be cut once.

#### 4d. SandboxConfig

```bash
go run ./cmd/ate-setup deploy sandboxconfig --config ~/ate-upgrade/new.yaml
```

This applies the SandboxConfig admission policy and the default SandboxConfig. It does not roll workers.

<!-- TODO: Today this step overwrites `gvisor-default` in place. Once each release ships its own SandboxConfig, it adds the new one, and WorkerPools without a `configRef` switch to it, so some suspended actors may cold boot. Link the sandbox upgrade guide. -->

- **Done:** the command exits 0.
- **Stuck:** the command fails. [Roll back step 4](#roll-back-step-4).

#### 4e. The rest of the system bundle

```bash
go run ./cmd/ate-setup deploy ate-system --config ~/ate-upgrade/new.yaml --setup-csi=none
```

This applies what the steps above do not, such as the ate-system NetworkPolicies and the bundled PostgreSQL. The components above are already on the new release and do not roll again. `--setup-csi=none` leaves the CSI drivers as they are. Rerunning is safe.

- **Done:** the command exits 0.
- **Stuck:** the command fails. [Roll back step 4](#roll-back-step-4).
- **API clients:** if the release changes the bundled PostgreSQL, it restarts once, and API calls fail until it is back.

Then install the new `kubectl ate` with `go install ./cmd/kubectl-ate`. Actor owners can now use API fields and commands new in this release, and you can set WorkerPool, SandboxConfig and CSIDriverConfig fields new in this release.

The upgrade is done when step 4 is done and [Where am I?](#where-am-i) shows every component and pool moved, every pool done rolling and none paused. Keep `~/ate-upgrade` while you might still roll back, then delete it.

## Something's wrong, or I need to roll back

Stopping between steps is safe, and so is stopping partway through step 3. A pool you have patched keeps rolling unless you pause it.

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

`deploy ate-controller` restores the old CRDs, RBAC and `ate-controller`. No pool rolls. Fields the new release added to WorkerPools, SandboxConfigs or CSIDriverConfigs are dropped from those objects. Remove them from your YAML files too, or `kubectl apply` rejects the files.

The database schema stays new; the old API server drops fields it does not know when it rewrites a record. Step 4e is not undone either: the NetworkPolicies and the bundled PostgreSQL stay as the new release left them.

#### Roll back step 3

Set each patched pool back to its image in `~/ate-upgrade/before-workerpools.txt`:

```bash
NS=my-namespace          # change me
WORKERPOOL=my-pool       # change me
OLD_IMAGE=$(awk -v ns=$NS -v name=$WORKERPOOL '$1 == ns && $2 == name {print $4}' ~/ate-upgrade/before-workerpools.txt)
kubectl -n $NS patch workerpool $WORKERPOOL --type merge -p "{\"spec\":{\"workerImage\":\"$OLD_IMAGE\"}}"
```

If you paused a WorkerPool, resume it now with `kubectl -n $NS rollout resume deployment/$WORKERPOOL`. Then watch:

```bash
kubectl -n $NS rollout status deployment/$WORKERPOOL
```

Only workers already on the new image are replaced, and their actors get `SIGTERM` again. Wait as in [step 3](#step-3-move-each-workerpool-to-the-new-worker-image).

#### Roll back step 2

> [!WARNING]
> Do not roll back step 2 before every pool is back on its old image and `kubectl get pods -A -l ate.dev/worker-pool` shows no Terminating pod. A new worker with an old atelet is not supported.

```bash
go run ./cmd/ate-setup deploy atelet --config ~/ate-upgrade/old.yaml --rollout-timeout 1h
```

#### Roll back step 1

```bash
go run ./cmd/ate-setup deploy podcertificate-controller --config ~/ate-upgrade/old.yaml
```
