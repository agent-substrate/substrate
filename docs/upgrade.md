# Upgrade runbook

This runbook upgrades a running Agent Substrate install to a newer release in the same release window, meaning the same `v0.x`: for example, from v0.2.0 to v0.2.1. Between windows, for example from v0.1.x to v0.2.x, reinstall instead.

An **actor** is an application, such as an agent, that Substrate runs, suspends and resumes. Each actor has a name within an **atespace**, and is usually driven by a program of yours, its **harness**. Actors run on **workers**, which are pods. A **WorkerPool** is a set of identical workers; ate-controller runs each one as a Deployment with the same name and namespace. **atelet** is the Substrate agent on every node, **ate-api-server** serves the Substrate API, and **atenet** routes traffic to and from actors. To **suspend** an actor is to save its state and free its worker.

| Step | What changes | What running actors see | How long |
|---|---|---|---|
| 1 | CRDs | Nothing | Seconds |
| 2 | ate-controller | Usually nothing; see step 2 | A minute, or as long as step 4 if it replaces workers |
| 3 | atelet, one node at a time | A suspend, pause or resume on a restarting node can fail | Seconds to about 6 minutes per node |
| 4 | Every worker | `SIGTERM`, then 30 minutes to be suspended | Minutes with free node room; hours per pool on full nodes |
| 5 | ate-api-server, then atenet | API calls and long HTTP requests are cut once | A few minutes |

ate-controller manages the WorkerPools, so it goes first. atelet and the workers go before ate-api-server and atenet, so that when the API server changes, every node already understands requests from either version. Undo is the same list in reverse, from the old release. An actor that crashes along the way goes back to its last snapshot with one revert call.

The upgrade does not change sandbox runtimes (it never touches a SandboxConfig), the database engine, Kubernetes or the node OS. The steps are the same on any Kubernetes cluster.

> [!NOTE]
> TODO: Version skew rules within a window, whether patch releases can be skipped (v0.2.0 straight to v0.2.2), the first release this runbook applies to, and release notes that say when a release changes the worker pods. Step 4 and undo assume that old and new workers in a window restore each other's snapshots; that is not yet in the release policy or tested.

## Check your progress

Run this after a break, or when something looks wrong. It compares the cluster with the files from [Save the current state](#save-the-current-state). Lines starting with `>` in the first command are components that changed since you started. The last matching row of the table is where you are.

```bash
kubectl -n ate-system get deploy,ds -o custom-columns='NAME:.metadata.name,IMAGE:.spec.template.spec.containers[*].image' | diff ~/ate-upgrade/before-images.txt -
kubectl -n ate-system rollout status ds/atelet --watch=false
kubectl get deploy -A -l ate.dev/worker-pool -o custom-columns='NAMESPACE:.metadata.namespace,POOL:.metadata.name,IMAGE:.spec.template.spec.containers[0].image,DESIRED:.spec.replicas,UPDATED:.status.updatedReplicas,AVAILABLE:.status.availableReplicas,DRAINING:.status.terminatingReplicas,ROLLOUT_PAUSED:.spec.paused'
```

| What you see | Where you are | Do next |
|---|---|---|
| No `>` lines | Before step 2 | Step 1. Steps 1 and 2 are safe to rerun. |
| `>` for `ate-controller` | Step 2 done, if the ate-controller pod is Running | Wait for pools that are rolling (UPDATED or AVAILABLE below DESIRED, or DRAINING above 0), as in step 2. Then step 3. |
| `>` for `atelet`, and `Waiting for daemon set "atelet" rollout to finish` | Step 3 running | Wait as in step 3. |
| `>` for `atelet`, and `daemon set "atelet" successfully rolled out` | Step 3 done, or step 4 running | Step 4 for pools whose IMAGE still matches `~/ate-upgrade/before-workerpools.txt`. Wait for pools that are rolling. |
| Every pool on its new IMAGE, with UPDATED and AVAILABLE equal to DESIRED, DRAINING 0 and ROLLOUT_PAUSED `<none>` | Step 4 done | Step 5. |
| `>` for `ate-api-server` | Step 5 half done | The actor check in step 5, then the atenet part. |
| `>` for `atenet-router` and `atenet-egress` too | Step 5 done | The end of step 5. |

In the pool list, UPDATED and AVAILABLE show `<none>` when they are 0. DRAINING `<none>` means the cluster does not report the count; see step 4.

## Before you start

### Set up the shell

The commands in this runbook are for bash. First read the installed release. Prebuilt images show it in the image tag, such as `...:v0.2.0@sha256:...`. Images built from source show only a digest; the second command then prints the version as its first word.

```bash
kubectl -n ate-system get deploy,ds -o custom-columns='NAME:.metadata.name,IMAGE:.spec.template.spec.containers[*].image'
kubectl -n ate-system exec deploy/ate-api-server -c ate-api-server -- /ko-app/ateapi --version
```

`kubectl ate --version` and `ate-setup --version` report your local copy, not the cluster.

Then get both releases and save a kubeconfig for this cluster only. Check that `kubectl config current-context` prints the cluster you are upgrading. Start without a `~/ate-upgrade` directory; rename one left from an earlier upgrade. Fill in the two tags and run the block as is. The old checkout is for undo, and its `kubectl ate` is the one to use until step 5 is done.

```bash
OLD_RELEASE=<installed release, for example v0.2.0>
NEW_RELEASE=<new release, for example v0.2.1>
mkdir ~/ate-upgrade && cd ~/ate-upgrade
git clone --branch $OLD_RELEASE https://github.com/agent-substrate/substrate.git old
git clone --branch $NEW_RELEASE https://github.com/agent-substrate/substrate.git new
(cd old && go install ./cmd/kubectl-ate)
(umask 077 && kubectl config view --minify --flatten > kubeconfig && touch settings.sh)
echo 'export KUBECONFIG=$HOME/ate-upgrade/kubeconfig' >> settings.sh
```

`kubectl`, `kubectl ate` and ate-setup all read `KUBECONFIG`, so every shell that sources `settings.sh` targets this cluster. Add an `export` line to `~/ate-upgrade/settings.sh` for each of these:

- **Images:** `KO_DOCKER_REPO` to build from source (needs push access), or `ATE_IMAGE_REPO` and `ATE_IMAGE_TAG` for prebuilt images: the new release's tag, matching the new checkout.
- **kind:** `ATE_INSTALL_KIND=true`, the same as `--kind`.
- **Wait time:** `ATE_INSTALL_ROLLOUT_TIMEOUT=1h`. The 60-second default is too short for step 3.

If the install read its settings from `.ate-dev-env.sh`, append that file to `settings.sh` (`cat <install checkout>/.ate-dev-env.sh >> ~/ate-upgrade/settings.sh`) and add `export NO_DEV_ENV=1`, so ate-setup reads only `settings.sh`. Now, and in every new shell:

```bash
cd ~/ate-upgrade/new
source ~/ate-upgrade/settings.sh
```

### Go/no-go checklist

- [ ] **Same release window.** `OLD_RELEASE` and `NEW_RELEASE` differ only in the last number.
- [ ] **One atelet DaemonSet, named `atelet`**, in `kubectl -n ate-system get ds -l app=atelet`. A name like `atelet-<suffix>` means an earlier release window: reinstall instead.
- [ ] **Pools are healthy.** READY equals DESIRED in `kubectl get workerpools -A`.
- [ ] **ate-controller runs.** `kubectl -n ate-system get pods -l app=ate-controller` shows one pod, Running, and RESTARTS does not rise over a minute. In step 4 it marks old workers as draining, so no new actors land on them.
- [ ] **podcertificate-controller runs.** `kubectl -n podcertificate-controller-system get deploy podcertificate-controller` shows AVAILABLE 1. Every new pod gets its certificate from it.
- [ ] **Idle workers.** Each pool has at least 10% of its workers idle, and at least one. In `kubectl ate get workers`, idle workers are the rows whose ACTORS column starts with `0/`. If a pool has fewer, raise its replicas first. Otherwise resumes fail with `no free workers available` while step 4 replaces workers.
- [ ] **Actor owners are ready.** Every actor owner has confirmed that their actors do [what actors must do](#what-actors-must-do). The operator and the actor owners are often different people; tell them before step 2. `kubectl ate get actor-templates -A` lists the templates whose owners to ask.
- [ ] **No actor is PAUSED.** Follow [Suspend paused actors](#suspend-paused-actors). Ask actor owners not to pause actors until you tell them the upgrade is done.
- [ ] **The database is backed up.** Step 5 moves the database schema forward, and undo does not move it back.
- [ ] **Tools.** `kubectl`, `go` and `git`. For prebuilt images, also `crane` or `gcloud`.

### Suspend paused actors

A paused actor's state is on one node's disk, and it can resume only on that node. Step 4 can leave no worker of its pool there, or an autoscaler can remove the node. List the paused actors:

```bash
kubectl ate get actors -A | awk '$4=="ACTOR_STATE_PAUSED" {print $1, $2}'
```

For each line, set the actor and find its node, the name in quotes under `nodeVmsWithLocalSnapshots`:

```bash
ATESPACE=<atespace from the line>
ACTOR=<name from the line>
kubectl ate get actors $ACTOR -a $ATESPACE -o json | grep -A2 nodeVmsWithLocalSnapshots
```

Check that the node and its atelet still exist, then suspend the actor:

```bash
NODE=<node name>
kubectl get node $NODE
kubectl -n ate-system get pods -l app=atelet -o wide --field-selector spec.nodeName=$NODE
kubectl ate suspend actor $ACTOR -a $ATESPACE
```

> [!WARNING]
> Do not suspend a paused actor whose node is gone. It gets stuck in SUSPENDING, which revert cannot undo. Revert it instead with `kubectl ate revert actor $ACTOR -a $ATESPACE`; it returns to its last snapshot.

If suspend fails with `paused with a Data snapshot; the template commits Full`, run `kubectl ate resume actor $ACTOR -a $ATESPACE`, then suspend it.

### Record the install settings

ate-setup does not store the install's settings. A command run without them silently goes back to the defaults, for example a different atenet proxy or database. This saves them to a file, because step 2 resets setting 5:

```bash
(umask 077 && {
echo "1 proxy:       $(kubectl -n ate-system get deploy atenet-router -o jsonpath='{.spec.template.spec.containers[*].name}')"
echo "2 sdsmint:     $(kubectl -n ate-system get deploy atenet-egress -o jsonpath='{.spec.template.spec.initContainers[*].name} {.spec.template.spec.volumes[*].name}' | grep -oE 'sdsmint|egress-mitm')"
echo "3 credentials: $(kubectl -n ate-system get deploy atenet-egress -o jsonpath='{range .spec.template.spec.containers[?(@.name=="ext-proc")].args[*]}{@}{"\n"}{end}' | grep -- --credential-provider-)"
echo "4 ext_proc:    $(kubectl -n ate-system get configmap atenet-egress -o jsonpath='{.data.envoy\.yaml}' | grep -A45 -- '- name: additional_egress_ext_proc$' | grep -E 'address: [^ ]+\.svc\.cluster\.local|port_value')"
echo "5 otlp:        $(kubectl -n ate-system get configmap ate-otel-config -o jsonpath='{.data.OTEL_EXPORTER_OTLP_ENDPOINT}')"
echo "6 database:    $(kubectl -n ate-system get secret ate-api-server-secret-envvars -o jsonpath='{.data.ATE_API_POSTGRES_CONNECTION_STRING}' | base64 --decode)"
echo "7 schema:      $(kubectl -n ate-system get secret ate-api-server-secret-envvars -o jsonpath='{.data.ATE_API_POSTGRES_SCHEMA}' | base64 --decode)"
echo "8 cloud sql:   $(kubectl -n ate-system get configmap ate-api-server-envvars -o jsonpath='{.data.ATE_API_POSTGRES_CLOUDSQL_INSTANCE}')"
} | tee ~/ate-upgrade/settings-check.txt)
```

For each setting that differs from the default, add the line to `settings.sh`, then source it again. If an earlier deploy already dropped a setting, go by your own records.

| Setting | Default | If yours differs, add to `settings.sh` |
|---|---|---|
| 1 | `atenet-router envoy` | `export ATE_ATENET_DATAPLANE=agentgateway` |
| 2 | empty | `export ATE_EXPERIMENTAL_USE_SDSMINT=true` |
| 3 | empty | `export ATE_CREDENTIAL_INJECTION_ENABLED=true`, plus `ATE_CREDENTIAL_PROVIDER_NAME` and `ATE_CREDENTIAL_PROVIDER_ADDRESS` with the printed values |
| 4 | empty | `export ATE_ADDITIONAL_EGRESS_EXTPROC_SERVICE=<namespace>/<service>:<port>`, from the printed `<service>.<namespace>.svc.cluster.local` and port |
| 5 | the value in `manifests/ate-install/ate-otel-config.yaml` (kind: `manifests/ate-install/kind/ate-otel-config.yaml`) | Nothing. See [A custom OTLP endpoint](#a-custom-otlp-endpoint). |
| 6 | `postgresql://postgres@postgres.ate-system.svc:5432/atepg?...`, ending in `credential-bundle.pem` | `export ATE_API_POSTGRES_CONNECTION_STRING='<value>'`, unless setting 8 is set. Anything after `credential-bundle.pem`, such as `&pool_max_conns=`, counts as a difference. |
| 7 | `public` | `export ATE_API_POSTGRES_SCHEMA=<value>` |
| 8 | empty | Nothing: ate-setup keeps Cloud SQL by itself. Do not export `ATE_API_POSTGRES_CLOUDSQL_INSTANCE`; an empty value removes Cloud SQL. |

### A custom OTLP endpoint

Skip this if setting 5 is the default, as on default GKE and kind installs.

`ate-setup deploy ate-controller` always resets the endpoint to the default. Every later ate-setup command that sees `ATE_OTLP_ENDPOINT` sets it back and restarts ate-controller, ate-api-server, atenet-router and atelet. So:

- Do not set `ATE_OTLP_ENDPOINT` during the upgrade or an undo: remove it from `settings.sh` if it came with `.ate-dev-env.sh`, and `unset` it in your shell.
- Step 2 replaces every worker once, on its old image, as described in step 2. From then on, components send telemetry to the default collector.
- At the end of step 5, you restore the endpoint once. That restarts the four components and replaces every worker again.

> [!NOTE]
> TODO: Drop this section once ate-setup keeps a custom endpoint.

### Save the current state

Run this once, before step 1. Undo and [Check your progress](#check-your-progress) use these files. The last file lists actors that are CRASHED before you start.

```bash
kubectl -n ate-system get deploy,ds -o custom-columns='NAME:.metadata.name,IMAGE:.spec.template.spec.containers[*].image' > ~/ate-upgrade/before-images.txt
kubectl get workerpools -A -o custom-columns='NAMESPACE:.metadata.namespace,NAME:.metadata.name,IMAGE:.spec.workerImage' > ~/ate-upgrade/before-workerpools.txt
kubectl get deploy -A -l ate.dev/worker-pool -o custom-columns='NAMESPACE:.metadata.namespace,POOL:.metadata.name,REVISION:.metadata.annotations.deployment\.kubernetes\.io/revision' > ~/ate-upgrade/before-revisions.txt
kubectl ate get actors -A | awk '$4=="ACTOR_STATE_CRASHED" {print $1, $2}' > ~/ate-upgrade/before-crashed.txt
```

> [!WARNING]
> Do not run `ate-setup deploy ate-system`, `deploy demo ...`, `deploy benchmarks`, `delete ate-system` or `delete all` during the upgrade, nor `hack/install-ate.sh` with the matching flags. They update components out of order, move pools to the checkout's worker image, or delete every WorkerPool.

## Your environment

A node autoscaler is optional. With one, a replacement worker in step 4 that does not fit waits a few minutes for a new node. Without one, on full nodes, it waits up to 30 minutes for an old worker to exit. A kind cluster usually has one node, so step 3 affects every actor at once.

## What actors must do

When a worker is replaced in step 4 (or in step 2, see its warning):

1. Substrate marks the worker `WORKER_STATE_DRAINING`, so no new actor is placed on it, and PID 1 of each actor container on it gets `SIGTERM`.
2. The actor has 30 minutes to be suspended, usually by its harness.
3. A suspended actor resumes the next time it is called, on any worker with room, old or new.
4. An actor still running after 30 minutes gets `SIGKILL` and becomes CRASHED, losing its work since its last snapshot. One revert call (`RevertActor`, or [`kubectl ate revert actor`](#recover-crashed-actors)) returns it to SUSPENDED at that snapshot, and it resumes as usual. Harnesses can call `RevertActor` themselves.

Each actor owner must make sure that:

- PID 1 of every actor container catches, ignores or blocks `SIGTERM`, and keeps running until the suspend finishes. Otherwise PID 1 dies within seconds and the actor crashes without the 30 minutes. Exiting cleanly is not enough: only a finished suspend saves state. A stock Python, Node or Go program, or an `sh -c` entrypoint, does not handle `SIGTERM`; an entrypoint script must trap it, or `exec` an app that does.
- The harness suspends the actor within 30 minutes of `SIGTERM`. A harness without Kubernetes access can use `SIGTERM` inside the actor as its signal, or the API: GetActor names the worker, and GetWorker shows `WORKER_STATE_DRAINING`.

## Upgrade

### Step 1. Apply the new CRDs

```bash
kubectl apply --server-side --field-manager=ate-setup --force-conflicts -f manifests/ate-install/generated
```

This updates the three CRDs and ate-controller's RBAC. The flags make kubectl apply the files the way ate-setup does, so step 2 does not conflict with this change.

- **Done:** five lines ending in `serverside-applied`.
- **Actors:** nothing changes. The step is safe to rerun. Go on to step 2 right away.

### Step 2. Upgrade ate-controller

> [!WARNING]
> Do not start step 2 before the checklist's actor-owner and PAUSED items are done. If this release changes the worker pod template, or you have [a custom OTLP endpoint](#a-custom-otlp-endpoint), step 2 replaces every worker on its old image, like step 4.

```bash
go run ./cmd/ate-setup deploy ate-controller
```

It applies the CRDs again, deploys the new ate-controller and waits for it.

- **Done:** the command exits 0, and `kubectl -n ate-system get pods -l app=ate-controller` shows one pod, Running, with RESTARTS 0. Ready only means the container started, so also run `kubectl -n ate-system logs deploy/ate-controller | grep -E 'atecontroller starting|Starting workers|Reconciler error'`: it should show the first two and not the third.
- **Stuck:** the new pod is Pending or in ImagePullBackOff: the old controller still runs. The new pod is in CrashLoopBackOff or its RESTARTS keeps rising: the old controller is already gone, and nothing manages the WorkerPools. Either way, do not go on; [undo step 2](#undo).

A minute after the new pod starts, check whether it is replacing workers:

```bash
kubectl get deploy -A -l ate.dev/worker-pool -o custom-columns='NAMESPACE:.metadata.namespace,POOL:.metadata.name,REVISION:.metadata.annotations.deployment\.kubernetes\.io/revision' | diff ~/ate-upgrade/before-revisions.txt -
```

No output means no pool is rolling. For each pool with a changed REVISION, run `kubectl -n <namespace> rollout status deployment/<pool>`, then wait until DRAINING is 0 in [Check your progress](#check-your-progress). This can take hours; step 4 says what is normal. Then step 3.

### Step 3. Upgrade atelet

> [!WARNING]
> Do not run step 3 while many actors are being suspended, paused or resumed, including automatic resumes when requests arrive, and do not create ActorTemplates during it. While a node's atelet restarts, those calls on that node can fail and leave the actor CRASHED, or stuck in SUSPENDING, PAUSING or RESUMING.

> [!NOTE]
> TODO: Shrink this warning if the API server retries these calls instead of crashing the actor.

```bash
go run ./cmd/ate-setup deploy atelet
kubectl -n ate-system rollout status ds/atelet
```

If ate-setup fails with `waiting for daemonset/atelet in ate-system: ...`, only its wait timed out, and the second command keeps watching. If it fails with any other error, it did not change atelet: fix the error and run step 3 again.

- **Progress:** `Waiting for daemon set "atelet" rollout to finish: 2 out of 5 new pods have been updated...`, then `... 4 of 5 updated pods are available...`.
- **Done:** `daemon set "atelet" successfully rolled out`, and [Check your progress](#check-your-progress) shows `>` for `atelet`.
- **Stuck:** the `N out of M` count does not move for more than about 6 minutes. Run `kubectl -n ate-system get pods -l app=atelet -o wide`. A pod that is not Running leaves its node without atelet. If it is in ImagePullBackOff or CrashLoopBackOff, [undo step 3](#undo). If it is stuck in ContainerCreating, check podcertificate-controller.
- **Actors:** running actors keep running and keep serving HTTP. A resume that reaches a restarting node can return HTTP 500 `error resuming actor <atespace>/<name>`; a retry usually works.

When it is done, [recover CRASHED actors and actors stuck mid-call](#recover-crashed-actors).

### Step 4. Move each WorkerPool to the new worker image

> [!WARNING]
> Do not start step 4 until step 3 is done: new workers need the new atelet. Do not start it while any actor is PAUSED. This must print nothing:
>
> ```bash
> kubectl ate get actors -A | awk '$4=="ACTOR_STATE_PAUSED" {print $1, $2}'
> ```
>
> Do not edit the pool's Deployment with `kubectl set image`, `kubectl rollout undo` or `kubectl edit`: ate-controller puts it back. Do not `kubectl apply -f` a whole WorkerPool: that also resets `spec.replicas` on a pool an autoscaler sizes. If a GitOps tool syncs your WorkerPools, turn off its automatic sync for them first, or it moves the pools back to the old image.

List the pools. Each needs the new image for its sandbox CLASS:

```bash
kubectl get workerpools -A -o custom-columns='NAMESPACE:.metadata.namespace,NAME:.metadata.name,CLASS:.spec.sandboxClass,IMAGE:.spec.workerImage'
```

If you build from source, build and push both worker images once. This touches nothing in the cluster and ends by printing `ateom-gvisor: <ref>` and `ateom-microvm: <ref>`:

```bash
go run ./cmd/ate-setup publish worker-images | tee ~/ate-upgrade/worker-images.txt
```

> [!NOTE]
> TODO: Document where release images are published and their tag format. Open source releases do not publish images yet.

Then move the pools one at a time. For each pool:

1. Set the pool:

   ```bash
   NS=<pool namespace>
   POOL=<pool name>
   CLASS=$(kubectl -n $NS get workerpool $POOL -o jsonpath='{.spec.sandboxClass}')
   ```

2. Set its new image. From source, copy the ref after `ateom-$CLASS: ` in `~/ate-upgrade/worker-images.txt`:

   ```bash
   NEW_IMAGE=<ref>
   ```

   With prebuilt images, look it up pinned by digest. Without crane, get the digest with `gcloud artifacts docker images describe <image> --format='value(image_summary.digest)'`.

   ```bash
   NEW_IMAGE=$ATE_IMAGE_REPO/ateom-$CLASS:$ATE_IMAGE_TAG@$(crane digest $ATE_IMAGE_REPO/ateom-$CLASS:$ATE_IMAGE_TAG)
   ```

3. Check that `echo "$NS/$POOL $CLASS $NEW_IMAGE"` shows `ateom-` plus the pool's CLASS and a full `@sha256:` digest. A bad image still removes the pool's first old worker.
4. Patch the pool, wait until ate-controller moves its Deployment to the new image, and watch the roll:

   ```bash
   kubectl -n $NS patch workerpool $POOL --type merge -p "{\"spec\":{\"workerImage\":\"$NEW_IMAGE\"}}"
   kubectl -n $NS wait deployment/$POOL --for=jsonpath='{.spec.template.spec.containers[0].image}'="$NEW_IMAGE" --timeout=2m
   kubectl -n $NS rollout status deployment/$POOL
   ```

5. Go to the next pool once rollout status prints `successfully rolled out`. The times of pools done one after another add up. Pools patched together roll in parallel and interrupt more actors at once.

What to expect:

- **Progress:** rollout status prints, in order:

  ```
  Waiting for deployment "<pool>" rollout to finish: 2 out of 10 new replicas have been updated...
  Waiting for deployment "<pool>" rollout to finish: 1 old replicas are pending termination...
  Waiting for deployment "<pool>" rollout to finish: 9 of 10 updated replicas are available...
  deployment "<pool>" successfully rolled out
  ```

  "Updated" includes workers still starting. Ctrl-C stops only the watch, not the roll.
- **Done:** `successfully rolled out` does not count old workers that are still draining, and their actors may still be inside their 30 minutes. Step 4 is done when every pool shows its new IMAGE and DRAINING 0 in the pool list of [Check your progress](#check-your-progress). DRAINING `<none>` means your cluster does not report the count (Kubernetes 1.35 and later do); then wait until `kubectl ate get workers` shows no `WORKER_STATE_DRAINING`.
- **Pace:** how many actors are interrupted at once depends on free node room, not on the batch size. A batch is 10% of the pool's workers, rounded down and at least one, so pools under 20 workers replace one worker at a time. At most one batch is not ready at a time. A 1-worker pool has no ready worker while its worker is replaced.
  - If the nodes have room, replacements start at once, and most old workers in the pool get `SIGTERM` within minutes.
  - If the nodes are full, each replacement waits for an old worker to exit: up to 30 minutes plus a pod start per batch. That is up to 30 minutes per worker for pools under 20 workers, and 5 to 8 hours for larger pools. A batch ends early once its actors are suspended, and idle workers exit at once.
- **Stuck, or normal?**
  - On full nodes, new pods Pending next to old pods Terminating, with rollout status on one line for up to about 30 minutes, are normal. With an autoscaler, pods Pending for a few minutes while a node is added are normal.
  - DESIRED minus READY in `kubectl get workerpools -A` stays above one batch for more than a few minutes, or rollout status fails with `error: deployment "<pool>" exceeded its progress deadline` (80 minutes without progress; the roll goes on). Look for Pending pods (no capacity) or ImagePullBackOff (bad image) with `kubectl -n $NS get pods -l ate.dev/worker-pool=$POOL` and fix the cause. Until the roll moves again, rollout status repeats the error at once.
  - A bad image stops the roll after the first batch. [Undo that pool](#undo).
  - DRAINING above 0 for more than 60 minutes, the longest a worker pod takes to exit, means a pod is stuck Terminating, usually on an unreachable node. Find it with `kubectl -n $NS get pods -l ate.dev/worker-pool=$POOL`.
- **Actors:** each goes through [what actors must do](#what-actors-must-do). A resume in flight on a worker when its batch starts can crash that actor at once. The actor had not started running there, so a revert loses nothing. The metric `ate.actor.crashes` with `ate.actor.operation.name` `unknown` counts actors that were not suspended in time.

When every pool is done, put the new `workerImage` in any files you apply WorkerPools from, turn GitOps sync back on, and [recover CRASHED actors](#recover-crashed-actors).

### Step 5. Upgrade ate-api-server, then atenet

> [!WARNING]
> Do not start step 5 until [Check your progress](#check-your-progress) shows every pool on its new IMAGE, with UPDATED and AVAILABLE equal to DESIRED, DRAINING 0 and ROLLOUT_PAUSED `<none>`. Do not use `deploy ate-system` here. `deploy apiserver` rewrites the API server's database settings from your shell every time, so it runs after a fresh `source` below.

First the API server. The last command lists your actors through it:

```bash
cd ~/ate-upgrade/new
source ~/ate-upgrade/settings.sh
go run ./cmd/ate-setup deploy apiserver
kubectl -n ate-system rollout status deploy/ate-api-server
kubectl ate get actors -A
```

- **Done:** rollout status prints `deployment "ate-api-server" successfully rolled out`, and the list shows your actors.
- **Stuck:** an empty list means the API server points at the wrong database: add settings 6 and 7 to `settings.sh` and run the block again. A new pod that never becomes ready makes `deploy apiserver` fail after about 10 minutes with `exceeded its progress deadline`, while the old pods keep serving. Check `kubectl -n ate-system get pods -l app=ate-api-server` and the new pod's logs: a database error has the same fix as an empty list, and ImagePullBackOff means wrong image settings. For anything else, [undo step 5](#undo).

Then atenet:

```bash
go run ./cmd/ate-setup deploy atenet
kubectl -n ate-system rollout status deploy/atenet-router
kubectl -n ate-system rollout status deploy/atenet-egress
```

- **Done:** both print `successfully rolled out`, and [Check your progress](#check-your-progress) shows `>` for `atenet-router` and `atenet-egress`.
- **Stuck:** a new pod stays Pending, in ImagePullBackOff or CrashLoopBackOff; the old pod keeps serving. Check `kubectl -n ate-system get pods` and [undo step 5](#undo) if you cannot fix it.
- **API clients:** a suspend or resume running when its API server pod stops can be cut off and left mid-call, so harnesses should retry API errors during this step. If a `kubectl ate` command fails once, run it again. A `kubectl port-forward` you opened yourself breaks when its pod goes.
- **Actors:** when the router restarts, an HTTP request that takes longer than about 10 seconds can be cut once, and new connections fail for a few seconds. Outbound connections through the egress gateway are reset once.

Then finish:

1. Install the new `kubectl ate` with `go install ./cmd/kubectl-ate`. Actor owners can now use API fields and commands new in this release.
2. With [a custom OTLP endpoint](#a-custom-otlp-endpoint), restore it now. Run [Suspend paused actors](#suspend-paused-actors) again, add `export ATE_OTLP_ENDPOINT=<setting 5 in ~/ate-upgrade/settings-check.txt>` to `settings.sh`, source it, and run `go run ./cmd/ate-setup deploy atenet`. Then run `kubectl -n ate-system rollout status ds/atelet`, and wait for every pool as in step 4, until DRAINING is 0. The step 3 warning applies. If you undo later, remove the line from `settings.sh` first.
3. [Recover CRASHED actors](#recover-crashed-actors).

The upgrade is done when [Check your progress](#check-your-progress) shows step 5 done and no new actor stays CRASHED. Tell actor owners they can pause actors again. Keep `~/ate-upgrade` while you might still undo, then delete it: `settings.sh`, `settings-check.txt` and `kubeconfig` can hold passwords and credentials.

> [!NOTE]
> TODO: podcertificate-controller is not part of the five steps. Say how to upgrade it when a release changes it.

## Something's wrong, or I need to undo

First run [Check your progress](#check-your-progress) to see which step you reached. Stopping between steps is safe.

### Symptoms

| You see | Meaning | Do |
|---|---|---|
| `waiting for daemonset/atelet in ate-system: ... (last status: 3/9 nodes updated)` | Only ate-setup's wait timed out. | `kubectl -n ate-system rollout status ds/atelet` |
| An atelet pod in ImagePullBackOff or CrashLoopBackOff | That node has no atelet, and step 3 stopped there. | [Undo step 3](#undo). |
| ate-controller pod restarting after step 2 | Nothing manages the WorkerPools. | [Undo step 2](#undo). |
| `exceeded its progress deadline` for a pool, or worker pods Pending or in ImagePullBackOff | Step 4 is slow or blocked. | See **Stuck, or normal?** in [step 4](#step-4-move-each-workerpool-to-the-new-worker-image). |
| HTTP 503 whose body mentions `ACTOR_STATE_CRASHED` | The actor crashed. | [Revert it](#recover-crashed-actors). |
| HTTP 503 `no free workers available` | During step 4 the pool is short by up to one batch. | Wait, or add capacity. If the actor is PAUSED, see [Suspend paused actors](#suspend-paused-actors), then resume it. |
| HTTP 503 `another operation is in progress for this actor` | A suspend is running. | Wait. |
| HTTP 503 `actor <atespace>/<name> unavailable`, with no detail | ate-api-server restarting (step 5). | Retry. |
| HTTP 500 `error resuming actor <atespace>/<name>` | atelet restarting (step 3). | Retry. |
| InvalidArgument or Unimplemented from a `kubectl ate` command or API call | It is new in this release, and the old API server still serves. | Finish step 5 first. |
| After step 5, the atenet proxy changed or credentials are no longer injected | Settings 1 to 4 were missing from the shell. | Add them to `settings.sh`, source it, run `deploy atenet` again. |

### Recover CRASHED actors

A CRASHED actor needs one revert. It returns the actor to SUSPENDED at its last snapshot, and the next call resumes it. Work since that snapshot is lost, and external volumes are not rewound.

This lists actors that became CRASHED since you started, each as `> <atespace> <name>`. An actor can show CRASHED for a moment while its suspend finishes, so run it again after a minute, and revert only actors that stay on the list.

```bash
kubectl ate get actors -A | awk '$4=="ACTOR_STATE_CRASHED" {print $1, $2}' | diff ~/ate-upgrade/before-crashed.txt -
```

For each one:

```bash
ATESPACE=<atespace>
ACTOR=<name>
kubectl ate revert actor $ACTOR -a $ATESPACE
```

A revert while a suspend is still finishing fails with `code = Aborted desc = another operation is in progress for this actor` and changes nothing; the actor turns SUSPENDED by itself.

After step 3, also list actors that a failed call left halfway, each as `<atespace> <name> <state>`. Actors in the middle of a normal call show up too, so run it again after a few minutes:

```bash
kubectl ate get actors -A | awk '$4 ~ /^ACTOR_STATE_(SUSPENDING|PAUSING|RESUMING)$/ {print $1, $2, $4}'
```

For each actor still listed, run its call again: `kubectl ate suspend actor` for SUSPENDING, `pause actor` for PAUSING (then suspend it, because step 4 must not start with a paused actor), or `resume actor` for RESUMING, each with `$ACTOR -a $ATESPACE`.

### Stop a worker roll partway

To stop step 4 between pools, stop patching; unpatched pools do not roll. To stop a pool that is rolling, pause its Deployment. This is the one Deployment change ate-controller leaves in place.

```bash
kubectl -n $NS rollout pause deployment/$POOL
```

No more old workers are removed. Workers already removed keep shutting down, and their actors still get the rest of their 30 minutes. The pool's size is not frozen: a change to its replicas, by you or an autoscaler, still adds or removes workers. While the pool is paused, rollout status never finishes; press Ctrl-C. To continue, run `kubectl -n $NS rollout resume deployment/$POOL` and watch as in step 4.

> [!WARNING]
> Do not leave a pool paused, and do not start step 5 while one is. A paused pool ignores every later change to its workers, including the next upgrade. [Check your progress](#check-your-progress) shows it as ROLLOUT_PAUSED `true`.

### Undo

To go back to the old release, undo every step you began, finished or not, starting with the last. To fix one pool, undo only that pool. Undo runs from the old checkout. Run this block now, and in every new shell during undo; the last line is for prebuilt images only:

```bash
cd ~/ate-upgrade/old
source ~/ate-upgrade/settings.sh
export ATE_IMAGE_TAG=<old release's image tag>
```

Do not edit `settings.sh` for undo: going forward again needs the new tag.

> [!WARNING]
> Do not undo step 3 before every pool is back on its old image, with DRAINING 0. A new worker with an old atelet is not supported.

- **Step 5:** tell actor owners to stop using API fields new in this release, because the old API server rejects them. Run `go run ./cmd/ate-setup deploy atenet`, then `go run ./cmd/ate-setup deploy apiserver`, then reinstall the old `kubectl ate` with `go install ./cmd/kubectl-ate`. The database schema stays new; the old API server drops fields it does not know when it rewrites a record.
- **Step 4:** run [Suspend paused actors](#suspend-paused-actors) again. Then set each patched pool back to its IMAGE in `~/ate-upgrade/before-workerpools.txt`, with the exact string; another reference to the same image counts as a new image and replaces every worker.

  ```bash
  NS=<pool namespace>
  POOL=<pool name>
  OLD_IMAGE=<IMAGE for this pool in before-workerpools.txt>
  kubectl -n $NS patch workerpool $POOL --type merge -p "{\"spec\":{\"workerImage\":\"$OLD_IMAGE\"}}"
  ```

  If you paused the pool, resume it now with `kubectl -n $NS rollout resume deployment/$POOL`. Then watch:

  ```bash
  kubectl -n $NS rollout status deployment/$POOL
  ```

  Partway through a roll, only workers already on the new image are replaced. After a finished roll, every worker is replaced again, with another round of `SIGTERM`. Wait as in step 4, until DRAINING is 0. If an actor turns CRASHED again right after each resume, the old workers cannot restore the snapshot a new worker wrote, and a revert does not help: move that pool forward to the new image again. If you put the new image in your WorkerPool files, put the old one back before turning GitOps sync on.
- **Step 3:** once every pool is back, run `go run ./cmd/ate-setup deploy atelet`. The step 3 warning applies.
- **Step 1 or 2:** run `go run ./cmd/ate-setup deploy ate-controller`. It restores the old CRDs, RBAC and controller. If the release changed the worker pod template, every pool rolls again: run [Suspend paused actors](#suspend-paused-actors) first, and wait as in step 4. Fields the new release added to WorkerPools, SandboxConfigs or CSIDriverConfigs disappear; remove them from your manifests first, or `kubectl apply` fails. A value only the new release accepts, such as a new `sandboxClass`, stays but is not understood; change it first. If the new release added an API version to a CRD, you cannot undo past step 1; reinstall.

> [!NOTE]
> TODO: The upgrade end-to-end test does not cover undo.
