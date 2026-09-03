# Rolling upgrade runbook

This runbook provides step by step guidance to upgrade a running
Agent Substrate install to a new build version. The roll moves one
node at a time, so no actor loses state and at most one node's worth
of capacity is out of service while the rest of the fleet keeps
serving. It needs `kubectl`, `kubectl ate`, `go run ./cmd/ate-setup`,
`jq`, and `grpcurl`; nothing in it is specific to one Kubernetes
provider. On GKE it also needs `gcloud` for the two node pool steps,
1 and 8, which other providers have their own equivalents of. All of
its state lives in cluster objects, so you can stop, look around, and
pick up again at any point.

The order is `ate-controller` first, then the dataplane, then the rest
of the control plane. The controller goes first because it manages
the worker pools the roll creates. The dataplane goes before the rest
of the control plane so that, by the time ate-api-server and atenet
change, every atelet and worker already understands requests from
either version of the control plane.

## What this runbook assumes

The cluster was installed from a versioned build: nodes carry the
`ate.dev/substrate-version` label, the atelet DaemonSet name carries
a version suffix, and the installed ate-api-server serves
`DrainWorker`. A cluster installed from an older build needs a fresh
install instead, because its DaemonSet selector cannot be changed in
place.

Actor snapshots are readable by both the old and the new build. An
actor can therefore suspend on one version and resume on the other in
either direction, which is what lets the two versions serve side by
side during the roll and lets a rollback pick up actors that already
ran on the new version.

Sandboxes and templates are outside the roll. `SandboxConfig` objects
are yours, and the roll does not change them; a release that changes
the default sandbox it installs says so in its notes, because a
snapshot restores in full only on the sandbox that wrote it.
ActorTemplates are immutable and every actor keeps its own.

## Ground rules

Three things break an upgrade.

1. **Flip the node's version label before deleting its old worker
   pods.** Otherwise the old pool reschedules replacements onto the
   same node, and old workers end up next to the new atelet: exactly
   the version skew the roll exists to prevent.
2. **Do not edit a serving worker pool.** The controller would roll
   the pool's Deployment straight through live actors.
3. **(If on GKE) Do not touch the node pool's label until every node
   is rolled.** A pool label update applies in place to every node in
   the pool, so the whole fleet flips at once, with no drain and no
   pacing.

## Preflight

The roll uses these names throughout. Collect them up front:

| name | what it is | how to get it |
|---|---|---|
| `$CLUSTER`, `$ZONE` | the GKE cluster and its location | `gcloud container clusters list` |
| `$NODEPOOL` | the GKE node pool | `gcloud container node-pools list --cluster $CLUSTER --zone $ZONE` |
| `$OLD_VERSION` | the installed version label value | `kubectl get ds -n ate-system -l app=atelet -L ate.dev/substrate-version` prints one DaemonSet before the upgrade; its label value is `$OLD_VERSION` |
| `$NEW_VERSION` | the new version label value | read off the cluster in step 4. |
| `$NS`, `$OLD_WORKERPOOL` | namespace and name of a serving WorkerPool | `kubectl get workerpools -A` |
| `$NEW_WORKERPOOL` | the clone's name | you pick it in step 5, for example `counter-v2` |
| `$NODE` | the node being rolled | picked per iteration in step 6 from `kubectl get nodes` |

A cluster usually serves more than one WorkerPool, and every serving
pool moves in the same upgrade. Step 5 clones each of them, the
per-node roll in step 6 covers all pools on a node together, and the
retire at the end deletes each old pool. Where the runbook says
`$OLD_WORKERPOOL`, read "each serving pool".

```bash
# Every node carries the same version label; that value is $OLD_VERSION.
# At least two nodes; on a single node the roll is a full stop.
kubectl get nodes -L ate.dev/substrate-version

# Every serving pool carries the version pin at $OLD_VERSION (see the
# WorkerPool section of docs/api-guide.md); an unpinned pool cannot
# take part in the roll.
kubectl get workerpools -A \
  -o custom-columns='NAMESPACE:.metadata.namespace,NAME:.metadata.name,PIN:.spec.template.nodeSelector.ate\.dev/substrate-version'

# The old pool is healthy (READY == DESIRED).
kubectl -n $NS get workerpool $OLD_WORKERPOOL

# The control plane answers.
kubectl ate get workers
```

A pool with an empty `PIN` column has to be pinned to `$OLD_VERSION`
first, as [the WorkerPool section of the API
guide](api-guide.md#pin-pools-to-the-installed-substrate-version-templatenodeselector)
describes. That edit re-renders the pool's Deployment (ground rule 2),
so do it while no actor is assigned to its workers.

### Checkout and environment

Every `go run ./cmd/ate-setup` command below runs from a checkout of
the new release, with the environment and flags the install used:
`PROJECT_ID`, `CLUSTER_NAME` and `CLUSTER_LOCATION` (or `--context`),
and either `KO_DOCKER_REPO` for a build from source or
`--image-repo`/`--image-tag` for prebuilt images. Keep a checkout of
the old release too as rollback runs the same commands from it.

`ate-setup` takes the version from `$VERSION` when it is set, else
from `git describe` on the checkout for a build from source, or from
`--image-tag` (`ATE_IMAGE_TAG`) for prebuilt images. That value must come out different
from `$OLD_VERSION`, otherwise dataplane upgrade rolls the running atelet in place
instead of adding a second DaemonSet. A tagged checkout differs by
construction; if the install pinned `VERSION`, pin a new value now
and keep it for every command of this upgrade.

Install the new `kubectl ate` with `go install ./cmd/kubectl-ate`.

## Upgrade

### 1. Park the autoscaler (GKE)

During the roll, the old pool's pods that lost their node sit Pending on
purpose: they are the rollback reserve. The autoscaler reads Pending
pods as demand and would add nodes for pods that must never schedule,
so park it. Save its config first; step 8 restores it. If a GKE
maintenance window falls inside the roll, add a maintenance exclusion
for it too: a node GKE recreates mid-roll comes back at the pool's
label, which is still `$OLD_VERSION`.

```bash
gcloud container node-pools describe $NODEPOOL --cluster $CLUSTER --zone $ZONE \
  --format='json(autoscaling)'   # save for step 8
gcloud container clusters update $CLUSTER --zone $ZONE \
  --node-pool $NODEPOOL --no-enable-autoscaling
```

One-time check: the node pool itself must carry
`ate.dev/substrate-version=$OLD_VERSION`, or nodes GKE creates later arrive
unlabeled and nothing schedules to them. If it is missing, stamp it
now with the old value:

```bash
gcloud container node-pools describe $NODEPOOL --cluster $CLUSTER --zone $ZONE \
  --format='get(config.labels)' # copy the value
gcloud container node-pools update $NODEPOOL --cluster $CLUSTER --zone $ZONE \
  --node-labels=<every existing user label>,ate.dev/substrate-version=$OLD_VERSION
```

### 2. Apply the new CRDs

From the new release's checkout, apply its CRDs. Nothing running
changes; the new schema is in place for the controller that follows.

```bash
# in a checkout of the new release
kubectl apply -f manifests/ate-install/generated
```

### 3. Upgrade ate-controller

```bash
go run ./cmd/ate-setup deploy ate-controller
```

This rolls `ate-controller` (and re-applies the CRDs, which is a
no-op now).

By convention, changes to how the controller renders worker
pods sit behind WorkerPool fields, so the new controller keeps
rendering the serving pools as they are. A release that breaks that
convention says so in its notes. Expect the pools' Deployments to
roll once here in that case. Every actor is suspended through the
worker eviction path, loses no state, and resumes on demand. If they
roll, wait for `READY` to equal `DESIRED` again on every serving pool
(`kubectl get workerpools -A`) before step 6.

### 4. Prepare the new dataplane

The dataplane roll starts with the new atelet DaemonSet, from the
same checkout:

```bash
go run ./cmd/ate-setup deploy atelet
```

It lands next to the old one with zero pods until step 6 flips a
node.

Then read `$NEW_VERSION` off the cluster:

```bash
kubectl get ds -n ate-system -l app=atelet -L ate.dev/substrate-version
```

Exactly two DaemonSets print. The SUBSTRATE-VERSION that is not `$OLD_VERSION` is
`$NEW_VERSION`, and the new DaemonSet shows 0 `DESIRED` because no node carries
its label yet. `kubectl get nodes -L ate.dev/substrate-version` still
shows every node at `$OLD_VERSION`. If only one DaemonSet prints, the
version did not change (see Checkout and environment) and the command
rolled the running atelet in place.

Then open access to the Control API for draining. Draining a worker
is one `DrainWorker` RPC on the installed ate-api-server. Step 6 calls
it with [grpcurl](https://github.com/fullstorydev/grpcurl).

```bash
kubectl -n ate-system port-forward svc/api 8443:443 >/dev/null 2>&1 &

kubectl get clustertrustbundles -l podcert.ate.dev/canarying=live \
  -o jsonpath='{range .items[?(@.spec.signerName=="servicedns.podcert.ate.dev/identity")]}{.spec.trustBundle}{end}' \
  > /tmp/ate-ca.pem

TOKEN=$(kubectl -n ate-system create token ate-client \
  --audience=api.ate-system.svc --duration=48h)
```

Last, build and push the new worker images. The refs print together
at the end. The one for your pool's `sandboxClass` goes into the clone
in step 5:

```bash
go run ./cmd/ate-setup publish worker-images
```

If you are using prebuilt images there is nothing to publish. The new worker image
is the release's `ateom-<sandboxClass>` image under the same repo and
tag as the control plane, pinned by digest:

```bash
NEW_IMAGE=$IMAGE_REPO/ateom-gvisor:$IMAGE_TAG@$(crane digest $IMAGE_REPO/ateom-gvisor:$IMAGE_TAG)
```

### 5. Create the new pool

**Repeat this step for every serving pool the preflight listed**.

Copy the old pool under a new name (`$NEW_WORKERPOOL`, for example
the old name plus `$NEW_VERSION`) and change only `workerImage` and
the version pin. The command below does exactly that; everything
else, including the `metadata.labels` the scheduler matches actors
by, carries over as is.

```bash
NEW_IMAGE=<the ateom ref from step 4, for this pool's sandboxClass>

kubectl -n $NS get workerpool $OLD_WORKERPOOL -o json \
  | jq --arg name "$NEW_WORKERPOOL" --arg image "$NEW_IMAGE" --arg version "$NEW_VERSION" '
      {apiVersion, kind,
       metadata: {name: $name, namespace: .metadata.namespace,
                  labels: .metadata.labels},
       spec: (.spec + {workerImage: $image})}
      | .spec.template.nodeSelector["ate.dev/substrate-version"] = $version
    ' \
  | kubectl apply -f -
```

Do not shrink `spec.template.resources.limits` while cloning. A
worker only accepts an actor whose limits fit under them.

Verify:

```bash
# $NEW_VERSION, then an image ref that contains @sha256:
kubectl -n $NS get workerpool $NEW_WORKERPOOL \
  -o jsonpath='{.spec.template.nodeSelector.ate\.dev/substrate-version}{"\n"}{.spec.workerImage}{"\n"}'

# One Deployment per pool; new-pool pods all Pending (no node carries
# the new label yet).
kubectl -n $NS get deploy -l ate.dev/worker-pool
kubectl -n $NS get pods -l ate.dev/worker-pool=$NEW_WORKERPOOL
```

While both pools serve, placement between them is random, and that
is fine: a snapshot written on either version restores on either
version. The roll converges because step 6 takes old workers out of
service node by node, not because the scheduler prefers the new pool.

### 6. Roll each node

Repeat for every node, one at a time.

**a. Drain the node's workers.** Bound actors keep running. Draining
only stops new placements.

```bash
# A worker's name is its pod's UID; that is what DrainWorker takes.
for w in $(kubectl ate get workers -o json \
             | jq -r --arg node "$NODE" '.workers[] | select(.nodeName == $node) | .metadata.name'); do
  grpcurl -cacert /tmp/ate-ca.pem -authority api.ate-system.svc \
    -H "authorization: Bearer ${TOKEN}" \
    -d "{\"worker\": {\"name\": \"${w}\"}}" \
    127.0.0.1:8443 ateapi.Control/DrainWorker
done
```

**b. See what is still on the node.** Two lists: the node's workers
with the actor each one hosts, and any paused actor whose local
snapshot lives on this node. A paused actor sits on no worker, so it
shows up only in the second list.

```bash
kubectl ate get workers -o json | jq -r --arg node "$NODE" '
  ["WORKER", "POD", "ASSIGNED ACTOR"],
  (.workers[] | select(.nodeName == $node)
   | [.metadata.name, .workerPod,
      (.status.assignment.actor | if . then .atespace + "/" + .name else "<none>" end)])
  | @tsv' | column -t -s $'\t'

kubectl ate get actors -A -o json | jq -r --arg node "$NODE" '
  ["PAUSED_ACTOR", "STATE"],
  (.actors[]
   | select(.status.localSnapshotInfo.nodeVmsWithLocalSnapshots // [] | index($node))
   | [.metadata.atespace + "/" + .metadata.name, .status.state])
  | @tsv' | column -t -s $'\t'
```

**c. Suspend them at your own pace.** Every actor in either list has to be suspended
before the node moves (`kubectl ate suspend actor <name> -a
<atespace>`). Suspend releases the worker and uploads the durable
snapshot, so the actor resumes on demand onto any free matching worker
afterwards. Repeat step b until the `ASSIGNED ACTOR` column reads
`<none>` throughout and the paused list is empty. Then rerun the drain
in step a once more right before flipping.

**d. Flip the label.** The old atelet pod leaves on its own and the
new one starts.

```bash
kubectl label node $NODE ate.dev/substrate-version=$NEW_VERSION --overwrite
```

Wait for the new `atelet` pod on the node to be Ready before going on. Usually that is seconds; the new `atelet` pod stays Pending only while the old one is still exiting, since it holds the node's host ports until then.

```bash
# The pod is named after atelet-<new suffix>.
kubectl get pods -n ate-system -l app=atelet --field-selector spec.nodeName=$NODE
```

**e. Delete the node's old-pool worker pods.** Step c emptied them,
but they are still Ready and hold capacity the new pool needs on this
node. Their Deployment cannot reschedule them here anymore. Repeat
per pool if the node hosts several.

```bash
kubectl -n $NS delete pod -l ate.dev/worker-pool=$OLD_WORKERPOOL \
  --field-selector spec.nodeName=$NODE
```

**f. Confirm the node has moved.** Two checks:

```bash
# New-pool workers came up here.
kubectl -n $NS get pods -l ate.dev/worker-pool=$NEW_WORKERPOOL --field-selector spec.nodeName=$NODE

# No old-pool pod is left here.
kubectl -n $NS get pods -l ate.dev/worker-pool=$OLD_WORKERPOOL --field-selector spec.nodeName=$NODE
```

New-pool pods Pending on other nodes are expected until those nodes
move.

**g. Take the next node.** Start again at step a once `kubectl ate get
workers` shows at least one FREE worker for the suspended actors to
land on.

You are done when `kubectl get nodes -L ate.dev/substrate-version`
shows every node at `$NEW_VERSION` (a node that joined mid-roll still
carries `$OLD_VERSION`; apply step 6 to roll it) and every assigned
actor in `kubectl ate get actors -A` sits on a new-pool pod.

### 7. Upgrade the rest of the control plane

Every actor is now on the new dataplane, running or suspended, and
ate-controller moved in step 3. From the same checkout, move
`ate-api-server` first, then everything else:

```bash
go run ./cmd/ate-setup deploy apiserver
go run ./cmd/ate-setup deploy ate-system
```

The second command rolls atenet and converges the rest of the
install; it re-resolves and re-applies everything, so it could take a
while. The step 4 port-forward dies when the API server rolls.
Restart it and mint a fresh token if you still need to drain. 

**NOTE**: Until
this step is done the old ate-api-server is still serving, so do not
start using API fields new in this release before upgrade finishes.

### 8. Move the pool label, restore the autoscaler (GKE)

Relabel the GKE node pool with `$NEW_VERSION` so nodes created later start at the new
version, then restore the autoscaler from the config step 1 saved:

```bash
# --node-labels REPLACES the pool's full user label set: list the
# current labels first and carry them all over.
gcloud container node-pools describe $NODEPOOL --cluster $CLUSTER --zone $ZONE \
  --format='get(config.labels)' # copy the output

gcloud container node-pools update $NODEPOOL --cluster $CLUSTER --zone $ZONE \
  --node-labels=<every existing user label>,ate.dev/substrate-version=$NEW_VERSION

# restore the autoscaler config
gcloud container clusters update $CLUSTER --zone $ZONE --node-pool $NODEPOOL \
  --enable-autoscaling --min-nodes <min> --max-nodes <max>
```

## Rollback

The roll never edits or deletes the old objects. The old `WorkerPool`
is intact, the pods that lost their nodes stay Pending, and the old atelet
DaemonSet is still installed. Rolling nodes back is a matter of label
flips.

Undo what you did, by how far you got, in **reverse** order: the control
plane back first, then the nodes, then ate-controller last. Run the
commands from the old release's checkout with the same environment as
the install, `VERSION` included if the install pinned it.

- Past step 8 (GKE): park the autoscaler again as in step 1, then
  move the node pool label back to `$OLD_VERSION` with the step 8
  command.
- Past step 7: `go run ./cmd/ate-setup deploy apiserver` now, and
  `go run ./cmd/ate-setup deploy ate-system` once the nodes are back.
- Past step 6: roll each flipped node back by running step 6 with the
  sides swapped: drain, get every actor off the node as in b and c,
  flip the label back to `$OLD_VERSION`, and delete the node's
  new-pool pods.
- Past step 3: `go run ./cmd/ate-setup deploy ate-controller`.

`kubectl get ds -n ate-system -l app=atelet` must still show two
DaemonSets afterwards. A third means the old checkout produced a
version other than `$OLD_VERSION`: delete it and check `VERSION`.

To abandon the upgrade entirely, roll every flipped node back, confirm
no actor is assigned to a new-pool worker, then delete the new objects:

```bash
kubectl -n $NS delete workerpool $NEW_WORKERPOOL
kubectl delete daemonset -n ate-system -l app=atelet,ate.dev/substrate-version=$NEW_VERSION
```

Retiring the old pool (below) is a separate, deliberate step. As
long as the old objects exist, rollback is one label flip per node.

## Retire the old pool

After the new version has soaked, reclaim the reserve. Save the old
pool's spec first: its `workerImage` ref is by digest and stays
pullable, so the saved file is the last-resort way to recreate the
pool. Then check the guards. Deleting the old objects is what ends
the rollback option.

```bash
kubectl -n $NS get workerpool $OLD_WORKERPOOL -o yaml > old-pool-backup.yaml

# Guards: no node still at $OLD_VERSION; no old-pool pod Running (Pending is
# expected); no actor assigned to an old-pool worker.
kubectl get nodes -l ate.dev/substrate-version=$OLD_VERSION
kubectl -n $NS get pods -l ate.dev/worker-pool=$OLD_WORKERPOOL
kubectl ate get workers

# Retire the old pool (its Deployment and pods go with it) and the
# old atelet DaemonSet.
kubectl -n $NS delete workerpool $OLD_WORKERPOOL
kubectl delete daemonset -n ate-system -l app=atelet,ate.dev/substrate-version=$OLD_VERSION
```

The new pool keeps its name. Names mean nothing to placement, so
`counter-v2` can serve indefinitely, and the next upgrade clones it
to `counter-v3`.

## If something goes wrong

- Every step is idempotent, so rerun the command that failed. The
  one exception is drain: there is no undrain. If you drained the
  wrong node, check that its workers are empty (suspending as
  needed), delete their pods, and let the Deployment's replacements
  register as fresh workers.
- `kubectl get nodes -L ate.dev/substrate-version`,
  `kubectl -n $NS get deploy -l ate.dev/worker-pool`, and
  `kubectl ate get workers` show everything about where the roll
  stands.
