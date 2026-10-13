# Running the security e2e suite by hand

This is the manual runbook for the security isolation tests: how to run them
on a local kind cluster, how to run a subset, and what to watch while they
run. CI runs them with every other e2e suite; nothing here is needed for that.

Prereqs: Go, kubectl, a running Docker. kind is created for you.

## Run the tests

1. Create the cluster, from the repo root:

   ```
   hack/create-kind-cluster.sh
   ```

2. Install the control plane. This is the slow step; it pulls about 570 MB:

   ```
   hack/install-ate-kind.sh --deploy-ate-system
   ```

   Stop there. The NFS/CSI and demo steps CI runs are for other suites.

3. Run one of the three top-level tests. They are split by the boundary under
   test: between two actors in one atespace, between two atespaces, or across
   two worker pods running concurrently in space.

   **`TestActorIsolation`** ([actor_isolation_test.go](actor_isolation_test.go)),
   one atespace, fixtures `actor-isolation.yaml.tmpl` and
   `actor-isolation-template.yaml.tmpl`:

   ```
   hack/run-e2e-kind.sh ./internal/e2e/suites/security -v -run TestActorIsolation
   ```

   - `VictimDurableMarkerNotVisibleThroughAttackerMount` — **ST-1 [time]** (durable-dir volume mount isolation across worker reuse)
   - `VictimHostStateNotReachableFromAttackerSandbox` — **EX-4 [time]** (per-actor host state and worker-pod credential isolation: T-18, T-19, T-22)
   - `VictimEphemeralStateNotPresentInAttackerSandbox` — **EX-5 [time]** (no residual `/tmp`, writable rootfs layer, `/dev` tmpfs, or open fd state across worker reuse: T-27)
   - `ProjectedIdentityNotCapturedInSnapshot` — **ST-3 [time]** (projected `/run/ate` `system-info` identity excluded from snapshots: T-23)
   - `DeletedActorSnapshotsRemoved` — **ST-5 [time]** (actor deletion removes external snapshots from object storage and prevents recovery)

   **`TestCrossAtespaceIsolation`** ([crossatespace_test.go](crossatespace_test.go)),
   two atespaces, fixtures `atespace-a-pool.yaml.tmpl`,
   `atespace-b-namespace.yaml.tmpl` and `atespace-template.yaml.tmpl`:

   ```
   hack/run-e2e-kind.sh ./internal/e2e/suites/security -v -run TestCrossAtespaceIsolation
   ```

   - `TagScopedToAtespaceCannotSeedAnotherAtespace` — **ST-4 [space]** (unpublished `ATESPACE`-scoped snapshot tag isolation across atespaces)

   **`TestConcurrentActorIsolation`** ([concurrent_isolation_test.go](concurrent_isolation_test.go)),
   two replicas, fixtures `concurrent-a-pool.yaml.tmpl`,
   `concurrent-b-namespace.yaml.tmpl` and `concurrent-template.yaml.tmpl`:

   ```
   hack/run-e2e-kind.sh ./internal/e2e/suites/security -v -run TestConcurrentActorIsolation
   ```

   - `RouterDeliversOnlyToNamedActorInSpace` — **NW-6 [space]** (ingress router atespace/actor isolation with concurrent actors on distinct workers: T-14)
   - `DirectActorToActorTrafficBlocked` — **NW-2 [space]** (no direct actor-to-actor traffic across the pod network; blocked by NetworkPolicy: T-17)
   - `ConcurrentDurableMountIsolation` — **ST-1 [space]** (durable-dir volume mount isolation across concurrent actors: T-18, T-27)
   - `ConcurrentHostStateNotReachableFromActorSandbox` — **EX-4 [space]** (live per-actor host state under shared `/var/lib/ate` not reachable across concurrent sandboxes: T-18)

   To run a subset of subtests:

   ```
   hack/run-e2e-kind.sh ./internal/e2e/suites/security -v \
     -run 'TestActorIsolation/(VictimDurableMarkerNotVisibleThroughAttackerMount|VictimHostStateNotReachableFromAttackerSandbox)'
   ```

   The setup still runs in full (victim runs, writes, suspends; attacker takes
   the worker). The filter only picks which assertions execute.

4. A failed run keeps its namespaces so you can read the worker logs:

   ```
   kubectl get ns | grep ate-e2e
   kubectl -n <ns> logs -l ate.dev/worker-pool -c ateom
   KIND_CLUSTER_NAME=kind hack/cleanup-e2e.sh   # reclaim when done
   ```

   `cleanup-e2e.sh` removes namespaces labeled `ate.dev/e2e`. Of this suite's
   fixtures only `actor-isolation.yaml.tmpl` carries that label, so delete
   `ate-e2e-sec-a` and `ate-e2e-sec-b` by hand if the cross-atespace test
   left them behind.

## Watch a run

Namespaces are `ate-e2e-sec-actor` for `TestActorIsolation` and
`ate-e2e-sec-a` plus `ate-e2e-sec-b` for `TestCrossAtespaceIsolation`. The
micro-VM lane inserts `-microvm` after `ate-e2e`. Worker pods are labeled
`ate.dev/worker-pool=<pool>`; the pools are `actor` and `shared`.

Start these before launching the test.

Lifecycle:

```
# Fixture namespaces appear and vanish; this is the run's heartbeat.
kubectl get ns -w | grep ate-e2e

# The single worker pod. It should be reused, never replaced.
kubectl get pods -n ate-e2e-sec-actor -w
```

The reuse, which is the point of `TestActorIsolation`. Actors are not
Kubernetes objects, so use the plugin:

```
# Actor state and worker assignment: victim runs, suspends, attacker binds
# to the same worker, then the clone, then the recreated victim.
watch -n1 'kubectl ate get actors -a ate-e2e-sec-actor'

kubectl ate get workers
kubectl ate top workers        # Actors used/total goes 1/1, 0/1, 1/1, ...
```

The worker name every actor shows is the string `requireReusedWorker`
asserts on.

The worker host:

```
# Sandbox lifecycle, checkpoints and restores.
kubectl logs -n ate-e2e-sec-actor -l ate.dev/worker-pool=actor -c ateom -f

# The per-actor layout under /var/lib/ateom-gvisor/actors that the
# host-state probes target. Only the running actor has durable-dir and
# system-info; atelet removes both at suspend, and a suspended actor's UID
# directory keeps just sandbox-assets.json.
POD=$(kubectl get pods -n ate-e2e-sec-actor -l ate.dev/worker-pool=actor -o name | head -1)
kubectl exec -n ate-e2e-sec-actor "$POD" -c ateom -- ls -la /var/lib/ateom-gvisor/actors
```

Control plane and snapshots:

```
kubectl ate get tags -a ate-e2e-sec-actor    # actor-snapshot-tag, during ProjectedIdentityNotCapturedInSnapshot
kubectl ate get tags -a ate-e2e-sec-a        # atespace-a-tag, during the cross-atespace test
kubectl ate get atespaces
kubectl logs -n ate-system -l app=ateapi -f
```

## Run the GitHub checks locally

The PR workflow (`.github/workflows/pr-workflow.yaml`) has two jobs:
`run-tests` and the `e2e-test` matrix, one entry per dataplane. Both compile
this suite, so a build error here fails both.

Fast, no cluster needed. This is what `run-tests` does, minus the JUnit
wrapping, and it type-checks every e2e suite:

```
go vet ./... && go test -race ./... && hack/verify-all.sh && (cd tools/apitool && go test -race ./...)
```

`hack/verify-all.sh` runs gofmt, boilerplate, licenses, go.mod, metrics
registry, and generated-code checks. Commit or stash first: the
generated-code check refuses a dirty tree. The metrics check needs Docker or
a local `weaver` at the version pinned in `hack/verify/metrics.sh`. The
root-gated tests CI runs under sudo need Linux:

```
hack/run-root-tests.sh -race -v
```

Full e2e, same as CI's gVisor lane. Do steps 1 and 2 above, then run every
suite with CI's flags:

```
hack/run-e2e-kind.sh -v -args --no-color
```

CI also deploys the NFS CSI driver and the demos before this, and repeats the
run with `E2E_SANDBOX_CLASS=microvm`. Neither matters for this suite.
