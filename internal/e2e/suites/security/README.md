# Security e2e suite

End-to-end tests for Substrate's isolation boundaries. This is the runbook
for running them on a local kind cluster and for watching what they do while
they run.

Prereqs: Go, kubectl, a running Docker. kind is auto-managed.

## Steps to run the security tests

1. Create the cluster (in the repo root):

```
hack/create-kind-cluster.sh
```

2. Install the control plane (slow step — pulls ~570 MB):

```
hack/install-ate-kind.sh --deploy-ate-system
```

Stop here — the NFS/CSI and demo steps CI runs are for other suites; this suite needs none of them.

3. Run the cross-atespace isolation test:

```
hack/run-e2e-kind.sh ./internal/e2e/suites/security -v -run TestCrossAtespaceIsolation
```

The wrapper sets KO_DOCKER_REPO, BUCKET_NAME, the kube context, and builds the probe image via ko. To run only the durable-mount and host-state subtests and skip the tag-scope one:

```
hack/run-e2e-kind.sh ./internal/e2e/suites/security -v \
  -run 'TestCrossAtespaceIsolation/(VictimDurableMarkerNotVisibleThroughAttackerMount|VictimHostStateNotReachableFromAttackerSandbox)'
```

Note the setup (victim run/write/suspend, attacker reuse) runs regardless — the subtest filter only selects which assertions execute.

4. On failure, a failed suite keeps its namespaces so you can read the worker logs:


kubectl get ns -l ate.dev/e2e
kubectl -n <ns> logs <worker-pod> -c ateom
KIND_CLUSTER_NAME=kind hack/cleanup-e2e.sh   # reclaim when done



## Run the GitHub checks locally

The PR workflow (`.github/workflows/pr-workflow.yaml`) runs three checks:
`run-tests`, `E2E (<dataplane>)`, and `e2e-test`, which only aggregates the
E2E matrix. Every one of them compiles this suite, so a build error here
fails all three.

Fast, no cluster needed. This is what CI's `run-tests` job does, minus the
JUnit wrapping, and it also type-checks every E2E suite:

```
go vet ./... && make verify && (cd tools/apitool && go test -race ./...)
```

`make verify` runs `go test -race ./...` and then `hack/verify-all.sh`
(gofmt, boilerplate, licenses, go.mod, metrics registry, generated code).
Commit or stash first: the generated-code check refuses a dirty tree. The
metrics check needs Docker or a local `weaver` at the version pinned in
`hack/verify/metrics.sh`. The root-gated tests CI runs under sudo need Linux:

```
hack/run-root-tests.sh -race -v
```

Full E2E, same as CI's gVisor lane. Do steps 1 and 2 above, then run every
suite with CI's flags instead of the `-run` filter from step 3:

```
hack/run-e2e-kind.sh -v -args --no-color
```

CI also deploys the NFS CSI driver and the demos before this, and repeats the
run with `E2E_SANDBOX_CLASS=microvm`. Neither matters for this suite.

## Commands to run while the test is running to observe
Watch the lifecycle (start these first, before/at test launch)

# The two atespace namespaces appear and vanish — this is the run's heartbeat
kubectl get ns -l ate.dev/e2e -w

# The one shared worker pod: watch the victim's pod get reused, not replaced
kubectl get pods -n ate-e2e-sec-a -w
The reuse itself (the point of the test)
Actors aren't in k8s, so use the plugin. This is where you see victim B run, suspend, then attacker A bind to the same worker:


# Actor state + worker assignment, atespace B (victim) then A (attacker)
watch -n1 'kubectl ate get actors -a ate-e2e-sec-b-mxy6vj23u335ddio2dev; echo ---; kubectl ate get actors -a ate-e2e-sec-a-mxy6vj23u335ddio2dev'

# Which worker each actor is on — the name should match across the two atespaces
kubectl ate get workers
kubectl ate top workers        # capacity: Actors used/total (you'll see 1/1 then 0/1 then 1/1)
The worker name that B's victim shows, then A's attacker shows after the suspend, is the same string requireReusedWorker asserts on. Seeing them match live is the manual version of that check.

The shared worker pod on the host

# The pod hosting both actors in turn
kubectl get pods -n ate-e2e-sec-a -o wide

# ateom logs — the sandbox lifecycle, checkpoints, resumes
kubectl logs -n ate-e2e-sec-a -l app=ateom -c ateom -f

# The on-host per-actor layout the host-state probes assert against.
# Two UID-keyed durable dirs (victim's + attacker's) live side by side here:
POD=$(kubectl get pods -n ate-e2e-sec-a -l app=ateom -o name | head -1)
kubectl exec -n ate-e2e-sec-a "$POD" -c ateom -- ls -la /var/lib/ateom-gvisor/actors
Control plane / snapshots (for the tag-scope subtest)

kubectl ate get tags -a ate-e2e-sec-a-mxy6vj23u335ddio2dev          # atespace-a-tag appears during the last subtest
kubectl ate get atespaces                       # ate-e2e-sec-a / -b listed while live
kubectl logs -n ate-system -l app=ateapi -f     # Create