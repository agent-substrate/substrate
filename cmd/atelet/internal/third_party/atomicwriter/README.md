# third_party/atomicwriter

Kubelet's atomic writer, copied from the Kubernetes monorepo:

- **Upstream source:** `k8s.io/kubernetes/pkg/volume/util/`
  (`atomic_writer.go`, `atomic_writer_linux.go`, `atomic_writer_unsupported.go`,
  `atomic_writer_test.go`)
- **Delta last verified against:** [`kubernetes/kubernetes@52ba9013`](https://github.com/kubernetes/kubernetes/commit/52ba90138eb40cab0987dac73e05c838149bdd1c) (master, 2026-08-13)
- **License:** Apache-2.0; the upstream copyright headers are retained in every file.

It is copied, not imported, because upstream lives in the `k8s.io/kubernetes`
monorepo, which is not consumable as a Go module. Expect this to remain a
permanent fork: the algorithm has been stable upstream since ~2016, and our
adaptations below are ones upstream would not take. Occasional manual re-syncs
for upstream bugfixes (e.g. path-validation hardening) are the only planned
convergence.

## Local modifications

Every modified file carries a `// substrate:` marker below its license header;
`grep -rn "substrate:" .` from this directory lists the patched files. The
changes, by class:

1. Package renamed `util` → `atomicwriter`.
2. Kubernetes-internal dependencies dropped: `k8s.io/klog/v2`,
   `k8s.io/apiserver/pkg/util/feature`, and `k8s.io/kubernetes/pkg/features`
   (`k8s.io/apimachinery/pkg/util/sets` is kept — substrate already vendors it).
   In the test file, `k8s.io/client-go/util/testing` is replaced by a local
   `mkTmpdir` helper.
3. Logging converted from klog to `log/slog`, threading a `context.Context`
   through `Write`, `pathsToRemove`, and `removeUserVisiblePaths` for
   `slog.*Context`. The `logContext` field/constructor parameter this obsoletes
   is removed: `NewAtomicWriter(targetDir, logContext)` →
   `NewAtomicWriter(targetDir)`.
4. Error handling restyled: upstream's log-then-`return err` sites return
   wrapped errors (`fmt.Errorf("while ...: %w", err)`) per substrate
   convention; klog error logs that accompanied a `return` are dropped in
   favor of the wrapped error.
5. Upstream's `ResolvesFsUser` helper (KEP-5936, feature-gate dependent) is
   omitted; substrate does not use FsUser resolution.

## Maintenance rules

- Only mechanical adaptations (the classes above) belong in the
  upstream-derived files. Anything behavioral goes in a separate,
  substrate-owned file in this package (none exist today).
- When touching these files, keep the diff against upstream minimal and update
  the modification list here if a new class of change is introduced.

## Re-syncing with upstream

1. Fetch the files listed above from `k8s.io/kubernetes` at the new commit.
2. Diff against this copy, ignoring the modification classes above
   (the delta is ~80 lines; it is meant to stay readable by hand).
3. Apply upstream's changes, re-apply our classes to any new code, run
   `go test ./cmd/atelet/internal/third_party/atomicwriter/`, and update the
   "Delta last verified against" commit above.
