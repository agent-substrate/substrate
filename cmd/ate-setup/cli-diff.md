# `hack/install-ate.sh` after the move to `ate-setup`

`hack/install-ate.sh` no longer installs anything. It is a translation shim: it
parses the flags and environment variables the installer has always accepted
and runs `go run ./cmd/ate-setup <command>` for each action, in command-line
order. [`commands.md`](commands.md) has the flag-by-flag mapping;
[`differences.md`](differences.md) explains how the Go implementation differs
from the shell one it replaced.

This document is for the person whose command line or CI job already worked.
Everything not listed here behaves as it did. What is listed here either could
not be reproduced through the shim, or was reproduced closely enough to keep
scripts working but not closely enough to keep a sharp-eyed reader from
noticing.

## What the shim preserves

Deliberately, and covered by `shim_test.go`:

- Every flag, with the same spelling, the same `=value` and separated-value
  forms, and the same "value flags may appear anywhere on the line" pre-scan.
- Several actions per invocation, executed in the order they were written.
- Every environment variable, including `SETUP_CSI`, `STORAGE_CLASS`,
  `NO_DEV_ENV`, `ATE_INSTALL_KIND`, `KUBECTL_CONTEXT`, `PROJECT_ID`, and the
  `ATE_API_POSTGRES_*` set. `ate-setup` reads them directly, so they need no
  translation.
- `--help` / `-h` anywhere on the line, no arguments at all (usage, exit 1),
  and an unrecognized flag (`Error: unknown option: …`, usage, exit 1) —
  including after earlier actions on the same line have already run.
- `--deploy-ate-system --setup-csi=nfs` still sets the CSI driver up twice,
  once inside the system deploy and once as its own action, as it always has.

## Differences you can observe

### One process per action

Each action is a separate `go run ./cmd/ate-setup`. The build is cached after
the first, but a line asking for six actions pays six process starts, and each
one re-resolves the configuration — including, on GKE with `PROJECT_ID` set and
no `KUBECTL_CONTEXT`, a `gcloud container clusters get-credentials` per action
rather than one for the whole line.

`go` is therefore required to run the installer. In exchange, `kubectl`, `jq`,
`openssl`, `sed`, `base64`, and `make` are not; see the external-binaries table
in [`differences.md`](differences.md#external-binaries) for what is still
needed.

### Value flags are validated only when an action uses them

The shell pre-scan validated `--atenet-dataplane`, `--podcert-workers-per-signer`,
`--rollout-timeout`, and `--benchmark-sandbox-class` on every run, whether or
not anything used them. Validation now happens inside `ate-setup`, so:

- A line with no action at all — `./hack/install-ate.sh --rollout-timeout=zzz`
  — exits 0 without complaining. It used to exit 1.
- `--benchmark-sandbox-class=zzz` is rejected only by `--deploy-benchmarks` /
  `--delete-benchmarks`. Paired with any other action it is ignored, where it
  used to fail the run up front.

The four flags that apply to every action (`--atenet-dataplane`,
`--podcert-workers-per-signer`, `--rollout-timeout`, `--otlp-endpoint`) are
passed to every `ate-setup` invocation, so as long as the line has one action
they are still rejected before that action touches the cluster.

### `--deploy-demo-autoscaled-workerpool` fails later, off Kind

The demo registered itself only under `ATE_INSTALL_KIND=true`, so on GKE the
flag was rejected as an unknown option, before anything ran, and `--help` did
not list it. The cobra command tree is built before flags are parsed, so the
subcommand now always exists: `--help` always lists the demo, and off Kind it
fails when it runs, with `demo-autoscaled-workerpool is only supported for Kind
installations; re-run with --kind`. Earlier actions on the same line will
already have completed. `--delete-all` still skips it off Kind.

### The demo list lives in two places

`ATE_DEMOS` in the shim is a literal list, so that `--help` answers without a
Go build and an unknown demo is rejected here rather than reaching `ate-setup`
as an unknown subcommand. A demo added to `internal/demos/all` and not to that
list is invisible to the shim; `TestShimDemoListMatchesTheRegistry` fails when
the two drift.

### Two step log lines changed

`log.Step` names are otherwise unchanged, but two lost a parenthesized
qualifier that CI log scrapers may be matching on:

| before | now |
|---|---|
| `demo-counter_deploy (with_external_volume=true)` | `demo-counter_deploy` |
| `setup_csi (nfs)` | `setup_csi` |

### `--setup-csi` on its own does more

The standalone action used to ensure the CRDs and then install the driver.
`ate-setup setup csi` also ensures the `ate-system` namespace, the
podcertificate CAs, and a ready podcertificate-controller with its trust
bundles first. The hostpath driver's ghostunnel sidecar projects a
podCertificate volume and cannot roll out without them, which is why
`deploy_ate_system` had always called CSI setup from that point in its
sequence; the standalone path just did not.

### `.ate-dev-env.sh` is layered, not sourced over you

The script sourced the file into its own shell, so an `export FOO=...` in the
file won over a `FOO` already exported in yours. `ate-setup` still runs the
file through bash — it can contain arbitrary shell — but treats the result as
the lowest-precedence layer: process environment first, then flags. If you
have been relying on the file to override your shell, unset the variable in
your shell instead.

Sourcing is also skipped for Kind installs. `hack/install-ate-kind.sh` exports
`NO_DEV_ENV`, so that path is unchanged, but running the shim yourself with
`ATE_INSTALL_KIND=true` no longer reads the file.

### `--rollout-timeout` reaches further

It used to govern only the 60s workload rollout waits. It now also governs the
two waits fixed at 120s (podcertificate-controller and the CSI drivers) —
but only when it is passed, so the 60s default still cannot shorten those slow
bootstrap paths.

It also has to be positive. The value reached `kubectl rollout status
--timeout=`, where `0` means "wait forever"; `ate-setup` has no unbounded wait,
so `--rollout-timeout=0` is rejected at startup rather than read as its
opposite.

### Applies are server-side

`kubectl apply -f -` was a client-side apply. `ate-setup` uses server-side
apply with field manager `ate-setup` and `force: true`. Objects previously
installed by the shell script show both managers in `managedFields` until the
next apply reconciles them. `last-applied-configuration` stops growing on the
generated CRDs.

### The first install after this change rolls `ate-api-server` once

Both installers stamp `ate.dev/env-hash` on the pod template so a changed
environment starts a rollout, but they compute the digest differently. The
value is opaque, so the only consequence is one extra rollout the first time
`ate-setup` installs over a shell-installed cluster.

### `--create-*-ca-pool-secret` is idempotent

Against a cluster that already has the pool, these used to fail with
`AlreadyExists`. They now log `already exists; keeping it` and succeed. They
still refuse to overwrite: regenerating would rotate the root out from under
every certificate already issued from it.

### `--setup-csi=hostpath` and `=both` off Kind are hard errors

They used to warn and continue. Only the hostpath plugin is patched for the
single-node Kind layout; `nfs` is accepted anywhere, as before.

### The per-demo scripts are gone

`hack/install-demo-*.sh` and `hack/experimental-additional-egress-extproc.sh`
have been deleted — each demo is a Go package under
`cmd/ate-setup/internal/demos/` now. Nothing that goes through
`hack/install-ate.sh` notices, but anything sourcing those files directly, or
calling a `demo-…_deploy` shell function, has to move to
`go run ./cmd/ate-setup deploy demo <name>`.

## Not replicated, and not worth replicating

Two behaviors of the old script were bugs that the shim does not reproduce:

- `--delete-atenet` worked but was missing from `--help`. It is listed now.
- `--deploy-demo-jupyter` was printed twice in `--help`, and the
  `demo-counter-microvm` flags were printed inside `demo-counter`'s block.
  Each demo now gets exactly one block.
