# ate-setup vs. the install shell scripts

`ate-setup` is a Go port of `hack/install-ate.sh`, the seven
`hack/install-demo-*.sh` scripts it sources, and `hack/setup-csi-*-kind.sh`.

The shell scripts are still present and still work; this is an additive
alternative to them, not yet a replacement. Retiring them behind compatibility
shims is left to a follow-up, so for now the two installers coexist and either
can be used against the same cluster. See [`commands.md`](commands.md) for the
flag-by-flag mapping between them.

This document covers what changed *behind* that mapping. For anything not listed
here, the port is intended to be behavior-preserving.

## What is deliberately unchanged

These were treated as contracts and reproduced exactly:

- **Step log lines.** `log.Step` prints the same cyan `[step]: name` and the
  same step names (`deploy_ate_system`, `create_api_server_env_vars`,
  `demo-counter_deploy (with_external_volume=true)`), so CI log scrapers keep
  working.
- **Manifest ordering.** Applying a directory is non-recursive and lexical, as
  `kubectl apply -f <dir>` was. `deploy_ate_system` depended on that ordering
  and the shell comments called out specific filename hazards, so
  `kube.LoadPath` keeps it.
- **Overlay selection.** `steps.SystemOverlay` is the same product of
  kind × router that `render_ate_system_manifests` computed with
  nested `if`s.
- **Timeouts.** 60s namespace, 60s rollout (`--rollout-timeout` /
  `ATE_INSTALL_ROLLOUT_TIMEOUT`, as in the scripts), 120s for the
  podcertificate-controller and CSI waits the scripts fixed there, 300s demo.
  `--rollout-timeout` now reaches the 120s waits too, which it did not in the
  shell, but only when it is passed: `Config.WaitTimeout` leaves each site at
  its historical value otherwise, so the 60s default cannot shorten the slow
  bootstrap paths.
- **Rendered bytes.** `authentication.yaml` is trimmed of its trailing newline
  because the shell built it inside `$(...)`, which strips them. Switching
  between the two installers must not rewrite the ConfigMap.
- **ko's ldflags.** `make ldflags` emitted `-X=<version pkg>.Version=$(git
  describe --tags --always --dirty)`; `ko.Runner.ldflags` computes the identical
  string without depending on make.

## Execution model

| | shell | ate-setup |
|---|---|---|
| Actions per run | many, one per flag, in command-line order | exactly one subcommand |
| Argument errors | detected when the dispatch loop reaches the flag, after earlier actions already ran | rejected by cobra before anything runs |
| Value flags | pre-scanned so they could appear anywhere | positional, per-command, standard flag parsing |
| Configuration | env vars (`ATE_INSTALL_KIND`, `ATE_ATENET_ROUTER`, `KUBECTL_CONTEXT`, …) | flags, with the env vars still honored as defaults |
| Repository root | `git rev-parse --show-toplevel`, then `cd` | walk up for `go.mod`; no `chdir`, all paths absolute |

The one-action-per-run change is the most visible: a line that passed
`--deploy-ate-system --deploy-demo-counter` becomes two `ate-setup` calls.
`hack/install-ate.sh` still accepts the combined form.

Invalid input now fails before any cluster mutation. `--atenet-router=nginx`
used to be caught by a pre-scan validation pass; `--worker-count 0` was not
caught at all and surfaced from inside `deploy_locust.sh` after the microvm
dependencies had already been installed.

`.ate-dev-env.sh` is still sourced through bash — a developer's file can contain
arbitrary shell — but its variables are then layered under the process
environment and the flags rather than being exported wholesale.

## External binaries

Dropped entirely: **kubectl**, **kubectl kustomize**, **jq**, **openssl**,
**sed**, **base64**, **grep**, **make**, and `go run ./cmd/kubectl-ate`.

Still required, and why:

| Binary | Used for |
|---|---|
| `ko` | building and publishing images (`ko resolve` only); not used with `--image-repo` |
| `go` | locating the pinned `ko` tool, exactly as `hack/run-tool.sh` does; not used with `--image-repo` |
| `git` | the `git describe` version stamp passed to ko; not used with `--image-repo` |
| `docker` | the kind CSI setup (`docker exec` into the node) and the claude-code-multiplex workload build |
| `gcloud` | GKE `get-credentials`, only when `PROJECT_ID` is set and no context was given |
| `bash` | sourcing `.ate-dev-env.sh` |

Two shell scripts are still invoked rather than reimplemented, because they
orchestrate image builds, asset assembly, and object-store staging that are out
of scope for an installer: `benchmarking/deploy_locust.sh` and
`hack/install-microvm-deps.sh`. They receive `Config.ScriptEnv()`, which
reconstructs the environment the shell installer would have exported to them.

`ko` is no longer asked to apply anything. The scripts ran `run_ko apply`, which
made ko shell out to kubectl and forced the awkward `-- --context=` special case
(only `apply`/`create`/`delete`/`run` accept args after `--`; `resolve` rejects
them). Now ko only resolves, and the resulting manifest is applied through
client-go.

## Image sources

The shell installer had exactly one: build every image from the checkout with
`ko` and push it to `KO_DOCKER_REPO`. That is still the default and is
unchanged. `--image-repo` adds a second, in which nothing is built.

Manifests carry `ko://<import path>` references either way. With `--image-repo`
each is looked up in `images.Components` and rewritten to
`<repo>/<base of the import path>:<tag>`, which is ko's own
`--base-import-paths` naming and so is how the release images are already
published — `ateapi`, `atelet`, `atenet`, `ateom-gvisor`, and the rest.
`--image-tag` supplies the tag. Each of the two requires the other: a tag with
no repository to pull from would be dropped, leaving a build from source that
looks like the release the tag names.

The rewritten reference then keeps that tag and adds the digest it names, as
`<repo>/<image>:<tag>@sha256:...`, which is the shape ko itself produces for a
tagged release. Resolving the digest is one HEAD request per image, cached for
the install, so `--image-repo` needs read access to the registry from wherever
`ate-setup` runs rather than only from the cluster. It is not optional: an
ActorTemplate's container image, an image volume's reference, and a
SandboxConfig's `pauseImage` each carry the CEL rule `self.contains('@')`, so an
unpinned reference is rejected by admission and every demo fails to deploy.
Pinning also gives a pre-built install the property a `ko` install had for free,
that a tag moving afterwards cannot change what is running.

That tag also becomes the substrate version, which names the atelet DaemonSet
and sets the node label partitioning nodes across coexisting versions. It has
to: that label must describe the atelet actually running, which came from the
image, not the checkout `git describe` happens to be sitting on. `VERSION`
overrides it, as it always has.

The rewrite is textual, one token at a time, matching `SubstituteVersion` above.
A reference is replaced wherever it appears, so the code needs no list of which
fields hold images — `workerImage:` in a CRD spec is rewritten like any other —
and the applied manifest differs from the checked-in YAML only in the image
references.

Every reference must be in the list. A reference outside the module path, one
carrying an unexpanded `${PLACEHOLDER}`, and one naming a package that is not
installable all fail the same way — an error naming every unmapped reference at
once, rather than an unpullable image reaching the cluster and failing as
`ImagePullBackOff` twenty minutes later. Matching whole references rather than
replacing the listed ones is what makes that hold: a reference extending a
listed package would otherwise inherit its image. `images.Components` is checked
against the real manifests by a test, so adding a component fails a test rather
than an install — though only the list is checked that way. The release also has
to publish the new image under the same repository and tag as the rest, and
until it does, the component can only be installed by building it.

## Cluster access

**Server-side apply.** `kubectl apply -f -` is a client-side apply by default.
`ate-setup` uses SSA through the dynamic client with field manager `ate-setup`
and `force: true`. This is the supported path for repeated reconciliation and
avoids the `last-applied-configuration` annotation growing without bound on the
large generated CRDs. Objects previously applied client-side will show both
managers in `managedFields` until the next apply reconciles them.

**Discovery caching is a new failure mode.** Every `kubectl` invocation started
with a cold discovery cache, so `kubectl apply -f generated` followed by
applying a `SandboxConfig` just worked. One long-lived process keeps a cached
RESTMapper, which would keep serving the pre-CRD discovery document and report
`no matches for kind`. Three mitigations are in place: `Apply` invalidates after
a CRD document in the same stream, `DeployCRDs` invalidates explicitly, and
`resourceFor` retries once against fresh discovery before giving up.

**Rollout status is reimplemented.** `kubectl rollout status` is not a library
call, so `kube.RolloutStatus` polls Deployments, DaemonSets, and StatefulSets
with the same conditions kubectl's status viewers use — including the
`ProgressDeadlineExceeded` check and the partitioned-StatefulSet case. Two
deliberate differences: a workload that does not exist yet is *waited for*
rather than erroring immediately, and the failure message carries the last
observed status (`3/5 replicas available`) instead of only a timeout.

**Deletes are more precise.** `kubectl delete --ignore-not-found -f` was the
model, so NotFound is ignored — and so is a kind that no longer resolves, since
teardown after the CRDs are gone must not fail. Beyond that, deletes are strict.

**`|| true` is gone.** The CSI hostpath bundle ships a `VolumeSnapshotClass`
whose CRD is absent on a stock Kind cluster; the shell script handled that by
ignoring kubectl's exit code for the whole bundle, which also hid real failures.
`ApplyTolerant` skips exactly the objects whose *kind* is unmappable, logs each
skip, and keeps everything else strict. The best-effort `docker exec` cleanup on
the Kind node stays best-effort, as before.

## Certificates and secrets

**CA pools are generated in-process.** `create_*_ca_pool_secret` shelled out to
`go run ./cmd/kubectl-ate admin make-ca-pool`, paying a compile per call.
`steps.createCAPool` calls `internal/localca` and `internal/localjwtauthority`
directly — the same libraries `kubectl-ate` uses.

**Creating an existing pool is no longer an error.** `make-ca-pool` issues a
bare `Create`, so `--create-actor-id-ca-pool-secret` against a cluster that
already had one failed with `AlreadyExists`. `ate-setup create actor-id-ca-pool`
logs `already exists; keeping it` and succeeds. It still refuses to overwrite:
regenerating would rotate the root out from under every certificate already
issued from it. (The `--deploy-ate-system` path was already guarded by
`ensure_apiserver_prerequisites`, so only the standalone `create` subcommands
change.)

**Pool extraction is exact.** `ca_pool_root_pem` piped a Secret through
`jsonpath | base64 -d | grep -o '"RootCertificateDER":"[^"]*' | sed | base64 -d
| openssl x509`. That has two problems the shell comments were already worried
about: `grep -o` returns only the **first** root, silently dropping the rest of
a multi-CA pool, and any failure in the pipeline yields an empty string that
becomes a trust bundle with no roots. `kube.CAPoolRootPEM` unmarshals the pool
with the library that wrote it, emits **every** CA's root, and returns an error
for a missing key, an undecodable pool, an empty pool, or a CA with no root
certificate.

## Demos

**A registry replaces naming conventions.** The scripts appended to an
`ATE_DEMOS` array and discovered `${name}_deploy`, `${name}_delete`,
`${name}_usage`, and `${name}_cmdline` with `declare -F`. A demo that misspelled
a function silently lost that capability. `demos.Demo` is an interface, each
demo is its own package registering itself from `init`, and
`internal/demos/all` is the single list of what gets linked in — with a test
asserting no demo package is missing from it.

**Kind-only demos are visible but refuse.** `demo-autoscaled-workerpool`
registered itself only when `ATE_INSTALL_KIND=true`, so on GKE
`--deploy-demo-autoscaled-workerpool` was rejected as an *unknown option*. The
cobra tree is built before flags are parsed, so `--kind` is not known yet; the
subcommand always exists and fails at run time with `demo-autoscaled-workerpool
is only supported for Kind installations; re-run with --kind`. `delete all`
still skips it on a non-Kind install, via `demos.For`.

**Template expansion is not sed.** The demo templates were expanded by a stack
of `sed -e` expressions doing two things: substitute `${PLACEHOLDER}`, or delete
the whole line (`/${NAME}/d`) when the placeholder is not in play.
`internal/render` implements exactly those two operations, so the output is
byte-identical, but values are no longer interpreted as sed replacement text —
an `ANTHROPIC_API_KEY` or bucket name containing `|`, `&`, or a backslash is now
substituted literally. A placeholder that is neither substituted nor dropped is
left in place, so an unexpected template variable surfaces as invalid YAML
instead of vanishing.

**Actor cleanup uses the API directly.** `delete_demo_actors` required `jq`,
listed actors with `kubectl-ate get actors -A -o json`, and filtered with a jq
expression. `steps.DeleteDemoActors` uses `internal/ateclient` and pages through
`ListActors`. The tolerances are preserved: no `ate-api-server`, or an apiserver
that cannot be reached, skips cleanup rather than failing, because `delete all`
runs this for every demo.

**`docker buildx imagetools inspect` output is parsed as JSON**, not piped
through `jq -r '.manifest.digest'`. Build output still goes to stderr so the
stdout/stderr split matches under CI log capture.

## Errors and diagnostics

- `set -o errexit` propagated failures but could not see a failing command
  substitution inside an argument list — the case the shell comments flagged
  twice around CA extraction. Go returns errors at each call site.
- Errors are wrapped with the object they concern: `while applying
  deployment/ate-api-server -n ate-system: ...`.
- Warnings go to stderr with a `Warning:` prefix; step and progress output goes
  to stdout, as before.
- `ate-setup` with no subcommand prints the help text and exits 0, without
  loading config or touching the cluster; the script printed usage and exited 1.
  Either way nothing is installed implicitly — the subcommand naming the setup
  to run is always explicit.

## Known differences worth flagging

**`--setup-csi` on a non-Kind cluster.** Both installers now accept `nfs` off
Kind — only the hostpath plugin is patched for the single-node Kind layout, and
both reject `hostpath` and `both` there with a hard error rather than the
shell's old warn-and-continue.

**Cloud SQL is shell-only.** `hack/install-ate.sh` automates Cloud SQL setup
with IAM authentication, synthesized DSNs, and the Auth Proxy sidecar
([`cloud-sql.md`](../../tools/setup-gcp/cloud-sql.md)). `ate-setup` does not yet
port this: while it honors `ATE_API_POSTGRES_CONNECTION_STRING` for generic
external databases, it ignores `ATE_API_POSTGRES_CLOUDSQL_*` and deploys the
bundled StatefulSet. Running `ate-setup` on a Cloud SQL cluster reverts the DSN
to the in-cluster database, leaving behind an orphaned proxy. Use
`hack/install-ate.sh` for Cloud SQL clusters until ported.

## Testing

The shell installer had no tests. `cmd/ate-setup` has unit tests for template
rendering, overlay selection, config resolution, the authentication config, the
apiserver environment ConfigMap, delegated script arguments, manifest deletion,
per-demo rendering, and image reference rewriting.
