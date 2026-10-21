# Agent Substrate

## Rules for contributors

- Read the "Using AI" section of `COLLABORATING.md`. You are responsible for everything your agent submits.
- Say in the PR description that you used AI, and how. Don't add AI co-author or assisted-by trailers. Submit commits and PRs from your own account. We can't accept contributions from agent or bot accounts (a human must sign the contribution agreement).
- Before asking for review, review your own diff. For substantial changes, also have a fresh agent session review it adversarially, then validate its findings and fix the confirmed issues.
- **Be brief.** Code comments, commit messages, PR descriptions and issues should be short, plain English, and read the way an experienced maintainer writes by hand. Reviewer time and agent context windows are limited.
  - `COLLABORATING.md` doesn't allow AI-generated commit messages, because they have usually been low quality and unreviewed. If you are asked to write one anyway, hold it to the same standard, and the person submitting it must review it.
  - Write short, direct sentences with one idea each, subject first.
  - Use the real identifiers from the code. Don't coin new names, and avoid noun stacks like "the store-wide pin-lock pre-flight".
  - Prefer verbs to abstract nouns: "evicts" instead of "performs eviction".
  - Say each thing once. Don't restate the diff, open with "This PR …" or "This commit …", or repeat the same explanation in several comments.
  - Explain why only when the code doesn't make it obvious, in a clause or a line.
  - Avoid:
    - em-dashes, and chains of semicolons
    - headings, bold, or test plans in commit messages
    - epigrams and contrast pairs, like "X, not Y" or "So treat ENOENT as what it is."
    - numbers added for color, like "measured at a flat 51ms"
    - giving code motives or editorializing about it, like "wants", "decides", "is responsible for", "lands", "owns", or "X is what makes Y". Plain active voice is fine: "the reaper collects" is how Go doc comments read.
    - filler words: load-bearing, footgun, surfaced, robust, comprehensive, seamless, leverage
    - "No behavior change" unless the diff is a pure refactor
    - long sentences that pile up clauses, or put the subject after a long lead-in

### Scope

- Keep each PR to one change. Send refactors, cleanups and follow-ups as separate PRs. For how to batch small fixes, see "Sizing PRs for review" in `CONTRIBUTING.md`.
- Follow "Contribution process" in `CONTRIBUTING.md`: open an issue first for anything beyond a small fix, and search open PRs and issues before you start.
- Start the PR description with why the change is needed, or link the issue. Only write "Fixes #N" if the PR fully fixes it, and keep the description in sync with the final diff.
- Rebase onto upstream `main` before opening a PR, and don't merge main into your branch. This file may have changed there. Follow that version.
- When updating a PR, keep rebases and other changes in separate pushes, so reviewers can see what changed:
  - To address review comments or test failures, push the fix without rebasing.
  - To rebase, push only the rebase.
- After a rebase, check your diff against what main renamed or rewrote since you branched: packages, the module path, helpers you call.

### Code

- Search the repo and its dependencies before writing a helper, cache, client, flag set or test fixture. Reuse or extend what exists, and handle a case the way nearby code already does.
- Build only what the change needs. Don't add options, interfaces, wrappers, flags, exports or API fields for later. Prefer a sensible default to a new knob. Delete code your change makes unused.
- Pass dependencies through constructors or functional options. Don't add callback hooks, setters or package globals just so tests can swap behavior.
- Fail loudly. Ignore only the specific errors you expect, like ENOENT. Don't guard against states the code can't reach, and don't fall back silently to an insecure or no-op mode.
- Comment the final state of the code. Bugs that only existed while you were writing the change, and links to planning docs outside the repository, are noise to the next reader.
- Check every claim in comments, docs and the PR description against the code. Don't claim a guarantee the code doesn't enforce. When you change behavior, update the comments, docs and scripts elsewhere that describe it. Link to existing docs instead of copying them.
- Find the root cause before fixing a symptom. Back performance claims with a measurement.
- Keep atelet and the worker runtimes opaque to each other. The only knowledge they may share is `internal/proto/ateworkerpb` and the paths in `internal/nodepath`. Using the same library code is fine; depending on how the other side is built is not. We want atelet to roll ahead of workers eventually, so don't widen that interface casually. Until v1.0.0 nothing enforces this; see Backward Compatibility.
- Actor features work on both gVisor and micro-VM, with e2e coverage for both, unless the issue or PR description scopes the change to one runtime.
- Don't assume kind, GKE, amd64 or a newer kernel than the repo already needs. Dev scripts must work on arm64 macOS. Don't change host-wide state.
- Put shell code in standalone `.sh` scripts so shellcheck can lint them. Place each script near what uses it, such as `hack/` for dev scripts or a directory under `.github/` for a workflow. Don't inline it in YAML, such as a GitHub workflow `run:` block, in JSON, or in another language's source, such as Go or Python. Have those call the script. Get a reviewer's agreement before making an exception.
- Read the `CI` environment variable only inside a named fail-closed predicate, never inline. See Testing Instructions 5.
- Before changing an API, read `docs/api-style-guide.md` and `docs/api-validation.md`.
- Pin image references you add or update by digest, and look the digest up yourself: `go run github.com/google/go-containerregistry/cmd/crane@latest digest <image>`.

### Tests

- Test new behavior in ways that help future changes. Update tests when you change existing behavior. Don't add tests just for coverage. Every test costs CI time on every PR.
- Make sure each new test fails without your change. Follow the Testing section of `docs/code-style-guide.md`; prefer adding rows to an existing table test.
- Don't use fixed sleeps or wall-clock waits. Run new tests locally with `-count=3 -shuffle=on`.

## Project Overview

Agent Substrate is a system built on top of Kubernetes which manages agent-like workloads to achieve higher scale and efficiency than Kubernetes alone can offer, with lower latency.
It takes the Kubernetes control-plane out of the critical path to achieve lower latency by mapping a larger set of “actors” (applications such as agents) onto a smaller set of ready “workers” (Kubernetes Pods).
Agent Substrate relies on the fact that agent-like applications tend to be idle most of the time to achieve heavy multiplexing.

For development, it's recommended to read the `README.md` and `CONTRIBUTING.md` in the root folder.
See `hack/install-ate.sh` and `tools/setup-gcp` for provisioning and deploying clusters and GCP resources.

## Repository Layout

```
cmd/          # One subdirectory per binary (ateapi, atelet, atenet, …)
internal/     # Shared packages, internal to this module only
pkg/          # Shared packages intended for external import
docs/         # Design docs and developer guides
hack/         # Dev/CI scripts and code generators
manifests/    # Kubernetes YAML for deploying Agent Substrate
demos/        # Self-contained example applications
benchmarking/ # Load-testing tools and workloads
tools/        # Standalone Go tools (go run ./tools/<name>) for Dev/CI
```

**Where to put new Go code, quick rules:**

| Situation | Location |
|---|---|
| Only used by one binary | `cmd/<binary>/internal/<pkg>` |
| Shared across binaries, not for external import | `internal/<pkg>` |
| Public API for external consumers | `pkg/<pkg>` |
| Public proto (control-plane gRPC API) | `pkg/proto/<name>` |
| Internal proto (atelet / ateworker) | `internal/proto/<name>` |
| Dev/CI scripts | `hack/` |
| Standalone Go dev/CI tools | `tools/<name>` with its own `go.mod` |
| Self-contained plugin that nothing in the repo depends on | `internal/plugins/<name>` with its own `go.mod`, importing only `pkg/`; see `internal/plugins/README.md` |

See `docs/dev/code-layout.md` for the full rationale and per-directory details.

## Build and Test Commands

The dev entry points are the scripts under `hack/` and the `cmd/ate-setup` installer.

### Building and Installing
- **Install from source**: `go run ./cmd/ate-setup deploy ate-system` (or the `hack/install-ate.sh` shim) builds every image with ko as part of the install; see `cmd/ate-setup/README.md`
- **Binaries**: `go build -o bin/kubectl-ate ./cmd/kubectl-ate`, `go build -o bin/ate-setup ./cmd/ate-setup`
- **Release images**: `VERSION=TAG go run ./cmd/ate-setup publish release-images --ko-docker-repo REPO` (every image a pre-built install needs, all tagged `TAG`, including the docker-built `envoy-dataplane`)

### Testing and Verification
- **Run Unit Tests**: `go test -race ./...`
- **Run E2E Tests**: `hack/run-e2e.sh` (Requires a cluster with Agent Substrate installed; `hack/run-e2e-kind.sh` for kind)
- **Run Linters and Verifiers**: `hack/verify-all.sh` (formatting, golangci-lint, boilerplate headers, licenses, go modules, generated code, and the metrics registry)

## Code Style Guidelines

- **Go Formatting**: Code must be formatted with `gofmt`. Run `hack/update/gofmt.sh` to automatically format all files before submitting changes.
- **Copyright Headers**: All files must contain appropriate copyright and license headers. See templates in `hack/boilerplate/`.
- **Modularity**: Submit small, focused Pull Requests that touch a limited part of the codebase for easier reviews and rebasing.
- **Go Modules**: Ensure `go.mod` is clean. Run `go mod tidy` if adding or removing dependencies.
- **Spelling**: American English. `golangci-lint` runs `misspell` with `locale: US`, so British spellings fail lint.

## Backward Compatibility

<!-- TODO #2004: Remove this section at the v1.0.0 release. -->

Agent Substrate makes no compatibility guarantee before v1.0.0. Do not add code to stay compatible with older clients, binaries, protos, flags, or config.
Remove or rename fields, RPCs, flags, and types outright and update every caller in the same change.
Do not add `reserved` statements to protos, deprecated aliases, fallbacks for old formats, or version negotiation.

## Commit Messages

- **Describe the change and why it was needed**: The message is read on `main` long after the PR branch is gone, so it should stand on its own.
- **No issue or PR references**: Leave out `#1234`, `Fixes #1234`, and GitHub URLs. GitHub renders them as cross-references on the linked thread, and rebases or force-pushes repeat them. Put that context in the pull request description instead, where it belongs to the review rather than to the permanent history.

## Metrics

`docs/metrics/registry/` is an [OpenTelemetry Weaver](https://github.com/open-telemetry/weaver) registry. `metrics.yaml` defines every metric instrument the ate system components emit, plus every attribute any signal uses and the permitted values of each. `events.yaml` defines the log events emitted over OTLP. Read them to find an instrument, an event, or their attributes.

If you add or rename an instrument or an event, or add an attribute, follow `docs/dev/best-practices/metrics.md`, update the registry, and run `hack/verify/metrics.sh`. `hack/verify-all.sh` runs the same check. Weaver reads the whole directory, so a new file in it is checked straight away.

Some attributes are defined in the registry but barred from metric labels: actor identity is the main one. The `note` on each says so, and `cardinality_rules` in `docs/metrics/substrate.yaml` is where the bar lives.

`docs/metrics/substrate.yaml` holds the rules Weaver cannot express: the cardinality rules, the known exceptions, and the subsystems that emit no metrics. Read `blind_spots` before you attribute a fault to a component.

See the [metric registry](docs/observability.md#the-metric-registry) section of the observability guide.

## Testing Instructions

1. Write tests for new behavior. Update tests when you change existing behavior. Consider the value of tests for future changes.
2. Ensure changes do not break existing tests.
3. Run `go test -race ./...` and `hack/verify-all.sh` locally before requesting a code review to catch common issues like missed copyright headers or formatting drift.
4. For end-to-end tests involving the actual infrastructure, ensure you have a running cluster (setup via `hack/ate-dev-env.sh.example` and `go run ./tools/setup-gcp bootstrap`).
5. A test that skips when a precondition is missing (Docker, a cluster, an artifact directory) must **fail** on it in CI, because a skip and a pass are the same exit code. Check for CI with a named predicate like `cmd/ateapi/internal/store/dockerenv.Required()` instead of an inline `os.Getenv("CI")`. Make sure the CI branch actually fails before merging. See `docs/dev/best-practices/ci-fail-closed.md`.

## Security Considerations

Agent Substrate is built for zero-trust, multi-tenant execution of untrusted actors. Preserve and extend these invariants when modifying the codebase (see `docs/security.md` and `docs/threat-model.md` for the full architecture):

- **Workload & Storage Isolation**: Actors run strictly inside `gVisor` (`cmd/ateworker-gvisor`) or `Cloud Hypervisor` microVM (`cmd/ateworker-microvm`) sandboxes; plain Linux containers are not supported. `atelet` scrubs actor directories across lifecycle transitions (`resetActorDirs`), and snapshots are stamped with `ActorSandboxRuntime` metadata and transferred via `cmd/objectstore-plugin`.
- **Component & Actor Identity (mTLS, JWTs, and JWKs)**: Inter-component communication uses mTLS backed by `cmd/podcertcontroller` (`podid`, `servicedns`, and `postgres` signers). Actor JWT signing (`internal/localjwtauthority`), verification (`internal/oidcjwt`), and key discovery (served via `cmd/ate-idp-server`) are built on the standard library's crypto packages. Extend those packages rather than adding `go-jose` or another JOSE library, so the code that decides whether a token is valid stays small and supports only the algorithms Substrate uses.
- **Control-Plane Authentication & Authorization**: `ate-api-server` checks every Control RPC against an embedded OpenFGA model (`cmd/ateapi/internal/authz/model.fga`), always enforced. Every served unary RPC needs an entry in `defaultRPCPermissions` (`registry.go`): an RPC without one is denied, and a test fails the build. Access is granted per global scope and atespace through AccessPolicy (`kubectl ate access-policy`); the install's bootstrap owner is the shared `ate-client` ServiceAccount that `kubectl ate` uses. See `docs/authorization.md` for roles and known gaps. Callers are authenticated via `SPIFFE` mTLS or configured OIDC `JWTProvider`s (`cmd/ateapi/internal/apiauthn`).
- **Network Isolation, Ingress, & Egress**: `manifests/ate-install/ate-system-networkpolicies.yaml` enforces default-deny ingress across `ate-system`, and `cmd/atecontroller` reconciles per-`WorkerPool` `NetworkPolicy` objects. Actors run in private `/30` point-to-point veth network namespaces (`internal/ateworker/actornet`) with link-local blocking, policy-enforced DNS filtering (`internal/atunnel/dns`), and zero ambient credentials. Outbound traffic traverses `atunnel` to `atenet-egress`, which enforces per-actor `EgressPolicy` and injects credentials out-of-band via `cmd/credential-provider/kubernetes-secrets` (`k8s-credential-provider`) or actor JWT minting.
