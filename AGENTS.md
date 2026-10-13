# Agent Substrate

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
| Internal proto (atelet / ateom) | `internal/proto/<name>` |
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
- **Comments**: Keep them brief and to the point. Comment the final state of the code, not the path taken to it — a problem that only existed partway through writing the change is noise to the next reader, as is a pointer to a scratch or planning file that isn't in the repository.
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

1. Write tests for all new code. We will not merge code that lacks tests.
2. Ensure changes do not break existing tests.
3. Run `go test -race ./...` and `hack/verify-all.sh` locally before requesting a code review to catch common issues like missed copyright headers or formatting drift.
4. For end-to-end tests involving the actual infrastructure, ensure you have a running cluster (setup via `hack/ate-dev-env.sh.example` and `go run ./tools/setup-gcp bootstrap`).
5. A test that skips when a precondition is missing — Docker, a cluster, an artifact directory — must **fail** on it in CI, because a skip and a pass are the same exit code. Resolve strictness through a named predicate rather than an inline `os.Getenv("CI")`; `cmd/ateapi/internal/store/dockerenv.Required()` is the reference implementation. Prove the strict branch red before merging: a guard only ever observed passing is not known to guard anything. See `docs/dev/best-practices/ci-fail-closed.md`.

## Security Considerations

The security story for Substrate is very early and many features are missing.
However! Take care to respect security best practices when writing code in order to improve Substrate's security over time.
The following is what Substrate currently offers.
Keep this up to date when updating AGENTS.md.

- **Workload Isolation**: The project uses `gVisor` (`runsc`) for sandboxing and security isolation of workloads on pods.
- **JWTs and JWKs**: Signing (`internal/localjwtauthority`), verification (`cmd/ateapi/internal/oidcjwt`), and JWK set publishing (`internal/oidcdiscovery`) are built on the standard library's crypto packages. Extend those packages rather than adding go-jose or another JOSE library, so the code that decides whether a token is valid stays small and supports only the algorithms Substrate uses.
- **Ingress Authentication**: `atenet-router --ingress-auth-mode=static-mtls` requires clients to present a SPIFFE certificate signed by `--ingress-client-ca-file` whose ID is in `--ingress-allowed-spiffe-ids`; that list is the whole authorization policy, so any listed client may reach any Actor. The default, `deprecated-insecure`, checks a certificate only when a client presents one, and also serves plaintext listeners that accept any client. Install with it via `ate-setup --ingress-auth-mode=static-mtls` (`manifests/ate-install/components/router-static-mtls`); the allowlist lives in `manifests/ate-install/atenet-router.yaml`.

For future plans for security, reference `docs/roadmap.md`.
