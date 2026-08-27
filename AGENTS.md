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

See `docs/dev/code-layout.md` for the full rationale and per-directory details.

## Build and Test Commands

Agent Substrate uses a `Makefile` for its build and test tasks.

### Building
- **Binaries**: `make build` (builds images and `kubectl-ate`) or `make build-atectl`
- **Images**: `make build-images` (uses ko to build container images)
- **Demos**: `make build-demos`

### Testing and Verification
- **Run Unit Tests**: `make test`
- **Run E2E Tests**: `make e2e` (Requires GCP cluster setup and built images)
- **Run Linters and Verifiers**: `make verify` (Includes `go vet` and checks for formatting, boilerplate headers, licenses, and go modules)

## Code Style Guidelines

- **Go Formatting**: Code must be formatted with `gofmt`. Run `make fmt` to automatically format all files before submitting changes.
- **Copyright Headers**: All files must contain appropriate copyright and license headers. See templates in `hack/boilerplate/`.
- **Modularity**: Submit small, focused Pull Requests that touch a limited part of the codebase for easier reviews and rebasing.
- **Go Modules**: Ensure `go.mod` is clean. Run `go mod tidy` if adding or removing dependencies.
- **Comments**: Keep them brief and to the point. Comment the final state of the code, not the path taken to it — a problem that only existed partway through writing the change is noise to the next reader, as is a pointer to a scratch or planning file that isn't in the repository.
- **Spelling**: American English. `golangci-lint` runs `misspell` with `locale: US`, so British spellings fail lint.

## Commit Messages

- **Describe the change and why it was needed**: The message is read on `main` long after the PR branch is gone, so it should stand on its own.
- **No issue or PR references**: Leave out `#1234`, `Fixes #1234`, and GitHub URLs. GitHub renders them as cross-references on the linked thread, and rebases or force-pushes repeat them. Put that context in the pull request description instead, where it belongs to the review rather than to the permanent history.

## Metrics

`docs/metrics/registry/metrics.yaml` is an [OpenTelemetry Weaver](https://github.com/open-telemetry/weaver) registry. It defines every metric instrument the ate system components emit, and the permitted values of each label. Read it to find an instrument or its labels.

If you add or rename an instrument, or add a metric label, update it and run `hack/verify/metrics.sh`. `make verify` runs the same check.

`docs/metrics/substrate.yaml` holds the rules Weaver cannot express: the cardinality rules, the known exceptions, and the subsystems that emit no metrics. Read `blind_spots` before you attribute a fault to a component.

See the [metric registry](docs/observability.md#the-metric-registry) section of the observability guide.

## Testing Instructions

1. Write tests for all new code. We will not merge code that lacks tests.
2. Ensure changes do not break existing tests.
3. Run `make verify` locally before requesting a code review to catch common issues like missed copyright headers or formatting drift.
4. For end-to-end tests involving the actual infrastructure, ensure you have a running cluster (setup via `hack/ate-dev-env.sh.example` and `go run ./tools/setup-gcp bootstrap`).

## Security Considerations

The security story for Substrate is very early and many features are missing.
However! Take care to respect security best practices when writing code in order to improve Substrate's security over time.
The following is what Substrate currently offers.
Keep this up to date when updating AGENTS.md.

- **Workload Isolation**: The project uses `gVisor` (`runsc`) for sandboxing and security isolation of workloads on pods.

For future plans for security, reference `docs/roadmap.md`.
