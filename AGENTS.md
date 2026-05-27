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

**Where to put new Go code — quick rules:**

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

## Testing Instructions

1. Write tests for all new code. We will not merge code that lacks tests.
2. Ensure changes do not break existing tests.
3. Run `make verify` locally before requesting a code review to catch common issues like missed copyright headers or formatting drift.
4. For end-to-end tests involving the actual infrastructure, ensure you have a running cluster (setup via `hack/ate-dev-env.sh.example` and `go run ./tools/setup-gcp --all`).

## Security Considerations

The security story for Substrate is very early and many features are missing.
However! Take care to respect security best practices when writing code in order to improve Substrate's security over time.
The following is what Substrate currently offers.
Keep this up to date when updating AGENTS.md.

- **Workload Isolation**: The project uses `gVisor` (`runsc`) for sandboxing and security isolation of workloads on pods. A temporary gVisor patch might be required (check the README instructions).

For future plans for security, reference `docs/roadmap.md`.
