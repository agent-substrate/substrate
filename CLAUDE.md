# CLAUDE.md

Guidance for Claude Code working in this repository.

> **Canonical agent docs live in [`AGENTS.md`](AGENTS.md).** Read it first — it
> covers the project overview, repository layout, build/test commands, code
> style, testing rules, and security considerations. This file only adds a quick
> reference and Claude Code–specific notes; keep both in sync when either changes.

## What this is

Agent Substrate (`github.com/agent-substrate/substrate`) is a Go system on top of
Kubernetes that multiplexes many idle "actors" (agent-like workloads) onto a small
pool of warm "worker" Pods, taking the Kubernetes control plane out of the latency
critical path. Actors are suspended/resumed via gVisor (`runsc`) checkpoint &
restore, with state stored in ValKey/Redis (dynamic instance state) and GCS
(snapshots). See [`docs/architecture.md`](docs/architecture.md) for the full design
(note: much of it is aspirational/not yet implemented) and
[`README.md`](README.md) for quickstart.

## Common commands

| Task | Command |
|---|---|
| Build everything (images + CLI) | `make build` |
| Build the `kubectl-ate` CLI only | `make build-atectl` |
| Build container images (via `ko`) | `make build-images` |
| Run unit tests | `make test` (`go test ./...`) |
| Format Go code | `make fmt` |
| Lint | `make lint` |
| Run all verifiers (tests, fmt, boilerplate, licenses, go.mod) | `make verify` |
| End-to-end tests (needs cluster + images) | `make e2e` |

**Always run `make verify` before considering a change done** — CI checks gofmt,
copyright headers, license accounting, and `go mod` cleanliness, which are easy to
miss.

## Repository layout (where new code goes)

```
cmd/          # One subdirectory per binary (ateapi, atelet, atenet, ateom-gvisor, …)
internal/     # Shared packages, internal to this module only
pkg/          # Shared packages intended for external import (use sparingly)
docs/         # Design docs and developer guides
hack/         # Dev/CI shell scripts and code generators
tools/        # Standalone Go tools (each with its own go.mod), e.g. tools/setup-gcp
manifests/    # Kubernetes YAML for deploying Substrate
demos/        # Self-contained example applications
benchmarking/ # Load-testing tools and workloads
```

| Situation | Location |
|---|---|
| Only used by one binary | `cmd/<binary>/internal/<pkg>` |
| Shared across binaries, not for external import | `internal/<pkg>` |
| Public API for external consumers | `pkg/<pkg>` |
| Public proto (control-plane gRPC API) | `pkg/proto/<name>` |
| Internal proto (atelet / ateom) | `internal/proto/<name>` |

See [`docs/dev/code-layout.md`](docs/dev/code-layout.md) for the full rationale.

## The binaries

- `cmd/ateapi` — control-plane gRPC API server (actor/worker lifecycle, scheduler).
- `cmd/atelet` — node-level DaemonSet supervisor; coordinates snapshotting and GCS transfers.
- `cmd/atecontroller` — reconciles `WorkerPool` and `ActorTemplate` CRDs.
- `cmd/atenet` — DNS + Envoy routing + proxy; resumes actors on inbound traffic.
- `cmd/ateom-gvisor` — in-pod helper running `runsc` checkpoint/restore.
- `cmd/podcertcontroller` — pod TLS certificate signer (polyfill for upstream Kubernetes).
- `cmd/kubectl-ate` — CLI plugin. See `cmd/kubectl-ate/README.md`.

## Conventions & expectations

- **Tests required.** New code without tests will not be merged; don't break existing tests.
- **Copyright headers** on every source file — templates in `hack/boilerplate/`.
- **Small, focused PRs.** This project moves fast and rebases often.
- **`gofmt`** is mandatory; run `make fmt`. Go version is pinned in `go.mod` (currently 1.26.1).
- **Run `go mod tidy`** when changing dependencies.

## Local dev quickstart

```shell
hack/create-kind-cluster.sh                       # kind cluster + local registry
hack/install-ate-kind.sh --deploy-ate-system      # install ate, valkey, rustfs
hack/install-ate-kind.sh --deploy-demo-counter    # install counter demo
go install ./cmd/kubectl-ate
```

For GKE: copy `hack/ate-dev-env.sh.example` to `.ate-dev-env.sh`, then
`go run ./tools/setup-gcp --all` and `hack/install-ate.sh --deploy-ate-system`.

## Security

Security is early-stage. Isolation relies on gVisor (`runsc`) — Substrate currently
needs a `runsc` build with `--allow-connected-on-save`. Internal traffic uses mTLS
with short-lived certs; routing is identity-aware via the DNS scheme
`<id>.actors.resources.substrate.ate.dev`. See the Security sections of
[`AGENTS.md`](AGENTS.md) and [`docs/architecture.md`](docs/architecture.md), and
[`docs/roadmap.md`](docs/roadmap.md) for planned work.
