# Code Layout

This document explains the top-level directory structure of this repository and
the rationale behind where code lives. When in doubt about where to put a new
file, read this first.

## Top-Level Directories

```
substrate/
├── cmd/          # Binary entry points (one subdirectory per binary)
├── internal/     # Shared Go packages, not importable outside this module
├── pkg/          # Shared Go packages, intentionally importable by external users
├── docs/         # Design documents and developer guides
├── hack/         # Scripts used during development, CI, and cluster management
├── manifests/    # Kubernetes manifests for deploying Substrate
├── demos/        # Self-contained demo applications
├── benchmarking/ # Load testing tools and workloads
├── tools/        # Standalone Go tools (runnable with `go run ./tools/<name>`)
└── bin/          # Vendored or pinned tool binaries (e.g. protoc)
```

## Decision Rules

### `pkg/` vs `internal/`

**`pkg/`** is for Go packages with an external API contract — code that users or
third-party tools may import directly from
`github.com/agent-substrate/substrate/pkg/...`. Putting a package here signals
a commitment to backwards-compatibility and discoverability.

**`internal/`** is for Go packages that are shared across multiple binaries
within this module but are not part of any external API. The Go toolchain
enforces that nothing outside `github.com/agent-substrate/substrate` can import
these.

> **Use `pkg/` sparingly.** Once the project reaches GA, any exported type,
> function, or field in `pkg/` becomes subject to a compatibility guarantee —
> removing or renaming it is a breaking change for external consumers. Prefer
> keeping new code in `internal/` until its API has stabilized. The bar for
> adding to `pkg/` is: "I am confident external users need to import this, and I
> am prepared to maintain its API indefinitely."

### `cmd/<binary>/`

Each subdirectory of `cmd/` corresponds to one compiled binary:

| Directory            | Binary / Purpose                                      |
|----------------------|-------------------------------------------------------|
| `cmd/ateapi`         | Control-plane API server (gRPC)                       |
| `cmd/atecontroller`  | Kubernetes controller for WorkerPool/ActorTemplate    |
| `cmd/atelet`         | Node supervisor (DaemonSet)                           |
| `cmd/atenet`         | Network proxy / Envoy external-processing server      |
| `cmd/ateom-gvisor`   | In-pod gVisor container image entry point             |
| `cmd/ateom-microvm`  | In-pod kata + cloud-hypervisor micro-VM container image entry point |
| `cmd/kubectl-ate`    | `kubectl` plugin for interacting with Substrate       |
| `cmd/podcertcontroller` | Controller that issues pod TLS certificates        |

Each `cmd/<binary>/` contains:
- `main.go` — the entry point, kept thin (flag parsing, wiring, signal handling)
- `internal/` — packages private to that binary; **not** shared with other binaries

If a package is only used by one binary, put it under `cmd/<binary>/internal/`.
If two or more binaries share a package, move it to `internal/` (or `pkg/` if
external consumers need it too).

### `hack/`

Scripts for humans: setting up dev environments, running verifiers, generating
code, managing clusters. Nothing in `hack/` is imported as a Go package. Prefer
shell scripts here; for more complex automation, put a Go tool in `tools/`.

### `tools/`

Dev/CI tools written in Go. These are build-time or
operations tools that are part of the repository but not compiled into any
shipped binary. Example: `tools/setup-gcp` provisions GCP resources.

Each tool must have its own dedicated `go.mod` (and `go.sum`).

### `manifests/`

Kubernetes YAML for deploying Substrate components.

### `demos/`

Example applications showing how to build on Agent Substrate.

### `benchmarking/`

Load-testing tooling and benchmark workloads.

## Placement Checklist

When adding a new Go package, ask:

1. **Is it only used by one binary?** → `cmd/<binary>/internal/<pkg>`
2. **Is it shared by multiple binaries but not meant for external import?** → `internal/<pkg>`
3. **Is it a deliberately public API for external users?** → `pkg/<pkg>`
4. **Is it a protobuf-generated package?**
   - Public gRPC API (control plane) → `pkg/proto/<name>`
   - Internal gRPC API (atelet, ateom) → `internal/proto/<name>`
5. **Is it a script or dev tool?** → `hack/` (shell) or `tools/<name>` (Go)
