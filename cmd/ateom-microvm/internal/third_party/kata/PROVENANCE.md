# third_party/kata — vendored kata-containers sources

Source copied from [kata-containers](https://github.com/kata-containers/kata-containers)
and used by `cmd/ateom-microvm`. The upstream `LICENSE` in this directory covers
everything under it.

- **Upstream:** github.com/kata-containers/kata-containers
- **Version:** tag `3.31.0` (matches the kata runtime assets the micro-VM demo fetches;
  see `hack/microvm-assets/assemble.sh` `KATA_VER`)
- **License:** Apache-2.0 — `./LICENSE` is the upstream license verbatim (mirrored to
  `LICENSES/third_party/kata/LICENSE` by `hack/update/licenses.sh`); the per-file
  copyright headers (HyperHQ, Ant Group, Intel, Databricks) are retained verbatim in
  each source file.

## agentpb — kata-agent ttrpc protobufs

A copy of the kata-agent protocol-buffer API, used to drive the kata-agent over ttrpc
when ateom boots the guest.

- **Path:** `src/libs/protocols/protos/{agent,oci,types,csi}.proto`

The `.proto` files are byte-identical to the 3.31.0 release except for a single line:
`option go_package` is repointed to this package's import path so the generated Go
lands in-tree.

### Why a copy instead of a module dependency

kata-containers is a large module; adding it to `go.mod` to use a handful of message
types would pull a heavy, unrelated dependency tree into the main module. We instead
vendor just these four `.proto` files and their generated Go.

### Generated code

The `*.pb.go` files were generated with `protoc-gen-go v1.36.11-devel` and
`protoc v4.25.3` (see each file's header). There are intentionally **no** generated
ttrpc service stubs: `internal/kata/agentclient.go` calls the agent by string method
name via `ttrpc.Client.Call(ctx, "grpc.AgentService", "<Method>", req, resp)`, so only
the message types are needed.

### RPCs actually used

ateom drives a small subset of `AgentService`:
`CreateSandbox`, `CreateContainer`, `StartContainer`, `UpdateInterface`, `UpdateRoutes`,
`AddARPNeighbors`, `ReadStdout`, `ReadStderr`.

### Regenerating

1. Check out the matching kata-containers tag and copy
   `src/libs/protocols/protos/{agent,oci,types,csi}.proto` into `agentpb/`.
2. Set each `option go_package` to
   `github.com/agent-substrate/substrate/cmd/ateom-microvm/internal/third_party/kata/agentpb;agentpb`.
3. Regenerate with `protoc --go_out=...` (protoc-gen-go), matching the versions above.
