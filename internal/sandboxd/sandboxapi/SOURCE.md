# Protocol source

The checked-in `sandbox.proto` is the authoritative source for this experimental
protocol. It is based on containerd's Sandbox v1 API and adds the separate
Checkpoint service required by the matching experimental Kata shim.

The optional, separate Checkpoint service uses `CheckpointRequest` and
`RestoreRequest`, including `TaskRestoreMode` negotiation. The public
`github.com/containerd/containerd/api v1.12.0-beta.0` module does not contain
this exact contract yet. Do not describe this directory as an upstream
containerd API. Replace it with the released module when the contract is
published.

The Go package path is deliberately local to Substrate so this experimental
contract cannot be mistaken for a released containerd API. Run
`hack/update/codegen.sh` to regenerate both protobuf and ttrpc bindings.
