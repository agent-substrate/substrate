# Worker seccomp profiles

`substrate-worker-amd64.json` and `substrate-worker-arm64.json` are seccomp
profiles for the worker container that hosts a sandbox runtime. Substrate does
not install or apply them yet. Workers run with the `Unconfined` profile because
`RuntimeDefault` denies `pivot_root`, which runsc and virtiofsd use to build
their own sandboxes.

## What the profiles allow

Each profile returns `EPERM` for any syscall it does not list. It allows:

- The allow list of moby's default profile
  ([github.com/moby/profiles](https://github.com/moby/profiles), `seccomp/v0.2.4`):
  every syscall it allows without a capability or architecture condition, plus
  `ptrace` and `process_vm_readv`/`process_vm_writev`, which it allows on
  kernels from 4.8.
- The namespace and mount syscalls the workers need: `chroot`, `clone`,
  `clone3`, `fsconfig`, `fsmount`, `fsopen`, `mount`, `mount_setattr`,
  `move_mount`, `open_tree`, `pivot_root`, `setns`, `umount2` and `unshare`.
- `socket` for moby's address families, which exclude `AF_ALG` and `AF_VSOCK`,
  and `personality` with moby's arguments.

Compared with `Unconfined`, the profiles deny every other syscall, including
module loading, `kexec_load`, the kernel keyring, `bpf`, `perf_event_open`,
`io_uring`, `userfaultfd` and `open_by_handle_at`. runsc, virtiofsd and
cloud-hypervisor still install their own, narrower filters on top.

## Regenerating

The syscall lists are in `hack/seccomp/syscalls.go`. After changing them, run
`go generate ./hack/seccomp` (`hack/update/codegen.sh` does the same).
`go test ./hack/seccomp` fails when a committed profile is out of date.
