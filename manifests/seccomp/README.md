# Worker seccomp profiles

`substrate-worker-amd64.json` and `substrate-worker-arm64.json` define the
seccomp allow lists for `ateworker-gvisor` and `ateworker-microvm`. Workers
keep `seccompProfile: Unconfined` in the pod spec because `RuntimeDefault`
denies `pivot_root`, which runsc and virtiofsd use to build their own
sandboxes, and because changing the pod template would roll existing
WorkerPools during `ate-controller` upgrades. Instead, `ateworker` compiles
the allow list for its architecture into a seccomp-bpf filter and installs it
on itself at startup (`internal/selfseccomp`, via `SECCOMP_FILTER_FLAG_TSYNC`
before `pid1.RunIfPID1`), so PID 1, `ateworker`, runsc, virtiofsd and
cloud-hypervisor all inherit it. Processes started directly via `kubectl exec`
do not inherit this filter.

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
`go generate ./hack/seccomp ./hack/selfseccomp` (`hack/update/codegen.sh` runs
both). `go test ./hack/seccomp ./hack/selfseccomp` fails when a committed
profile or syscall table is out of date.
