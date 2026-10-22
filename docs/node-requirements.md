# Node requirements

What a cluster node needs to run Agent Substrate workers. GKE node images from
COS 121 (kernel 6.6 LTS) on meet all of it, and so does kind on a current Linux
Docker host.

| Requirement | Why |
|---|---|
| Linux 6.7, or a 6.6 LTS kernel with the overlayfs `lowerdir+` backport (in 6.6.y since March 2024) | Every actor's rootfs is an overlay mounted through the new mount API, one `lowerdir+` option per image layer. |
| cgroup v2 only (unified hierarchy), with pods in a private cgroup namespace | Workers delegate their pod cgroup to give each actor a leaf. A hybrid hierarchy or the host cgroup namespace fails at worker startup. |
| nf_tables, with IPv6 enabled | Each actor's network namespace is fenced by nftables rules in both families, and the gateways bind `::`. |
| `/dev/net/tun`, and `/dev/kvm` for the micro-VM class | Tap devices for the guest; KVM for cloud-hypervisor. Cloud VMs need nested virtualization. |
| `/var/lib/ate` on one filesystem and not mounted `noexec` | The caches hand out hard links into actor directories, and atelet runs `runsc` from there. See [node storage](node-storage.md). |
| Unconfined AppArmor and seccomp allowed for worker pods | runsc and the workers mount inside the pod and pivot_root, which the default profiles deny. |

Nodes that host hundreds of sandboxes also need larger neighbor-table and
inotify limits; see the sysctls in [tools/setup-gcp](../tools/setup-gcp/README.md).
