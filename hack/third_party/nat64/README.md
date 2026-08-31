# kubernetes-sigs/nat64

The NAT64 agent [`hack/create-kind-cluster.sh`](../../create-kind-cluster.sh)
deploys when `IPV6_DNS64_PREFIX` is set. To use it, see
[docs/dev/ipv6-local.md](../../../docs/dev/ipv6-local.md).

| | |
|---|---|
| Upstream | https://github.com/kubernetes-sigs/nat64 |
| License | Apache-2.0 (`LICENSE`) |
| Pin | `VERSION` — upstream commit and image digest |
| Regenerate | `hack/update/nat64.sh` |

`install.yaml` is generated: edit `VERSION` and re-run the update script rather
than the manifest. The script is deterministic for a given pin, so
`hack/update/nat64.sh && git diff --exit-code` is a drift check.

The only change from upstream's bytes is that the agent image is pinned by
digest. It is vendored rather than fetched by URL because upstream ships no
release asset and `install.yaml` at tag `v0.4.1` still names the `v0.2.1`
image — the copy matching the release exists only on a moving branch.
