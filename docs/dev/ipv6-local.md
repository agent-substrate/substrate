# Running an IPv6-only cluster locally

## Overview

```sh
IP_FAMILY=ipv6 ./hack/create-kind-cluster.sh
```

That is the whole setup on a host with IPv6 egress of its own, which includes a
Lima VM on macOS.

A host without it — a CI runner, most corporate networks — gets a cluster that
comes up clean and then reaches nothing; set `IPV6_DNS64_PREFIX` as well and the
script deploys a translator, see
[When you also need NAT64](#when-you-also-need-nat64).

[Measuring whether you have IPv6 egress](#measuring-whether-you-have-ipv6-egress)
tells you whether yours qualifies. On macOS the cluster runs inside the VM, so
`kubectl` from the host needs one extra step:
[Reaching the cluster from macOS](#reaching-the-cluster-from-macos).

## Prerequisites

**The Docker daemon needs IPv6.** The script checks this and prints the fix, so
you can let it fail rather than checking up front. On Linux, add to
`/etc/docker/daemon.json` and restart dockerd:

```json
{"ipv6": true, "ip6tables": true}
```

## Measuring whether you have IPv6 egress

Keep DNS out of the path. `curl -6 <hostname>` conflates the two: a resolver
that returns no AAAA fails identically to a host that cannot route, and the
error text (`Could not resolve host`) names neither. Ping and fetch a literal:

```sh
ping6 -c2 2001:4860:4860::8888
# Egress:    2 packets transmitted, 2 received, 0% packet loss
# No egress: ping6: connect: Network unreachable

curl -6 -sS -m 5 -o /dev/null -w '%{http_code}\n' \
  'http://[2607:f8b0:4004:c1b::5e]/generate_204'
# Egress:    204
# No egress: curl: (7) Failed to connect ..., then 000
```

Both succeed and you have IPv6 egress; either fails and you do not. The wording
of the failures varies by platform — what matters is that they come back
immediately rather than timing out, which is what a firewalled but routable path
looks like.

Nor does the absence of a global address mean the absence of egress — a Lima
guest holds only a ULA on `lima0` and reaches the v6 internet through vzNAT.

Without egress, [NAT64](#when-you-also-need-nat64) is the way out.

## When you also need NAT64

`IPV6_DNS64_PREFIX` deploys the
[kubernetes-sigs/nat64](https://github.com/kubernetes-sigs/nat64) agent and
points cluster DNS at the prefix it translates. Two cases need it:

- **A host with no IPv6 egress at all**, which is the CI runner case. Without
  NAT64 the cluster comes up clean and then reaches nothing: every image pull
  and outbound call from a pod times out.
- **Reaching IPv4-only destinations.** A plain IPv6-only cluster reaches
  anything with a AAAA record, which today is most things — but not, for
  example, `github.com`.

It is off by default because it routes *every* external name through the
translator, including names whose AAAA records already work.

### Forwarding must be on

The script does *not* check this. Without it the agent deploys, the rollout
succeeds, the restart-count check passes, and the run fails at the connectivity
probe with nothing pointing at the cause:

```sh
sudo sysctl -w net.ipv4.ip_forward=1
sudo sysctl -w net.ipv6.conf.all.forwarding=1
```

> [!WARNING]
> Forwarding also stops the kernel accepting router advertisements, which can
> drop the default route on a machine with real IPv6. Set it in the VM, not on
> your Mac.

### In CI

The IPv6-only e2e job configures nothing itself — it sets two variables and
lets this script build the cluster:

```yaml
env:
  IP_FAMILY: ipv6
  IPV6_DNS64_PREFIX: '64:ff9b::/96'
```

plus the two sysctls above, ordered *after* the dockerd restart that enables
IPv6, because that restart rebuilds the chains they affect.

### On Linux

```sh
sudo sysctl -w net.ipv4.ip_forward=1
sudo sysctl -w net.ipv6.conf.all.forwarding=1

IP_FAMILY=ipv6 IPV6_DNS64_PREFIX='64:ff9b::/96' ./hack/create-kind-cluster.sh
```

### On Apple Silicon

The published agent images do not run on arm64: every tag ships an x86-64
binary in the arm64 slot of its image index
([upstream issue](https://github.com/kubernetes-sigs/nat64/issues/103)), so the
agent exits with `exec format error` and nothing is translated. Build one and
push it to the local registry:

```sh
git clone https://github.com/kubernetes-sigs/nat64 /tmp/nat64
docker build --build-arg GOARCH=arm64 -t localhost:5001/nat64:local /tmp/nat64
docker push localhost:5001/nat64:local
```

Push to the registry rather than `kind load`: the script deletes and recreates
the cluster on every run, which drops a loaded image, while the registry
container outlives it. The registry is created by the cluster script, so on a
first run let it create the cluster once, then build, push, and re-run.

```sh
IP_FAMILY=ipv6 \
IPV6_DNS64_PREFIX='64:ff9b::/96' \
NAT64_IMAGE=localhost:5001/nat64:local \
  ./hack/create-kind-cluster.sh
```

## Reaching the cluster from macOS

Everything that needs Docker — `ko`, kind, the e2e harness — runs inside the
VM, so that is the primary path. For `kubectl` from macOS there is one wrinkle:
kind publishes the API server on the guest's `[::1]`, Lima forwards that to the
*host's* `127.0.0.1`, and the apiserver certificate has no `127.0.0.1` SAN.
Rewrite the address and verify against the `::1` SAN instead of turning
verification off:

```sh
limactl shell docker-nested -- \
  bash -lc 'cd <checkout> && ./hack/kind.sh get kubeconfig --name kind' \
  | sed 's|https://\[::1\]:|https://127.0.0.1:|' > /tmp/kind-ipv6.kubeconfig

KUBECONFIG=~/.kube/config:/tmp/kind-ipv6.kubeconfig \
  kubectl config view --flatten > /tmp/merged && cp /tmp/merged ~/.kube/config
kubectl config set-cluster kind-kind --tls-server-name='::1'
```

The forwarded port changes every time the cluster is recreated, so this has to
be redone after each run.

## Verifying

With `IPV6_DNS64_PREFIX` set, the script proves both halves meet before it
exits — it fetches `NAT64_PROBE_URL` from a pod and prints `NAT64 is
translating.` To look at what it built:

```sh
kubectl -n kube-system get pod -l app=nat64
kubectl -n kube-system get cm coredns -o jsonpath='{.data.Corefile}'
```

## Troubleshooting

| Symptom | Cause |
|---|---|
| `the 'kind' Docker network has no IPv6` | Daemon IPv6 off; see [Prerequisites](#prerequisites). |
| `a pod could not reach ... through 64:ff9b::/96` | Forwarding sysctls not set on the host. |
| `exec format error` in the agent log | arm64 node running a published image; set `NAT64_IMAGE`. |
| `curl -6 <host>` fails but the cluster works | A resolver returning no AAAA, not an egress fault; see [Measuring whether you have IPv6 egress](#measuring-whether-you-have-ipv6-egress). |
| In-cluster Service names stop resolving | The Corefile's `dns64` block was merged into the cluster-zone block. It has to stay separate — `dns64` synthesizes AAAA from A, and an AAAA-only ClusterIP synthesizes to nothing. |
| Only names *without* AAAA records get translated | `translate_all` is not in effect. The prefix belongs inside the `dns64 { }` block; on the `dns64` line it parses and the block is silently dropped. |
| `certificate is valid for ... ::1, not 127.0.0.1` | kubectl on macOS against the Lima-forwarded port; see [Reaching the cluster from macOS](#reaching-the-cluster-from-macos). |
