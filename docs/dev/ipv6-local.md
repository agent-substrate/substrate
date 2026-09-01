# Running an IPv6-only cluster locally

## Overview

```sh
IP_FAMILY=ipv6 ./hack/create-kind-cluster.sh
```

That is the whole setup on a host with IPv6 egress of its own, which includes a
Lima VM on macOS.

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

## Troubleshooting

| Symptom | Cause |
|---|---|
| `the 'kind' Docker network has no IPv6` | Daemon IPv6 off; see [Prerequisites](#prerequisites). |
| `curl -6 <host>` fails but the cluster works | A resolver returning no AAAA, not an egress fault; see [Measuring whether you have IPv6 egress](#measuring-whether-you-have-ipv6-egress). |
| `certificate is valid for ... ::1, not 127.0.0.1` | kubectl on macOS against the Lima-forwarded port; see [Reaching the cluster from macOS](#reaching-the-cluster-from-macos). |
