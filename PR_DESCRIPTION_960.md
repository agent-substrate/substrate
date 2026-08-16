## ateomnet: restrict actor masquerade to cluster DNS resolver

Fixes: https://github.com/agent-substrate/substrate/issues/960

### Problem

`InstallActorNftablesRules` installs a postrouting masquerade that matches
all traffic from the actor veth IP — effectively NATting every packet the
actor sends, including traffic that should be dropped by policy.

### Fix

When a cluster DNS resolver IP is available (read from the pod's
/etc/resolv.conf), the postrouting masquerade is now restricted to a single
nftables rule matching all four conditions:

- source = actor veth IP
- protocol = UDP
- destination = cluster resolver IP
- destination port = 53

All other non-tunneled actor egress is no longer masqueraded and will be
dropped by the kernel's default forward policy.

When no IPv4 resolver is available (empty /etc/resolv.conf, IPv6-only
resolver, or missing file), the legacy broad masquerade is preserved for
backward compatibility.

### New helpers

- `readDNSResolver()` — parses /etc/resolv.conf, returns first IPv4 nameserver
- `UDPProtocol()` — nftables L4 proto match for UDP (IPPROTO_UDP = 17)
- `IPDestEqual(ip)` — IPv4 destination address match (header offset 16)

### Files changed

- `internal/ateomnet/net.go` — signature extended, restricted masquerade, helpers

### Verification

- `gofmt -l` clean
- `go build ./internal/ateomnet/` passes
- `go test ./internal/ateomnet/ -count=1` passes (all tests)