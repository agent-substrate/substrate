//go:build linux

// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package actornet

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/netip"
	"sync"

	"go.ate.dev/substrate/internal/atunnel/dns"
	"go.ate.dev/substrate/internal/netns"
	"go.ate.dev/substrate/internal/nodepath"

	"github.com/google/nftables"
	"github.com/google/nftables/binaryutil"
	"github.com/google/nftables/expr"
	"github.com/vishvananda/netlink"
	"golang.org/x/sys/unix"
)

// SandboxNetwork holds a sandbox's private network namespace.
type SandboxNetwork struct {
	// ActorUID is the Actor resource's UID. It names the namespace, so a
	// teardown can find it again.
	ActorUID string
	// NetNS holds the runtime interfaces, default gateway, DNS relay, and
	// atunnel sockets. The actor's network stack lives inside its sandbox.
	NetNS netns.Handle
}

func (n *SandboxNetwork) holdsNetNS() bool { return n.NetNS > 0 }

// SandboxNetworkConfig describes one actor's private networking.
type SandboxNetworkConfig struct {
	ActorUID string

	// Veth connects gVisor's network stack to the kernel-owned gateway.
	// MicroVMs use a tap in a single namespace instead.
	Veth bool

	// EgressPort is where atunnel serves this actor. Every TCP connection the
	// actor makes is redirected to it, whatever port it was aimed at.
	EgressPort uint16

	// DNSPolicy supplies the egress policy the actor's DNS queries are checked
	// against. Nil refuses every query.
	DNSPolicy dns.PolicySource
}

// SetupSandboxNetwork creates isolated networking with fixed sandbox addresses.
// gVisor uses a veth pair in one namespace. ActivateGateway completes its setup
// after runsc imports eth0. MicroVMs use a tap in one namespace.
// nftables at the sandbox's default gateway redirect outbound TCP to atunnel, preserving
// SO_ORIGINAL_DST. Traffic to the gateway address is not redirected, so DNS
// over UDP and TCP reaches the relay's own sockets.
func SetupSandboxNetwork(ctx context.Context, cfg SandboxNetworkConfig) (_ *SandboxNetwork, retErr error) {
	actorUID := cfg.ActorUID
	if actorUID == "" {
		return nil, fmt.Errorf("actornet: actor UID is required")
	}

	actorNSName := nodepath.ActorNetNSName(actorUID)
	actorNS, err := netns.CreateNamed(actorNSName)
	if err != nil {
		return nil, fmt.Errorf("while creating the actor netns %s: %w", actorNSName, err)
	}
	defer func() {
		if retErr != nil {
			actorNS.Close()
			_ = netns.RemoveNamed(actorNSName)
		}
	}()
	if err := netns.Do(ctx, actorNS, prepareNamespace); err != nil {
		return nil, err
	}

	if cfg.Veth {
		if err := setupVethPair(ctx, actorNS); err != nil {
			return nil, err
		}
	}
	if err := installEgressRules(actorNS, cfg.EgressPort); err != nil {
		return nil, err
	}

	return &SandboxNetwork{
		ActorUID: actorUID,
		NetNS:    actorNS,
	}, nil
}

// prepareNamespace pins sandboxSysctls and brings up lo. It must run before
// the namespace has any link but lo: a link copies conf/default when it is
// created, and a later write to conf/default does not reach links whose
// settings the node had set explicitly.
func prepareNamespace(context.Context) error {
	if err := netns.SetSysctls(sandboxSysctls...); err != nil {
		return err
	}
	// lo carries the namespace's own traffic. gVisor builds the actor's
	// loopback from it.
	return linkUp("lo")
}

// sandboxSysctls are set in every sandbox namespace, in this order. A new
// namespace copies its IPv4 conf/all and conf/default from the node, where
// kube-proxy and CNI plugins set their own values, and takes the kernel's
// IPv6 defaults. Neither suits a link with an untrusted actor on the other end.
var sandboxSysctls = []netns.Sysctl{
	// atunnel answers the actor's DNS on 53, and the worker holds no
	// CAP_NET_BIND_SERVICE. Per-namespace, so it grants nothing outside.
	{Key: "net/ipv4/ip_unprivileged_port_start", Value: "0"},
	// Nothing in a sandbox namespace routes. The forward chain in
	// installEgressRules backs this up.
	{Key: "net/ipv4/conf/all/forwarding", Value: "0"},
	{Key: "net/ipv4/conf/default/forwarding", Value: "0"},
	{Key: "net/ipv6/conf/all/forwarding", Value: "0"},
	{Key: "net/ipv6/conf/default/forwarding", Value: "0"},
	// conf/all/accept_redirects defaults to 1 and the kernel sets it back to
	// 1 whenever conf/all/forwarding changes to 0, so it comes after
	// forwarding. With forwarding off the kernel takes ICMP redirects when
	// either conf/all or the interface allows them.
	{Key: "net/ipv4/conf/all/accept_redirects", Value: "0"},
	{Key: "net/ipv4/conf/default/accept_redirects", Value: "0"},
	// kube-proxy's iptables mode enables this on nodes. It would deliver
	// actor packets addressed to 127.0.0.0/8 to sockets in the gateway
	// namespace.
	{Key: "net/ipv4/conf/all/route_localnet", Value: "0"},
	{Key: "net/ipv4/conf/default/route_localnet", Value: "0"},
	// Drop packets from the actor that claim a gateway address as their
	// source, carry a source route, or come from outside the /30.
	{Key: "net/ipv4/conf/all/accept_local", Value: "0"},
	{Key: "net/ipv4/conf/default/accept_local", Value: "0"},
	{Key: "net/ipv4/conf/all/accept_source_route", Value: "0"},
	{Key: "net/ipv4/conf/default/accept_source_route", Value: "0"},
	{Key: "net/ipv4/conf/all/rp_filter", Value: "1"},
	{Key: "net/ipv4/conf/default/rp_filter", Value: "1"},
	// Don't take router advertisements or redirects from the actor. IPv6
	// reads both only from the link's own copy, which it takes from
	// conf/default when the link is created.
	{Key: "net/ipv6/conf/default/accept_ra", Value: "0"},
	{Key: "net/ipv6/conf/default/accept_redirects", Value: "0"},
}

// setupVethPair leaves atside addressless so runsc imports only eth0.
func setupVethPair(ctx context.Context, actorNS netns.Handle) error {
	return netns.Do(ctx, actorNS, func(context.Context) error {
		veth := &netlink.Veth{
			LinkAttrs: netlink.LinkAttrs{Name: gatewayVethName},
			PeerName:  ActorVethName,
		}
		if err := netlink.LinkAdd(veth); err != nil {
			return fmt.Errorf("while creating the veth pair: %w", err)
		}
		// A link-local IPv6 address would make runsc import atside too.
		if err := netns.SetSysctls(netns.Sysctl{Key: "net/ipv6/conf/" + gatewayVethName + "/disable_ipv6", Value: "1"}); err != nil {
			return err
		}
		if err := linkUp(gatewayVethName); err != nil {
			return err
		}
		eth0, err := netlink.LinkByName(ActorVethName)
		if err != nil {
			return err
		}
		if err := netlink.AddrReplace(eth0, ActorVethAddr); err != nil {
			return fmt.Errorf("while assigning the actor address: %w", err)
		}
		if err := netlink.LinkSetUp(eth0); err != nil {
			return err
		}
		return netlink.RouteReplace(&netlink.Route{LinkIndex: eth0.Attrs().Index, Gw: ActorVethGwIP})
	})
}

// ActivateGateway assigns the Linux gateway after runsc has imported eth0.
// Call it after starting or restoring the empty sandbox, before its containers.
func (n *SandboxNetwork) ActivateGateway(ctx context.Context) error {
	return netns.Do(ctx, n.NetNS, func(context.Context) error {
		eth0, err := netlink.LinkByName(ActorVethName)
		if err != nil {
			return err
		}
		addrs, err := netlink.AddrList(eth0, netlink.FAMILY_V4)
		if err != nil {
			return err
		}
		if len(addrs) != 0 {
			return fmt.Errorf("runsc has not removed eth0's IPv4 addresses")
		}
		gateway, err := netlink.LinkByName(gatewayVethName)
		if err != nil {
			return err
		}
		return netlink.AddrReplace(gateway, HostVethAddr)
	})
}

func linkUp(name string) error {
	link, err := netlink.LinkByName(name)
	if err != nil {
		return err
	}
	return netlink.LinkSetUp(link)
}

// installEgressRules rejects link-local traffic before it reaches the egress PEP,
// then redirects other TCP egress to atunnel. The actor's own /30 is excluded
// from both rules so ingress replies and DNS reach their local sockets.
func installEgressRules(ns netns.Handle, egressPort uint16) error {
	const (
		icmpNetUnreachable                          = 0
		icmpDestinationAdministrativelyProhibited   = 13
		icmpv6DestinationAdministrativelyProhibited = 1
	)

	if egressPort == 0 {
		return fmt.Errorf("actornet: atunnel egress port is required")
	}
	c, err := nftables.New(nftables.WithNetNSFd(int(ns)))
	if err != nil {
		return fmt.Errorf("while opening nftables in the actor namespace: %w", err)
	}
	defer func() { _ = c.CloseLasting() }()

	var ipv4Table *nftables.Table
	for _, family := range []struct {
		tableFamily nftables.TableFamily
		name        string
		addressLen  uint32
		dstOffset   uint32
		rejectCode  uint8
		isPrefix    func(netip.Prefix) bool
	}{
		{nftables.TableFamilyIPv4, "worker-actor", 4, 16, icmpDestinationAdministrativelyProhibited, func(prefix netip.Prefix) bool { return prefix.Addr().Is4() }},
		{nftables.TableFamilyIPv6, "worker-actor-v6", 16, 24, icmpv6DestinationAdministrativelyProhibited, func(prefix netip.Prefix) bool { return prefix.Addr().Is6() }},
	} {
		table := c.AddTable(&nftables.Table{Family: family.tableFamily, Name: family.name})
		if family.tableFamily == nftables.TableFamilyIPv4 {
			ipv4Table = table
		}
		// Backs up the forwarding sysctls: nothing in this namespace is routed.
		dropPolicy := nftables.ChainPolicyDrop
		c.AddChain(&nftables.Chain{
			Name: "forward", Table: table, Type: nftables.ChainTypeFilter,
			Hooknum: nftables.ChainHookForward, Priority: nftables.ChainPriorityFilter,
			Policy: &dropPolicy,
		})
		filter := c.AddChain(&nftables.Chain{
			Name: "disallowed-egress", Table: table, Type: nftables.ChainTypeFilter,
			Hooknum: nftables.ChainHookPrerouting, Priority: nftables.ChainPriorityRaw,
		})
		if family.tableFamily == nftables.TableFamilyIPv4 {
			c.AddRule(&nftables.Rule{Table: table, Chain: filter, Exprs: []expr.Any{
				&expr.Payload{DestRegister: 1, Base: expr.PayloadBaseNetworkHeader, Offset: family.dstOffset, Len: family.addressLen},
				&expr.Bitwise{SourceRegister: 1, DestRegister: 1, Len: 4, Mask: actorSubnetMask, Xor: []byte{0, 0, 0, 0}},
				&expr.Cmp{Op: expr.CmpOpEq, Register: 1, Data: actorSubnetBase},
				&expr.Verdict{Kind: expr.VerdictAccept},
			}})
		}
		for _, prefix := range linkLocalEgressCIDRRanges {
			if !family.isPrefix(prefix) {
				continue
			}
			mask := net.CIDRMask(prefix.Bits(), int(family.addressLen*8))
			address := prefix.Addr().AsSlice()
			expressions := []expr.Any{
				&expr.Payload{DestRegister: 1, Base: expr.PayloadBaseNetworkHeader, Offset: family.dstOffset, Len: family.addressLen},
			}
			expressions = append(expressions,
				&expr.Bitwise{SourceRegister: 1, DestRegister: 1, Len: family.addressLen, Mask: mask, Xor: make([]byte, family.addressLen)},
				&expr.Cmp{Op: expr.CmpOpEq, Register: 1, Data: address},
				&expr.Reject{Type: unix.NFT_REJECT_ICMP_UNREACH, Code: family.rejectCode},
			)
			c.AddRule(&nftables.Rule{Table: table, Chain: filter, Exprs: expressions})
		}
		if family.tableFamily == nftables.TableFamilyIPv4 {
			// With forwarding off, the kernel drops unroutable packets silently instead
			// of returning ICMP net unreachable. Reject non-TCP egress leaving the /30
			// so UDP or ping fails fast rather than waiting out a timeout.
			expressions := append(l4ProtocolNotEqual(unix.IPPROTO_TCP),
				&expr.Reject{Type: unix.NFT_REJECT_ICMP_UNREACH, Code: icmpNetUnreachable},
			)
			c.AddRule(&nftables.Rule{Table: table, Chain: filter, Exprs: expressions})
		}
	}

	prerouting := c.AddChain(&nftables.Chain{
		Name: "prerouting", Table: ipv4Table, Type: nftables.ChainTypeNAT,
		Hooknum: nftables.ChainHookPrerouting, Priority: nftables.ChainPriorityNATDest,
	})

	// Offset of the destination address in an IPv4 header.
	const ipv4HeaderDst = 16
	exprs := []expr.Any{
		&expr.Payload{DestRegister: 1, Base: expr.PayloadBaseNetworkHeader, Offset: ipv4HeaderDst, Len: 4},
		&expr.Bitwise{
			SourceRegister: 1, DestRegister: 1, Len: 4,
			Mask: actorSubnetMask, Xor: []byte{0, 0, 0, 0},
		},
		&expr.Cmp{Op: expr.CmpOpNeq, Register: 1, Data: actorSubnetBase},
	}
	exprs = append(exprs, l4ProtocolEqual(unix.IPPROTO_TCP)...)
	exprs = append(exprs,
		&expr.Immediate{Register: 1, Data: binaryutil.BigEndian.PutUint16(egressPort)},
		&expr.Redir{RegisterProtoMin: 1},
	)
	c.AddRule(&nftables.Rule{Table: ipv4Table, Chain: prerouting, Exprs: exprs})

	if err := c.Flush(); err != nil {
		return fmt.Errorf("while installing the actor egress redirect: %w", err)
	}
	return nil
}

// linkLocalEgressCIDRRanges cover all IPv4 and IPv6 link-local space. The
// nftables chain accepts the actor's 169.254.17.0/30 first because that network
// carries actor ingress and DNS relay traffic.
var linkLocalEgressCIDRRanges = []netip.Prefix{
	netip.MustParsePrefix("169.254.0.0/16"),
	netip.MustParsePrefix("fe80::/10"),
}

// The actor subnet, in the form the nftables comparison takes.
var actorSubnetBase, actorSubnetMask = func() ([]byte, []byte) {
	_, subnet, err := net.ParseCIDR(ActorVethSubnet)
	if err != nil {
		panic(fmt.Sprintf("parsing constant actor subnet %q: %v", ActorVethSubnet, err))
	}
	return subnet.IP.To4(), subnet.Mask
}()

// gatewayVethName is the veth peer in the gateway namespace.
const gatewayVethName = "atside"

// CleanupSandboxNetwork closes the namespace handle and removes its name.
func CleanupSandboxNetwork(network *SandboxNetwork) error {
	if network == nil {
		return nil
	}
	var errs error
	if network.holdsNetNS() {
		if err := network.NetNS.Close(); err != nil {
			errs = errors.Join(errs, fmt.Errorf("while closing the sandbox netns: %w", err))
		}
	}
	// Deleting the namespace takes the veth pair with it.
	name := nodepath.ActorNetNSName(network.ActorUID)
	if err := netns.RemoveNamed(name); err != nil {
		errs = errors.Join(errs, fmt.Errorf("while deleting netns %s: %w", name, err))
	}
	return errs
}

// egressServer serves one actor's captured connections. Satisfied by
// egress.Proxy; an interface so this package does not depend on it.
type egressServer interface {
	Bind(actorUID string) (func(context.Context, net.Listener) error, error)
}

// ServeSandboxEgress serves redirected TCP in the gateway namespace.
// Closing the returned listeners stops accepting new connections.
func serveSandboxEgress(ctx context.Context, e egressServer, actorUID string, ns netns.Handle, ports []uint16) ([]io.Closer, []func(), error) {
	listeners, err := netns.Listen(ctx, ns, ports)
	if err != nil {
		return nil, nil, fmt.Errorf("while opening actor egress listeners: %w", err)
	}
	// Bound before anything is served, so a connection accepted here cannot
	// pick up a later activation's credentials.
	serveBound, err := e.Bind(actorUID)
	if err != nil {
		for _, listener := range listeners {
			_ = listener.Close()
		}
		return nil, nil, err
	}
	serve := make([]func(), 0, len(listeners))
	closers := make([]io.Closer, 0, len(listeners))
	for _, l := range listeners {
		closers = append(closers, l)
		serve = append(serve, func() {
			// Background rather than the caller's context: these outlive the
			// activation and are stopped by closing the listener.
			if err := serveBound(context.Background(), l); err != nil {
				slog.WarnContext(ctx, "Sandbox egress listener stopped", slog.Any("err", err))
			}
		})
	}
	return closers, serve, nil
}

// SandboxSession owns a sandbox's network and serving sockets.
type SandboxSession struct {
	Network *SandboxNetwork

	mu      sync.Mutex
	dns     *dns.Server
	sockets []io.Closer
	// serving counts the egress goroutines, so Close can wait for them rather
	// than just closing their sockets. dns.Server waits for its own.
	serving sync.WaitGroup
}

// ServeSandbox builds a sandbox's network and serves egress and DNS from its
// gateway namespace. A nil server leaves that unserved, which fails closed.
// DNS queries are checked against the policy cfg.DNSPolicy supplies.
func ServeSandbox(ctx context.Context, cfg SandboxNetworkConfig, egress egressServer, resolver *dns.Relay) (_ *SandboxSession, retErr error) {
	network, err := SetupSandboxNetwork(ctx, cfg)
	if err != nil {
		return nil, err
	}
	session := &SandboxSession{Network: network}
	defer func() {
		if retErr != nil {
			_ = session.Close(ctx)
		}
	}()

	if resolver != nil {
		dnsServer, err := resolver.Serve(ctx, network.NetNS, cfg.DNSPolicy)
		if err != nil {
			return nil, err
		}
		session.dns = dnsServer
	}
	// Egress last: its binding is released by the serve goroutine, so nothing
	// may fail between binding and starting it.
	if egress != nil {
		closers, serveEgress, err := serveSandboxEgress(ctx, egress, cfg.ActorUID, network.NetNS, []uint16{cfg.EgressPort})
		if err != nil {
			return nil, err
		}
		session.sockets = append(session.sockets, closers...)
		// Started here rather than inside the helper so the session owns them
		// and Close can report when they have stopped.
		for _, fn := range serveEgress {
			session.serving.Add(1)
			go func() {
				defer session.serving.Done()
				fn()
			}()
		}
	}
	return session, nil
}

// Close cancels the work in flight, closes the sockets, waits for the serving
// goroutines, then removes the namespace. ctx bounds only the wait; the
// namespace goes either way, since leaving them wedges the next activation.
// Idempotent, and every step's error is returned.
func (s *SandboxSession) Close(ctx context.Context) error {
	s.mu.Lock()
	dnsServer, sockets, network := s.dns, s.sockets, s.Network
	s.dns, s.sockets, s.Network = nil, nil, nil
	s.mu.Unlock()

	var errs error
	for _, c := range sockets {
		if err := c.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			errs = errors.Join(errs, err)
		}
	}
	if dnsServer != nil {
		errs = errors.Join(errs, dnsServer.Stop(ctx))
	}

	stopped := make(chan struct{})
	go func() {
		s.serving.Wait()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-ctx.Done():
		errs = errors.Join(errs, fmt.Errorf("while waiting for the sandbox's serving goroutines: %w", ctx.Err()))
	}

	if network != nil {
		errs = errors.Join(errs, CleanupSandboxNetwork(network))
	}
	return errs
}

// SessionHolder is the one sandbox session a worker is serving, for the workers
// to share what is otherwise the same locking, replacement and dialing in both.
type SessionHolder struct {
	mu      sync.Mutex
	session *SandboxSession
}

// Replace closes whatever session is held and installs next. A previous
// actor's session can still be here if its teardown never ran, and overwriting
// it would leak the namespace, its /run/netns mount and the goroutines
// serving it.
func (h *SessionHolder) Replace(ctx context.Context, next *SandboxSession) error {
	if err := h.Close(ctx); err != nil {
		return fmt.Errorf("while releasing the previous sandbox network: %w", err)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.session = next
	return nil
}

// Session is what is held, or nil between activations.
func (h *SessionHolder) Session() *SandboxSession {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.session
}

// Close releases the held session, if any.
func (h *SessionHolder) Close(ctx context.Context) error {
	h.mu.Lock()
	session := h.session
	h.session = nil
	h.mu.Unlock()
	if session == nil {
		return nil
	}
	return session.Close(ctx)
}

// Dialer reaches whichever sandbox is held when the dial happens.
func (h *SessionHolder) Dialer() func(context.Context, string, string) (net.Conn, error) {
	return func(ctx context.Context, network, address string) (net.Conn, error) {
		session := h.Session()
		if session == nil {
			return nil, errors.New("no actor is active on this worker")
		}
		return session.Dialer()(ctx, network, address)
	}
}

// Dialer reaches the sandbox from the gateway namespace, the only place its
// address is routable.
func (s *SandboxSession) Dialer() func(context.Context, string, string) (net.Conn, error) {
	return func(ctx context.Context, network, address string) (net.Conn, error) {
		s.mu.Lock()
		if s.Network == nil {
			s.mu.Unlock()
			return nil, net.ErrClosed
		}
		fd, err := unix.FcntlInt(uintptr(s.Network.NetNS), unix.F_DUPFD_CLOEXEC, 0)
		s.mu.Unlock()
		if err != nil {
			return nil, fmt.Errorf("while retaining the sandbox namespace: %w", err)
		}
		ns := netns.Handle(fd)
		defer ns.Close()
		return netns.Dialer(ns)(ctx, network, address)
	}
}
