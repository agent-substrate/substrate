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

package ateomnet

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"

	"github.com/agent-substrate/substrate/internal/ateomnet/dns"
	"github.com/agent-substrate/substrate/internal/ateomnet/netns"
	"github.com/agent-substrate/substrate/internal/ateompath"

	"github.com/google/nftables"
	"github.com/google/nftables/binaryutil"
	"github.com/google/nftables/expr"
	"github.com/vishvananda/netlink"
	"golang.org/x/sys/unix"
)

// SandboxNetwork holds a sandbox's runtime and gateway namespaces.
type SandboxNetwork struct {
	// ActorUID is the Actor resource's UID. It names the namespaces, so a
	// teardown can find them again.
	ActorUID string
	// RuntimeNetNS is what the sandbox runs in, whichever runtime that is: the
	// micro-VM's tap lives here, and gVisor claims every interface here and
	// moves their addresses into its own stack.
	RuntimeNetNS netns.Handle
	// GatewayNetNS holds the sandbox's default gateway, DNS relay, and atunnel
	// sockets. This is local to the sandbox, not the external egress gateway.
	// For microVMs it shares RuntimeNetNS.
	//
	// TODO: we hope gVisor can take that same single-namespace shape soon,
	// once runsc can be given one interface rather than claiming every
	// interface in the namespace it runs in.
	GatewayNetNS netns.Handle
}

func (n *SandboxNetwork) holdsNetNS() bool { return n.RuntimeNetNS > 0 }

// SandboxNetworkConfig describes one actor's private networking.
type SandboxNetworkConfig struct {
	ActorUID string

	// Veth separates gVisor's interfaces from the kernel-owned gateway.
	// MicroVMs use a tap in a single namespace instead.
	Veth bool

	// EgressPort is where atunnel serves this actor. Every TCP connection the
	// actor makes is redirected to it, whatever port it was aimed at.
	EgressPort uint16

	// DNSPort is where the DNS relay answers, on the sandbox's default gateway.
	DNSPort uint16
}

// SetupSandboxNetwork creates isolated networking with fixed sandbox addresses.
// gVisor uses a veth pair across runtime and gateway namespaces because it takes
// over every interface in its namespace. MicroVMs use a tap in one namespace.
// nftables at the sandbox's default gateway redirect outbound TCP to atunnel, preserving
// SO_ORIGINAL_DST. Traffic to the gateway address is not redirected, so DNS
// over UDP and TCP reaches the relay's own sockets.
func SetupSandboxNetwork(ctx context.Context, cfg SandboxNetworkConfig) (_ *SandboxNetwork, retErr error) {
	actorUID := cfg.ActorUID
	if actorUID == "" {
		return nil, fmt.Errorf("actornet: actor UID is required")
	}

	actorNSName := ateompath.ActorNetNSName(actorUID)
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

	// Without a veth, atunnel shares the namespace with the runtime's tap.
	atunnelNS := actorNS
	if cfg.Veth {
		outer, err := setupVethPair(ctx, cfg, actorNS)
		if err != nil {
			return nil, err
		}
		defer func() {
			if retErr != nil {
				outer.Close()
				_ = netns.RemoveNamed(SandboxGatewayNetNSName(cfg.ActorUID))
			}
		}()
		atunnelNS = outer
	}
	if err := setupGatewaySide(ctx, atunnelNS, cfg.EgressPort); err != nil {
		return nil, err
	}

	return &SandboxNetwork{
		ActorUID:     actorUID,
		RuntimeNetNS: actorNS,
		GatewayNetNS: atunnelNS,
	}, nil
}

// setupVethPair creates the gateway namespace and the veth pair joining it to
// actorNS. The caller owns the returned handle and its name.
func setupVethPair(ctx context.Context, cfg SandboxNetworkConfig, actorNS netns.Handle) (_ netns.Handle, retErr error) {
	gatewayNSName := SandboxGatewayNetNSName(cfg.ActorUID)
	outer, err := netns.CreateNamed(gatewayNSName)
	if err != nil {
		return 0, fmt.Errorf("while creating the outer netns %s: %w", gatewayNSName, err)
	}
	defer func() {
		if retErr != nil {
			outer.Close()
			_ = netns.RemoveNamed(gatewayNSName)
		}
	}()

	// Keep the kernel-owned peer outside gVisor's namespace.
	if err := netns.Do(ctx, outer, func(context.Context) error {
		veth := &netlink.Veth{
			LinkAttrs: netlink.LinkAttrs{Name: gatewayVethName},
			PeerName:  ActorVethName,
			// Create the peer directly in the actor's namespace. Moving a
			// netdev across namespaces afterwards costs several times the
			// whole setup, all of it under the global RTNL lock, and this
			// runs on the resume path.
			PeerNamespace: netlink.NsFd(int(actorNS)),
		}
		if err := netlink.LinkAdd(veth); err != nil {
			return fmt.Errorf("while creating the veth pair: %w", err)
		}
		atSide, err := netlink.LinkByName(gatewayVethName)
		if err != nil {
			return err
		}
		if err := netlink.AddrReplace(atSide, HostVethAddr); err != nil {
			return fmt.Errorf("while assigning the atunnel-side address: %w", err)
		}
		if err := netlink.LinkSetUp(atSide); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return 0, err
	}

	// gVisor imports these addresses and routes into its network stack.
	if err := netns.Do(ctx, actorNS, func(context.Context) error {
		// Loopback lets the actor reach its own address.
		if err := linkUp("lo"); err != nil {
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
		return netlink.RouteReplace(&netlink.Route{
			LinkIndex: eth0.Attrs().Index,
			Gw:        ActorVethGwIP,
		})
	}); err != nil {
		return 0, err
	}
	return outer, nil
}

func linkUp(name string) error {
	link, err := netlink.LinkByName(name)
	if err != nil {
		return err
	}
	return netlink.LinkSetUp(link)
}

// setupGatewaySide brings up lo and puts atunnel in front of the actor's TCP.
func setupGatewaySide(ctx context.Context, ns netns.Handle, egressPort uint16) error {
	if err := netns.Do(ctx, ns, func(context.Context) error {
		// atunnel answers the actor's DNS on 53, and the worker holds no
		// CAP_NET_BIND_SERVICE.
		if err := netns.AllowUnprivilegedPorts(); err != nil {
			return err
		}
		return linkUp("lo")
	}); err != nil {
		return err
	}
	return installEgressRedirect(ns, egressPort)
}

// installEgressRedirect redirects TCP egress to atunnel, excluding the sandbox's
// own /30: that keeps ingress replies and DNS over TCP to the gateway off the
// redirect, so the relay serves them on its own listener.
func installEgressRedirect(ns netns.Handle, egressPort uint16) error {
	if egressPort == 0 {
		return fmt.Errorf("actornet: atunnel egress port is required")
	}
	c, err := nftables.New(nftables.WithNetNSFd(int(ns)))
	if err != nil {
		return fmt.Errorf("while opening nftables in the actor namespace: %w", err)
	}
	defer func() { _ = c.CloseLasting() }()

	table := c.AddTable(&nftables.Table{Family: nftables.TableFamilyIPv4, Name: "ateom-actor"})
	prerouting := c.AddChain(&nftables.Chain{
		Name: "prerouting", Table: table, Type: nftables.ChainTypeNAT,
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
	c.AddRule(&nftables.Rule{Table: table, Chain: prerouting, Exprs: exprs})

	if err := c.Flush(); err != nil {
		return fmt.Errorf("while installing the actor egress redirect: %w", err)
	}
	return nil
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

// SandboxGatewayNetNSName names the namespace holding the veth peer and atunnel's
// sockets for one actor.
func SandboxGatewayNetNSName(actorUID string) string {
	return ateompath.ActorNetNSName(actorUID) + "-at"
}

// CleanupSandboxNetwork closes namespace handles and removes their names.
func CleanupSandboxNetwork(network *SandboxNetwork) error {
	if network == nil {
		return nil
	}
	var errs error
	// Compare before Close sets the handle to -1; microVMs share one descriptor.
	separateNS := network.GatewayNetNS != network.RuntimeNetNS
	if network.holdsNetNS() {
		if err := network.RuntimeNetNS.Close(); err != nil {
			errs = errors.Join(errs, fmt.Errorf("while closing the sandbox netns: %w", err))
		}
	}
	if separateNS && network.GatewayNetNS > 0 {
		if err := network.GatewayNetNS.Close(); err != nil {
			errs = errors.Join(errs, fmt.Errorf("while closing the gateway netns: %w", err))
		}
	}
	// Deleting the namespaces takes any veth pair with them.
	for _, name := range []string{ateompath.ActorNetNSName(network.ActorUID), SandboxGatewayNetNSName(network.ActorUID)} {
		if err := netns.RemoveNamed(name); err != nil {
			errs = errors.Join(errs, fmt.Errorf("while deleting netns %s: %w", name, err))
		}
	}
	return errs
}

// egressServer serves one actor's captured connections. Satisfied by
// atunnel.Egress; an interface so this package does not depend on it.
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
	sockets []io.Closer
	// serving counts the goroutines serving this sandbox, so Close can wait
	// for them rather than just closing their sockets.
	serving sync.WaitGroup
}

// ServeSandbox builds a sandbox's network and serves egress and DNS from its
// gateway namespace. A nil server leaves that unserved, which fails closed.
func ServeSandbox(ctx context.Context, cfg SandboxNetworkConfig, egress egressServer, resolver dns.Server) (_ *SandboxSession, retErr error) {
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

	var serve []func()
	if resolver != nil {
		closers, serveDNS, err := dns.Serve(ctx, resolver, network.GatewayNetNS, cfg.DNSPort)
		if err != nil {
			return nil, err
		}
		session.sockets = append(session.sockets, closers...)
		serve = append(serve, serveDNS...)
	}
	// Egress last: its binding is released by the serve goroutine, so nothing
	// may fail between binding and starting it.
	if egress != nil {
		closers, serveEgress, err := serveSandboxEgress(ctx, egress, cfg.ActorUID, network.GatewayNetNS, []uint16{cfg.EgressPort})
		if err != nil {
			return nil, err
		}
		session.sockets = append(session.sockets, closers...)
		serve = append(serve, serveEgress...)
	}

	// Started here rather than inside the helpers so the session owns them and
	// Close can report when they have stopped.
	for _, fn := range serve {
		session.serving.Add(1)
		go func() {
			defer session.serving.Done()
			fn()
		}()
	}
	return session, nil
}

// Close cancels the work in flight, closes the sockets, waits for the serving
// goroutines, then removes the namespaces. ctx bounds only the wait; the
// namespaces go either way, since leaving them wedges the next activation.
// Idempotent, and every step's error is returned.
func (s *SandboxSession) Close(ctx context.Context) error {
	s.mu.Lock()
	sockets, network := s.sockets, s.Network
	s.sockets, s.Network = nil, nil
	s.mu.Unlock()

	var errs error
	for _, c := range sockets {
		if err := c.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			errs = errors.Join(errs, err)
		}
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

// SessionHolder is the one sandbox session a worker is serving, for the ateoms
// to share what is otherwise the same locking, replacement and dialing in both.
type SessionHolder struct {
	mu      sync.Mutex
	session *SandboxSession
}

// Replace closes whatever session is held and installs next. A previous
// actor's session can still be here if its teardown never ran, and overwriting
// it would leak both namespaces, their /run/netns mounts and the goroutines
// serving them.
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
		fd, err := unix.FcntlInt(uintptr(s.Network.GatewayNetNS), unix.F_DUPFD_CLOEXEC, 0)
		s.mu.Unlock()
		if err != nil {
			return nil, fmt.Errorf("while retaining the sandbox namespace: %w", err)
		}
		ns := netns.Handle(fd)
		defer ns.Close()
		return netns.Dialer(ns)(ctx, network, address)
	}
}
