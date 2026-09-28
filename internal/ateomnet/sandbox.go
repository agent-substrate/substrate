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
	"net/netip"
	"runtime"
	"sync"
	"syscall"

	"github.com/agent-substrate/substrate/internal/nodepath"

	"github.com/google/nftables"
	"github.com/google/nftables/binaryutil"
	"github.com/google/nftables/expr"
	"github.com/vishvananda/netlink"
	"github.com/vishvananda/netns"
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
	RuntimeNetNS netns.NsHandle
	// GatewayNetNS holds the sandbox's default gateway, DNS relay, and atunnel
	// sockets. This is local to the sandbox, not the external egress gateway.
	// For microVMs it shares RuntimeNetNS.
	//
	// TODO: we hope gVisor can take that same single-namespace shape soon,
	// once runsc can be given one interface rather than claiming every
	// interface in the namespace it runs in.
	GatewayNetNS netns.NsHandle
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

	actorNSName := nodepath.ActorNetNSName(actorUID)
	actorNS, err := CreateNetNSWithoutSwitching(actorNSName)
	if err != nil {
		return nil, fmt.Errorf("while creating the actor netns %s: %w", actorNSName, err)
	}
	defer func() {
		if retErr != nil {
			actorNS.Close()
			_ = removeNamedNetNS(actorNSName)
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
				_ = removeNamedNetNS(SandboxGatewayNetNSName(cfg.ActorUID))
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
func setupVethPair(ctx context.Context, cfg SandboxNetworkConfig, actorNS netns.NsHandle) (_ netns.NsHandle, retErr error) {
	gatewayNSName := SandboxGatewayNetNSName(cfg.ActorUID)
	outer, err := CreateNetNSWithoutSwitching(gatewayNSName)
	if err != nil {
		return 0, fmt.Errorf("while creating the outer netns %s: %w", gatewayNSName, err)
	}
	defer func() {
		if retErr != nil {
			outer.Close()
			_ = removeNamedNetNS(gatewayNSName)
		}
	}()

	// Keep the kernel-owned peer outside gVisor's namespace.
	if err := NetNSDo(ctx, outer, func(context.Context) error {
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
	if err := NetNSDo(ctx, actorNS, func(context.Context) error {
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
func setupGatewaySide(ctx context.Context, ns netns.NsHandle, egressPort uint16) error {
	if err := NetNSDo(ctx, ns, func(context.Context) error {
		// atunnel answers the actor's DNS on 53, and the worker holds no
		// CAP_NET_BIND_SERVICE.
		if err := AllowUnprivilegedPorts(); err != nil {
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
func installEgressRedirect(ns netns.NsHandle, egressPort uint16) error {
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
	return nodepath.ActorNetNSName(actorUID) + "-at"
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
	for _, name := range []string{nodepath.ActorNetNSName(network.ActorUID), SandboxGatewayNetNSName(network.ActorUID)} {
		if err := removeNamedNetNS(name); err != nil {
			errs = errors.Join(errs, fmt.Errorf("while deleting netns %s: %w", name, err))
		}
	}
	return errs
}

// ListenInNetNS opens wildcard TCP listeners inside ns.
// Sockets retain their namespace and can be served from another namespace.
func ListenInNetNS(ctx context.Context, ns netns.NsHandle, ports []uint16) (_ []net.Listener, retErr error) {
	var listeners []net.Listener
	defer func() {
		if retErr != nil {
			for _, l := range listeners {
				_ = l.Close()
			}
		}
	}()
	if err := NetNSDo(ctx, ns, func(context.Context) error {
		for _, port := range ports {
			l, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", port))
			if err != nil {
				return fmt.Errorf("while listening on port %d: %w", port, err)
			}
			listeners = append(listeners, l)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return listeners, nil
}

// egressServer serves one actor's captured connections. Satisfied by
// atunnel.Egress; an interface so this package does not depend on it.
type egressServer interface {
	Bind(actorUID string) (func(context.Context, net.Listener) error, error)
}

// ServeSandboxEgress serves redirected TCP in the gateway namespace.
// Closing the returned listeners stops accepting new connections.
func serveSandboxEgress(ctx context.Context, e egressServer, actorUID string, ns netns.NsHandle, ports []uint16) ([]io.Closer, []func(), error) {
	listeners, err := ListenInNetNS(ctx, ns, ports)
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

// closerFunc adapts a cancel function to io.Closer, so a caller takes a
// sandbox's sockets and the work behind them down as one list.
type closerFunc func() error

func (f closerFunc) Close() error { return f() }

// withNetNS switches to targetNS, calls run, then restores the original namespace.
// run can call restore to switch back and unlock the OS thread before returning.
// Calling restore again after it succeeds has no effect.
//
// A separate goroutine lets us leave the thread locked if restoration fails.
// Go then discards that thread when the goroutine exits.
func withNetNS(targetNS netns.NsHandle, run func(restore func() error) error) error {
	var resultErr error
	var done sync.WaitGroup
	done.Add(1)
	go func() {
		defer done.Done()
		runtime.LockOSThread()
		originalNS, err := netns.Get()
		if err != nil {
			runtime.UnlockOSThread()
			resultErr = fmt.Errorf("while reading the current netns: %w", err)
			return
		}
		defer originalNS.Close()
		if err := netns.Set(targetNS); err != nil {
			runtime.UnlockOSThread()
			resultErr = fmt.Errorf("while entering the actor netns: %w", err)
			return
		}

		restored := false
		restore := func() error {
			if restored {
				return nil
			}
			if err := netns.Set(originalNS); err != nil {
				return fmt.Errorf("while restoring the worker netns: %w", err)
			}
			runtime.UnlockOSThread()
			restored = true
			return nil
		}

		resultErr = run(restore)
		if err := restore(); err != nil {
			resultErr = err
		}
	}()
	done.Wait()
	return resultErr
}

// NetNSDialer dials TCP or UDP IP literals in ns, pinning a thread only until
// the socket is created.
func NetNSDialer(ns netns.NsHandle) func(context.Context, string, string) (net.Conn, error) {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := validateNetNSDialTarget(network, addr); err != nil {
			return nil, err
		}

		var conn net.Conn
		dialErr := withNetNS(ns, func(restore func() error) error {
			// Only creating the socket needs the namespace, and
			// ControlContext runs once it exists: restore there rather than
			// holding the thread for the whole connect.
			socketCreated := false
			dialer := net.Dialer{ControlContext: func(context.Context, string, string, syscall.RawConn) error {
				if socketCreated {
					return errors.New("sandbox dial cannot recreate its socket outside the namespace")
				}
				socketCreated = true
				return restore()
			}}
			var err error
			conn, err = dialer.DialContext(ctx, network, addr)
			return err
		})
		if dialErr != nil || ctx.Err() != nil {
			if conn != nil {
				_ = conn.Close()
			}
			if dialErr != nil {
				return nil, dialErr
			}
			return nil, ctx.Err()
		}
		return conn, nil
	}
}

func validateNetNSDialTarget(network, addr string) error {
	switch network {
	case "tcp", "tcp4", "tcp6", "udp", "udp4", "udp6":
	default:
		return net.UnknownNetworkError(network)
	}
	hostname, _, err := net.SplitHostPort(addr)
	if err != nil {
		return err
	}
	if _, err := netip.ParseAddr(hostname); err != nil {
		return fmt.Errorf("NetNSDialer supports only IP literals (got %q): %w", hostname, err)
	}
	return nil
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
func ServeSandbox(ctx context.Context, cfg SandboxNetworkConfig, egress egressServer, dns dnsServer) (_ *SandboxSession, retErr error) {
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
	if dns != nil {
		closers, serveDNS, err := serveSandboxDNS(ctx, dns, network.GatewayNetNS, cfg.DNSPort)
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
		ns := netns.NsHandle(fd)
		defer ns.Close()
		return NetNSDialer(ns)(ctx, network, address)
	}
}
