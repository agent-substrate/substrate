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

package atunnel

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/nftables"
	"github.com/google/nftables/binaryutil"
	"github.com/google/nftables/expr"
	"github.com/vishvananda/netlink"
	"github.com/vishvananda/netns"
	"golang.org/x/sys/unix"

	"github.com/agent-substrate/substrate/internal/ateomnet"
	"github.com/agent-substrate/substrate/internal/roottest"
)

// TestTCPOriginalDestinationPreservesErrno covers the failure path on an
// ordinary connection that no REDIRECT rule touched. The IPv4 lookup misses
// and reports ENOENT; that error must reach the caller. Retrying the IPv6
// option on an AF_INET socket would replace it with EOPNOTSUPP, which says
// nothing about why the lookup failed.
//
// It runs in a fresh namespace because conntrack tracks loopback in any
// namespace that has nftables rules — including the one Docker runs in — and a
// tracked connection returns its real destination instead of missing.
func TestTCPOriginalDestinationPreservesErrno(t *testing.T) {
	roottest.Require(t, "CAP_SYS_ADMIN for a network namespace with no conntrack hooks")

	ns := newTestNetNS(t)
	if err := ateomnet.NetNSDo(context.Background(), ns, func(context.Context) error {
		loopback, err := netlink.LinkByName("lo")
		if err != nil {
			return err
		}
		if err := netlink.LinkSetUp(loopback); err != nil {
			return err
		}

		listener, err := net.Listen("tcp4", "127.0.0.1:0")
		if err != nil {
			return err
		}
		defer listener.Close()
		client, err := net.DialTimeout("tcp4", listener.Addr().String(), time.Second)
		if err != nil {
			return err
		}
		defer client.Close()
		server, err := listener.Accept()
		if err != nil {
			return err
		}
		defer server.Close()

		if _, err := TCPOriginalDestination(server); !errors.Is(err, unix.ENOENT) {
			return fmt.Errorf("want the IPv4 lookup's ENOENT, got %w", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestTCPOriginalDestination(t *testing.T) {
	roottest.Require(t, "CAP_NET_ADMIN + CAP_SYS_ADMIN for an actor-like network namespace and nftables REDIRECT rule")

	// Model the production path rather than redirecting a locally generated
	// connection through OUTPUT. Actor egress enters the worker netns through a
	// veth and is redirected in PREROUTING; that is the path on which Linux
	// preserves SO_ORIGINAL_DST for atunnel.
	actorNS := newTestNetNS(t)
	actorIP, hostIP := setupTestVeth(t, actorNS)
	// targetListener reserves the port the actor intends to reach. The NAT rule
	// below must prevent connections from reaching it.
	//
	// redirectListener represents atunnel's local egress listener. It receives
	// the redirected connection and is therefore the connection on which we ask
	// Linux for the original destination.
	redirectListener := listenTCP(t, hostIP)
	defer redirectListener.Close()
	targetListener := listenTCP(t, hostIP)
	defer targetListener.Close()
	targetPort := targetListener.Addr().(*net.TCPAddr).Port

	table := &nftables.Table{Family: nftables.TableFamilyIPv4, Name: fmt.Sprintf("atunnel_original_dst_test_%d", os.Getpid())}
	installOriginalDstRedirect(t, table, actorIP, targetPort, redirectListener.Addr().(*net.TCPAddr).Port)

	clientDone := make(chan error, 1)
	go func() {
		// From the actor's perspective this is an ordinary connection to
		// hostIP:targetPort. The worker's PREROUTING rule redirects it before
		// it reaches the host network stack's local delivery path.
		clientDone <- ateomnet.NetNSDo(context.Background(), actorNS, func(context.Context) error {
			conn, err := net.DialTimeout("tcp4", net.JoinHostPort(hostIP.String(), fmt.Sprint(targetPort)), time.Second)
			if err == nil {
				_ = conn.Close()
			}
			return err
		})
	}()

	if err := redirectListener.(*net.TCPListener).SetDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	redirected, err := redirectListener.Accept()
	if err != nil {
		t.Fatalf("accepting redirected connection: %v", err)
	}
	defer redirected.Close()

	// The accepted socket is addressed to redirectListener, but the kernel's
	// SO_ORIGINAL_DST record must still contain the destination chosen by the
	// actor before nftables rewrote it.
	got, err := TCPOriginalDestination(redirected)
	if err != nil {
		t.Fatalf("TCPOriginalDestination: %v", err)
	}
	want := net.JoinHostPort(hostIP.String(), fmt.Sprint(targetPort))
	if got != want {
		t.Errorf("original destination = %q, want %q", got, want)
	}
	if err := <-clientDone; err != nil {
		t.Fatalf("dialing redirected connection: %v", err)
	}
}

func TestTCPOriginalDestinationIPv6(t *testing.T) {
	roottest.Require(t, "CAP_NET_ADMIN + CAP_SYS_ADMIN for an actor-like network namespace and nftables REDIRECT rule")

	actorNS := newTestNetNS(t)
	actorIP, hostIP := setupTestIPv6Veth(t, actorNS)
	redirectListener := listenTCP6(t, hostIP)
	defer redirectListener.Close()
	targetListener := listenTCP6(t, hostIP)
	defer targetListener.Close()
	targetPort := targetListener.Addr().(*net.TCPAddr).Port

	table := &nftables.Table{Family: nftables.TableFamilyIPv6, Name: fmt.Sprintf("atunnel_original_dst_ipv6_test_%d", os.Getpid())}
	installOriginalDstIPv6Redirect(t, table, actorIP, targetPort, redirectListener.Addr().(*net.TCPAddr).Port)

	clientDone := make(chan error, 1)
	go func() {
		clientDone <- ateomnet.NetNSDo(context.Background(), actorNS, func(context.Context) error {
			conn, err := net.DialTimeout("tcp6", net.JoinHostPort(hostIP.String(), fmt.Sprint(targetPort)), time.Second)
			if err == nil {
				_ = conn.Close()
			}
			return err
		})
	}()

	if err := redirectListener.(*net.TCPListener).SetDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	redirected, err := redirectListener.Accept()
	if err != nil {
		t.Fatalf("accepting redirected IPv6 connection: %v", err)
	}
	defer redirected.Close()

	// This assertion captures the IPv6 behavior required by #686.
	got, err := TCPOriginalDestination(redirected)
	if err != nil {
		t.Fatalf("TCPOriginalDestination: %v", err)
	}
	want := net.JoinHostPort(hostIP.String(), fmt.Sprint(targetPort))
	if got != want {
		t.Errorf("original IPv6 destination = %q, want %q", got, want)
	}
	if err := <-clientDone; err != nil {
		t.Fatalf("dialing redirected IPv6 connection: %v", err)
	}
}

func newTestNetNS(t *testing.T) netns.NsHandle {
	t.Helper()
	name := fmt.Sprintf("atunnel-original-dst-%d", os.Getpid())
	ns, err := ateomnet.CreateNetNSWithoutSwitching(name)
	if err != nil {
		if errors.Is(err, unix.EPERM) || strings.Contains(err.Error(), "operation not permitted") {
			t.Skipf("needs CAP_SYS_ADMIN to create network namespace: %v", err)
		}
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = ns.Close()
		if err := netns.DeleteNamed(name); err != nil {
			t.Errorf("deleting test network namespace: %v", err)
		}
	})
	return ns
}

func setupTestVeth(t *testing.T, actorNS netns.NsHandle) (actorIP, hostIP net.IP) {
	t.Helper()
	hostName := fmt.Sprintf("atod%d", os.Getpid())
	peerName := fmt.Sprintf("atop%d", os.Getpid())
	if err := netlink.LinkAdd(&netlink.Veth{LinkAttrs: netlink.LinkAttrs{Name: hostName}, PeerName: peerName}); err != nil {
		if errors.Is(err, unix.EPERM) || strings.Contains(err.Error(), "operation not permitted") {
			t.Skipf("needs CAP_NET_ADMIN to create veth: %v", err)
		}
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if link, err := netlink.LinkByName(hostName); err == nil {
			if err := netlink.LinkDel(link); err != nil {
				t.Errorf("deleting test veth: %v", err)
			}
		}
	})
	hostLink, err := netlink.LinkByName(hostName)
	if err != nil {
		t.Fatal(err)
	}
	// Allocate one of the /30s in 198.18.0.0/16 from the PID so concurrent
	// test processes do not try to use the same host-side address.
	network := uint16(os.Getpid() % (1 << 14))
	thirdOctet := byte(network >> 6)
	fourthOctet := byte(network&0x3f) << 2
	hostIP = net.IPv4(198, 18, thirdOctet, fourthOctet+1)
	actorIP = net.IPv4(198, 18, thirdOctet, fourthOctet+2)
	if err := netlink.AddrAdd(hostLink, &netlink.Addr{IPNet: &net.IPNet{IP: hostIP, Mask: net.CIDRMask(30, 32)}}); err != nil {
		t.Fatal(err)
	}
	if err := netlink.LinkSetUp(hostLink); err != nil {
		t.Fatal(err)
	}
	peer, err := netlink.LinkByName(peerName)
	if err != nil {
		t.Fatal(err)
	}
	if err := netlink.LinkSetNsFd(peer, int(actorNS)); err != nil {
		t.Fatal(err)
	}
	// Complete the actor end of the point-to-point link inside its own netns.
	if err := ateomnet.NetNSDo(context.Background(), actorNS, func(context.Context) error {
		lo, err := netlink.LinkByName("lo")
		if err != nil {
			return err
		}
		if err := netlink.LinkSetUp(lo); err != nil {
			return err
		}
		link, err := netlink.LinkByName(peerName)
		if err != nil {
			return err
		}
		if err := netlink.AddrAdd(link, &netlink.Addr{IPNet: &net.IPNet{IP: actorIP, Mask: net.CIDRMask(30, 32)}}); err != nil {
			return err
		}
		return netlink.LinkSetUp(link)
	}); err != nil {
		t.Fatal(err)
	}
	return actorIP, hostIP
}

func listenTCP(t *testing.T, hostIP net.IP) net.Listener {
	t.Helper()
	listener, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: hostIP, Port: 0})
	if err != nil {
		t.Fatal(err)
	}
	return listener
}

func setupTestIPv6Veth(t *testing.T, actorNS netns.NsHandle) (actorIP, hostIP net.IP) {
	t.Helper()
	hostName := fmt.Sprintf("atod6%d", os.Getpid())
	peerName := fmt.Sprintf("atop6%d", os.Getpid())
	if err := netlink.LinkAdd(&netlink.Veth{LinkAttrs: netlink.LinkAttrs{Name: hostName}, PeerName: peerName}); err != nil {
		if errors.Is(err, unix.EPERM) || strings.Contains(err.Error(), "operation not permitted") {
			t.Skipf("needs CAP_NET_ADMIN to create veth: %v", err)
		}
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if link, err := netlink.LinkByName(hostName); err == nil {
			if err := netlink.LinkDel(link); err != nil {
				t.Errorf("deleting test IPv6 veth: %v", err)
			}
		}
	})
	hostLink, err := netlink.LinkByName(hostName)
	if err != nil {
		t.Fatal(err)
	}
	prefix := uint16(os.Getpid())
	hostIP = net.ParseIP(fmt.Sprintf("fd00:198:18:%x::1", prefix))
	actorIP = net.ParseIP(fmt.Sprintf("fd00:198:18:%x::2", prefix))
	// This isolated veth has no competing IPv6 peers. Suppress DAD so the
	// address can be bound immediately instead of remaining tentative while
	// the test is trying to start its listener.
	if err := netlink.AddrAdd(hostLink, &netlink.Addr{IPNet: &net.IPNet{IP: hostIP, Mask: net.CIDRMask(64, 128)}, Flags: unix.IFA_F_NODAD}); err != nil {
		t.Fatal(err)
	}
	if err := netlink.LinkSetUp(hostLink); err != nil {
		t.Fatal(err)
	}
	peer, err := netlink.LinkByName(peerName)
	if err != nil {
		t.Fatal(err)
	}
	if err := netlink.LinkSetNsFd(peer, int(actorNS)); err != nil {
		t.Fatal(err)
	}
	if err := ateomnet.NetNSDo(context.Background(), actorNS, func(context.Context) error {
		lo, err := netlink.LinkByName("lo")
		if err != nil {
			return err
		}
		if err := netlink.LinkSetUp(lo); err != nil {
			return err
		}
		link, err := netlink.LinkByName(peerName)
		if err != nil {
			return err
		}
		if err := netlink.AddrAdd(link, &netlink.Addr{IPNet: &net.IPNet{IP: actorIP, Mask: net.CIDRMask(64, 128)}, Flags: unix.IFA_F_NODAD}); err != nil {
			return err
		}
		return netlink.LinkSetUp(link)
	}); err != nil {
		t.Fatal(err)
	}
	return actorIP, hostIP
}

func listenTCP6(t *testing.T, hostIP net.IP) net.Listener {
	t.Helper()
	listener, err := net.ListenTCP("tcp6", &net.TCPAddr{IP: hostIP, Port: 0})
	if err != nil {
		t.Fatal(err)
	}
	return listener
}

func installOriginalDstRedirect(t *testing.T, table *nftables.Table, actorIP net.IP, targetPort, redirectPort int) {
	t.Helper()
	c := &nftables.Conn{}
	c.AddTable(table)
	chain := c.AddChain(&nftables.Chain{
		Name:     "prerouting",
		Table:    table,
		Type:     nftables.ChainTypeNAT,
		Hooknum:  nftables.ChainHookPrerouting,
		Priority: nftables.ChainPriorityNATDest,
	})
	c.AddRule(&nftables.Rule{
		Table: table,
		Chain: chain,
		Exprs: []expr.Any{
			&expr.Meta{Key: expr.MetaKeyL4PROTO, Register: 1},
			&expr.Cmp{Op: expr.CmpOpEq, Register: 1, Data: []byte{unix.IPPROTO_TCP}},
			// Restrict the rule to this test's actor so the temporary table cannot
			// affect unrelated local TCP traffic.
			&expr.Payload{DestRegister: 1, Base: expr.PayloadBaseNetworkHeader, Offset: 12, Len: 4},
			&expr.Cmp{Op: expr.CmpOpEq, Register: 1, Data: actorIP.To4()},
			&expr.Payload{DestRegister: 1, Base: expr.PayloadBaseTransportHeader, Offset: 2, Len: 2},
			&expr.Cmp{Op: expr.CmpOpEq, Register: 1, Data: binaryutil.BigEndian.PutUint16(uint16(targetPort))},
			&expr.Immediate{Register: 1, Data: binaryutil.BigEndian.PutUint16(uint16(redirectPort))},
			&expr.Redir{RegisterProtoMin: 1},
		},
	})
	if err := c.Flush(); err != nil {
		if errors.Is(err, unix.EPERM) || strings.Contains(err.Error(), "operation not permitted") {
			t.Skipf("needs CAP_NET_ADMIN to install nftables rule: %v", err)
		}
		t.Fatalf("installing nftables redirect: %v", err)
	}
	t.Cleanup(func() {
		cleanup := &nftables.Conn{}
		cleanup.DelTable(table)
		if err := cleanup.Flush(); err != nil {
			t.Errorf("removing nftables redirect: %v", err)
		}
	})
}

func installOriginalDstIPv6Redirect(t *testing.T, table *nftables.Table, actorIP net.IP, targetPort, redirectPort int) {
	t.Helper()
	c := &nftables.Conn{}
	c.AddTable(table)
	chain := c.AddChain(&nftables.Chain{
		Name:     "prerouting",
		Table:    table,
		Type:     nftables.ChainTypeNAT,
		Hooknum:  nftables.ChainHookPrerouting,
		Priority: nftables.ChainPriorityNATDest,
	})
	c.AddRule(&nftables.Rule{
		Table: table,
		Chain: chain,
		Exprs: []expr.Any{
			&expr.Meta{Key: expr.MetaKeyL4PROTO, Register: 1},
			&expr.Cmp{Op: expr.CmpOpEq, Register: 1, Data: []byte{unix.IPPROTO_TCP}},
			// An IPv6 source address begins eight bytes into the IPv6 header.
			&expr.Payload{DestRegister: 1, Base: expr.PayloadBaseNetworkHeader, Offset: 8, Len: 16},
			&expr.Cmp{Op: expr.CmpOpEq, Register: 1, Data: actorIP.To16()},
			&expr.Payload{DestRegister: 1, Base: expr.PayloadBaseTransportHeader, Offset: 2, Len: 2},
			&expr.Cmp{Op: expr.CmpOpEq, Register: 1, Data: binaryutil.BigEndian.PutUint16(uint16(targetPort))},
			&expr.Immediate{Register: 1, Data: binaryutil.BigEndian.PutUint16(uint16(redirectPort))},
			&expr.Redir{RegisterProtoMin: 1},
		},
	})
	if err := c.Flush(); err != nil {
		if errors.Is(err, unix.EPERM) || strings.Contains(err.Error(), "operation not permitted") {
			t.Skipf("needs CAP_NET_ADMIN to install IPv6 nftables rule: %v", err)
		}
		t.Fatalf("installing IPv6 nftables redirect: %v", err)
	}
	t.Cleanup(func() {
		cleanup := &nftables.Conn{}
		cleanup.DelTable(table)
		if err := cleanup.Flush(); err != nil {
			t.Errorf("removing IPv6 nftables redirect: %v", err)
		}
	})
}
