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
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/nftables"
	"github.com/google/nftables/binaryutil"
	"github.com/google/nftables/expr"
	"github.com/vishvananda/netlink"
	"golang.org/x/sys/unix"

	"github.com/agent-substrate/substrate/internal/ateomnet/netns"
	"github.com/agent-substrate/substrate/internal/roottest"
)

var testNetNSSequence uint64

// TestTCPOriginalDestinationPreservesErrno covers the failure path on an
// ordinary connection that no REDIRECT rule touched. Each IPv4 lookup misses
// and reports ENOENT; that error must reach the caller. A dual-stack listener
// receives the IPv4 connection with a v4-mapped local address, so it must also
// select the IPv4 option.
//
// It runs in a fresh namespace because conntrack tracks loopback in any
// namespace that has nftables rules — including the one Docker runs in — and a
// tracked connection returns its real destination instead of missing.
func TestTCPOriginalDestinationPreservesErrno(t *testing.T) {
	roottest.Require(t, "CAP_SYS_ADMIN for a network namespace with no conntrack hooks")

	for _, test := range []struct {
		name          string
		listenNetwork string
		listenAddress string
	}{
		{name: "IPv4", listenNetwork: "tcp4", listenAddress: "127.0.0.1:0"},
		// An unspecified "tcp" listener is a dual-stack AF_INET6 socket on
		// Linux. A tcp4 client reaches it through a v4-mapped local address,
		// so TCPOriginalDestination must select the IPv4 socket option via
		// local.IP.To4(), rather than the listener's socket domain.
		{name: "dual-stack v4-mapped", listenNetwork: "tcp", listenAddress: ":0"},
	} {
		t.Run(test.name, func(t *testing.T) {
			ns := newTestNetNS(t)
			if err := netns.Do(context.Background(), ns, func(context.Context) error {
				loopback, err := netlink.LinkByName("lo")
				if err != nil {
					return err
				}
				if err := netlink.LinkSetUp(loopback); err != nil {
					return err
				}

				listener, err := net.Listen(test.listenNetwork, test.listenAddress)
				if err != nil {
					return err
				}
				defer listener.Close()
				_, port, err := net.SplitHostPort(listener.Addr().String())
				if err != nil {
					return err
				}
				client, err := net.DialTimeout("tcp4", net.JoinHostPort("127.0.0.1", port), time.Second)
				if err != nil {
					return err
				}
				defer client.Close()
				server, err := listener.Accept()
				if err != nil {
					return err
				}
				defer server.Close()

				_, lookupErr := TCPOriginalDestination(server)
				if !errors.Is(lookupErr, unix.ENOENT) {
					return fmt.Errorf("want the IPv4 lookup's ENOENT, got %v", lookupErr)
				}
				if !strings.Contains(lookupErr.Error(), "original IPv4 TCP destination") {
					return fmt.Errorf("want the error to name the IPv4 lookup, got %v", lookupErr)
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// originalDstFamily is one IP family's addressing for the redirect test: the
// veth's two ends, and where nftables finds the source address in that
// family's header.
type originalDstFamily struct {
	name    string
	network string
	table   nftables.TableFamily
	// linkSuffix keeps the two families' veth names apart.
	linkSuffix      string
	hostIP, actorIP net.IP
	mask            net.IPMask
	addrFlags       int
	// srcOffset and srcLen locate the source address in the IP header.
	srcOffset, srcLen uint32
}

// originalDstFamilies derives each family's addresses from the PID so
// concurrent test processes do not try to use the same host-side address.
func originalDstFamilies() []originalDstFamily {
	pid := os.Getpid()
	// One of the /30s in 198.18.0.0/16.
	network := uint16(pid % (1 << 14))
	thirdOctet := byte(network >> 6)
	fourthOctet := byte(network&0x3f) << 2
	return []originalDstFamily{
		{
			name:      "IPv4",
			network:   "tcp4",
			table:     nftables.TableFamilyIPv4,
			hostIP:    net.IPv4(198, 18, thirdOctet, fourthOctet+1),
			actorIP:   net.IPv4(198, 18, thirdOctet, fourthOctet+2),
			mask:      net.CIDRMask(30, 32),
			srcOffset: 12,
			srcLen:    4,
		},
		{
			name:       "IPv6",
			network:    "tcp6",
			table:      nftables.TableFamilyIPv6,
			linkSuffix: "6",
			hostIP:     net.ParseIP(fmt.Sprintf("fd00:198:18:%x::1", uint16(pid))),
			actorIP:    net.ParseIP(fmt.Sprintf("fd00:198:18:%x::2", uint16(pid))),
			mask:       net.CIDRMask(64, 128),
			// This isolated veth has no competing IPv6 peers. Suppress DAD so
			// the address can be bound immediately instead of remaining
			// tentative while the test is trying to start its listener.
			addrFlags: unix.IFA_F_NODAD,
			srcOffset: 8,
			srcLen:    16,
		},
	}
}

// Model the production path rather than redirecting a locally generated
// connection through OUTPUT. Actor egress enters the worker netns through a
// veth and is redirected in PREROUTING; that is the path on which Linux
// preserves SO_ORIGINAL_DST for atunnel.
func TestTCPOriginalDestination(t *testing.T) {
	roottest.Require(t, "CAP_NET_ADMIN + CAP_SYS_ADMIN for an actor-like network namespace and nftables REDIRECT rule")

	for _, family := range originalDstFamilies() {
		t.Run(family.name, func(t *testing.T) {
			actorNS := newTestNetNS(t)
			withTestWorkerNS(t, func() {
				setupTestVeth(t, family, actorNS)
				// targetListener reserves the port the actor intends to reach. The NAT rule
				// below must prevent connections from reaching it.
				//
				// redirectListener represents atunnel's local egress listener. It receives
				// the redirected connection and is therefore the connection on which we ask
				// Linux for the original destination.
				redirectListener := listenTCP(t, family)
				defer redirectListener.Close()
				targetListener := listenTCP(t, family)
				defer targetListener.Close()
				targetPort := targetListener.Addr().(*net.TCPAddr).Port

				installOriginalDstRedirect(t, family, targetPort, redirectListener.Addr().(*net.TCPAddr).Port)

				clientDone := make(chan error, 1)
				go func() {
					// From the actor's perspective this is an ordinary connection to
					// hostIP:targetPort. The worker's PREROUTING rule redirects it before
					// it reaches the host network stack's local delivery path.
					clientDone <- netns.Do(context.Background(), actorNS, func(context.Context) error {
						conn, err := net.DialTimeout(family.network, net.JoinHostPort(family.hostIP.String(), fmt.Sprint(targetPort)), 10*time.Second)
						if err == nil {
							_ = conn.Close()
						}
						return err
					})
				}()

				if err := redirectListener.(*net.TCPListener).SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
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
				want := net.JoinHostPort(family.hostIP.String(), fmt.Sprint(targetPort))
				if got != want {
					t.Errorf("original destination = %q, want %q", got, want)
				}
				if err := <-clientDone; err != nil {
					t.Fatalf("dialing redirected connection: %v", err)
				}
			})
		})
	}
}

// withTestWorkerNS runs the worker half of the test in a private namespace.
// The worker's listeners and nftables PREROUTING rule then cannot be affected
// by default-deny INPUT rules in the host namespace.
func withTestWorkerNS(t *testing.T, fn func()) {
	t.Helper()
	workerNS := newTestNetNS(t)
	if err := netns.Do(context.Background(), workerNS, func(context.Context) error {
		fn()
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func newTestNetNS(t *testing.T) netns.Handle {
	t.Helper()
	name := fmt.Sprintf("atunnel-original-dst-%d-%d", os.Getpid(), atomic.AddUint64(&testNetNSSequence, 1))
	ns, err := netns.CreateNamed(name)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = ns.Close()
		if err := netns.RemoveNamed(name); err != nil {
			t.Errorf("deleting test network namespace: %v", err)
		}
	})
	return ns
}

// setupTestVeth joins the current namespace to actorNS with a veth carrying
// the family's addresses, the actor end inside actorNS.
func setupTestVeth(t *testing.T, family originalDstFamily, actorNS netns.Handle) {
	t.Helper()
	hostName := fmt.Sprintf("atod%s%d", family.linkSuffix, os.Getpid())
	peerName := fmt.Sprintf("atop%s%d", family.linkSuffix, os.Getpid())
	if err := netlink.LinkAdd(&netlink.Veth{LinkAttrs: netlink.LinkAttrs{Name: hostName}, PeerName: peerName}); err != nil {
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
	if err := netlink.AddrAdd(hostLink, &netlink.Addr{IPNet: &net.IPNet{IP: family.hostIP, Mask: family.mask}, Flags: family.addrFlags}); err != nil {
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
	if err := netns.Do(context.Background(), actorNS, func(context.Context) error {
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
		if err := netlink.AddrAdd(link, &netlink.Addr{IPNet: &net.IPNet{IP: family.actorIP, Mask: family.mask}, Flags: family.addrFlags}); err != nil {
			return err
		}
		return netlink.LinkSetUp(link)
	}); err != nil {
		t.Fatal(err)
	}
}

func listenTCP(t *testing.T, family originalDstFamily) net.Listener {
	t.Helper()
	listener, err := net.ListenTCP(family.network, &net.TCPAddr{IP: family.hostIP, Port: 0})
	if err != nil {
		t.Fatal(err)
	}
	return listener
}

func installOriginalDstRedirect(t *testing.T, family originalDstFamily, targetPort, redirectPort int) {
	t.Helper()
	// Restrict the rule to this test's actor so the temporary table cannot
	// affect unrelated local TCP traffic.
	actorIP := family.actorIP.To16()
	if family.table == nftables.TableFamilyIPv4 {
		actorIP = family.actorIP.To4()
	}
	c := &nftables.Conn{}
	table := c.AddTable(&nftables.Table{Family: family.table, Name: fmt.Sprintf("atunnel_original_dst_%s_test_%d", strings.ToLower(family.name), os.Getpid())})
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
			&expr.Payload{DestRegister: 1, Base: expr.PayloadBaseNetworkHeader, Offset: family.srcOffset, Len: family.srcLen},
			&expr.Cmp{Op: expr.CmpOpEq, Register: 1, Data: actorIP},
			&expr.Payload{DestRegister: 1, Base: expr.PayloadBaseTransportHeader, Offset: 2, Len: 2},
			&expr.Cmp{Op: expr.CmpOpEq, Register: 1, Data: binaryutil.BigEndian.PutUint16(uint16(targetPort))},
			&expr.Immediate{Register: 1, Data: binaryutil.BigEndian.PutUint16(uint16(redirectPort))},
			&expr.Redir{RegisterProtoMin: 1},
		},
	})
	if err := c.Flush(); err != nil {
		t.Fatalf("installing nftables redirect: %v", err)
	}
}
