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
	"net"
	"net/http"
	"net/url"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/ateompath"
	"github.com/vishvananda/netlink"

	"github.com/agent-substrate/substrate/internal/ateomnet/netns"
	"github.com/agent-substrate/substrate/internal/roottest"
)

const testEgressPort = 15001

func TestSandboxSessionDialerAfterClose(t *testing.T) {
	session := &SandboxSession{}
	dial := session.Dialer()
	if err := session.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := dial(context.Background(), "tcp", "127.0.0.1:1"); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("dial after close: got %v, want closed", err)
	}
}

func TestSandboxSessionDialerConcurrentClose(t *testing.T) {
	roottest.Require(t, "creates network namespaces")
	session, err := ServeSandbox(context.Background(), SandboxNetworkConfig{
		ActorUID: "concurrent-close", EgressPort: testEgressPort,
	}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = session.Close(context.Background()) })
	dial := session.Dialer()
	start := make(chan struct{})
	var workers sync.WaitGroup
	for range 32 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			for range 32 {
				conn, err := dial(context.Background(), "udp", "127.0.0.1:9")
				if err != nil {
					if !errors.Is(err, net.ErrClosed) {
						t.Errorf("dial during close: %v", err)
					}
					return
				}
				_ = conn.Close()
			}
		}()
	}
	close(start)
	if err := session.Close(context.Background()); err != nil {
		t.Error(err)
	}
	workers.Wait()
}

func TestSetupSandboxNetwork(t *testing.T) {
	roottest.Require(t, "creates network namespaces")
	ctx := context.Background()

	type actor struct {
		net  *SandboxNetwork
		body string
	}
	actors := map[string]*actor{}
	for _, uid := range []string{"11111111-1111-1111-1111-111111111111", "22222222-2222-2222-2222-222222222222"} {
		n, err := SetupSandboxNetwork(ctx, SandboxNetworkConfig{ActorUID: uid, Veth: true, EgressPort: testEgressPort})
		if err != nil {
			t.Fatalf("SetupSandboxNetwork(%s): %v", uid, err)
		}
		t.Cleanup(func() {
			if err := CleanupSandboxNetwork(n); err != nil {
				t.Errorf("cleanup %s: %v", uid, err)
			}
		})

		// The actor's app, bound where a real one binds, inside its namespace.
		var lis net.Listener
		if err := netns.Do(ctx, n.RuntimeNetNS, func(context.Context) error {
			l, err := net.Listen("tcp", net.JoinHostPort(ActorVethIP, "80"))
			lis = l
			return err
		}); err != nil {
			t.Fatalf("actor %s listen: %v", uid, err)
		}
		t.Cleanup(func() { lis.Close() })
		body := "i-am-" + uid[:8]
		go http.Serve(lis, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			io.WriteString(w, body)
		}))
		actors[uid] = &actor{net: n, body: body}
	}

	// Reaching each actor is a matter of which namespace the dial is made from.
	for uid, a := range actors {
		client := &http.Client{Transport: &http.Transport{DialContext: netns.Dialer(a.net.RuntimeNetNS)}, Timeout: 5 * time.Second}
		resp, err := client.Get((&url.URL{Scheme: "http", Host: net.JoinHostPort(ActorVethIP, "80")}).String())
		if err != nil {
			t.Fatalf("reaching actor %s: %v", uid, err)
		}
		got, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if string(got) != a.body {
			t.Errorf("actor %s answered %q, want %q", uid, got, a.body)
		}
	}

	// Sandbox addresses must not be reachable from the worker namespace.
	direct := &http.Client{Timeout: 2 * time.Second}
	if _, err := direct.Get("http://" + net.JoinHostPort(ActorVethIP, "80")); err == nil {
		t.Error("the worker namespace reached an actor directly; addresses are not isolated")
	}
}

func TestActorEgressIsFailClosedWithoutAtunnel(t *testing.T) {
	roottest.Require(t, "creates network namespaces")
	ctx := context.Background()

	n, err := SetupSandboxNetwork(ctx, SandboxNetworkConfig{
		ActorUID: "33333333-3333-3333-3333-333333333333", Veth: true, EgressPort: testEgressPort,
	})
	if err != nil {
		t.Fatalf("SetupSandboxNetwork: %v", err)
	}
	t.Cleanup(func() { CleanupSandboxNetwork(n) })

	for _, destination := range []string{"93.184.216.34:443", "93.184.216.34:8080"} {
		if err := netns.Do(ctx, n.RuntimeNetNS, func(context.Context) error {
			c, err := net.DialTimeout("tcp", destination, 3*time.Second)
			if err != nil {
				return err
			}
			c.Close()
			return nil
		}); err == nil {
			t.Errorf("the actor reached %s with no atunnel listening; egress is not fail-closed", destination)
		}
	}
}

func TestIngressCrossesThePairWhileEgressIsCaptured(t *testing.T) {
	roottest.Require(t, "creates network namespaces")
	ctx := context.Background()

	n, err := SetupSandboxNetwork(ctx, SandboxNetworkConfig{ActorUID: "44444444-4444-4444-4444-444444444444", Veth: true, EgressPort: testEgressPort})
	if err != nil {
		t.Fatalf("SetupSandboxNetwork: %v", err)
	}
	t.Cleanup(func() { CleanupSandboxNetwork(n) })

	var app net.Listener
	if err := netns.Do(ctx, n.RuntimeNetNS, func(context.Context) error {
		l, e := net.Listen("tcp", net.JoinHostPort(ActorVethIP, "80"))
		app = l
		return e
	}); err != nil {
		t.Fatalf("actor listen: %v", err)
	}
	defer app.Close()
	go func() {
		for {
			c, e := app.Accept()
			if e != nil {
				return
			}
			io.WriteString(c, "the-actor")
			c.Close()
		}
	}()

	dial := netns.Dialer(n.GatewayNetNS)
	c, err := dial(ctx, "tcp", net.JoinHostPort(ActorVethIP, "80"))
	if err != nil {
		t.Fatalf("ingress dial: %v", err)
	}
	got, _ := io.ReadAll(c)
	c.Close()
	if string(got) != "the-actor" {
		t.Errorf("ingress reached %q, want %q", got, "the-actor")
	}

	// An unopened port must be refused, not redirected to atunnel.
	cctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if c2, e := dial(cctx, "tcp", net.JoinHostPort(ActorVethIP, "81")); e == nil {
		c2.Close()
		t.Error("a port with no listener was accepted, so ingress is not reaching the sandbox")
	}
}

// This tests namespace setup only; testing microVM egress requires a tap.
func TestSetupSandboxNetworkWithoutVeth(t *testing.T) {
	roottest.Require(t, "creates network namespaces")
	ctx := context.Background()

	n, err := SetupSandboxNetwork(ctx, SandboxNetworkConfig{
		ActorUID:   "77777777-7777-7777-7777-777777777777",
		EgressPort: testEgressPort,
	})
	if err != nil {
		t.Fatalf("SetupSandboxNetwork: %v", err)
	}
	t.Cleanup(func() { CleanupSandboxNetwork(n) })

	if n.GatewayNetNS != n.RuntimeNetNS {
		t.Errorf("got a second namespace (%v vs %v); this shape needs one", n.GatewayNetNS, n.RuntimeNetNS)
	}

	// No veth was built, so nothing but lo is here until the tap arrives.
	if err := netns.Do(ctx, n.RuntimeNetNS, func(context.Context) error {
		links, err := netlink.LinkList()
		if err != nil {
			return err
		}
		for _, l := range links {
			if l.Attrs().Name != "lo" {
				t.Errorf("unexpected interface %q in the actor namespace", l.Attrs().Name)
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("listing links: %v", err)
	}
}

func TestSetupSucceedsOverALeftoverNamespace(t *testing.T) {
	roottest.Require(t, "creates network namespaces")
	ctx := context.Background()
	const uid = "aaaaaaaa-0000-0000-0000-00000000000a"
	cfg := SandboxNetworkConfig{ActorUID: uid, Veth: true, EgressPort: testEgressPort}

	first, err := SetupSandboxNetwork(ctx, cfg)
	if err != nil {
		t.Fatalf("first SetupSandboxNetwork: %v", err)
	}
	// Simulate interrupted teardown by leaving the namespace names mounted.
	first.RuntimeNetNS.Close()
	if first.GatewayNetNS != first.RuntimeNetNS {
		first.GatewayNetNS.Close()
	}
	for _, name := range []string{ateompath.ActorNetNSName(uid), SandboxGatewayNetNSName(uid)} {
		if _, err := os.Stat("/var/run/netns/" + name); err != nil {
			t.Fatalf("expected leftover netns %s: %v", name, err)
		}
	}

	second, err := SetupSandboxNetwork(ctx, cfg)
	if err != nil {
		t.Fatalf("the actor is wedged by its own leftover namespace: %v", err)
	}
	t.Cleanup(func() { CleanupSandboxNetwork(second) })

	if err := netns.Do(ctx, second.RuntimeNetNS, func(context.Context) error {
		if _, err := netlink.LinkByName(ActorVethName); err != nil {
			return fmt.Errorf("actor interface missing after reuse: %w", err)
		}
		return nil
	}); err != nil {
		t.Error(err)
	}
}

// Check from the gateway: a successful UDP send from the actor does not prove delivery.
func TestActorUDPHasNowhereToGoBeyondTheNamespacePair(t *testing.T) {
	roottest.Require(t, "creates network namespaces")
	ctx := context.Background()

	n, err := SetupSandboxNetwork(ctx, SandboxNetworkConfig{
		ActorUID: "bbbbbbbb-0000-0000-0000-00000000000b", Veth: true, EgressPort: testEgressPort,
	})
	if err != nil {
		t.Fatalf("SetupSandboxNetwork: %v", err)
	}
	t.Cleanup(func() { CleanupSandboxNetwork(n) })

	for _, destination := range []string{"93.184.216.34:443", "93.184.216.34:53"} {
		if err := netns.Do(ctx, n.GatewayNetNS, func(context.Context) error {
			c, err := net.Dial("udp", destination)
			if err != nil {
				return err
			}
			defer c.Close()
			_, err = c.Write([]byte("probe"))
			return err
		}); err == nil {
			t.Errorf("the atunnel namespace can reach %s over UDP; actor UDP could follow it out", destination)
		}
	}
}

func TestCleanupClosesEachDescriptorOnce(t *testing.T) {
	roottest.Require(t, "creates network namespaces")
	network, err := SetupSandboxNetwork(context.Background(), SandboxNetworkConfig{
		ActorUID:   "close-once",
		EgressPort: 15001,
	})
	if err != nil {
		t.Fatal(err)
	}
	if network.RuntimeNetNS != network.GatewayNetNS {
		t.Fatalf("expected one namespace without a veth, got %d and %d", network.RuntimeNetNS, network.GatewayNetNS)
	}

	// A second close of the same descriptor reports EBADF.
	if err := CleanupSandboxNetwork(network); err != nil {
		t.Fatalf("CleanupSandboxNetwork closed a descriptor twice: %v", err)
	}
}

// slowDNS holds its serving goroutines open until released, so a test can tell
// whether Close waits for them or merely closes their sockets.
type slowDNS struct{ release chan struct{} }

func (d *slowDNS) ServePacket(ctx context.Context, pc net.PacketConn) error {
	<-ctx.Done()
	<-d.release
	return pc.Close()
}

func (d *slowDNS) Serve(ctx context.Context, l net.Listener) error {
	<-ctx.Done()
	<-d.release
	return l.Close()
}

// Close's contract is that serving has stopped when it returns, not just that
// the sockets are shut: a caller tearing an actor down needs the relay's
// capacity back.
func TestSessionCloseWaitsForServingToStop(t *testing.T) {
	roottest.Require(t, "creates network namespaces")
	relay := &slowDNS{release: make(chan struct{})}
	session, err := ServeSandbox(context.Background(), SandboxNetworkConfig{
		ActorUID: "close-waits", EgressPort: testEgressPort, DNSPort: 53,
	}, nil, relay)
	if err != nil {
		t.Fatal(err)
	}

	returned := make(chan error, 1)
	go func() { returned <- session.Close(context.Background()) }()
	select {
	case <-returned:
		t.Fatal("Close returned while the relay was still serving")
	case <-time.After(250 * time.Millisecond):
	}

	close(relay.release)
	select {
	case err := <-returned:
		if err != nil {
			t.Errorf("Close: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Error("Close did not return after serving stopped")
	}
}

// A caller that cannot wait forever gets its deadline back as an error, and
// the namespaces are still removed -- leaving them would wedge the next
// activation of this actor.
func TestSessionCloseReportsAWaitItCouldNotFinish(t *testing.T) {
	roottest.Require(t, "creates network namespaces")
	relay := &slowDNS{release: make(chan struct{})}
	defer close(relay.release)
	session, err := ServeSandbox(context.Background(), SandboxNetworkConfig{
		ActorUID: "close-deadline", EgressPort: testEgressPort, DNSPort: 53,
	}, nil, relay)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := session.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Close = %v, want the deadline reported", err)
	}
	if _, err := netns.GetFromName(ateompath.ActorNetNSName("close-deadline")); err == nil {
		t.Error("the namespace survived a Close whose wait timed out")
	}
}
