//go:build linux

// Copyright 2026 The Agent Substrate Authors
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
	"log/slog"
	"net"
	"net/netip"
	"strconv"
	"testing"
	"time"

	"golang.org/x/net/dns/dnsmessage"

	"go.ate.dev/substrate/internal/atunnel/dns"
	"go.ate.dev/substrate/internal/atunnel/egress"
	"go.ate.dev/substrate/internal/atunnel/fakeip"
	"go.ate.dev/substrate/internal/linuxsys"
	"go.ate.dev/substrate/internal/netns"
	"go.ate.dev/substrate/internal/proto/ateletpb"
	"go.ate.dev/substrate/internal/roottest"
)

// correlationName is the name the test actors look up, and correlationPort
// the port they connect to. No rule type reads either from the connection.
const (
	correlationName = "db.example.com"
	correlationPort = 5432
)

// allowAllDNS is an egress policy that lets an actor look up every name.
type allowAllDNS struct{}

func (allowAllDNS) EgressPolicy(context.Context) (*ateletpb.EgressPolicy, error) {
	return &ateletpb.EgressPolicy{Rules: []*ateletpb.EgressRule{{
		Http: &ateletpb.EgressDestinations{Hostnames: []string{"*"}},
	}}}, nil
}

// recordingGateway stands in for the egress gateway: it records the
// destination of every tunnel and accepts it.
type recordingGateway struct{ tunnels chan string }

func (g recordingGateway) DialContext(_ context.Context, destination string) (net.Conn, error) {
	g.tunnels <- destination
	tunnel, gateway := net.Pipe()
	_ = gateway.Close()
	return tunnel, nil
}

func (recordingGateway) Close() error { return nil }

type validCertificate struct{}

func (validCertificate) MintWorkerCertificate(context.Context) (time.Time, error) {
	return time.Now().Add(time.Hour), nil
}

// serveUpstreamDNS answers every A query with a real-looking address, as the
// worker pod's resolver would, and returns its address.
func serveUpstreamDNS(t *testing.T) string {
	t.Helper()
	conn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	go func() {
		buf := make([]byte, 1500)
		for {
			n, from, err := conn.ReadFrom(buf)
			if err != nil {
				return
			}
			var query dnsmessage.Message
			if err := query.Unpack(buf[:n]); err != nil || len(query.Questions) != 1 {
				continue
			}
			q := query.Questions[0]
			answer := dnsmessage.Message{
				Header:    dnsmessage.Header{ID: query.ID, Response: true, RecursionDesired: true, RecursionAvailable: true},
				Questions: query.Questions,
			}
			if q.Type == dnsmessage.TypeA {
				answer.Answers = []dnsmessage.Resource{{
					Header: dnsmessage.ResourceHeader{Name: q.Name, Type: dnsmessage.TypeA, Class: dnsmessage.ClassINET, TTL: 300},
					Body:   &dnsmessage.AResource{A: [4]byte{93, 184, 216, 34}},
				}}
			}
			packed, err := answer.Pack()
			if err != nil {
				continue
			}
			_, _ = conn.WriteTo(packed, from)
		}
	}()
	return conn.LocalAddr().String()
}

// correlatedActor is one actor's network, served with DNS correlation.
type correlatedActor struct {
	uid     string
	session *SandboxSession
	// tunnels receives the destination of every tunnel the actor opens.
	tunnels chan string
}

// hostCorrelatedActor serves an actor's network with table as its fake
// addresses, the way a worker hosts an actor.
func hostCorrelatedActor(t *testing.T, uid string, proxy *egress.Proxy, relay *dns.Relay, table *fakeip.Table) *correlatedActor {
	t.Helper()
	ctx := context.Background()
	session, err := ServeSandbox(ctx, SandboxNetworkConfig{
		ActorUID: uid, Veth: true, EgressPort: testEgressPort, DNSPolicy: allowAllDNS{}, FakeIPs: table,
	}, proxy, relay)
	if err != nil {
		t.Fatalf("ServeSandbox: %v", err)
	}
	actor := &correlatedActor{uid: uid, session: session, tunnels: make(chan string, 4)}
	t.Cleanup(func() { actor.unhost(t, proxy) })
	if err := proxy.Activate(uid, recordingGateway{tunnels: actor.tunnels}, validCertificate{}, time.Now().Add(time.Hour)); err != nil {
		t.Fatalf("Activate: %v", err)
	}
	return actor
}

// unhost tears the actor's network down, the way a worker does on suspend.
// Repeated calls are safe.
func (a *correlatedActor) unhost(t *testing.T, proxy *egress.Proxy) {
	t.Helper()
	if a.session == nil {
		return
	}
	ctx := context.Background()
	if err := proxy.Deactivate(ctx, a.uid); err != nil {
		t.Errorf("Deactivate: %v", err)
	}
	if err := a.session.Close(ctx); err != nil {
		t.Errorf("closing the actor's network: %v", err)
	}
	a.session = nil
}

// lookup resolves name from inside the actor, through the sandbox's DNS.
func (a *correlatedActor) lookup(t *testing.T, name string) netip.Addr {
	t.Helper()
	var conn net.Conn
	if err := netns.Do(context.Background(), a.session.Network.RuntimeNetNS, func(context.Context) error {
		c, err := net.DialTimeout("udp", net.JoinHostPort(ActorVethGateway, "53"), 3*time.Second)
		conn = c
		return err
	}); err != nil {
		t.Fatalf("reaching the sandbox's DNS: %v", err)
	}
	defer conn.Close()

	query := dnsmessage.Message{
		Header:    dnsmessage.Header{ID: 7, RecursionDesired: true},
		Questions: []dnsmessage.Question{{Name: dnsmessage.MustNewName(name + "."), Type: dnsmessage.TypeA, Class: dnsmessage.ClassINET}},
	}
	packed, err := query.Pack()
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write(packed); err != nil {
		t.Fatalf("sending the query for %s: %v", name, err)
	}
	buf := make([]byte, 1500)
	n, err := conn.Read(buf)
	if err != nil {
		t.Fatalf("reading the answer for %s: %v", name, err)
	}
	var answer dnsmessage.Message
	if err := answer.Unpack(buf[:n]); err != nil {
		t.Fatalf("parsing the answer for %s: %v", name, err)
	}
	if answer.RCode != dnsmessage.RCodeSuccess || len(answer.Answers) != 1 {
		t.Fatalf("answer for %s has rcode %v and %d records, want one A record", name, answer.RCode, len(answer.Answers))
	}
	a4, ok := answer.Answers[0].Body.(*dnsmessage.AResource)
	if !ok {
		t.Fatalf("answer for %s holds a %T, want an A record", name, answer.Answers[0].Body)
	}
	return netip.AddrFrom4(a4.A)
}

// connect opens a connection from inside the actor to addr and returns the
// destination of the tunnel it was carried in.
func (a *correlatedActor) connect(t *testing.T, addr netip.Addr) string {
	t.Helper()
	target := netip.AddrPortFrom(addr, correlationPort).String()
	if err := netns.Do(context.Background(), a.session.Network.RuntimeNetNS, func(context.Context) error {
		conn, err := net.DialTimeout("tcp", target, 3*time.Second)
		if err != nil {
			return err
		}
		return conn.Close()
	}); err != nil {
		t.Fatalf("the actor could not connect to %s: %v", target, err)
	}
	select {
	case destination := <-a.tunnels:
		return destination
	case <-time.After(5 * time.Second):
		t.Fatalf("no tunnel was opened for the actor's connection to %s", target)
		return ""
	}
}

// TestActorConnectionIsTunneledByName drives the worker's whole part of DNS
// correlation on real namespaces: an actor looks a name up, connects to the
// address it got, and the tunnel opens to the name. The same holds after the
// actor's table went through a snapshot, for an address the actor kept.
func TestActorConnectionIsTunneledByName(t *testing.T) {
	roottest.Require(t, "creates network namespaces")

	relay, err := dns.NewRelayForUpstreams([]string{serveUpstreamDNS(t)})
	if err != nil {
		t.Fatal(err)
	}
	proxy, err := egress.New(linuxsys.TCPOriginalDestination)
	if err != nil {
		t.Fatal(err)
	}
	named := net.JoinHostPort(correlationName, strconv.Itoa(correlationPort))

	table := fakeip.New(slog.Default())
	actor := hostCorrelatedActor(t, "correlated-a", proxy, relay, table)
	fake := actor.lookup(t, correlationName)
	if !fakeip.Contains(fake) {
		t.Fatalf("%s resolved to %s, want a fake address", correlationName, fake)
	}
	if got := actor.connect(t, fake); got != named {
		t.Fatalf("the connection to %s was tunneled to %q, want %q", fake, got, named)
	}

	// Another actor on the worker has its own table: the first actor's
	// address is nothing to it, and its own lookups do not depend on it.
	other := hostCorrelatedActor(t, "correlated-b", proxy, relay, fakeip.New(slog.Default()))
	literal := netip.AddrPortFrom(fake, correlationPort).String()
	if got := other.connect(t, fake); got != literal {
		t.Fatalf("another actor's connection to %s was tunneled to %q, want %q", fake, got, literal)
	}
	otherFake := other.lookup(t, correlationName)
	if got := other.connect(t, otherFake); got != named {
		t.Fatalf("another actor's connection to %s was tunneled to %q, want %q", otherFake, got, named)
	}

	// Suspend and resume: the table is saved, the network is torn down, and a
	// new one is served with the loaded table.
	dir := t.TempDir()
	if _, err := table.Save(dir); err != nil {
		t.Fatalf("saving the table: %v", err)
	}
	actor.unhost(t, proxy)
	restored, err := fakeip.Load(dir, slog.Default())
	if err != nil {
		t.Fatalf("loading the table: %v", err)
	}
	actor = hostCorrelatedActor(t, "correlated-a", proxy, relay, restored)

	// The actor connects to the address it kept, without looking it up again.
	if got := actor.connect(t, fake); got != named {
		t.Fatalf("after a restore, the connection to %s was tunneled to %q, want %q", fake, got, named)
	}
	if again := actor.lookup(t, correlationName); again != fake {
		t.Fatalf("after a restore, %s resolved to %s, want %s as before", correlationName, again, fake)
	}
}
