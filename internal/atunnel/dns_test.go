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
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

func TestResolvConfNameservers(t *testing.T) {
	for _, tc := range []struct {
		name    string
		content string
		want    []string
		wantErr bool
	}{
		{
			name:    "cluster resolv.conf",
			content: "search ate-system.svc.cluster.local svc.cluster.local\nnameserver 10.96.0.10\noptions ndots:5\n",
			want:    []string{"10.96.0.10:53"},
		},
		{
			name:    "several, in order",
			content: "nameserver 10.96.0.10\nnameserver 8.8.8.8\n",
			want:    []string{"10.96.0.10:53", "8.8.8.8:53"},
		},
		{
			name:    "comments and blanks",
			content: "# generated\n\n  nameserver 10.96.0.10  # cluster\n;nameserver 1.1.1.1\n",
			want:    []string{"10.96.0.10:53"},
		},
		{name: "no nameserver", content: "search cluster.local\n", wantErr: true},
		{name: "unparsable address", content: "nameserver not-an-ip\n", wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "resolv.conf")
			if err := os.WriteFile(path, []byte(tc.content), 0o644); err != nil {
				t.Fatal(err)
			}
			got, err := ResolvConfNameservers(path)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("ResolvConfNameservers() = %v, want an error", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("ResolvConfNameservers: %v", err)
			}
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("nameservers mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestDNSRelayCancelsUDPExchange(t *testing.T) {
	// A resolver that receives the query and never answers, so the exchange is
	// blocked on the read when the context is canceled.
	silent, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer silent.Close()
	asked := make(chan struct{}, 1)
	go func() {
		buf := make([]byte, maxDNSDatagram)
		for {
			if _, _, err := silent.ReadFrom(buf); err != nil {
				return
			}
			select {
			case asked <- struct{}{}:
			default:
			}
		}
	}()

	// Two upstreams: a canceled exchange must not move on to the second.
	second := newFakeResolver(t, func(query []byte) []byte { return query })
	relay, err := NewDNSRelay([]string{silent.LocalAddr().String(), second})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := relay.exchangeUDP(ctx, dnsQuery(0x1234))
		done <- err
	}()
	select {
	case <-asked:
	case <-time.After(5 * time.Second):
		t.Fatal("the resolver never saw the query")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("exchange returned %v, want cancellation", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("canceled exchange remained blocked reading upstream")
	}
}

func TestDNSRelayForwardsUDPVerbatim(t *testing.T) {
	upstream := newFakeResolver(t, func(query []byte) []byte {
		return append([]byte{0xff}, query...)
	})

	relay, err := NewDNSRelay([]string{upstream})
	if err != nil {
		t.Fatal(err)
	}
	client := serveRelayUDP(t, relay)

	query := []byte{0xab, 0xcd, 0x01, 0x00, 0x00, 0x01}
	if _, err := client.Write(query); err != nil {
		t.Fatal(err)
	}
	answer := readWithin(t, client)
	if diff := cmp.Diff(append([]byte{0xff}, query...), answer); diff != "" {
		t.Errorf("answer mismatch (-want +got):\n%s", diff)
	}
}

func TestDNSRelayFallsBackToTheNextResolver(t *testing.T) {
	dead, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	deadAddress := dead.LocalAddr().String()
	// Closed, so the exchange fails rather than hanging to its deadline.
	dead.Close()

	live := newFakeResolver(t, func(query []byte) []byte { return []byte("answered") })
	relay, err := NewDNSRelay([]string{deadAddress, live})
	if err != nil {
		t.Fatal(err)
	}
	client := serveRelayUDP(t, relay)

	if _, err := client.Write([]byte("query")); err != nil {
		t.Fatal(err)
	}
	if got := string(readWithin(t, client)); got != "answered" {
		t.Errorf("answer = %q, want %q", got, "answered")
	}
}

func TestNewDNSRelayRejects(t *testing.T) {
	if _, err := NewDNSRelay(nil); err == nil {
		t.Error("NewDNSRelay(nil) succeeded; a relay with no upstream can answer nothing")
	}
	if _, err := NewDNSRelay([]string{"10.96.0.10"}); err == nil {
		t.Error("NewDNSRelay accepted an address with no port")
	}
}

// newFakeResolver answers UDP with respond(query), and returns its address.
func newFakeResolver(t *testing.T, respond func([]byte) []byte) string {
	t.Helper()
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { pc.Close() })
	go func() {
		buf := make([]byte, maxDNSDatagram)
		for {
			n, from, err := pc.ReadFrom(buf)
			if err != nil {
				return
			}
			query := make([]byte, n)
			copy(query, buf[:n])
			_, _ = pc.WriteTo(respond(query), from)
		}
	}()
	return pc.LocalAddr().String()
}

// serveRelayUDP runs the relay on a loopback socket and returns a connection to it.
func serveRelayUDP(t *testing.T, relay *DNSRelay) net.Conn {
	t.Helper()
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = relay.ServePacket(ctx, pc) }()

	client, err := net.Dial("udp", pc.LocalAddr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Close() })
	return client
}

func readWithin(t *testing.T, conn net.Conn) []byte {
	t.Helper()
	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, maxDNSDatagram)
	n, err := conn.Read(buf)
	if err != nil {
		t.Fatalf("reading the relay's answer: %v", err)
	}
	return buf[:n]
}

func TestDNSRelayForwardsAnswersLargerThanTheCommonBuffer(t *testing.T) {
	const size = 9000
	answer := make([]byte, size)
	for i := range answer {
		answer[i] = byte(i)
	}
	upstream := newFakeResolver(t, func([]byte) []byte { return answer })

	relay, err := NewDNSRelay([]string{upstream})
	if err != nil {
		t.Fatal(err)
	}
	client := serveRelayUDP(t, relay)
	if _, err := client.Write([]byte("query")); err != nil {
		t.Fatal(err)
	}

	got := readWithin(t, client)
	if len(got) != size {
		t.Fatalf("answer is %d bytes, want %d: it was cut down in the relay", len(got), size)
	}
	if !bytes.Equal(got, answer) {
		t.Error("answer differs from what the resolver sent")
	}
}

func TestDNSRelayDropsQueriesBeyondItsInFlightLimit(t *testing.T) {
	// Hold concurrent queries to exercise the relay's limit.
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { pc.Close() })
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	var inFlight atomic.Int64
	go func() {
		buf := make([]byte, maxDNSDatagram)
		for {
			n, from, err := pc.ReadFrom(buf)
			if err != nil {
				return
			}
			answer := make([]byte, n)
			copy(answer, buf[:n])
			go func() {
				inFlight.Add(1)
				<-release
				_, _ = pc.WriteTo(answer, from)
			}()
		}
	}()

	relay, err := NewDNSRelay([]string{pc.LocalAddr().String()})
	if err != nil {
		t.Fatal(err)
	}
	client := serveRelayUDP(t, relay)

	for range maxInFlightDNS * 4 {
		if _, err := client.Write([]byte("query")); err != nil {
			t.Fatal(err)
		}
	}
	// Wait for the accepted-query count to stabilize.
	deadline := time.Now().Add(10 * time.Second)
	last := int64(-1)
	for time.Now().Before(deadline) {
		time.Sleep(100 * time.Millisecond)
		if n := inFlight.Load(); n == last {
			break
		} else {
			last = n
		}
	}
	if got := inFlight.Load(); got > maxInFlightDNS {
		t.Errorf("the relay had %d queries in flight, want at most %d", got, maxInFlightDNS)
	}
}

// newHeldTCPResolver accepts DNS connections and answers none, holding each
// until the test ends. It reports how many it is holding.
func newHeldTCPResolver(t *testing.T) (address string, accepted *atomic.Int64) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { lis.Close() })

	var held atomic.Int64
	var mu sync.Mutex
	var conns []net.Conn
	t.Cleanup(func() {
		mu.Lock()
		defer mu.Unlock()
		for _, c := range conns {
			c.Close()
		}
	})
	go func() {
		for {
			conn, err := lis.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			conns = append(conns, conn)
			mu.Unlock()
			held.Add(1)
		}
	}()
	return lis.Addr().String(), &held
}

// serveRelayTCP runs the relay's TCP side on a loopback listener.
func serveRelayTCP(t *testing.T, relay *DNSRelay, ctx context.Context) net.Addr {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { lis.Close() })
	go func() { _ = relay.Serve(ctx, lis) }()
	return lis.Addr()
}

// waitFor polls until cond holds, failing the test if it never does.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestDNSRelayRefusesTCPConnectionsBeyondItsLimit(t *testing.T) {
	upstream, held := newHeldTCPResolver(t)
	relay, err := NewDNSRelay([]string{upstream})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	relayAddr := serveRelayTCP(t, relay, ctx)

	for range maxDNSConnections {
		conn, err := net.Dial("tcp", relayAddr.String())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { conn.Close() })
	}
	waitFor(t, "the relay to fill up", func() bool { return held.Load() == maxDNSConnections })

	// The relay should close a connection accepted beyond its limit.
	extra, err := net.Dial("tcp", relayAddr.String())
	if err != nil {
		t.Fatal(err)
	}
	defer extra.Close()
	if err := extra.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := extra.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
		t.Errorf("reading the refused connection = %v, want %v", err, io.EOF)
	}
	if got := held.Load(); got != maxDNSConnections {
		t.Errorf("the relay holds %d upstream connections, want %d", got, maxDNSConnections)
	}
}

func TestDNSRelayClosesTCPConnectionsWhenServingEnds(t *testing.T) {
	upstream, held := newHeldTCPResolver(t)
	relay, err := NewDNSRelay([]string{upstream})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	relayAddr := serveRelayTCP(t, relay, ctx)

	conn, err := net.Dial("tcp", relayAddr.String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	waitFor(t, "the connection to reach an upstream", func() bool { return held.Load() == 1 })

	cancel()

	// Cancellation must close the connection before dnsTCPTimeout.
	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
		t.Errorf("reading after teardown = %v, want %v: the connection outlived the actor", err, io.EOF)
	}
}

// dnsQuery builds a real query for example.com A, so tests exercise messages a
// resolver would actually send rather than an arbitrary string.
func dnsQuery(id uint16) []byte {
	msg := []byte{byte(id >> 8), byte(id), 0x01, 0x00, 0, 1, 0, 0, 0, 0, 0, 0}
	for _, label := range []string{"example", "com"} {
		msg = append(msg, byte(len(label)))
		msg = append(msg, label...)
	}
	return append(msg, 0, 0, 1, 0, 1) // root label, QTYPE A, QCLASS IN
}

// dnsAnswer echoes a query back as a response carrying rcode.
func dnsAnswer(query []byte, rcode byte) []byte {
	resp := make([]byte, len(query))
	copy(resp, query)
	resp[2] |= 0x80 // QR: this is a response
	resp[3] = resp[3]&0xf0 | rcode
	return resp
}

// A resolver that answers SERVFAIL has not answered the question, and the
// sandbox can no longer consult the pod's other resolvers itself: it is given
// the gateway as its only nameserver. The relay has to fail over for it.
func TestDNSRelayFailsOverOnServerFailure(t *testing.T) {
	var sickCalls, healthyCalls atomic.Int32
	sick := newFakeResolver(t, func(query []byte) []byte {
		sickCalls.Add(1)
		return dnsAnswer(query, rcodeServFail)
	})
	healthy := newFakeResolver(t, func(query []byte) []byte {
		healthyCalls.Add(1)
		return dnsAnswer(query, 0)
	})

	relay, err := NewDNSRelay([]string{sick, healthy})
	if err != nil {
		t.Fatal(err)
	}
	answer, err := relay.exchangeUDP(context.Background(), dnsQuery(0x1234))
	if err != nil {
		t.Fatalf("exchange: %v", err)
	}
	if got := answer[3] & 0x0f; got != 0 {
		t.Errorf("answer rcode = %d, want 0: the relay returned the failing resolver's answer", got)
	}
	if healthyCalls.Load() != 1 {
		t.Errorf("the healthy resolver was asked %d times, want 1", healthyCalls.Load())
	}
}

// With every resolver failing there is nothing better to return, and a real
// SERVFAIL beats a timeout: the sandbox's resolver can act on it.
func TestDNSRelayReturnsServerFailureWhenAllFail(t *testing.T) {
	first := newFakeResolver(t, func(query []byte) []byte { return dnsAnswer(query, rcodeServFail) })
	second := newFakeResolver(t, func(query []byte) []byte { return dnsAnswer(query, rcodeRefused) })

	relay, err := NewDNSRelay([]string{first, second})
	if err != nil {
		t.Fatal(err)
	}
	answer, err := relay.exchangeUDP(context.Background(), dnsQuery(0x2345))
	if err != nil {
		t.Fatalf("exchange: %v", err)
	}
	if got := answer[3] & 0x0f; got != rcodeRefused {
		t.Errorf("answer rcode = %d, want the last resolver's %d", got, rcodeRefused)
	}
}

// NXDOMAIN is an answer, not a failure: failing over would ask every resolver
// about a name that does not exist.
func TestDNSRelayReturnsNXDomainWithoutFailover(t *testing.T) {
	const rcodeNXDomain = 3
	var secondCalls atomic.Int32
	first := newFakeResolver(t, func(query []byte) []byte { return dnsAnswer(query, rcodeNXDomain) })
	second := newFakeResolver(t, func(query []byte) []byte {
		secondCalls.Add(1)
		return dnsAnswer(query, 0)
	})

	relay, err := NewDNSRelay([]string{first, second})
	if err != nil {
		t.Fatal(err)
	}
	answer, err := relay.exchangeUDP(context.Background(), dnsQuery(0x3456))
	if err != nil {
		t.Fatalf("exchange: %v", err)
	}
	if got := answer[3] & 0x0f; got != rcodeNXDomain {
		t.Errorf("answer rcode = %d, want NXDOMAIN %d", got, rcodeNXDomain)
	}
	if secondCalls.Load() != 0 {
		t.Error("the relay failed over on NXDOMAIN, which is a valid answer")
	}
}
