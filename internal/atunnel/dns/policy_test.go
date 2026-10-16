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

package dns

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"golang.org/x/net/dns/dnsmessage"

	"go.ate.dev/substrate/internal/atunnel/dns/protocol"
	"go.ate.dev/substrate/internal/proto/ateletpb"
)

// staticPolicy supplies policy, or fails with err.
type staticPolicy struct {
	policy *ateletpb.EgressPolicy
	err    error
}

func (p staticPolicy) EgressPolicy(context.Context) (*ateletpb.EgressPolicy, error) {
	return p.policy, p.err
}

// withPolicy supplies an egress policy that allows every hostname.
var withPolicy = staticPolicy{policy: &ateletpb.EgressPolicy{Rules: []*ateletpb.EgressRule{{
	Http: &ateletpb.EgressDestinations{Hostnames: []string{"*"}},
}}}}

// switchablePolicy supplies whichever policy was stored last.
type switchablePolicy struct {
	policy atomic.Pointer[ateletpb.EgressPolicy]
}

func (p *switchablePolicy) EgressPolicy(context.Context) (*ateletpb.EgressPolicy, error) {
	return p.policy.Load(), nil
}

// blockingPolicy holds each fetch until its context ends or the test releases
// it, as a fetch from an atelet that never answers would. It signals called
// when a fetch starts.
type blockingPolicy struct {
	called  chan struct{}
	release chan struct{}
}

func (p blockingPolicy) EgressPolicy(ctx context.Context) (*ateletpb.EgressPolicy, error) {
	select {
	case p.called <- struct{}{}:
	default:
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-p.release:
		return nil, errors.New("released by the test")
	}
}

// namesAsStrings compares DNS names as text rather than as 255-byte arrays.
var namesAsStrings = cmp.Transformer("Name", func(n dnsmessage.Name) string { return n.String() })

// checkAnswer fails the test unless answer answers dnsQuery(id) with rcode.
func checkAnswer(t *testing.T, answer []byte, id uint16, rcode dnsmessage.RCode) {
	t.Helper()
	checkAnswerForQuestion(t, answer, id, exampleQuestion, rcode)
}

func checkAnswerForQuestion(t *testing.T, answer []byte, id uint16, wantQ dnsmessage.Question, rcode dnsmessage.RCode) {
	t.Helper()
	var p dnsmessage.Parser
	h, err := p.Start(answer)
	if err != nil {
		t.Fatalf("parsing the relay's answer: %v", err)
	}
	if h.ID != id || !h.Response || h.RCode != rcode {
		t.Errorf("answer header = %+v, want a response to query %#x with %v", h, id, rcode)
	}
	q, err := p.Question()
	if err != nil {
		t.Fatalf("parsing the question of the relay's answer: %v", err)
	}
	if diff := cmp.Diff(wantQ, q, namesAsStrings); diff != "" {
		t.Errorf("answer question mismatch (-want +got):\n%s", diff)
	}
}

func dnsQueryForName(t *testing.T, id uint16, name string) ([]byte, dnsmessage.Question) {
	t.Helper()
	q := dnsmessage.Question{
		Name:  dnsmessage.MustNewName(name),
		Type:  dnsmessage.TypeA,
		Class: dnsmessage.ClassINET,
	}
	raw := buildTestMessage(t, dnsmessage.Header{
		ID:               id,
		RecursionDesired: true,
	}, []dnsmessage.Question{q})
	return raw, q
}

// newTCPResolver answers each DNS message on its TCP connections with
// respond(query). It returns its address and how many queries it was asked.
func newTCPResolver(t *testing.T, respond func([]byte) []byte) (string, *atomic.Int64) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { lis.Close() })
	var asked atomic.Int64
	go func() {
		for {
			conn, err := lis.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				for {
					query, err := protocol.ReadTCPFrame(conn)
					if err != nil {
						return
					}
					asked.Add(1)
					if err := protocol.WriteTCPFrame(conn, respond(query)); err != nil {
						return
					}
				}
			}()
		}
	}()
	return lis.Addr().String(), &asked
}

// dialRelay connects to the relay at addr over network, with a deadline that
// bounds the test's reads and writes.
func dialRelay(t *testing.T, network string, addr net.Addr) net.Conn {
	t.Helper()
	conn, err := net.Dial(network, addr.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	return conn
}

// askUDP sends query to the relay and returns its answer.
func askUDP(t *testing.T, conn net.Conn, query []byte) []byte {
	t.Helper()
	if _, err := conn.Write(query); err != nil {
		t.Fatal(err)
	}
	return readWithin(t, conn)
}

// askTCP sends query on a TCP connection to the relay and returns the next
// message it sends back.
func askTCP(t *testing.T, conn net.Conn, query []byte) []byte {
	t.Helper()
	if err := protocol.WriteTCPFrame(conn, query); err != nil {
		t.Fatal(err)
	}
	answer, err := protocol.ReadTCPFrame(conn)
	if err != nil {
		t.Fatalf("reading the relay's answer: %v", err)
	}
	return answer
}

// policySources are the ways an actor can be without a usable egress policy.
var policySources = []struct {
	name   string
	policy PolicySource
}{
	{"no policy", staticPolicy{}},
	{"policy unavailable", staticPolicy{err: errors.New("atelet is unreachable")}},
	{"no policy source", nil},
}

// An actor without an egress policy can reach nothing through the egress
// gateway, so it may resolve nothing either. The relay refuses its queries
// without asking a resolver, and does the same when it cannot learn the
// policy.
func TestRelayRefusesUDPQueriesWithoutPolicy(t *testing.T) {
	for _, tc := range policySources {
		t.Run(tc.name, func(t *testing.T) {
			var asked atomic.Int64
			upstream := newFakeResolver(t, func(query []byte) []byte {
				asked.Add(1)
				return dnsAnswer(query, 0)
			})
			relay, err := NewRelayForUpstreams([]string{upstream})
			if err != nil {
				t.Fatal(err)
			}
			_, udpAddr, _ := serveLoopbackWithPolicy(t, relay, tc.policy)
			client := dialRelay(t, "udp", udpAddr)

			checkAnswer(t, askUDP(t, client, dnsQuery(0x1111)), 0x1111, dnsmessage.RCodeRefused)
			if got := asked.Load(); got != 0 {
				t.Errorf("the resolver was asked %d times, want 0", got)
			}
		})
	}
}

func TestRelayRefusesTCPQueriesWithoutPolicy(t *testing.T) {
	for _, tc := range policySources {
		t.Run(tc.name, func(t *testing.T) {
			upstream, asked := newTCPResolver(t, func(query []byte) []byte { return dnsAnswer(query, 0) })
			relay, err := NewRelayForUpstreams([]string{upstream})
			if err != nil {
				t.Fatal(err)
			}
			_, _, tcpAddr := serveLoopbackWithPolicy(t, relay, tc.policy)
			conn := dialRelay(t, "tcp", tcpAddr)

			// The connection stays open for the actor's next query.
			checkAnswer(t, askTCP(t, conn, dnsQuery(0x1111)), 0x1111, dnsmessage.RCodeRefused)
			checkAnswer(t, askTCP(t, conn, dnsQuery(0x2222)), 0x2222, dnsmessage.RCodeRefused)
			if got := asked.Load(); got != 0 {
				t.Errorf("the resolver was asked %d queries, want 0", got)
			}
		})
	}
}

// The relay checks each query against the actor's policy as it is then, so a
// policy created or deleted while the actor runs applies to its next query,
// even on a TCP connection it holds open.
func TestRelayFollowsPolicyChanges(t *testing.T) {
	answer := func(query []byte) []byte { return dnsAnswer(query, 0) }
	for _, tc := range []struct {
		network  string
		upstream func(t *testing.T) string
		ask      func(*testing.T, net.Conn, []byte) []byte
	}{
		{
			network:  "udp",
			upstream: func(t *testing.T) string { return newFakeResolver(t, answer) },
			ask:      askUDP,
		},
		{
			network: "tcp",
			upstream: func(t *testing.T) string {
				address, _ := newTCPResolver(t, answer)
				return address
			},
			ask: askTCP,
		},
	} {
		t.Run(tc.network, func(t *testing.T) {
			relay, err := NewRelayForUpstreams([]string{tc.upstream(t)})
			if err != nil {
				t.Fatal(err)
			}
			var policy switchablePolicy
			_, udpAddr, tcpAddr := serveLoopbackWithPolicy(t, relay, &policy)
			addr := udpAddr
			if tc.network == "tcp" {
				addr = tcpAddr
			}
			conn := dialRelay(t, tc.network, addr)

			checkAnswer(t, tc.ask(t, conn, dnsQuery(1)), 1, dnsmessage.RCodeRefused)
			policy.policy.Store(&ateletpb.EgressPolicy{Rules: []*ateletpb.EgressRule{{
				Http: &ateletpb.EgressDestinations{Hostnames: []string{"example.com", "*.wild.example"}},
			}}})
			checkAnswer(t, tc.ask(t, conn, dnsQuery(2)), 2, dnsmessage.RCodeSuccess)
			wildQuery, wildQ := dnsQueryForName(t, 3, "api.wild.example.")
			checkAnswerForQuestion(t, tc.ask(t, conn, wildQuery), 3, wildQ, dnsmessage.RCodeSuccess)
			unlistedQuery, unlistedQ := dnsQueryForName(t, 4, "other.example.com.")
			checkAnswerForQuestion(t, tc.ask(t, conn, unlistedQuery), 4, unlistedQ, dnsmessage.RCodeRefused)
			policy.policy.Store(nil)
			checkAnswer(t, tc.ask(t, conn, dnsQuery(5)), 5, dnsmessage.RCodeRefused)
		})
	}
}

// A query waiting on its policy must not hold the relay up when the actor goes
// away: Stop cancels the fetch rather than wait for an atelet that may never
// answer, and returns the query's slot.
func TestStopCancelsAPolicyFetchInProgress(t *testing.T) {
	upstream := newFakeResolver(t, func(query []byte) []byte { return dnsAnswer(query, 0) })
	relay, err := NewRelayForUpstreams([]string{upstream})
	if err != nil {
		t.Fatal(err)
	}
	policy := blockingPolicy{called: make(chan struct{}, 1), release: make(chan struct{})}
	srv, udpAddr, _ := serveLoopbackWithPolicy(t, relay, policy)
	// Runs before the server's own cleanup, so a relay that ignores Stop fails
	// the test rather than hang it there.
	t.Cleanup(func() { close(policy.release) })
	client := dialRelay(t, "udp", udpAddr)

	if _, err := client.Write(dnsQuery(0x1111)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-policy.called:
	case <-time.After(5 * time.Second):
		t.Fatal("the relay never fetched the actor's policy")
	}

	stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := srv.Stop(stopCtx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if got := srv.limiter.inFlight.occupied(); got != 0 {
		t.Errorf("%d in-flight slots still held after Stop, want 0", got)
	}
}

func TestAllowQuery(t *testing.T) {
	question := func(name string) dnsmessage.Question {
		return dnsmessage.Question{
			Name:  dnsmessage.MustNewName(name),
			Type:  dnsmessage.TypeA,
			Class: dnsmessage.ClassINET,
		}
	}
	for _, tc := range []struct {
		name   string
		policy *ateletpb.EgressPolicy
		q      dnsmessage.Question
		want   bool
	}{
		{name: "no policy", policy: nil, q: exampleQuestion, want: false},
		{name: "policy without rules", policy: &ateletpb.EgressPolicy{}, q: exampleQuestion, want: false},
		{
			name: "matching exact host",
			policy: &ateletpb.EgressPolicy{Rules: []*ateletpb.EgressRule{{
				Https: &ateletpb.EgressDestinations{Hostnames: []string{"example.com"}},
			}}},
			q:    exampleQuestion,
			want: true,
		},
		{
			name: "unlisted host",
			policy: &ateletpb.EgressPolicy{Rules: []*ateletpb.EgressRule{{
				Https: &ateletpb.EgressDestinations{Hostnames: []string{"example.com"}},
			}}},
			q:    question("other.example.com."),
			want: false,
		},
		{
			name: "matching wildcard host",
			policy: &ateletpb.EgressPolicy{Rules: []*ateletpb.EgressRule{{
				Https: &ateletpb.EgressDestinations{Hostnames: []string{"*.example.com"}},
			}}},
			q:    question("api.example.com."),
			want: true,
		},
		{
			name: "wildcard does not match apex",
			policy: &ateletpb.EgressPolicy{Rules: []*ateletpb.EgressRule{{
				Https: &ateletpb.EgressDestinations{Hostnames: []string{"*.example.com"}},
			}}},
			q:    exampleQuestion,
			want: false,
		},
		{
			name: "wildcard does not match nested subdomain",
			policy: &ateletpb.EgressPolicy{Rules: []*ateletpb.EgressRule{{
				Https: &ateletpb.EgressDestinations{Hostnames: []string{"*.example.com"}},
			}}},
			q:    question("a.b.example.com."),
			want: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := allowQuery(tc.policy, tc.q); got != tc.want {
				t.Errorf("allowQuery = %t, want %t", got, tc.want)
			}
		})
	}
}
