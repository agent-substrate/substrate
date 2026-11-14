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

package dns

import (
	"context"
	"errors"
	"net"
	"testing"

	"github.com/google/go-cmp/cmp"
	"golang.org/x/net/dns/dnsmessage"

	"go.ate.dev/substrate/internal/atunnel/dns/protocol"
)

type exchangerFunc func(context.Context, []byte) ([]byte, error)

func (f exchangerFunc) Exchange(ctx context.Context, query []byte) ([]byte, error) {
	return f(ctx, query)
}

// serveDoHLoopback runs a relay that sends queries to exchanger. Its upstream
// is a closed port and its policy is nil, so a query that reached either would
// not be answered as these tests expect.
func serveDoHLoopback(t *testing.T, exchanger Exchanger) (udp, tcp net.Addr) {
	t.Helper()
	relay, err := NewRelayForUpstreams([]string{"127.0.0.1:1"})
	if err != nil {
		t.Fatal(err)
	}
	_, udp, tcp = serveLoopbackWith(t, relay, nil, exchanger)
	return udp, tcp
}

// dohQuery builds a query for name. A non-zero udpSize adds an EDNS OPT record
// advertising that UDP payload size.
func dohQuery(t *testing.T, id uint16, name string, udpSize uint16) []byte {
	t.Helper()
	b := dnsmessage.NewBuilder(nil, dnsmessage.Header{ID: id, RecursionDesired: true})
	if err := b.StartQuestions(); err != nil {
		t.Fatal(err)
	}
	if err := b.Question(dnsmessage.Question{Name: dnsmessage.MustNewName(name), Type: dnsmessage.TypeA, Class: dnsmessage.ClassINET}); err != nil {
		t.Fatal(err)
	}
	if udpSize != 0 {
		if err := b.StartAdditionals(); err != nil {
			t.Fatal(err)
		}
		var opt dnsmessage.ResourceHeader
		if err := opt.SetEDNS0(int(udpSize), dnsmessage.RCodeSuccess, false); err != nil {
			t.Fatal(err)
		}
		if err := b.OPTResource(opt, dnsmessage.OPTResource{}); err != nil {
			t.Fatal(err)
		}
	}
	msg, err := b.Finish()
	if err != nil {
		t.Fatal(err)
	}
	return msg
}

// dohAnswer builds a response to query with rcode and the given number of A
// records. Each record adds 16 bytes.
func dohAnswer(t *testing.T, query []byte, rcode dnsmessage.RCode, records int) []byte {
	t.Helper()
	var p dnsmessage.Parser
	hdr, err := p.Start(query)
	if err != nil {
		t.Fatal(err)
	}
	q, err := p.Question()
	if err != nil {
		t.Fatal(err)
	}
	b := dnsmessage.NewBuilder(nil, dnsmessage.Header{ID: hdr.ID, Response: true, RecursionDesired: true, RecursionAvailable: true, RCode: rcode})
	b.EnableCompression()
	if err := b.StartQuestions(); err != nil {
		t.Fatal(err)
	}
	if err := b.Question(q); err != nil {
		t.Fatal(err)
	}
	if err := b.StartAnswers(); err != nil {
		t.Fatal(err)
	}
	for i := range records {
		if err := b.AResource(
			dnsmessage.ResourceHeader{Name: q.Name, Class: dnsmessage.ClassINET, TTL: 30},
			dnsmessage.AResource{A: [4]byte{192, 0, 2, byte(i)}},
		); err != nil {
			t.Fatal(err)
		}
	}
	msg, err := b.Finish()
	if err != nil {
		t.Fatal(err)
	}
	return msg
}

func withID(msg []byte, id uint16) []byte {
	out := append([]byte(nil), msg...)
	protocol.SetTxnID(out, id)
	return out
}

func TestDoHRelayUDP(t *testing.T) {
	const clientID = 0x1234
	query := dohQuery(t, clientID, "example.com.", 0)
	ednsQuery := dohQuery(t, clientID, "example.com.", 1232)
	other := dohQuery(t, clientID, "other.example.", 0)
	// 40 A records: larger than 512 bytes, smaller than 1232.
	large := dohAnswer(t, query, dnsmessage.RCodeSuccess, 40)

	tests := []struct {
		name  string
		query []byte
		// respond is the PEP. Nil fails the test if the PEP is asked.
		respond func(query []byte) ([]byte, error)
		want    []byte
	}{
		{
			name:    "answer is returned with the actor's ID",
			query:   query,
			respond: func(q []byte) ([]byte, error) { return dohAnswer(t, q, dnsmessage.RCodeSuccess, 1), nil },
			want:    dohAnswer(t, query, dnsmessage.RCodeSuccess, 1),
		},
		{
			name:  "NXDOMAIN from the PEP is returned unchanged",
			query: query,
			respond: func(q []byte) ([]byte, error) {
				var p dnsmessage.Parser
				hdr, _ := p.Start(q)
				question, _ := p.Question()
				return protocol.NXDomainPacket(hdr, question), nil
			},
			want: func() []byte {
				var p dnsmessage.Parser
				hdr, _ := p.Start(query)
				question, _ := p.Question()
				return protocol.NXDomainPacket(hdr, question)
			}(),
		},
		{
			name:    "PEP failure is SERVFAIL",
			query:   query,
			respond: func([]byte) ([]byte, error) { return nil, errors.New("gateway unreachable") },
			want:    serverFailure(query),
		},
		{
			name:    "response to a different question is SERVFAIL",
			query:   query,
			respond: func([]byte) ([]byte, error) { return dohAnswer(t, withID(other, 0), dnsmessage.RCodeSuccess, 1), nil },
			want:    serverFailure(query),
		},
		{
			name:    "response that is not a DNS message is SERVFAIL",
			query:   query,
			respond: func([]byte) ([]byte, error) { return []byte("egress denied"), nil },
			want:    serverFailure(query),
		},
		{
			name:    "answer larger than 512 bytes is truncated without EDNS",
			query:   query,
			respond: func([]byte) ([]byte, error) { return withID(large, 0), nil },
			want: func() []byte {
				truncated := dohAnswer(t, query, dnsmessage.RCodeSuccess, 0)
				truncated[2] |= 0x02 // TC
				return truncated
			}(),
		},
		{
			name:    "answer within the EDNS payload size is returned whole",
			query:   ednsQuery,
			respond: func([]byte) ([]byte, error) { return withID(large, 0), nil },
			want:    large,
		},
		{
			name: "unsupported opcode is answered without asking the PEP",
			query: func() []byte {
				q := append([]byte(nil), query...)
				q[2] |= 0x10 // OPCODE 2, STATUS
				return q
			}(),
			want: func() []byte {
				q := append([]byte(nil), query...)
				q[2] |= 0x10
				var p dnsmessage.Parser
				hdr, _ := p.Start(q)
				question, _ := p.Question()
				return protocol.ErrorPacket(hdr, dnsmessage.RCodeNotImplemented, &question)
			}(),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			udp, _ := serveDoHLoopback(t, exchangerFunc(func(_ context.Context, q []byte) ([]byte, error) {
				if tc.respond == nil {
					t.Error("the PEP was asked")
					return nil, errors.New("unexpected exchange")
				}
				if id := protocol.TxnID(q); id != 0 {
					t.Errorf("query reached the PEP with ID %#x, want 0", id)
				}
				return tc.respond(q)
			}))
			client, err := net.Dial("udp", udp.String())
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			if _, err := client.Write(tc.query); err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(tc.want, readWithin(t, client)); diff != "" {
				t.Errorf("answer mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// Queries on one TCP connection are exchanged concurrently: the answer to the
// second query returns while the first is still with the PEP.
func TestDoHRelayTCPAnswersOutOfOrder(t *testing.T) {
	slow := dohQuery(t, 1, "slow.example.", 0)
	fast := dohQuery(t, 2, "fast.example.", 0)
	// 300 A records: larger than a UDP relay answer, so TCP carries it whole.
	slowAnswer := dohAnswer(t, slow, dnsmessage.RCodeSuccess, 300)

	releaseSlow := make(chan struct{})
	_, tcp := serveDoHLoopback(t, exchangerFunc(func(ctx context.Context, q []byte) ([]byte, error) {
		var p dnsmessage.Parser
		_, _ = p.Start(q)
		question, _ := p.Question()
		if question.Name.String() == "slow.example." {
			select {
			case <-releaseSlow:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			return withID(slowAnswer, 0), nil
		}
		return dohAnswer(t, q, dnsmessage.RCodeSuccess, 1), nil
	}))

	conn, err := net.Dial("tcp", tcp.String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	for _, q := range [][]byte{slow, fast} {
		if err := protocol.WriteTCPFrame(conn, q); err != nil {
			t.Fatal(err)
		}
	}

	first, err := protocol.ReadTCPFrame(conn)
	if err != nil {
		t.Fatal(err)
	}
	if diff := cmp.Diff(dohAnswer(t, fast, dnsmessage.RCodeSuccess, 1), first); diff != "" {
		t.Errorf("first answer is not the fast query's (-want +got):\n%s", diff)
	}
	close(releaseSlow)
	second, err := protocol.ReadTCPFrame(conn)
	if err != nil {
		t.Fatal(err)
	}
	if diff := cmp.Diff(slowAnswer, second); diff != "" {
		t.Errorf("second answer is not the slow query's (-want +got):\n%s", diff)
	}
}

// UDP and TCP queries share the in-flight limit. A TCP query over the limit is
// answered SERVFAIL.
func TestDoHRelayLimitsQueriesInFlight(t *testing.T) {
	asked := make(chan struct{}, maxInFlight)
	release := make(chan struct{})
	defer close(release)
	udp, tcp := serveDoHLoopback(t, exchangerFunc(func(ctx context.Context, _ []byte) ([]byte, error) {
		asked <- struct{}{}
		select {
		case <-release:
		case <-ctx.Done():
		}
		return nil, errors.New("released")
	}))

	client, err := net.Dial("udp", udp.String())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	for i := range maxInFlight {
		if _, err := client.Write(dohQuery(t, uint16(i), "example.com.", 0)); err != nil {
			t.Fatal(err)
		}
		<-asked
	}

	conn, err := net.Dial("tcp", tcp.String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	query := dohQuery(t, 0x7777, "example.com.", 0)
	if err := protocol.WriteTCPFrame(conn, query); err != nil {
		t.Fatal(err)
	}
	got, err := protocol.ReadTCPFrame(conn)
	if err != nil {
		t.Fatal(err)
	}
	if diff := cmp.Diff(serverFailure(query), got); diff != "" {
		t.Errorf("answer over the limit mismatch (-want +got):\n%s", diff)
	}
}
