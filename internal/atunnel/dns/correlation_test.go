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
	"bytes"
	"encoding/binary"
	"fmt"
	"log/slog"
	"net"
	"net/netip"
	"testing"

	"golang.org/x/net/dns/dnsmessage"

	"go.ate.dev/substrate/internal/atunnel/dns/protocol"
	"go.ate.dev/substrate/internal/atunnel/fakeip"
)

var realAddress = [4]byte{93, 184, 216, 34}

func question(name string, qtype dnsmessage.Type) dnsmessage.Question {
	return dnsmessage.Question{Name: dnsmessage.MustNewName(name), Type: qtype, Class: dnsmessage.ClassINET}
}

func aRecord(name string, addr [4]byte) dnsmessage.Resource {
	return dnsmessage.Resource{
		Header: dnsmessage.ResourceHeader{Name: dnsmessage.MustNewName(name), Type: dnsmessage.TypeA, Class: dnsmessage.ClassINET, TTL: 300},
		Body:   &dnsmessage.AResource{A: addr},
	}
}

func cnameRecord(name, target string) dnsmessage.Resource {
	return dnsmessage.Resource{
		Header: dnsmessage.ResourceHeader{Name: dnsmessage.MustNewName(name), Type: dnsmessage.TypeCNAME, Class: dnsmessage.ClassINET, TTL: 300},
		Body:   &dnsmessage.CNAMEResource{CNAME: dnsmessage.MustNewName(target)},
	}
}

func aaaaRecord(name string) dnsmessage.Resource {
	return dnsmessage.Resource{
		Header: dnsmessage.ResourceHeader{Name: dnsmessage.MustNewName(name), Type: dnsmessage.TypeAAAA, Class: dnsmessage.ClassINET, TTL: 300},
		Body:   &dnsmessage.AAAAResource{AAAA: [16]byte{0x20, 0x01, 0x0d, 0xb8, 15: 1}},
	}
}

// upstreamResponse packs a response to q the way an upstream resolver sends it.
func upstreamResponse(t *testing.T, hdr dnsmessage.Header, q dnsmessage.Question, answers ...dnsmessage.Resource) []byte {
	t.Helper()
	hdr.Response = true
	hdr.RecursionDesired = true
	hdr.RecursionAvailable = true
	msg := dnsmessage.Message{Header: hdr, Questions: []dnsmessage.Question{q}, Answers: answers}
	raw, err := msg.Pack()
	if err != nil {
		t.Fatalf("packing the upstream response: %v", err)
	}
	return raw
}

// fakeAnswer fails the test unless raw answers q with exactly one A record,
// and returns that record's address.
func fakeAnswer(t *testing.T, raw []byte, id uint16, q dnsmessage.Question) netip.Addr {
	t.Helper()
	var msg dnsmessage.Message
	if err := msg.Unpack(raw); err != nil {
		t.Fatalf("parsing the answer: %v", err)
	}
	wantHdr := dnsmessage.Header{ID: id, Response: true, RecursionDesired: true, RecursionAvailable: true}
	if msg.Header != wantHdr {
		t.Errorf("answer header = %+v, want %+v", msg.Header, wantHdr)
	}
	if len(msg.Questions) != 1 || msg.Questions[0] != q {
		t.Errorf("answer questions = %+v, want the question as it was asked: %+v", msg.Questions, q)
	}
	if len(msg.Answers) != 1 || len(msg.Authorities) != 0 || len(msg.Additionals) != 0 {
		t.Fatalf("answer holds %d answers, %d authorities and %d additionals, want one answer only",
			len(msg.Answers), len(msg.Authorities), len(msg.Additionals))
	}
	answer := msg.Answers[0]
	wantAnswerHdr := dnsmessage.ResourceHeader{Name: q.Name, Type: dnsmessage.TypeA, Class: dnsmessage.ClassINET, TTL: fakeAddressTTL, Length: 4}
	if answer.Header != wantAnswerHdr {
		t.Errorf("answer record header = %+v, want %+v", answer.Header, wantAnswerHdr)
	}
	a, ok := answer.Body.(*dnsmessage.AResource)
	if !ok {
		t.Fatalf("answer record = %T, want an A record", answer.Body)
	}
	return netip.AddrFrom4(a.A)
}

func TestUpstreamAnswerGetsFakeAddress(t *testing.T) {
	tests := []struct {
		name    string
		q       dnsmessage.Question
		hdr     dnsmessage.Header
		answers []dnsmessage.Resource
		// wantName is the name the answer's fake address belongs to. Empty
		// when the upstream response reaches the actor unchanged.
		wantName string
	}{
		{
			name:     "A record",
			q:        question("db.example.com.", dnsmessage.TypeA),
			answers:  []dnsmessage.Resource{aRecord("db.example.com.", realAddress)},
			wantName: "db.example.com",
		},
		{
			name: "several A records",
			q:    question("db.example.com.", dnsmessage.TypeA),
			answers: []dnsmessage.Resource{
				aRecord("db.example.com.", realAddress),
				aRecord("db.example.com.", [4]byte{93, 184, 216, 35}),
			},
			wantName: "db.example.com",
		},
		{
			// The address belongs to the name the actor asked for, which is
			// the one its policy was checked against.
			name: "A record behind a CNAME",
			q:    question("www.example.com.", dnsmessage.TypeA),
			answers: []dnsmessage.Resource{
				cnameRecord("www.example.com.", "cdn.example.net."),
				aRecord("cdn.example.net.", realAddress),
			},
			wantName: "www.example.com",
		},
		{
			name:     "name asked in mixed case",
			q:        question("Db.ExAmPlE.cOm.", dnsmessage.TypeA),
			answers:  []dnsmessage.Resource{aRecord("Db.ExAmPlE.cOm.", realAddress)},
			wantName: "db.example.com",
		},
		{
			// The whole answer fits now, so the actor need not retry over TCP.
			name:     "truncated response with an A record",
			q:        question("db.example.com.", dnsmessage.TypeA),
			hdr:      dnsmessage.Header{Truncated: true},
			answers:  []dnsmessage.Resource{aRecord("db.example.com.", realAddress)},
			wantName: "db.example.com",
		},
		{
			// The actor reaches its own loopback, which a tunnel would not.
			name:    "name that resolves to loopback",
			q:       question("app.localtest.me.", dnsmessage.TypeA),
			answers: []dnsmessage.Resource{aRecord("app.localtest.me.", [4]byte{127, 0, 0, 1})},
		},
		{
			name:    "name that resolves to a link-local address",
			q:       question("metadata.example.com.", dnsmessage.TypeA),
			answers: []dnsmessage.Resource{aRecord("metadata.example.com.", [4]byte{169, 254, 169, 254})},
		},
		{
			name:    "name that resolves to the unspecified address",
			q:       question("blocked.example.com.", dnsmessage.TypeA),
			answers: []dnsmessage.Resource{aRecord("blocked.example.com.", [4]byte{0, 0, 0, 0})},
		},
		{
			name: "loopback among other addresses",
			q:    question("mixed.example.com.", dnsmessage.TypeA),
			answers: []dnsmessage.Resource{
				aRecord("mixed.example.com.", realAddress),
				aRecord("mixed.example.com.", [4]byte{127, 0, 0, 1}),
			},
		},
		{
			name: "name that does not exist",
			q:    question("typo.example.com.", dnsmessage.TypeA),
			hdr:  dnsmessage.Header{RCode: dnsmessage.RCodeNameError},
		},
		{
			name: "name with no A record",
			q:    question("v6only.example.com.", dnsmessage.TypeA),
		},
		{
			name:    "CNAME that leads to no address",
			q:       question("www.example.com.", dnsmessage.TypeA),
			answers: []dnsmessage.Resource{cnameRecord("www.example.com.", "gone.example.net.")},
		},
		{
			name: "truncated response without an A record",
			q:    question("db.example.com.", dnsmessage.TypeA),
			hdr:  dnsmessage.Header{Truncated: true},
		},
		{
			name: "server failure",
			q:    question("db.example.com.", dnsmessage.TypeA),
			hdr:  dnsmessage.Header{RCode: dnsmessage.RCodeServerFailure},
		},
		{
			name:    "AAAA query",
			q:       question("db.example.com.", dnsmessage.TypeAAAA),
			answers: []dnsmessage.Resource{aaaaRecord("db.example.com.")},
		},
		{
			name:    "query for every record type",
			q:       question("db.example.com.", dnsmessage.TypeALL),
			answers: []dnsmessage.Resource{aRecord("db.example.com.", realAddress)},
		},
		{
			name: "A query outside the Internet class",
			q: dnsmessage.Question{
				Name: dnsmessage.MustNewName("db.example.com."), Type: dnsmessage.TypeA, Class: dnsmessage.ClassCHAOS,
			},
			answers: []dnsmessage.Resource{aRecord("db.example.com.", realAddress)},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fakeIPs := fakeip.New(slog.Default())
			h := newDNSHandler(nil, newLimiter(), nil, nil, fakeIPs)
			tc.hdr.ID = 0x4242
			raw := upstreamResponse(t, tc.hdr, tc.q, tc.answers...)

			act := h.onTCPResponse(bytes.Clone(raw))
			if act.kind != actionDeliver {
				t.Fatalf("action = %v, want %v", act.kind, actionDeliver)
			}
			if tc.wantName == "" {
				if !bytes.Equal(act.payload, raw) {
					t.Fatalf("the upstream response was changed:\n got %x\nwant %x", act.payload, raw)
				}
				return
			}
			addr := fakeAnswer(t, act.payload, 0x4242, tc.q)
			if name, ok := fakeIPs.Name(addr); !ok || name != tc.wantName {
				t.Errorf("the answer's address %s belongs to %q, %v, want %q", addr, name, ok, tc.wantName)
			}
		})
	}
}

func TestUpstreamAnswerKeepsRealAddressWithoutFakeIPs(t *testing.T) {
	h := newDNSHandler(nil, newLimiter(), nil, nil, nil)
	q := question("db.example.com.", dnsmessage.TypeA)
	raw := upstreamResponse(t, dnsmessage.Header{ID: 0x4242}, q, aRecord("db.example.com.", realAddress))

	act := h.onTCPResponse(bytes.Clone(raw))
	if act.kind != actionDeliver || !bytes.Equal(act.payload, raw) {
		t.Fatalf("action = %v with a changed payload, want the upstream response delivered as it is", act.kind)
	}
}

// An answer the relay has no fake address for reaches the actor as it is, so
// the name resolves as it does without fake addresses.
func TestUpstreamAnswerWithoutFakeAddressIsKept(t *testing.T) {
	q := question("db.example.com.", dnsmessage.TypeA)
	brokenAnswers := upstreamResponse(t, dnsmessage.Header{ID: 0x4242}, q, cnameRecord("db.example.com.", "cdn.example.net."))
	// Claims a second answer that is not there.
	binary.BigEndian.PutUint16(brokenAnswers[6:8], 2)

	notHostname := question("_db._tcp.example.com.", dnsmessage.TypeA)

	usedUp := fakeip.New(slog.Default())
	for i := 0; ; i++ {
		if _, ok := usedUp.Address(fmt.Sprintf("host-%d.example.com", i)); !ok {
			break
		}
	}

	tests := []struct {
		name    string
		fakeIPs *fakeip.Table
		raw     []byte
	}{
		{name: "answers that do not parse", fakeIPs: fakeip.New(slog.Default()), raw: brokenAnswers},
		{
			name:    "name that is not a hostname",
			fakeIPs: fakeip.New(slog.Default()),
			raw:     upstreamResponse(t, dnsmessage.Header{ID: 0x4242}, notHostname, aRecord("_db._tcp.example.com.", realAddress)),
		},
		{
			name:    "no fake address left",
			fakeIPs: usedUp,
			raw:     upstreamResponse(t, dnsmessage.Header{ID: 0x4242}, q, aRecord("db.example.com.", realAddress)),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := newDNSHandler(nil, newLimiter(), nil, nil, tc.fakeIPs)
			act := h.onTCPResponse(bytes.Clone(tc.raw))
			if act.kind != actionDeliver || !bytes.Equal(act.payload, tc.raw) {
				t.Fatalf("action = %v with a changed payload, want the upstream response delivered as it is", act.kind)
			}
		})
	}
}

func TestFakeAddressIsStablePerName(t *testing.T) {
	fakeIPs := fakeip.New(slog.Default())
	h := newDNSHandler(nil, newLimiter(), nil, nil, fakeIPs)
	answerFor := func(name string) netip.Addr {
		t.Helper()
		q := question(name, dnsmessage.TypeA)
		act := h.onTCPResponse(upstreamResponse(t, dnsmessage.Header{ID: 0x4242}, q, aRecord(name, realAddress)))
		return fakeAnswer(t, act.payload, 0x4242, q)
	}

	db := answerFor("db.example.com.")
	if again := answerFor("db.example.com."); again != db {
		t.Errorf("db.example.com. was answered with %s, then %s", db, again)
	}
	if upper := answerFor("DB.EXAMPLE.COM."); upper != db {
		t.Errorf("DB.EXAMPLE.COM. was answered with %s, want %s as for db.example.com.", upper, db)
	}
	// Both names resolve to the same real address upstream.
	if api := answerFor("api.example.com."); api == db {
		t.Errorf("api.example.com. and db.example.com. share the fake address %s", db)
	}
}

func TestUDPResponseGetsFakeAddress(t *testing.T) {
	lim := newLimiter()
	pending := newPendingRequests(lim)
	fakeIPs := fakeip.New(slog.Default())
	h := newDNSHandler(pending, lim, nil, nil, fakeIPs)
	upstream := "10.96.0.10:53"
	clientAddr := &net.UDPAddr{IP: net.IPv4(169, 254, 17, 2), Port: 40000}

	query, q := dnsQueryForName(t, 0x1111, "db.example.com.")
	act := h.onUDPRequest(query, clientAddr, []string{upstream})
	if act.kind != actionForward {
		t.Fatalf("request action = %v, want %v", act.kind, actionForward)
	}

	response := upstreamResponse(t, dnsmessage.Header{ID: act.upstreamID}, q, aRecord("db.example.com.", realAddress))
	from, err := net.ResolveUDPAddr("udp", upstream)
	if err != nil {
		t.Fatal(err)
	}
	act = h.onUDPResponse(response, from)
	if act.kind != actionDeliver {
		t.Fatalf("response action = %v, want %v", act.kind, actionDeliver)
	}
	// The answer carries the actor's transaction ID, as any relayed one does.
	addr := fakeAnswer(t, act.payload, 0x1111, q)
	if name, ok := fakeIPs.Name(addr); !ok || name != "db.example.com" {
		t.Errorf("the answer's address %s belongs to %q, %v, want db.example.com", addr, name, ok)
	}
}

func TestRelayAnswersWithFakeAddresses(t *testing.T) {
	respond := func(query []byte) []byte {
		var p dnsmessage.Parser
		hdr, err := p.Start(query)
		if err != nil {
			return nil
		}
		q, err := p.Question()
		if err != nil {
			return nil
		}
		msg := dnsmessage.Message{
			Header:    dnsmessage.Header{ID: hdr.ID, Response: true, RecursionDesired: true, RecursionAvailable: true},
			Questions: []dnsmessage.Question{q},
			Answers:   []dnsmessage.Resource{aRecord(q.Name.String(), realAddress)},
		}
		raw, err := msg.Pack()
		if err != nil {
			return nil
		}
		return raw
	}
	udpUpstream := newFakeResolver(t, respond)
	tcpUpstream, _ := newTCPResolver(t, respond)

	tests := []struct {
		name     string
		upstream string
		// ask sends query to the relay and returns its answer.
		ask func(t *testing.T, udpAddr, tcpAddr net.Addr, query []byte) []byte
	}{
		{
			name:     "UDP",
			upstream: udpUpstream,
			ask: func(t *testing.T, udpAddr, _ net.Addr, query []byte) []byte {
				client, err := net.Dial("udp", udpAddr.String())
				if err != nil {
					t.Fatal(err)
				}
				defer client.Close()
				if _, err := client.Write(query); err != nil {
					t.Fatal(err)
				}
				return readWithin(t, client)
			},
		},
		{
			name:     "TCP",
			upstream: tcpUpstream,
			ask: func(t *testing.T, _, tcpAddr net.Addr, query []byte) []byte {
				client, err := net.Dial("tcp", tcpAddr.String())
				if err != nil {
					t.Fatal(err)
				}
				defer client.Close()
				if err := protocol.WriteTCPFrame(client, query); err != nil {
					t.Fatal(err)
				}
				answer, err := protocol.ReadTCPFrame(client)
				if err != nil {
					t.Fatalf("reading the relay's answer: %v", err)
				}
				return answer
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			relay, err := NewRelayForUpstreams([]string{tc.upstream})
			if err != nil {
				t.Fatal(err)
			}
			fakeIPs := fakeip.New(slog.Default())
			_, udpAddr, tcpAddr := serveLoopbackWithFakeIPs(t, relay, withPolicy, fakeIPs)

			query, q := dnsQueryForName(t, 0x2222, "db.example.com.")
			addr := fakeAnswer(t, tc.ask(t, udpAddr, tcpAddr, query), 0x2222, q)
			if !fakeip.Contains(addr) {
				t.Errorf("answer address %s is not a fake address", addr)
			}
			if name, ok := fakeIPs.Name(addr); !ok || name != "db.example.com" {
				t.Errorf("the answer's address %s belongs to %q, %v, want db.example.com", addr, name, ok)
			}
		})
	}
}
