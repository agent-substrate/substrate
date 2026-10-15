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
	"bytes"
	"encoding/binary"
	"net"
	"testing"

	"github.com/agent-substrate/substrate/internal/ateomnet/dns/protocol"
	"golang.org/x/net/dns/dnsmessage"
)

func buildTestMessage(t *testing.T, hdr dnsmessage.Header, questions []dnsmessage.Question) []byte {
	t.Helper()
	b := dnsmessage.NewBuilder(nil, hdr)
	if len(questions) > 0 {
		if err := b.StartQuestions(); err != nil {
			t.Fatal(err)
		}
		for _, q := range questions {
			if err := b.Question(q); err != nil {
				t.Fatal(err)
			}
		}
	}
	msg, err := b.Finish()
	if err != nil {
		t.Fatal(err)
	}
	return msg
}

func TestDNSHandlerOnRequestDrop(t *testing.T) {
	lim := &limiter{
		inFlight:    &limiterSlots{max: 1},
		connections: &limiterSlots{max: 1},
	}
	pending := newPendingRequests(lim)
	h := newDNSHandler(pending, lim, nil)
	upstreams := []string{"10.96.0.10:53"}

	// 1. Short packet (< 12 bytes).
	if act := h.onUDPRequest([]byte{1, 2, 3, 4}, nil, upstreams); act.kind != actionDrop {
		t.Errorf("short UDP packet action = %v, want actionDrop", act.kind)
	}
	if act := h.onTCPRequest([]byte{1, 2, 3, 4}); act.kind != actionDrop {
		t.Errorf("short TCP packet action = %v, want actionDrop", act.kind)
	}

	// 2. Packet with QR == 1 (response bit set).
	resp := dnsAnswer(dnsQuery(0x1234), 0)
	if act := h.onUDPRequest(resp, nil, upstreams); act.kind != actionDrop {
		t.Errorf("QR=1 UDP packet action = %v, want actionDrop", act.kind)
	}
	if act := h.onTCPRequest(resp); act.kind != actionDrop {
		t.Errorf("QR=1 TCP packet action = %v, want actionDrop", act.kind)
	}

	// 3. Rate-limited (inFlight full) drops UDP requests.
	lim.inFlight.tryAcquire()
	defer lim.inFlight.release()
	if act := h.onUDPRequest(dnsQuery(0x1234), nil, upstreams); act.kind != actionDrop {
		t.Errorf("rate-limited UDP packet action = %v, want actionDrop", act.kind)
	}
}

func TestDNSHandlerOnRequestSynthesizedReplies(t *testing.T) {
	lim := newLimiter()
	pending := newPendingRequests(lim)
	allow := func(q dnsmessage.Question) bool {
		return q.Name.String() != "blocked.example.com." && q.Type != dnsmessage.TypeTXT
	}
	h := newDNSHandler(pending, lim, allow)
	upstreams := []string{"10.96.0.10:53"}

	qExample := dnsmessage.Question{
		Name:  dnsmessage.MustNewName("example.com."),
		Type:  dnsmessage.TypeA,
		Class: dnsmessage.ClassINET,
	}

	tests := []struct {
		name      string
		raw       []byte
		wantRCode dnsmessage.RCode
	}{
		{
			name: "QDCOUNT == 0 -> FORMERR",
			raw: buildTestMessage(t, dnsmessage.Header{
				ID:               0x1001,
				RecursionDesired: true,
			}, nil),
			wantRCode: dnsmessage.RCodeFormatError,
		},
		{
			name: "QDCOUNT == 2 -> FORMERR",
			raw: buildTestMessage(t, dnsmessage.Header{
				ID:               0x1002,
				RecursionDesired: true,
			}, []dnsmessage.Question{qExample, qExample}),
			wantRCode: dnsmessage.RCodeFormatError,
		},
		{
			name: "malformed question name -> FORMERR",
			raw: []byte{
				0x10, 0x03, 0x01, 0x00,
				0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				0x10, 'a', 'b', // label length 16 but truncated
			},
			wantRCode: dnsmessage.RCodeFormatError,
		},
		{
			name: "non-zero OpCode -> NOTIMP",
			raw: buildTestMessage(t, dnsmessage.Header{
				ID:               0x1004,
				OpCode:           1, // IQUERY
				RecursionDesired: true,
			}, []dnsmessage.Question{qExample}),
			wantRCode: dnsmessage.RCodeNotImplemented,
		},
		{
			name: "QTYPE AXFR -> NOTIMP",
			raw: buildTestMessage(t, dnsmessage.Header{
				ID:               0x1005,
				RecursionDesired: true,
			}, []dnsmessage.Question{{
				Name:  dnsmessage.MustNewName("example.com."),
				Type:  dnsmessage.TypeAXFR,
				Class: dnsmessage.ClassINET,
			}}),
			wantRCode: dnsmessage.RCodeNotImplemented,
		},
		{
			name: "QTYPE IXFR -> NOTIMP",
			raw: buildTestMessage(t, dnsmessage.Header{
				ID:               0x1006,
				RecursionDesired: true,
			}, []dnsmessage.Question{{
				Name:  dnsmessage.MustNewName("example.com."),
				Type:  protocol.TypeIXFR,
				Class: dnsmessage.ClassINET,
			}}),
			wantRCode: dnsmessage.RCodeNotImplemented,
		},
		{
			name: "disallowed domain -> REFUSED",
			raw: buildTestMessage(t, dnsmessage.Header{
				ID:               0x1007,
				RecursionDesired: true,
			}, []dnsmessage.Question{{
				Name:  dnsmessage.MustNewName("Blocked.Example.COM."),
				Type:  dnsmessage.TypeA,
				Class: dnsmessage.ClassINET,
			}}),
			wantRCode: dnsmessage.RCodeRefused,
		},
		{
			name: "disallowed record type -> REFUSED",
			raw: buildTestMessage(t, dnsmessage.Header{
				ID:               0x1008,
				RecursionDesired: true,
			}, []dnsmessage.Question{{
				Name:  dnsmessage.MustNewName("example.com."),
				Type:  dnsmessage.TypeTXT,
				Class: dnsmessage.ClassINET,
			}}),
			wantRCode: dnsmessage.RCodeRefused,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			wantID := binary.BigEndian.Uint16(tc.raw[0:2])
			for _, act := range []action{
				h.onUDPRequest(tc.raw, nil, upstreams),
				h.onTCPRequest(tc.raw),
			} {
				if act.kind != actionReply {
					t.Fatalf("action.kind = %v, want actionReply", act.kind)
				}
				var p dnsmessage.Parser
				hdr, err := p.Start(act.payload)
				if err != nil {
					t.Fatalf("parsing synthesized reply: %v", err)
				}
				if hdr.ID != wantID {
					t.Errorf("reply ID = %#x, want %#x", hdr.ID, wantID)
				}
				if !hdr.Response {
					t.Error("reply QR = false, want true")
				}
				if hdr.RCode != tc.wantRCode {
					t.Errorf("reply RCode = %v, want %v", hdr.RCode, tc.wantRCode)
				}
				if got := lim.inFlight.occupied(); got != 0 {
					t.Errorf("inFlight slots = %d after synthesized reply, want 0", got)
				}
			}
		})
	}
}

func TestDNSHandlerUDPForwardAndResponseValidation(t *testing.T) {
	lim := newLimiter()
	pending := newPendingRequests(lim)
	h := newDNSHandler(pending, lim, nil)

	upstream1 := &net.UDPAddr{IP: net.ParseIP("10.96.0.10"), Port: 53}
	upstream2 := &net.UDPAddr{IP: net.ParseIP("10.96.0.11"), Port: 53}
	wrongUpstream := &net.UDPAddr{IP: net.ParseIP("10.96.0.99"), Port: 53}
	clientAddr := &net.UDPAddr{IP: net.ParseIP("169.254.0.2"), Port: 54321}

	// Send query with mixed-case QNAME to verify case-insensitive RFC 4343 matching.
	query := buildTestMessage(t, dnsmessage.Header{
		ID:               0xbeef,
		RecursionDesired: true,
	}, []dnsmessage.Question{{
		Name:  dnsmessage.MustNewName("ExAmPlE.CoM."),
		Type:  dnsmessage.TypeA,
		Class: dnsmessage.ClassINET,
	}})

	act := h.onUDPRequest(bytes.Clone(query), clientAddr, []string{upstream1.String(), upstream2.String()})
	if act.kind != actionForward {
		t.Fatalf("onUDPRequest kind = %v, want actionForward", act.kind)
	}
	rewritten := act.payload
	upstreamID := act.upstreamID
	if gotID := binary.BigEndian.Uint16(rewritten[0:2]); gotID != upstreamID {
		t.Fatalf("payload ID = %#x, want upstreamID %#x", gotID, upstreamID)
	}

	// 1. Response with QR == 0 is dropped.
	if res := h.onUDPResponse(rewritten, upstream1); res.kind != actionDrop {
		t.Errorf("QR=0 response kind = %v, want actionDrop", res.kind)
	}

	// 2. Response from unexpected upstream address is dropped without evicting pending request.
	validResp := dnsAnswer(rewritten, 0)
	if res := h.onUDPResponse(validResp, wrongUpstream); res.kind != actionDrop {
		t.Errorf("wrong upstream source kind = %v, want actionDrop", res.kind)
	}

	// 3. Response with wrong transaction ID is dropped.
	wrongIDResp := bytes.Clone(validResp)
	binary.BigEndian.PutUint16(wrongIDResp[0:2], upstreamID^0xffff)
	if res := h.onUDPResponse(wrongIDResp, upstream1); res.kind != actionDrop {
		t.Errorf("wrong ID response kind = %v, want actionDrop", res.kind)
	}

	// 4. Response with mismatched QNAME is dropped.
	wrongQNameResp := buildTestMessage(t, dnsmessage.Header{
		ID:       upstreamID,
		Response: true,
	}, []dnsmessage.Question{{
		Name:  dnsmessage.MustNewName("other.com."),
		Type:  dnsmessage.TypeA,
		Class: dnsmessage.ClassINET,
	}})
	if res := h.onUDPResponse(wrongQNameResp, upstream1); res.kind != actionDrop {
		t.Errorf("wrong QNAME response kind = %v, want actionDrop", res.kind)
	}

	// 5. Response with mismatched QTYPE is dropped.
	wrongQTypeResp := buildTestMessage(t, dnsmessage.Header{
		ID:       upstreamID,
		Response: true,
	}, []dnsmessage.Question{{
		Name:  dnsmessage.MustNewName("example.com."),
		Type:  dnsmessage.TypeAAAA,
		Class: dnsmessage.ClassINET,
	}})
	if res := h.onUDPResponse(wrongQTypeResp, upstream1); res.kind != actionDrop {
		t.Errorf("wrong QTYPE response kind = %v, want actionDrop", res.kind)
	}

	// 6. SERVFAIL from upstream1 triggers ActionFailover to upstream2.
	servFailResp := dnsAnswer(rewritten, byte(dnsmessage.RCodeServerFailure))
	failoverAct := h.onUDPResponse(servFailResp, upstream1)
	if failoverAct.kind != actionFailover {
		t.Fatalf("SERVFAIL on first upstream kind = %v, want actionFailover", failoverAct.kind)
	}
	if failoverAct.upstreamIdx != 1 {
		t.Errorf("failover upstreamIdx = %d, want 1", failoverAct.upstreamIdx)
	}
	if got := lim.inFlight.occupied(); got != 1 {
		t.Errorf("inFlight slots during failover = %d, want 1", got)
	}

	// 7. Valid response from upstream2 (with different QNAME casing) delivers and restores clientRequestID.
	lowerResp := buildTestMessage(t, dnsmessage.Header{
		ID:       upstreamID,
		Response: true,
		RCode:    dnsmessage.RCodeSuccess,
	}, []dnsmessage.Question{{
		Name:  dnsmessage.MustNewName("example.com."),
		Type:  dnsmessage.TypeA,
		Class: dnsmessage.ClassINET,
	}})
	deliverAct := h.onUDPResponse(lowerResp, upstream2)
	if deliverAct.kind != actionDeliver {
		t.Fatalf("valid response on second upstream kind = %v, want actionDeliver", deliverAct.kind)
	}
	if gotID := binary.BigEndian.Uint16(deliverAct.payload[0:2]); gotID != 0xbeef {
		t.Errorf("delivered ID = %#x, want 0xbeef", gotID)
	}
	if deliverAct.clientAddr != clientAddr {
		t.Errorf("delivered clientAddr = %v, want %v", deliverAct.clientAddr, clientAddr)
	}
	if got := lim.inFlight.occupied(); got != 0 {
		t.Errorf("inFlight slots after delivery = %d, want 0", got)
	}
}

func TestDNSHandlerTCPForwardAndResponseValidation(t *testing.T) {
	lim := newLimiter()
	pending := newPendingRequests(lim)
	h := newDNSHandler(pending, lim, nil)

	query := dnsQuery(0xbeef)
	act := h.onTCPRequest(bytes.Clone(query))
	if act.kind != actionForward {
		t.Fatalf("onTCPRequest kind = %v, want actionForward", act.kind)
	}
	// TCP does not rewrite the transaction ID or record into pendingRequests.
	if !bytes.Equal(act.payload, query) {
		t.Errorf("onTCPRequest modified query payload")
	}
	if len(pending.entries) != 0 || lim.inFlight.occupied() != 0 {
		t.Errorf("onTCPRequest touched pendingRequests (%d entries) or inFlight (%d slots)", len(pending.entries), lim.inFlight.occupied())
	}

	// Invalid responses are dropped.
	if res := h.onTCPResponse([]byte{1, 2, 3}); res.kind != actionDrop {
		t.Errorf("short TCP response kind = %v, want actionDrop", res.kind)
	}
	if res := h.onTCPResponse(query); res.kind != actionDrop {
		t.Errorf("QR=0 TCP response kind = %v, want actionDrop", res.kind)
	}

	// Valid response is delivered unmodified.
	resp := dnsAnswer(query, 0)
	deliverAct := h.onTCPResponse(resp)
	if deliverAct.kind != actionDeliver {
		t.Fatalf("valid TCP response kind = %v, want actionDeliver", deliverAct.kind)
	}
	if !bytes.Equal(deliverAct.payload, resp) {
		t.Errorf("delivered TCP response differs from input")
	}
}
