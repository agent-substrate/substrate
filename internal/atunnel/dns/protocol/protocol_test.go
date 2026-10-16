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

package protocol

import (
	"encoding/binary"
	"testing"

	"golang.org/x/net/dns/dnsmessage"
)

func TestRequestSanityCheck(t *testing.T) {
	tests := []struct {
		name string
		raw  []byte
		want bool
	}{
		{
			name: "nil slice",
			raw:  nil,
			want: false,
		},
		{
			name: "empty slice",
			raw:  []byte{},
			want: false,
		},
		{
			name: "short packet (11 bytes)",
			raw:  make([]byte, 11),
			want: false,
		},
		{
			name: "valid 12-byte query (QR=0)",
			raw:  make([]byte, 12),
			want: true,
		},
		{
			name: "valid query with non-QR bits set in byte 2",
			raw: []byte{
				0x12, 0x34, 0x7f, 0x00,
				0x00, 0x01, 0x00, 0x00,
				0x00, 0x00, 0x00, 0x00,
			},
			want: true,
		},
		{
			name: "response packet (QR=1)",
			raw: []byte{
				0x12, 0x34, 0x80, 0x00,
				0x00, 0x01, 0x00, 0x00,
				0x00, 0x00, 0x00, 0x00,
			},
			want: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := RequestSanityCheck(tc.raw); got != tc.want {
				t.Errorf("RequestSanityCheck(%x) = %v, want %v", tc.raw, got, tc.want)
			}
		})
	}
}

func TestResponseSanityCheck(t *testing.T) {
	tests := []struct {
		name string
		raw  []byte
		want bool
	}{
		{
			name: "nil slice",
			raw:  nil,
			want: false,
		},
		{
			name: "short packet (11 bytes)",
			raw:  make([]byte, 11),
			want: false,
		},
		{
			name: "query packet (QR=0) with QDCOUNT=1",
			raw: []byte{
				0x12, 0x34, 0x00, 0x00,
				0x00, 0x01, 0x00, 0x00,
				0x00, 0x00, 0x00, 0x00,
			},
			want: false,
		},
		{
			name: "response packet (QR=1) with QDCOUNT=0",
			raw: []byte{
				0x12, 0x34, 0x80, 0x00,
				0x00, 0x00, 0x00, 0x00,
				0x00, 0x00, 0x00, 0x00,
			},
			want: false,
		},
		{
			name: "response packet (QR=1) with QDCOUNT=2",
			raw: []byte{
				0x12, 0x34, 0x80, 0x00,
				0x00, 0x02, 0x00, 0x00,
				0x00, 0x00, 0x00, 0x00,
			},
			want: false,
		},
		{
			name: "valid response packet (QR=1, QDCOUNT=1)",
			raw: []byte{
				0x12, 0x34, 0x81, 0x80,
				0x00, 0x01, 0x00, 0x01,
				0x00, 0x00, 0x00, 0x00,
			},
			want: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := ResponseSanityCheck(tc.raw); got != tc.want {
				t.Errorf("ResponseSanityCheck(%x) = %v, want %v", tc.raw, got, tc.want)
			}
		})
	}
}

func TestQDCount(t *testing.T) {
	tests := []struct {
		qdCount uint16
	}{
		{0},
		{1},
		{2},
		{0x1234},
		{0xffff},
	}

	for _, tc := range tests {
		var raw [12]byte
		binary.BigEndian.PutUint16(raw[4:6], tc.qdCount)
		if got := QDCount(raw[:]); got != tc.qdCount {
			t.Errorf("QDCount() = %#x, want %#x", got, tc.qdCount)
		}
	}
}

func TestSetTxnID(t *testing.T) {
	var raw [12]byte
	SetTxnID(raw[:], 0xcafe)
	if got := binary.BigEndian.Uint16(raw[0:2]); got != 0xcafe {
		t.Errorf("SetTxnID wrote %#x, want 0xcafe", got)
	}
}

func TestErrorPacket(t *testing.T) {
	validQ := dnsmessage.Question{
		Name:  dnsmessage.MustNewName("example.com."),
		Type:  dnsmessage.TypeA,
		Class: dnsmessage.ClassINET,
	}

	t.Run("with valid question", func(t *testing.T) {
		reqHdr := dnsmessage.Header{
			ID:               0xabcd,
			OpCode:           0,
			RecursionDesired: true,
		}
		pkt := ErrorPacket(reqHdr, dnsmessage.RCodeRefused, &validQ)

		var p dnsmessage.Parser
		gotHdr, err := p.Start(pkt)
		if err != nil {
			t.Fatalf("p.Start() unexpected error: %v", err)
		}
		if gotHdr.ID != reqHdr.ID {
			t.Errorf("ID = %#x, want %#x", gotHdr.ID, reqHdr.ID)
		}
		if !gotHdr.Response {
			t.Error("Response = false, want true")
		}
		if gotHdr.OpCode != reqHdr.OpCode {
			t.Errorf("OpCode = %v, want %v", gotHdr.OpCode, reqHdr.OpCode)
		}
		if !gotHdr.RecursionDesired {
			t.Error("RecursionDesired = false, want true")
		}
		if !gotHdr.RecursionAvailable {
			t.Error("RecursionAvailable = false, want true")
		}
		if gotHdr.RCode != dnsmessage.RCodeRefused {
			t.Errorf("RCode = %v, want %v", gotHdr.RCode, dnsmessage.RCodeRefused)
		}

		questions, err := p.AllQuestions()
		if err != nil {
			t.Fatalf("p.AllQuestions() unexpected error: %v", err)
		}
		if len(questions) != 1 || questions[0] != validQ {
			t.Errorf("questions = %+v, want [%+v]", questions, validQ)
		}
	})

	t.Run("with nil question", func(t *testing.T) {
		reqHdr := dnsmessage.Header{
			ID:               0x1234,
			OpCode:           1,
			RecursionDesired: false,
		}
		pkt := ErrorPacket(reqHdr, dnsmessage.RCodeNotImplemented, nil)

		var p dnsmessage.Parser
		gotHdr, err := p.Start(pkt)
		if err != nil {
			t.Fatalf("p.Start() unexpected error: %v", err)
		}
		if gotHdr.ID != reqHdr.ID {
			t.Errorf("ID = %#x, want %#x", gotHdr.ID, reqHdr.ID)
		}
		if !gotHdr.Response {
			t.Error("Response = false, want true")
		}
		if gotHdr.OpCode != reqHdr.OpCode {
			t.Errorf("OpCode = %v, want %v", gotHdr.OpCode, reqHdr.OpCode)
		}
		if gotHdr.RecursionDesired {
			t.Error("RecursionDesired = true, want false")
		}
		if !gotHdr.RecursionAvailable {
			t.Error("RecursionAvailable = false, want true")
		}
		if gotHdr.RCode != dnsmessage.RCodeNotImplemented {
			t.Errorf("RCode = %v, want %v", gotHdr.RCode, dnsmessage.RCodeNotImplemented)
		}

		questions, err := p.AllQuestions()
		if err != nil {
			t.Fatalf("p.AllQuestions() unexpected error: %v", err)
		}
		if len(questions) != 0 {
			t.Errorf("len(questions) = %d, want 0", len(questions))
		}
	})

	t.Run("with unencodable question falls back to empty question section", func(t *testing.T) {
		// Zero-value Name (Length == 0, non-canonical) causes Builder.Question to fail,
		// but Builder.Finish still succeeds with 0 questions.
		badQ := dnsmessage.Question{
			Type:  dnsmessage.TypeA,
			Class: dnsmessage.ClassINET,
		}
		reqHdr := dnsmessage.Header{
			ID:               0x4321,
			RecursionDesired: true,
		}
		pkt := ErrorPacket(reqHdr, dnsmessage.RCodeFormatError, &badQ)

		var p dnsmessage.Parser
		gotHdr, err := p.Start(pkt)
		if err != nil {
			t.Fatalf("p.Start() unexpected error: %v", err)
		}
		if gotHdr.ID != reqHdr.ID {
			t.Errorf("ID = %#x, want %#x", gotHdr.ID, reqHdr.ID)
		}
		if gotHdr.RCode != dnsmessage.RCodeFormatError {
			t.Errorf("RCode = %v, want %v", gotHdr.RCode, dnsmessage.RCodeFormatError)
		}
		if qd := QDCount(pkt); qd != 0 {
			t.Errorf("QDCount = %d, want 0", qd)
		}
	})
}

func TestNXDomainPacket(t *testing.T) {
	q := dnsmessage.Question{
		Name:  dnsmessage.MustNewName("blocked.example.com."),
		Type:  dnsmessage.TypeA,
		Class: dnsmessage.ClassINET,
	}
	reqHdr := dnsmessage.Header{ID: 0xabcd, RecursionDesired: true}
	pkt := NXDomainPacket(reqHdr, q)

	var p dnsmessage.Parser
	gotHdr, err := p.Start(pkt)
	if err != nil {
		t.Fatalf("p.Start() unexpected error: %v", err)
	}
	if gotHdr.ID != reqHdr.ID {
		t.Errorf("ID = %#x, want %#x", gotHdr.ID, reqHdr.ID)
	}
	if !gotHdr.Response || !gotHdr.RecursionDesired || !gotHdr.RecursionAvailable {
		t.Errorf("header = %+v, want QR, RD, and RA set", gotHdr)
	}
	if gotHdr.RCode != dnsmessage.RCodeNameError {
		t.Errorf("RCode = %v, want %v", gotHdr.RCode, dnsmessage.RCodeNameError)
	}
	questions, err := p.AllQuestions()
	if err != nil {
		t.Fatalf("p.AllQuestions() unexpected error: %v", err)
	}
	if len(questions) != 1 || questions[0] != q {
		t.Errorf("questions = %+v, want [%+v]", questions, q)
	}
	answers, err := p.AllAnswers()
	if err != nil {
		t.Fatalf("p.AllAnswers() unexpected error: %v", err)
	}
	if len(answers) != 0 {
		t.Errorf("answers = %+v, want none", answers)
	}

	// The SOA record's TTL and MINIMUM bound how long a resolver may cache the
	// negative answer (RFC 2308 section 5); both must be 0.
	authorities, err := p.AllAuthorities()
	if err != nil {
		t.Fatalf("p.AllAuthorities() unexpected error: %v", err)
	}
	if len(authorities) != 1 {
		t.Fatalf("authorities = %+v, want one SOA record", authorities)
	}
	soaHdr := authorities[0].Header
	soa, ok := authorities[0].Body.(*dnsmessage.SOAResource)
	if !ok {
		t.Fatalf("authority record = %T, want *dnsmessage.SOAResource", authorities[0].Body)
	}
	if soaHdr.Name.String() != "." || soaHdr.Class != dnsmessage.ClassINET {
		t.Errorf("SOA owner/class = %v %v, want . ClassINET", soaHdr.Name, soaHdr.Class)
	}
	if soaHdr.TTL != 0 {
		t.Errorf("SOA TTL = %d, want 0", soaHdr.TTL)
	}
	if soa.MinTTL != 0 {
		t.Errorf("SOA MINIMUM = %d, want 0", soa.MinTTL)
	}
}

func TestNXDomainPacketUnencodableQuestion(t *testing.T) {
	// A zero-value Name cannot be encoded; the reply is still NXDOMAIN.
	badQ := dnsmessage.Question{Type: dnsmessage.TypeA, Class: dnsmessage.ClassINET}
	reqHdr := dnsmessage.Header{ID: 0x4321}
	pkt := NXDomainPacket(reqHdr, badQ)

	var p dnsmessage.Parser
	gotHdr, err := p.Start(pkt)
	if err != nil {
		t.Fatalf("p.Start() unexpected error: %v", err)
	}
	if gotHdr.ID != reqHdr.ID {
		t.Errorf("ID = %#x, want %#x", gotHdr.ID, reqHdr.ID)
	}
	if gotHdr.RCode != dnsmessage.RCodeNameError {
		t.Errorf("RCode = %v, want %v", gotHdr.RCode, dnsmessage.RCodeNameError)
	}
}
