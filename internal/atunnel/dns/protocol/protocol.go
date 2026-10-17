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

	"golang.org/x/net/dns/dnsmessage"
)

const (
	dnsHeaderLen = 12

	// TypeIXFR is the incremental zone transfer QTYPE (RFC 1995).
	TypeIXFR dnsmessage.Type = 251

	flagQuery    = 0
	flagResponse = 1
)

// qrFlag is the query (0) or response (1) bit
func qrFlag(raw []byte) int {
	if raw[2]&0x80 == 0 {
		return 0
	}
	return 1
}

// RequestSanityCheck filters out obviously invalid packets.
func RequestSanityCheck(raw []byte) bool {
	// DNS packets need to be at least 12 bytes.
	if len(raw) < dnsHeaderLen {
		return false
	}
	if qrFlag(raw) != flagQuery {
		return false
	}
	return true
}

// ResponseSanityCheck filters out obviously invalid packets.
func ResponseSanityCheck(raw []byte) bool {
	// DNS packets need to be at least 12 bytes.
	const dnsHeaderLen = 12
	if len(raw) < dnsHeaderLen {
		return false
	}
	if qrFlag(raw) != flagResponse {
		return false
	}
	if QDCount(raw) != 1 {
		return false
	}
	return true
}

// SetTxnID in the DNS packet.
func SetTxnID(raw []byte, id uint16) {
	binary.BigEndian.PutUint16(raw[0:2], id)
}

// qdCountFromRaw from the raw packet.
func QDCount(raw []byte) uint16 {
	// +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
	// |                  QDCOUNT = 0                  |  bytes 4..5
	// +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
	return binary.BigEndian.Uint16(raw[4:6])
}

// responseHeader returns the header of a synthesized response to a request
// with header hdr.
func responseHeader(hdr dnsmessage.Header, rcode dnsmessage.RCode) dnsmessage.Header {
	return dnsmessage.Header{
		ID:                 hdr.ID,
		Response:           true,
		OpCode:             hdr.OpCode,
		RecursionDesired:   hdr.RecursionDesired,
		RecursionAvailable: true,
		RCode:              rcode,
	}
}

// NXDomainPacket returns a serialized NXDOMAIN response to the question q.
//
// The authority section carries an SOA record for the root zone whose TTL and
// MINIMUM are both 0. Resolvers that cache negative answers take the cache
// lifetime from that record (RFC 2308 section 5), so they do not cache this
// one; without an SOA record some, such as dnspython, cache it indefinitely.
func NXDomainPacket(hdr dnsmessage.Header, q dnsmessage.Question) []byte {
	root := dnsmessage.MustNewName(".")
	b := dnsmessage.NewBuilder(nil, responseHeader(hdr, dnsmessage.RCodeNameError))
	if err := b.StartQuestions(); err != nil {
		return ErrorPacket(hdr, dnsmessage.RCodeNameError, &q)
	}
	if err := b.Question(q); err != nil {
		return ErrorPacket(hdr, dnsmessage.RCodeNameError, &q)
	}
	if err := b.StartAuthorities(); err != nil {
		return ErrorPacket(hdr, dnsmessage.RCodeNameError, &q)
	}
	if err := b.SOAResource(
		dnsmessage.ResourceHeader{Name: root, Class: dnsmessage.ClassINET, TTL: 0},
		dnsmessage.SOAResource{NS: root, MBox: root, MinTTL: 0},
	); err != nil {
		return ErrorPacket(hdr, dnsmessage.RCodeNameError, &q)
	}
	out, err := b.Finish()
	if err != nil {
		return ErrorPacket(hdr, dnsmessage.RCodeNameError, &q)
	}
	return out
}

// ErrorPacket returns a serialized DNS packet signaling an error.
func ErrorPacket(hdr dnsmessage.Header, rcode dnsmessage.RCode, q *dnsmessage.Question) []byte {
	b := dnsmessage.NewBuilder(nil, responseHeader(hdr, rcode))
	// Try to tack on the question, if possible.
	if q != nil {
		if err := b.StartQuestions(); err == nil {
			_ = b.Question(*q)
		}
	}

	out, err := b.Finish()
	if err == nil {
		return out
	}

	// Backstop reply: we had errors generating a response. Synthesize an empty
	// DNS response that matches the request instead.
	fallback := make([]byte, dnsHeaderLen)
	SetTxnID(fallback, hdr.ID)
	fallback[2] = 0x80 | (byte(hdr.OpCode)&0x0f)<<3
	if hdr.RecursionDesired {
		fallback[2] |= 0x01
	}
	fallback[3] = 0x80 | (byte(rcode) & 0x0f)

	return fallback
}
