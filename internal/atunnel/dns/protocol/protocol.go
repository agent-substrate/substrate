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

package protocol

import (
	"encoding/binary"

	"golang.org/x/net/dns/dnsmessage"
)

// DNS over HTTPS (RFC 8484), as spoken between atunnel and the egress PEP.
const (
	// DoHPath is the path the PEP serves DNS queries on.
	DoHPath = "/dns-query"
	// DoHQueryParam is the GET parameter that carries the query, base64url
	// encoded without padding.
	DoHQueryParam = "dns"
	// DoHMediaType is the media type of a DNS message in wire format.
	DoHMediaType = "application/dns-message"
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

// TxnID of the DNS packet.
func TxnID(raw []byte) uint16 {
	return binary.BigEndian.Uint16(raw[0:2])
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

// minUDPPayloadSize is the largest UDP DNS message a client accepts when its
// query does not advertise a size with EDNS (RFC 1035 section 4.2.1).
const minUDPPayloadSize = 512

// UDPPayloadSize returns the largest UDP response the sender of query accepts:
// the size in the query's EDNS OPT record (RFC 6891 section 6.1.2), or 512
// when there is none.
func UDPPayloadSize(query []byte) int {
	opt, ok := optHeader(query)
	if !ok {
		return minUDPPayloadSize
	}
	return max(minUDPPayloadSize, int(opt.Class))
}

// HasEDNS reports whether msg carries an EDNS OPT record.
func HasEDNS(msg []byte) bool {
	_, ok := optHeader(msg)
	return ok
}

// optHeader returns the header of msg's EDNS OPT record.
func optHeader(msg []byte) (dnsmessage.ResourceHeader, bool) {
	var p dnsmessage.Parser
	if _, err := p.Start(msg); err != nil {
		return dnsmessage.ResourceHeader{}, false
	}
	if p.SkipAllQuestions() != nil || p.SkipAllAnswers() != nil || p.SkipAllAuthorities() != nil {
		return dnsmessage.ResourceHeader{}, false
	}
	for {
		h, err := p.AdditionalHeader()
		if err != nil {
			return dnsmessage.ResourceHeader{}, false
		}
		if h.Type == dnsmessage.TypeOPT {
			return h, true
		}
		if p.SkipAdditional() != nil {
			return dnsmessage.ResourceHeader{}, false
		}
	}
}

// TruncatedPacket returns a response to the question q that carries the
// response header hdr with the TC bit set and no records, which tells the
// client to retry over TCP.
func TruncatedPacket(hdr dnsmessage.Header, q dnsmessage.Question) []byte {
	hdr.Response = true
	hdr.Truncated = true
	b := dnsmessage.NewBuilder(nil, hdr)
	if err := b.StartQuestions(); err == nil {
		_ = b.Question(q)
	}
	out, err := b.Finish()
	if err != nil {
		return ErrorPacket(hdr, dnsmessage.RCodeServerFailure, nil)
	}
	return out
}
