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
	"log/slog"
	"net"
	"time"

	"github.com/agent-substrate/substrate/internal/ateomnet/dns/protocol"
	"golang.org/x/net/dns/dnsmessage"
)

// actionKind describes the transport-agnostic decision returned by the DNS
// protocol handler.
type actionKind string

const (
	// actionDrop the DNS packet.
	actionDrop actionKind = "Drop"
	// actionReply immediately to the Actor request.
	actionReply actionKind = "Reply"
	// actionForward the packet upstream.
	actionForward actionKind = "Forward"
	// actionFailover to send the request to the next upstream.
	actionFailover actionKind = "Failover"
	// actionDeliver the DNS response to the Actor.
	actionDeliver actionKind = "Deliver"
)

// action is the decision returned by onUDPRequest, onUDPResponse,
// onTCPRequest, and onTCPResponse.
type action struct {
	kind        actionKind
	payload     []byte
	upstreamID  uint16
	clientAddr  net.Addr
	upstreamIdx int
}

// queryPolicy decides whether a parsed DNS question is permitted.
type queryPolicy func(q dnsmessage.Question) bool

// dnsHandler inspects DNS requests and responses and returns transport-agnostic
// routing decisions.
type dnsHandler struct {
	pending *pendingRequests
	limiter *limiter
	allow   queryPolicy
}

func newDNSHandler(pending *pendingRequests, lim *limiter, allow queryPolicy) *dnsHandler {
	if allow == nil {
		allow = func(dnsmessage.Question) bool { return true }
	}
	return &dnsHandler{
		pending: pending,
		limiter: lim,
		allow:   allow,
	}
}

// onRequestCommon checks packet structure and policy, returning actionDrop,
// actionReply (with a synthesized error response), or actionForward along with
// the parsed header and canonicalized question.
func (h *dnsHandler) onRequestCommon(raw []byte) (dnsmessage.Header, dnsmessage.Question, action) {
	if !protocol.RequestSanityCheck(raw) {
		return dnsmessage.Header{}, dnsmessage.Question{}, action{kind: actionDrop}
	}

	var p dnsmessage.Parser
	hdr, err := p.Start(raw)
	if err != nil {
		return hdr, dnsmessage.Question{}, errReplyAction(hdr, dnsmessage.RCodeFormatError, nil)
	}

	qdCount := protocol.QDCount(raw)

	// OpCode must be QUERY (== 0).
	if hdr.OpCode != 0 {
		var qPtr *dnsmessage.Question
		if qdCount == 1 {
			if q, qErr := p.Question(); qErr == nil {
				qPtr = &q
			}
		}
		return hdr, dnsmessage.Question{}, errReplyAction(hdr, dnsmessage.RCodeNotImplemented, qPtr)
	}

	// Must have one query.
	if qdCount != 1 {
		return hdr, dnsmessage.Question{}, errReplyAction(hdr, dnsmessage.RCodeFormatError, nil)
	}

	// Question is valid.
	q, err := p.Question()
	if err != nil {
		return hdr, dnsmessage.Question{}, errReplyAction(hdr, dnsmessage.RCodeFormatError, nil)
	}

	// Question type must be supported.
	if q.Type == dnsmessage.TypeAXFR || q.Type == protocol.TypeIXFR {
		return hdr, dnsmessage.Question{}, errReplyAction(hdr, dnsmessage.RCodeNotImplemented, &q)
	}

	// Check with query policy callout.
	cq := canonicalizeQuestion(q)
	if !h.allow(cq) {
		return hdr, dnsmessage.Question{}, errReplyAction(hdr, dnsmessage.RCodeRefused, &q)
	}

	return hdr, cq, action{
		kind:    actionForward,
		payload: raw,
	}
}

// onUDPRequest inspects an incoming UDP DNS query packet from the actor and
// returns whether to drop it, reply immediately with a synthesized error, or
// record and forward it to an upstream resolver.
func (h *dnsHandler) onUDPRequest(raw []byte, clientAddr net.Addr, upstreams []string) action {
	if !h.limiter.inFlight.tryAcquire() {
		slog.Debug("dns relay dropped query; too many in flight")
		return action{kind: actionDrop}
	}

	forwarded := false
	defer func() {
		if !forwarded {
			h.limiter.inFlight.release()
		}
	}()

	hdr, cq, act := h.onRequestCommon(raw)
	if act.kind != actionForward {
		return act
	}

	// Set to true before calling record, which takes ownership of the in-flight slot.
	forwarded = true
	upstreamID, ok := h.pending.record(raw, hdr.ID, cq, clientAddr, upstreams)
	if !ok {
		return action{kind: actionDrop}
	}

	return action{
		kind:       actionForward,
		payload:    raw,
		upstreamID: upstreamID,
	}
}

// onUDPResponse validates an incoming UDP DNS response packet from an upstream
// resolver against pendingRequests and returns whether to drop it, fail over to
// the next upstream, or deliver it to the actor.
func (h *dnsHandler) onUDPResponse(raw []byte, from net.Addr) action {
	if !protocol.ResponseSanityCheck(raw) {
		return action{kind: actionDrop}
	}

	// Parse response.
	var p dnsmessage.Parser
	hdr, err := p.Start(raw)
	if err != nil {
		return action{kind: actionDrop}
	}
	q, err := p.Question()
	if err != nil {
		return action{kind: actionDrop}
	}

	h.pending.mu.Lock()
	defer h.pending.mu.Unlock()

	// Look for request in the pending table.
	entry, ok := h.pending.entries[hdr.ID]
	cq := canonicalizeQuestion(q)

	if !ok || normalizeAddrString(entry.upstreams[entry.upstreamIdx]) != normalizeAddr(from) || entry.question != cq {
		// Response does not match any pending request.
		return action{kind: actionDrop}
	}

	// Check if we need to failover to the next upstream AND there are more
	// upstreams to try.
	if isFailoverRCode(hdr.RCode) && entry.upstreamIdx+1 < len(entry.upstreams) {
		entry.deferredResp = bytes.Clone(raw)
		entry.upstreamIdx++
		entry.expiry = time.Now().Add(h.pending.timeoutForAttempt(entry.upstreamIdx, len(entry.upstreams)))

		return action{
			kind:        actionFailover,
			payload:     bytes.Clone(entry.rawQuery),
			upstreamIdx: entry.upstreamIdx,
		}
	}

	// Forward response to Actor.
	clientID := entry.clientRequestID
	clientAddr := entry.clientAddr
	h.pending.deleteEntryLocked(hdr.ID)

	out := bytes.Clone(raw)
	binary.BigEndian.PutUint16(out[0:2], clientID)

	return action{
		kind:       actionDeliver,
		payload:    out,
		clientAddr: clientAddr,
	}
}

// onTCPRequest inspects an incoming TCP DNS query frame from the actor and
// returns whether to drop it, reply immediately with a synthesized error, or
// forward it unmodified on the connection's dedicated upstream stream.
func (h *dnsHandler) onTCPRequest(raw []byte) action {
	_, _, act := h.onRequestCommon(raw)
	return act
}

// onTCPResponse validates an incoming TCP DNS response frame from the
// connection's upstream stream and returns whether to drop or deliver it.
func (h *dnsHandler) onTCPResponse(raw []byte) action {
	if !protocol.ResponseSanityCheck(raw) {
		return action{kind: actionDrop}
	}
	var p dnsmessage.Parser
	if _, err := p.Start(raw); err != nil {
		return action{kind: actionDrop}
	}
	if _, err := p.Question(); err != nil {
		return action{kind: actionDrop}
	}
	return action{
		kind:    actionDeliver,
		payload: raw,
	}
}

// errReplyAction returns an Action for an error situation.
func errReplyAction(hdr dnsmessage.Header, rcode dnsmessage.RCode, q *dnsmessage.Question) action {
	return action{
		kind:    actionReply,
		payload: protocol.ErrorPacket(hdr, rcode, q),
	}
}

// isFailoverRCode returns true if the response error makes sense to try with
// the next DNS upstream server.
func isFailoverRCode(rcode dnsmessage.RCode) bool {
	switch rcode {
	case dnsmessage.RCodeServerFailure, dnsmessage.RCodeNotImplemented, dnsmessage.RCodeRefused:
		return true
	default:
		return false
	}
}

// canonicalizeQuestion normalizes the contents of the Question.
func canonicalizeQuestion(q dnsmessage.Question) dnsmessage.Question {
	for i := range int(q.Name.Length) {
		c := q.Name.Data[i]
		if 'A' <= c && c <= 'Z' {
			q.Name.Data[i] = c + ('a' - 'A')
		}
	}
	clear(q.Name.Data[q.Name.Length:])
	return q
}
