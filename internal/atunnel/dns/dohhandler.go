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
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	"golang.org/x/net/dns/dnsmessage"

	"go.ate.dev/substrate/internal/atunnel/dns/protocol"
)

// Exchanger sends an actor's DNS query, in wire format, to the egress PEP and
// returns the PEP's response. The PEP enforces policy and resolves the name.
type Exchanger interface {
	Exchange(ctx context.Context, query []byte) ([]byte, error)
}

// dohHandler answers an actor's UDP and TCP DNS queries by exchanging each
// one with the egress PEP. It applies no policy.
type dohHandler struct {
	actorSock net.PacketConn
	listener  net.Listener

	dns       *dnsHandler
	limiter   *limiter
	exchanger Exchanger

	tcpTimeout time.Duration
}

func newDoHHandler(actorSock net.PacketConn, listener net.Listener, lim *limiter, exchanger Exchanger) *dohHandler {
	return &dohHandler{
		actorSock:  actorSock,
		listener:   listener,
		dns:        newDNSHandler(nil, lim, nil),
		limiter:    lim,
		exchanger:  exchanger,
		tcpTimeout: defaultConnectionTimeout,
	}
}

// exchange returns the PEP's response to the query raw, which has passed
// onRequestCommon, or a SERVFAIL when the PEP gives no usable response.
func (h *dohHandler) exchange(ctx context.Context, raw []byte, cq dnsmessage.Question) []byte {
	clientID := protocol.TxnID(raw)
	query := bytes.Clone(raw)
	// RFC 8484 section 4.1: an ID of 0 lets HTTP caches match equal queries.
	protocol.SetTxnID(query, 0)

	ctx, cancel := context.WithTimeout(ctx, dnsExchangeTimeout)
	defer cancel()
	resp, err := h.exchanger.Exchange(ctx, query)
	if err == nil {
		err = checkResponse(resp, cq)
	}
	if err != nil {
		// Debug, because an actor without an egress gateway fails every query.
		slog.DebugContext(ctx, "dns relay: no DNS response from the egress gateway", slog.Any("err", err))
		return serverFailure(raw)
	}
	protocol.SetTxnID(resp, clientID)
	return resp
}

// checkResponse reports whether resp is a response to the question cq.
func checkResponse(resp []byte, cq dnsmessage.Question) error {
	if !protocol.ResponseSanityCheck(resp) {
		return errors.New("not a DNS response with one question")
	}
	var p dnsmessage.Parser
	if _, err := p.Start(resp); err != nil {
		return fmt.Errorf("parsing response header: %w", err)
	}
	q, err := p.Question()
	if err != nil {
		return fmt.Errorf("parsing response question: %w", err)
	}
	if canonicalizeQuestion(q) != cq {
		return errors.New("response answers a different question")
	}
	return nil
}

// serverFailure returns a SERVFAIL response to the query raw.
func serverFailure(raw []byte) []byte {
	var p dnsmessage.Parser
	hdr, err := p.Start(raw)
	if err != nil {
		return protocol.ErrorPacket(dnsmessage.Header{ID: protocol.TxnID(raw)}, dnsmessage.RCodeServerFailure, nil)
	}
	var qPtr *dnsmessage.Question
	if q, err := p.Question(); err == nil {
		qPtr = &q
	}
	return protocol.ErrorPacket(hdr, dnsmessage.RCodeServerFailure, qPtr)
}

// fitUDP returns resp, or a truncated response when resp is larger than the
// sender of query accepts over UDP.
func fitUDP(query, resp []byte) []byte {
	if len(resp) <= protocol.UDPPayloadSize(query) {
		return resp
	}
	var p dnsmessage.Parser
	hdr, err := p.Start(resp)
	if err != nil {
		return serverFailure(query)
	}
	q, err := p.Question()
	if err != nil {
		return serverFailure(query)
	}
	return protocol.TruncatedPacket(hdr, q)
}

// serveUDP answers actor UDP queries until ctx is canceled or the socket
// closes.
func (h *dohHandler) serveUDP(ctx context.Context) error {
	var wg sync.WaitGroup
	defer wg.Wait()

	buf := make([]byte, maxDatagramSize)
	for {
		n, from, err := h.actorSock.ReadFrom(buf)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return nil
			}
			return fmt.Errorf("dns relay: reading actor DNS query: %w", err)
		}
		if !h.limiter.inFlight.tryAcquire() {
			slog.DebugContext(ctx, "dns relay dropped query; too many in flight")
			continue
		}
		raw := bytes.Clone(buf[:n])
		_, cq, act := h.dns.onRequestCommon(raw)
		switch act.kind {
		case actionDrop:
			h.limiter.inFlight.release()
		case actionReply:
			h.writeUDP(ctx, act.payload, from)
			h.limiter.inFlight.release()
		case actionForward:
			wg.Go(func() {
				defer h.limiter.inFlight.release()
				h.writeUDP(ctx, fitUDP(raw, h.exchange(ctx, raw, cq)), from)
			})
		}
	}
}

func (h *dohHandler) writeUDP(ctx context.Context, msg []byte, to net.Addr) {
	if _, err := h.actorSock.WriteTo(msg, to); err != nil && ctx.Err() == nil && !errors.Is(err, net.ErrClosed) {
		slog.WarnContext(ctx, "dns relay: could not return a DNS answer", slog.Any("err", err))
	}
}

// serveTCP accepts actor TCP DNS connections until ctx is canceled or the
// listener closes.
func (h *dohHandler) serveTCP(ctx context.Context) error {
	var wg sync.WaitGroup
	defer wg.Wait()

	for {
		conn, err := h.listener.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return nil
			}
			return fmt.Errorf("dns: accepting actor DNS connection: %w", err)
		}
		if !h.limiter.connections.tryAcquire() {
			slog.DebugContext(ctx, "dns relay: refusing connection, at limit")
			_ = conn.Close()
			continue
		}
		wg.Go(func() {
			defer h.limiter.connections.release()
			h.onConnect(ctx, conn)
		})
	}
}

// onConnect answers the frames on one actor connection. Queries are exchanged
// concurrently, so responses can return out of order (RFC 7766 section 6.2.1.1).
func (h *dohHandler) onConnect(ctx context.Context, conn net.Conn) {
	defer conn.Close()
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()

	client := &tcpClientConn{conn: conn}
	// Queries in flight when the actor stops sending are still answered.
	var wg sync.WaitGroup
	defer wg.Wait()

	write := func(msg []byte) bool {
		_ = conn.SetDeadline(time.Now().Add(h.tcpTimeout))
		if err := client.writeFrame(msg); err != nil {
			if ctx.Err() == nil && !errors.Is(err, net.ErrClosed) {
				slog.DebugContext(ctx, "dns relay: error on TCP reply", slog.Any("err", err))
			}
			_ = conn.Close()
			return false
		}
		return true
	}

	for {
		_ = conn.SetDeadline(time.Now().Add(h.tcpTimeout))
		raw, err := protocol.ReadTCPFrame(conn)
		if err != nil {
			return
		}
		_, cq, act := h.dns.onRequestCommon(raw)
		switch act.kind {
		case actionDrop:
			slog.DebugContext(ctx, "dns relay: dropping request and closing stream")
			return
		case actionReply:
			if !write(act.payload) {
				return
			}
		case actionForward:
			if !h.limiter.inFlight.tryAcquire() {
				if !write(serverFailure(raw)) {
					return
				}
				continue
			}
			wg.Go(func() {
				defer h.limiter.inFlight.release()
				write(h.exchange(ctx, raw, cq))
			})
		}
	}
}
