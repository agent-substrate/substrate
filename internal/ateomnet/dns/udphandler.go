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
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"
)

const (
	// defaultSweepInterval is how often pendingRequests is checked for expired
	// UDP queries.
	defaultSweepInterval = 50 * time.Millisecond
	// defaultUpstreamAttemptTimeout bounds a single UDP upstream attempt
	// when fallback upstreams remain.
	defaultUpstreamAttemptTimeout = 1 * time.Second
	// maxDatagramSize that we accept. Raising this consumes more memory while
	// processing packets.
	//
	// References:
	// - 4096: https://www.rfc-editor.org/info/rfc6891/#section-6.2.5
	// - 1232: CoreDNS default, https://www.dnsflagday.net/2020/#dns-flag-day-2020
	maxDatagramSize = 4 * 1024
)

// udpHandler manages actor UDP DNS traffic using an actorSock from
// the Actor and upstreamSock out to the upstream DNS server.
type udpHandler struct {
	// actorSock that receives DNS requests from the Actor.
	actorSock net.PacketConn
	// upstreamSock for the outbound DNS requests.
	upstreamSock net.PacketConn

	upstreams    []*net.UDPAddr
	upstreamStrs []string

	dns           *dnsHandler
	pending       *pendingRequests
	sweepInterval time.Duration
}

func newUDPHandler(
	actorSock net.PacketConn,
	upstreamSock net.PacketConn,
	upstreams []*net.UDPAddr,
	dns *dnsHandler,
	pending *pendingRequests,
) *udpHandler {
	strs := make([]string, len(upstreams))
	for i, u := range upstreams {
		strs[i] = normalizeAddr(u)
	}
	return &udpHandler{
		actorSock:     actorSock,
		upstreamSock:  upstreamSock,
		upstreams:     upstreams,
		upstreamStrs:  strs,
		dns:           dns,
		pending:       pending,
		sweepInterval: defaultSweepInterval,
	}
}

// serve runs the readers, and timeout sweep ticker until ctx is canceled
// or the sockets close.
func (h *udpHandler) serve(ctx context.Context) error {
	var wg sync.WaitGroup
	errCh := make(chan error, 2)

	wg.Add(3)
	go func() {
		defer wg.Done()
		if err := h.processRequests(ctx); err != nil {
			errCh <- err
		}
	}()
	go func() {
		defer wg.Done()
		if err := h.processResponses(ctx); err != nil {
			errCh <- err
		}
	}()
	go func() {
		defer wg.Done()
		h.sweepLoop(ctx)
	}()

	wg.Wait()
	h.pending.clearAll()
	close(errCh)

	var errs error
	for err := range errCh {
		errs = errors.Join(errs, err)
	}

	return errs
}

func (h *udpHandler) processRequests(ctx context.Context) error {
	buf := make([]byte, maxDatagramSize)
	for {
		n, from, err := h.actorSock.ReadFrom(buf)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return nil
			}
			return fmt.Errorf("dns relay: reading actor DNS query: %w", err)
		}
		raw := bytes.Clone(buf[:n])
		act := h.dns.onUDPRequest(raw, from, h.upstreamStrs)
		switch act.kind {
		case actionDrop:
			continue
		case actionReply:
			if _, err := h.actorSock.WriteTo(act.payload, from); err != nil && ctx.Err() == nil && !errors.Is(err, net.ErrClosed) {
				slog.WarnContext(ctx, "dns relay: could not return a synthesized DNS reply", slog.Any("err", err))
			}
		case actionForward:
			h.sendToUpstream(ctx, act.upstreamID, 0, act.payload)
		}
	}
}

func (h *udpHandler) sendToUpstream(ctx context.Context, upstreamID uint16, idx int, query []byte) {
	for idx < len(h.upstreams) {
		_, err := h.upstreamSock.WriteTo(query, h.upstreams[idx])
		if err == nil {
			return
		}
		if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
			slog.ErrorContext(ctx, "dns relay: upstream UDP socket closed, stopping forwarding")
			h.pending.clearAll()
			return
		}
		slog.WarnContext(ctx, "dns relay: could not send query to upstream", slog.String("upstream", h.upstreamStrs[idx]), slog.Any("err", err))
		nextIdx, nextQuery, ok := h.pending.failOverOnSendError(upstreamID, idx)
		if !ok {
			return
		}
		idx = nextIdx
		query = nextQuery
	}
}

func (h *udpHandler) processResponses(ctx context.Context) error {
	buf := make([]byte, maxDatagramSize)
	for {
		n, from, err := h.upstreamSock.ReadFrom(buf)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return nil
			}
			slog.DebugContext(ctx, "dns relay: upstream socket read error", slog.Any("err", err))
			continue
		}
		raw := bytes.Clone(buf[:n])
		act := h.dns.onUDPResponse(raw, from)
		switch act.kind {
		case actionDrop:
			continue
		case actionFailover:
			if act.upstreamIdx >= 0 && act.upstreamIdx < len(h.upstreams) && len(act.payload) >= 2 {
				upstreamID := binary.BigEndian.Uint16(act.payload[0:2])
				h.sendToUpstream(ctx, upstreamID, act.upstreamIdx, act.payload)
			}
		case actionDeliver:
			if act.clientAddr != nil {
				if _, err := h.actorSock.WriteTo(act.payload, act.clientAddr); err != nil && ctx.Err() == nil && !errors.Is(err, net.ErrClosed) {
					slog.WarnContext(ctx, "dns relay: could not return a DNS answer", slog.Any("err", err))
				}
			}
		}
	}
}

func (h *udpHandler) sweepLoop(ctx context.Context) {
	interval := h.sweepInterval
	if interval <= 0 {
		interval = defaultSweepInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			failovers, deliveries := h.pending.sweep(now)
			for _, f := range failovers {
				h.sendToUpstream(ctx, f.upstreamID, f.upstreamIdx, f.query)
			}
			for _, d := range deliveries {
				if _, err := h.actorSock.WriteTo(d.payload, d.clientAddr); err != nil && ctx.Err() == nil && !errors.Is(err, net.ErrClosed) {
					slog.WarnContext(ctx, "dns relay: could not return a deferred DNS answer", slog.Any("err", err))
				}
			}
		}
	}
}
