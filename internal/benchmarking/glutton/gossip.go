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

package glutton

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	gluttonpb "github.com/agent-substrate/substrate/internal/proto/glutton"
)

type peerGossip struct {
	host    string
	delayMs int32
	cancel  context.CancelFunc
	done    chan struct{}
}

// Sends network traffic to a peer glutton. Messages will be sent
// on regular intervals separated by delay_ms.
func (s *Service) Gossip(_ context.Context, req *gluttonpb.GossipRequest) (*gluttonpb.GossipResponse, error) {
	want := make(map[string]*gluttonpb.Peer, len(req.GetPeers()))
	for _, p := range req.GetPeers() {
		if p.GetHost() == "" {
			return nil, status.Error(codes.InvalidArgument, "peer host is required")
		}
		if p.GetDelayMs() <= 0 {
			return nil, status.Errorf(codes.InvalidArgument, "peer %q delay_ms must be positive", p.GetHost())
		}
		want[p.GetHost()] = p
	}

	s.mu.Lock()
	var toStop []*peerGossip
	for host, existing := range s.peers {
		w, ok := want[host]
		if !ok || w.GetDelayMs() != existing.delayMs {
			toStop = append(toStop, existing)
			delete(s.peers, host)
		}
	}
	var toStart []*gluttonpb.Peer
	for host, w := range want {
		if _, ok := s.peers[host]; !ok {
			toStart = append(toStart, w)
		}
	}
	s.mu.Unlock()

	for _, p := range toStop {
		p.cancel()
		<-p.done
	}

	for _, w := range toStart {
		gctx, cancel := context.WithCancel(context.Background())
		pg := &peerGossip{
			host:    w.GetHost(),
			delayMs: w.GetDelayMs(),
			cancel:  cancel,
			done:    make(chan struct{}),
		}
		s.mu.Lock()
		s.peers[w.GetHost()] = pg
		s.mu.Unlock()
		go s.runGossip(gctx, pg)
	}

	return &gluttonpb.GossipResponse{}, nil
}

func (s *Service) runGossip(ctx context.Context, pg *peerGossip) {
	defer close(pg.done)

	// grpc.NewClient resolves and connects lazily; the first RPC surfaces
	// any failure, so the peer doesn't have to be reachable at start time.
	conn, err := grpc.NewClient(pg.host,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
	)
	if err != nil {
		slog.ErrorContext(ctx, "Failed to dial gossip peer", slog.String("host", pg.host), slog.Any("err", err))
		return
	}
	defer conn.Close()
	client := gluttonpb.NewGluttonClient(conn)

	hostAttr := attribute.String("host", pg.host)
	ticker := time.NewTicker(time.Duration(pg.delayMs) * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		msg := uuid.NewString()
		start := time.Now()
		resp, err := client.Ping(ctx, &gluttonpb.PingRequest{Message: msg})
		latency := time.Since(start).Seconds()
		outcome := "ok"
		cancelled := err != nil && errors.Is(ctx.Err(), context.Canceled)
		switch {
		case cancelled:
			outcome = "cancelled"
		case err != nil:
			outcome = "error"
		}
		attrs := metric.WithAttributes(hostAttr, attribute.String("outcome", outcome))
		s.gossipSent.Add(ctx, 1, attrs)
		s.gossipLatency.Record(ctx, latency, attrs)
		if cancelled {
			return
		}
		if err != nil {
			slog.WarnContext(ctx, "Gossip ping failed", slog.String("host", pg.host), slog.Any("err", err))
			continue
		}
		if resp.GetMessage() != msg {
			slog.WarnContext(ctx, "Gossip ping returned unexpected message",
				slog.String("host", pg.host),
				slog.String("sent", msg),
				slog.String("received", resp.GetMessage()),
			)
		}
	}
}
