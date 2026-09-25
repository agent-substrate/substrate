//go:build linux

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

package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/agent-substrate/substrate/internal/ateomnet"
	"github.com/agent-substrate/substrate/internal/atunnel"
	"github.com/agent-substrate/substrate/internal/resources"
)

// hostedActor holds one actor's attribution, network, and runtime state.
type hostedActor struct {
	// Attribution is available during boot.
	attribution resources.ActorAttribution
	network     *ateomnet.SandboxSession
	// Set once the containers exist.
	session *workloadSession
}

// admitActor reserves capacity before network setup. An actor that is already
// hosted keeps its slot; its old network is returned for the caller to close.
func (s *AteomService) admitActor(attribution resources.ActorAttribution) (*hostedActor, *ateomnet.SandboxSession, error) {
	s.actorsMu.Lock()
	defer s.actorsMu.Unlock()
	var stale *ateomnet.SandboxSession
	if old, ok := s.actors[attribution.UID]; ok {
		stale = old.network
	} else if len(s.actors)+s.draining >= s.maxActors {
		return nil, nil, status.Errorf(codes.ResourceExhausted, "worker is full: %d actors", s.maxActors)
	}
	hosted := &hostedActor{attribution: attribution}
	s.actors[attribution.UID] = hosted
	return hosted, stale, nil
}

// hostActor sets up the actor's network, replacing any stale one.
func (s *AteomService) hostActor(ctx context.Context, attribution resources.ActorAttribution) (*hostedActor, error) {
	uid := attribution.UID
	if uid == "" {
		return nil, fmt.Errorf("actor UID is required")
	}

	hosted, stale, err := s.admitActor(attribution)
	if err != nil {
		return nil, err
	}
	if stale != nil {
		if err := stale.Close(ctx); err != nil {
			s.actorsMu.Lock()
			delete(s.actors, uid)
			s.actorsMu.Unlock()
			return nil, fmt.Errorf("while clearing stale state for actor %s: %w", uid, err)
		}
	}

	session, err := ateomnet.ServeSandbox(ctx, ateomnet.SandboxNetworkConfig{
		ActorUID:   uid,
		Veth:       true,
		EgressPort: s.atunnelEgressPort,
		DNSPort:    atunnel.DNSPort,
	}, s.atunnelEgress, s.dnsRelay)
	if err != nil {
		s.actorsMu.Lock()
		delete(s.actors, uid)
		s.actorsMu.Unlock()
		return nil, fmt.Errorf("while setting up the sandbox network: %w", err)
	}

	// Use the actor's gateway as its DNS resolver.
	if _, err := actorResolvConf(uid); err != nil {
		_ = session.Close(ctx)
		s.actorsMu.Lock()
		delete(s.actors, uid)
		s.actorsMu.Unlock()
		return nil, err
	}

	s.actorsMu.Lock()
	hosted.network = session
	s.actorsMu.Unlock()
	return hosted, nil
}

// unhostActor removes an actor and its network. Repeated calls are safe.
func (s *AteomService) unhostActor(ctx context.Context, actorUID string) error {
	s.actorsMu.Lock()
	hosted, ok := s.actors[actorUID]
	delete(s.actors, actorUID)
	if ok {
		// Retain capacity until network cleanup completes.
		s.draining++
	}
	s.actorsMu.Unlock()
	if !ok {
		return nil
	}
	defer func() {
		s.actorsMu.Lock()
		s.draining--
		s.actorsMu.Unlock()
	}()

	removeActorResolvConf(ctx, actorUID)
	if hosted.network == nil {
		return nil
	}
	if err := hosted.network.Close(ctx); err != nil {
		slog.WarnContext(ctx, "Failed to remove the sandbox network", slog.Any("err", err))
		return err
	}
	return nil
}

// lookupActor returns the actor or nil without taking its lifecycle lock.
func (s *AteomService) lookupActor(actorUID string) *hostedActor {
	s.actorsMu.RLock()
	defer s.actorsMu.RUnlock()
	return s.actors[actorUID]
}

// hostedActors returns a snapshot of the actor map.
func (s *AteomService) hostedActors() []*hostedActor {
	s.actorsMu.RLock()
	defer s.actorsMu.RUnlock()
	out := make([]*hostedActor, 0, len(s.actors))
	for _, hosted := range s.actors {
		out = append(out, hosted)
	}
	return out
}

// hostedSessions returns the runsc sessions for shutdown.
func (s *AteomService) hostedSessions() []*workloadSession {
	s.actorsMu.RLock()
	defer s.actorsMu.RUnlock()
	var out []*workloadSession
	for _, hosted := range s.actors {
		if hosted.session != nil {
			out = append(out, hosted.session)
		}
	}
	return out
}

// setSession records the runsc state for an actor once its containers exist.
func (s *AteomService) setSession(actorUID string, session *workloadSession) {
	s.actorsMu.Lock()
	defer s.actorsMu.Unlock()
	if hosted, ok := s.actors[actorUID]; ok {
		hosted.session = session
	}
}

// sandboxDialer looks up the actor on each dial.
func (s *AteomService) sandboxDialer(actorUID string) atunnel.DialFunc {
	return func(ctx context.Context, network, address string) (net.Conn, error) {
		s.actorsMu.RLock()
		var session *ateomnet.SandboxSession
		if hosted := s.actors[actorUID]; hosted != nil {
			session = hosted.network
		}
		s.actorsMu.RUnlock()
		if session == nil {
			return nil, fmt.Errorf("actor %s is not hosted here", actorUID)
		}
		return session.Dialer()(ctx, network, address)
	}
}
