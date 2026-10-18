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
	"sync/atomic"
	"time"

	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/ateworker/actornet"
	"go.ate.dev/substrate/internal/atunnel/ingress"
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/internal/workerstats"
)

// hostedActor holds one actor's attribution, network, and runtime state.
type hostedActor struct {
	// Attribution is available during boot.
	attribution resources.ActorAttribution
	network     *actornet.SandboxSession
	// Set once the containers exist.
	session *workloadSession
	// usage is this activation's epoch, CPU baseline, and latest sample.
	usage *workerstats.Activation
	// deadReported keeps the usage sweep to one warning per activation.
	deadReported atomic.Bool
	// ctx is canceled when the actor is replaced or unhosted.
	ctx    context.Context
	cancel context.CancelFunc
}

// admitActor reserves capacity before network setup. An actor that is already
// hosted keeps its slot; its old network is returned for the caller to close.
func (s *AteWorkerService) admitActor(attribution resources.ActorAttribution) (*hostedActor, *actornet.SandboxSession, error) {
	s.actorsMu.Lock()
	defer s.actorsMu.Unlock()
	var stale *actornet.SandboxSession
	if old, ok := s.actors[attribution.UID]; ok {
		stale = old.network
		old.cancel()
	} else if len(s.actors)+s.draining >= s.maxActors {
		return nil, nil, apierror.ResourceExhausted("worker is full: %d actors", s.maxActors)
	}
	// The sandbox cgroup is new with every activation, so its counters start
	// at zero.
	ctx, cancel := context.WithCancel(context.Background())
	hosted := &hostedActor{attribution: attribution, usage: workerstats.NewActivation(time.Now(), false), ctx: ctx, cancel: cancel}
	s.actors[attribution.UID] = hosted
	return hosted, stale, nil
}

// hostActor sets up the actor's network, replacing any stale one.
func (s *AteWorkerService) hostActor(ctx context.Context, attribution resources.ActorAttribution, actorDirs *ateworkerpb.ActorDirs) (*hostedActor, error) {
	uid := attribution.UID

	hosted, stale, err := s.admitActor(attribution)
	if err != nil {
		return nil, err
	}
	// evict drops the reservation when network setup fails.
	evict := func() {
		hosted.cancel()
		s.actorsMu.Lock()
		delete(s.actors, uid)
		s.actorsMu.Unlock()
	}
	if stale != nil {
		if err := stale.Close(ctx); err != nil {
			evict()
			return nil, fmt.Errorf("while clearing stale state for actor %s: %w", uid, err)
		}
	}

	session, err := actornet.ServeSandbox(ctx, actornet.SandboxNetworkConfig{
		ActorUID:   uid,
		Veth:       true,
		EgressPort: s.tunnel.EgressPort,
		DNSPolicy:  s.tunnel.ActorEgressPolicy(attribution),
	}, s.tunnel.Egress, s.tunnel.DNSRelay)
	if err != nil {
		evict()
		return nil, fmt.Errorf("while setting up the sandbox network: %w", err)
	}

	s.actorsMu.Lock()
	hosted.network = session
	s.actorsMu.Unlock()
	return hosted, nil
}

// unhostActor removes an actor and its network. Repeated calls are safe.
func (s *AteWorkerService) unhostActor(ctx context.Context, actorUID string) error {
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
	hosted.cancel()
	defer func() {
		s.actorsMu.Lock()
		s.draining--
		s.actorsMu.Unlock()
	}()

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
func (s *AteWorkerService) lookupActor(actorUID string) *hostedActor {
	s.actorsMu.RLock()
	defer s.actorsMu.RUnlock()
	return s.actors[actorUID]
}

// hostedActors returns a snapshot of the actor map.
func (s *AteWorkerService) hostedActors() []*hostedActor {
	s.actorsMu.RLock()
	defer s.actorsMu.RUnlock()
	out := make([]*hostedActor, 0, len(s.actors))
	for _, hosted := range s.actors {
		out = append(out, hosted)
	}
	return out
}

// hostedSessions returns the runsc sessions for shutdown.
func (s *AteWorkerService) hostedSessions() []*workloadSession {
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
func (s *AteWorkerService) setSession(actorUID string, session *workloadSession) {
	s.actorsMu.Lock()
	defer s.actorsMu.Unlock()
	if hosted, ok := s.actors[actorUID]; ok {
		hosted.session = session
	}
}

// sandboxDialer looks up the actor on each dial.
func (s *AteWorkerService) sandboxDialer(actorUID string) ingress.DialFunc {
	return func(ctx context.Context, network, address string) (net.Conn, error) {
		s.actorsMu.RLock()
		var session *actornet.SandboxSession
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
