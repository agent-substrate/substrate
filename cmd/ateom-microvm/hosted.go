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
	"time"

	"github.com/vishvananda/netns"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/agent-substrate/substrate/cmd/ateom-microvm/internal/kata"
	"github.com/agent-substrate/substrate/internal/ateomcgroup"
	"github.com/agent-substrate/substrate/internal/ateomnet"
	"github.com/agent-substrate/substrate/internal/atunnel"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/internal/sizing"
)

// hostedActor holds one actor's attribution, network, and runtime state.
type hostedActor struct {
	// Immutable after admission.
	attribution resources.ActorAttribution

	// Mutable fields are guarded by actorsMu.
	network *ateomnet.SandboxSession
	// Nil until boot publishes the VM.
	vm *runningActor
	// Nil when no guest stats connection is available.
	guest *guestStatsTarget
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

	// The tap and atunnel share a namespace; the guest owns the other end.
	session, err := ateomnet.ServeSandbox(ctx, ateomnet.SandboxNetworkConfig{
		ActorUID:   uid,
		EgressPort: s.atunnelEgressPort,
		DNSPort:    atunnel.DNSPort,
	}, s.atunnelEgress, s.dnsRelay)
	if err != nil {
		s.actorsMu.Lock()
		delete(s.actors, uid)
		s.actorsMu.Unlock()
		return nil, fmt.Errorf("while setting up the sandbox network: %w", err)
	}

	s.actorsMu.Lock()
	hosted.network = session
	s.actorsMu.Unlock()
	return hosted, nil
}

// actorLeaf opens the actor's cgroup, or returns nil when the worker has none.
func (s *AteomService) actorLeaf(actorUID string, size sizing.SandboxSize) (*ateomcgroup.ActorLeaf, error) {
	if !s.actorCgroups {
		return nil, nil
	}
	return ateomcgroup.OpenActorLeaf(actorUID, size.MilliCPU)
}

// cleanupSandboxState kills what is left of the actor's host processes and
// clears its sandbox directories.
func (s *AteomService) cleanupSandboxState(ctx context.Context, actorUID string) {
	if s.actorCgroups {
		killCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		if err := ateomcgroup.KillActorLeaf(killCtx, actorUID); err != nil {
			slog.WarnContext(ctx, "Failed to kill the actor's leftover processes", slog.Any("err", err))
		}
		cancel()
	}
	kata.CleanupSandboxState(ctx, actorUID)
}

// unhostActor removes an actor, its network, and its cgroup. Repeated calls are
// safe. The caller must stop the VM first.
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

	if s.actorCgroups {
		if err := ateomcgroup.RemoveActorLeaf(actorUID); err != nil {
			slog.WarnContext(ctx, "Failed to remove the actor's cgroup", slog.Any("err", err))
		}
	}
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

// setRunningVM publishes the live micro-VM for an actor.
func (s *AteomService) setRunningVM(actorUID string, vm *runningActor) {
	s.actorsMu.Lock()
	defer s.actorsMu.Unlock()
	if hosted, ok := s.actors[actorUID]; ok {
		hosted.vm = vm
	}
}

// runningVM is the live micro-VM for an actor, or nil.
func (s *AteomService) runningVM(actorUID string) *runningActor {
	hosted := s.lookupActor(actorUID)
	if hosted == nil {
		return nil
	}
	s.actorsMu.RLock()
	defer s.actorsMu.RUnlock()
	return hosted.vm
}

// setGuestStats sets or clears the actor's stats target.
func (s *AteomService) setGuestStats(actorUID string, guest *guestStatsTarget) {
	s.actorsMu.Lock()
	defer s.actorsMu.Unlock()
	if hosted, ok := s.actors[actorUID]; ok {
		hosted.guest = guest
	}
}

// guestStatsFor is the stats target for an actor, or nil.
func (s *AteomService) guestStatsFor(actorUID string) *guestStatsTarget {
	hosted := s.lookupActor(actorUID)
	if hosted == nil {
		return nil
	}
	s.actorsMu.RLock()
	defer s.actorsMu.RUnlock()
	return hosted.guest
}

// sandboxNetNS is where an actor's tap and atunnel's sockets live, or -1.
func (s *AteomService) sandboxNetNS(actorUID string) netns.NsHandle {
	hosted := s.lookupActor(actorUID)
	if hosted == nil {
		return -1
	}
	s.actorsMu.RLock()
	defer s.actorsMu.RUnlock()
	if hosted.network == nil {
		return -1
	}
	return hosted.network.Network.GatewayNetNS
}

// sandboxDialer connects through the actor's gateway namespace.
func (s *AteomService) sandboxDialer(actorUID string) atunnel.DialFunc {
	return func(ctx context.Context, network, address string) (net.Conn, error) {
		s.actorsMu.RLock()
		hosted := s.actors[actorUID]
		var session *ateomnet.SandboxSession
		if hosted != nil {
			session = hosted.network
		}
		s.actorsMu.RUnlock()
		if session == nil {
			return nil, fmt.Errorf("actor %s is not hosted here", actorUID)
		}
		return session.Dialer()(ctx, network, address)
	}
}
