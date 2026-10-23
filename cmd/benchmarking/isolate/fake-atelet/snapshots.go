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
	"sync"
	"time"

	"go.ate.dev/substrate/internal/proto/ateletpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// actorSnapshots records each actor's latest local and durable snapshot, as
// atelet's snapshot tracker does, so Checkpoint and DetachActor report the
// state atelet would. ate-api-server keeps an actor on its node while this
// state names a local copy.
//
// It is kept in memory only. After a restart, DetachActor finds no local
// snapshot and returns an empty state, and ate-api-server keeps the actor
// pinned to its node until it detaches it.
//
// The zero value is ready to use.
type actorSnapshots struct {
	mu     sync.Mutex
	actors map[string]*ateletpb.ActorSnapshotState // by actor UID
}

// commitLocal records snap as the actor's latest local snapshot and returns
// the actor's state.
func (s *actorSnapshots) commitLocal(actorUID string, snap *ateletpb.Snapshot, now time.Time) *ateletpb.ActorSnapshotState {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.findOrAddLocked(actorUID)
	st.LatestLocal = &ateletpb.SnapshotReport{
		Snapshot:        proto.CloneOf(snap),
		LocalCommitTime: timestamppb.New(now),
	}
	return proto.CloneOf(st)
}

// commitDurable records snap as the actor's latest durable snapshot and
// returns the actor's state. It drops an older local snapshot, which snap
// supersedes.
func (s *actorSnapshots) commitDurable(actorUID string, snap *ateletpb.Snapshot, now time.Time) *ateletpb.ActorSnapshotState {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.findOrAddLocked(actorUID)
	st.LatestDurable = &ateletpb.SnapshotReport{
		Snapshot:            proto.CloneOf(snap),
		DurableCompleteTime: timestamppb.New(now),
	}
	if st.GetLatestLocal().GetSnapshot().GetGeneration() < snap.GetGeneration() {
		st.LatestLocal = nil
	}
	return proto.CloneOf(st)
}

// get returns the actor's state, empty when nothing is recorded for it.
func (s *actorSnapshots) get(actorUID string) *ateletpb.ActorSnapshotState {
	s.mu.Lock()
	defer s.mu.Unlock()
	if st, ok := s.actors[actorUID]; ok {
		return proto.CloneOf(st)
	}
	return &ateletpb.ActorSnapshotState{}
}

// forget drops what is recorded for the actor.
func (s *actorSnapshots) forget(actorUID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.actors, actorUID)
}

func (s *actorSnapshots) findOrAddLocked(actorUID string) *ateletpb.ActorSnapshotState {
	if s.actors == nil {
		s.actors = map[string]*ateletpb.ActorSnapshotState{}
	}
	st, ok := s.actors[actorUID]
	if !ok {
		st = &ateletpb.ActorSnapshotState{}
		s.actors[actorUID] = st
	}
	return st
}
