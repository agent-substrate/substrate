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
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/agent-substrate/substrate/internal/resources"
)

// Re-admitting an actor that is already hosted keeps its slot, so it succeeds
// on a full worker and a retry cannot lose its place to another actor.
func TestAdmitActorKeepsAHostedActorsSlot(t *testing.T) {
	s := &AteomService{actors: map[string]*hostedActor{}, maxActors: 1}
	first, _, err := s.admitActor(resources.ActorAttribution{UID: "actor-a"})
	if err != nil {
		t.Fatal(err)
	}
	again, _, err := s.admitActor(resources.ActorAttribution{UID: "actor-a"})
	if err != nil {
		t.Fatalf("re-admitting a hosted actor on a full worker: %v", err)
	}
	if again == first {
		t.Error("re-admission kept the old record; readers could not tell the incarnations apart")
	}
	// ResourceExhausted, so the control plane treats it as a capacity miss.
	if _, _, err := s.admitActor(resources.ActorAttribution{UID: "actor-b"}); status.Code(err) != codes.ResourceExhausted {
		t.Errorf("admitting past the ceiling: got %v, want ResourceExhausted", err)
	}
	if got := len(s.hostedActors()); got != 1 {
		t.Errorf("hosting %d actors, want 1", got)
	}
}
