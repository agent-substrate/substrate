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
	"sync"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"time"

	"github.com/agent-substrate/substrate/internal/actorlock"
	"github.com/agent-substrate/substrate/internal/resources"
)

func TestAdmitActorEnforcesTheCeilingConcurrently(t *testing.T) {
	const ceiling = 8
	s := &AteomService{
		locks:     actorlock.New(),
		actors:    map[string]*hostedActor{},
		maxActors: ceiling,
	}

	var wg sync.WaitGroup
	for i := range ceiling * 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, _ = s.admitActor(resources.ActorAttribution{UID: fmt.Sprintf("actor-%d", i)})
		}()
	}
	wg.Wait()

	if got := len(s.hostedActors()); got != ceiling {
		t.Errorf("admitted %d actors, want the ceiling of %d", got, ceiling)
	}
}

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

// TestDrainingActorsStillCountAgainstTheCeiling checks capacity during teardown.
func TestDrainingActorsStillCountAgainstTheCeiling(t *testing.T) {
	s := &AteomService{
		locks:     actorlock.New(),
		actors:    map[string]*hostedActor{},
		maxActors: 1,
		draining:  1,
	}
	if _, err := s.hostActor(context.Background(), resources.ActorAttribution{UID: "actor-a"}); err == nil {
		t.Error("admitted an actor while a draining one still held the only place")
	}
}

// TestGracefulShutdownWaitsForInFlightRPCs verifies checkpoints finish before shutdown.
func TestGracefulShutdownWaitsForInFlightRPCs(t *testing.T) {
	s := &AteomService{
		locks:    actorlock.New(),
		inFlight: actorlock.NewInFlight(),
		actors:   map[string]*hostedActor{},
	}
	release := s.inFlight.Add("actor-a", rpcCheckpointWorkload, nil)

	returned := make(chan struct{})
	go func() {
		defer close(returned)
		s.gracefulShutdown(context.Background())
	}()

	select {
	case <-returned:
		t.Fatal("gracefulShutdown returned while a checkpoint was still in flight")
	case <-time.After(250 * time.Millisecond):
	}

	release()
	select {
	case <-returned:
	case <-time.After(30 * time.Second):
		t.Error("gracefulShutdown did not return after the checkpoint finished")
	}
}

func TestGracefulShutdownCancelsEveryStartup(t *testing.T) {
	s := &AteomService{
		locks:    actorlock.New(),
		inFlight: actorlock.NewInFlight(),
		actors:   map[string]*hostedActor{},
	}
	canceled := make(chan string, 2)
	for _, uid := range []string{"actor-a", "actor-b"} {
		defer s.inFlight.Add(uid, rpcRunWorkload, func() { canceled <- uid })()
	}

	cancelStartups(context.Background(), s.inFlight)

	seen := map[string]bool{}
	for range 2 {
		select {
		case uid := <-canceled:
			seen[uid] = true
		case <-time.After(5 * time.Second):
			t.Fatalf("only %v were canceled", seen)
		}
	}
}

// TestBeginRPCRegistersBeforeCheckingForADrain checks registration and drain rejection.
func TestBeginRPCRegistersBeforeCheckingForADrain(t *testing.T) {
	s := &AteomService{locks: actorlock.New(), inFlight: actorlock.NewInFlight()}

	release, err := s.beginRPC("actor-a", rpcRunWorkload, func() {})
	if err != nil {
		t.Fatalf("beginRPC on a live ateom: %v", err)
	}
	if _, ok := s.inFlight.Names()["actor-a"]; !ok {
		t.Error("an admitted RPC is not visible to shutdown")
	}
	release()

	// Draining: refused, and nothing left behind for shutdown to wait on.
	s.shuttingDown.Store(true)
	if _, err := s.beginRPC("actor-b", rpcRunWorkload, func() {}); err == nil {
		t.Error("beginRPC admitted an RPC while draining")
	}
	if got := s.inFlight.Names(); len(got) != 0 {
		t.Errorf("a refused RPC stayed registered: %v", got)
	}
}
