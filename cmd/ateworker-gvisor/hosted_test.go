//go:build linux

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

package main

import (
	"context"
	"fmt"
	"io/fs"
	"sync"
	"testing"
	"testing/synctest"

	"google.golang.org/grpc/codes"

	"go.ate.dev/substrate/cmd/ateworker-gvisor/internal/cgroupstats"
	"go.ate.dev/substrate/internal/actorlock"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/resources"
)

func TestAdmitActorEnforcesTheCeilingConcurrently(t *testing.T) {
	const ceiling = 8
	s := &AteWorkerService{
		locks:     actorlock.New(),
		actors:    map[string]*hostedActor{},
		maxActors: ceiling,
	}

	var wg sync.WaitGroup
	for i := range ceiling * 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, _ = s.admitActor(resources.ActorAttribution{UID: fmt.Sprintf("actor-%d", i)}, "", 0)
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
	const firstUID = "754f3dc4-2996-41c4-8e5a-a1b79e6678d5"
	const nextUID = "a6ba5a75-ad36-48fc-ad2e-e845527a25b1"
	s := &AteWorkerService{actors: map[string]*hostedActor{}, maxActors: 1}
	first, _, err := s.admitActor(resources.ActorAttribution{UID: "actor-a"}, firstUID, 0)
	if err != nil {
		t.Fatal(err)
	}
	again, _, err := s.admitActor(resources.ActorAttribution{UID: "actor-a"}, nextUID, 0)
	if err != nil {
		t.Fatalf("re-admitting a hosted actor on a full worker: %v", err)
	}
	if first.executionUID != firstUID || again.executionUID != nextUID {
		t.Errorf("execution UIDs = %q, %q, want %q, %q", first.executionUID, again.executionUID, firstUID, nextUID)
	}
	if again == first {
		t.Error("re-admission kept the old record; readers could not tell the incarnations apart")
	}
	// ResourceExhausted, so the control plane treats it as a capacity miss.
	if _, _, err := s.admitActor(resources.ActorAttribution{UID: "actor-b"}, "", 0); apierror.Code(err) != codes.ResourceExhausted {
		t.Errorf("admitting past the ceiling: got %v, want ResourceExhausted", err)
	}
	if got := len(s.hostedActors()); got != 1 {
		t.Errorf("hosting %d actors, want 1", got)
	}
}

// TestDrainingActorsStillCountAgainstTheCeiling checks capacity during teardown.
func TestDrainingActorsStillCountAgainstTheCeiling(t *testing.T) {
	s := &AteWorkerService{
		locks:     actorlock.New(),
		actors:    map[string]*hostedActor{},
		maxActors: 1,
		draining:  1,
	}
	if _, err := s.hostActor(context.Background(), resources.ActorAttribution{UID: "actor-a"}, "", nil, 0); err == nil {
		t.Error("admitted an actor while a draining one still held the only place")
	}
}

// TestGracefulShutdownStopsAnActorRestoredDuringTheWait checks that an actor
// whose restore finishes while shutdown waits still gets SIGTERM.
func TestGracefulShutdownStopsAnActorRestoredDuringTheWait(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := &AteWorkerService{
			locks:     actorlock.New(),
			inFlight:  actorlock.NewInFlight(),
			actors:    map[string]*hostedActor{},
			maxActors: 1,
			readSandboxCgroup: func(string) (cgroupstats.Sample, error) {
				return cgroupstats.Sample{}, fs.ErrNotExist
			},
		}
		release := s.inFlight.Add("actor-a", rpcRestoreWorkload)

		returned := make(chan struct{})
		go func() {
			defer close(returned)
			s.gracefulShutdown(context.Background())
		}()
		synctest.Wait()

		// The restore finishes.
		if _, _, err := s.admitActor(resources.ActorAttribution{UID: "actor-a"}, 0); err != nil {
			t.Fatal(err)
		}
		f := newFakeRunsc("app")
		s.setSession("actor-a", &workloadSession{rcmd: f, containers: []string{"app"}})
		release()
		<-returned

		assertCalls(t, f, "kill app SIGTERM", "wait app")
	})
}

// TestGracefulShutdownLetsASuspendFinish checks that shutdown never signals an
// actor whose checkpoint is in flight.
func TestGracefulShutdownLetsASuspendFinish(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := &AteWorkerService{
			locks:     actorlock.New(),
			inFlight:  actorlock.NewInFlight(),
			actors:    map[string]*hostedActor{},
			maxActors: 1,
			readSandboxCgroup: func(string) (cgroupstats.Sample, error) {
				return cgroupstats.Sample{}, fs.ErrNotExist
			},
		}
		if _, _, err := s.admitActor(resources.ActorAttribution{UID: "actor-a"}, 0); err != nil {
			t.Fatal(err)
		}
		f := newFakeRunsc("app")
		s.setSession("actor-a", &workloadSession{rcmd: f, containers: []string{"app"}})
		release := s.inFlight.Add("actor-a", rpcCheckpointWorkload)

		returned := make(chan struct{})
		go func() {
			defer close(returned)
			s.gracefulShutdown(context.Background())
		}()
		synctest.Wait()

		select {
		case <-returned:
			t.Fatal("gracefulShutdown returned while the checkpoint was in flight")
		default:
		}
		assertCalls(t, f)

		// The checkpoint finishes and unhosts the actor.
		if err := s.unhostActor(context.Background(), "actor-a"); err != nil {
			t.Fatal(err)
		}
		release()
		<-returned

		assertCalls(t, f)
	})
}

// TestBeginRPCRegistersBeforeCheckingForADrain checks registration and drain rejection.
func TestBeginRPCRegistersBeforeCheckingForADrain(t *testing.T) {
	s := &AteWorkerService{locks: actorlock.New(), inFlight: actorlock.NewInFlight()}

	release, err := s.beginRPC("actor-a", rpcRunWorkload)
	if err != nil {
		t.Fatalf("beginRPC on a live ateworker: %v", err)
	}
	if _, ok := s.inFlight.Names()["actor-a"]; !ok {
		t.Error("an admitted RPC is not visible to shutdown")
	}
	release()

	// Draining: refused, and nothing left behind for shutdown to wait on.
	s.shuttingDown.Store(true)
	if _, err := s.beginRPC("actor-b", rpcRunWorkload); err == nil {
		t.Error("beginRPC admitted an RPC while draining")
	}
	if got := s.inFlight.Names(); len(got) != 0 {
		t.Errorf("a refused RPC stayed registered: %v", got)
	}
}

func TestUnbookedMemory(t *testing.T) {
	const limit = 4 << 30
	s := &AteWorkerService{actors: map[string]*hostedActor{
		"a": {memoryBytes: 1 << 30},
		"b": {memoryBytes: 256 << 20},
	}}
	if got, want := s.unbookedMemory(limit), int64(limit-memoryReservedBytes-1<<30-256<<20); got != want {
		t.Errorf("unbookedMemory() = %d, want %d", got, want)
	}
}
