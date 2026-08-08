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

// Tests for the actor pool, driven against a fake ate-api-server control plane.

package routercap

import (
	"context"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// fakeControl implements only the lifecycle calls the pool makes; the embedded
// interface satisfies the rest and panics if anything unexpected is called.
type fakeControl struct {
	ateapipb.ControlClient

	mu         sync.Mutex
	atespaces  []string
	created    []string
	resumed    []string
	bootFlags  []bool
	suspended  []string
	deleted    []string
	createFail map[string]error
	resumeFail map[string]error
	// existing is what ListActors reports, i.e. what an earlier arm left behind.
	existing []string
	// refuseFor makes the first n suspend/delete calls for an actor answer
	// FailedPrecondition, standing in for an actor still mid-transition.
	refuseFor map[string]int
	// resumeDelay holds each ResumeActor until released, so a test can observe
	// what happens to calls that are still in flight when a sibling fails.
	resumeDelay chan struct{}
}

func newFakeControl() *fakeControl {
	return &fakeControl{
		createFail: map[string]error{},
		resumeFail: map[string]error{},
		refuseFor:  map[string]int{},
	}
}

func (f *fakeControl) ListActors(_ context.Context, in *ateapipb.ListActorsRequest, _ ...grpc.CallOption) (*ateapipb.ListActorsResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []*ateapipb.Actor
	for _, n := range f.existing {
		out = append(out, &ateapipb.Actor{
			Metadata: &ateapipb.ResourceMetadata{Atespace: in.GetAtespace(), Name: n},
		})
	}
	return &ateapipb.ListActorsResponse{Actors: out}, nil
}

// refuse reports whether this call should answer FailedPrecondition, consuming
// one of the actor's remaining refusals. Caller holds f.mu.
func (f *fakeControl) refuse(name string) bool {
	if f.refuseFor[name] <= 0 {
		return false
	}
	f.refuseFor[name]--
	return true
}

func (f *fakeControl) CreateAtespace(_ context.Context, in *ateapipb.CreateAtespaceRequest, _ ...grpc.CallOption) (*ateapipb.Atespace, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	name := in.GetAtespace().GetMetadata().GetName()
	for _, a := range f.atespaces {
		if a == name {
			return nil, status.Error(codes.AlreadyExists, "atespace exists")
		}
	}
	f.atespaces = append(f.atespaces, name)
	return &ateapipb.Atespace{}, nil
}

func (f *fakeControl) CreateActor(_ context.Context, in *ateapipb.CreateActorRequest, _ ...grpc.CallOption) (*ateapipb.Actor, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	name := in.GetActor().GetMetadata().GetName()
	if err, ok := f.createFail[name]; ok {
		return nil, err
	}
	f.created = append(f.created, name)
	return &ateapipb.Actor{}, nil
}

func (f *fakeControl) ResumeActor(ctx context.Context, in *ateapipb.ResumeActorRequest, _ ...grpc.CallOption) (*ateapipb.ResumeActorResponse, error) {
	f.mu.Lock()
	gate := f.resumeDelay
	f.mu.Unlock()
	if gate != nil {
		// Mirrors the real hazard: the RPC is what holds the resume open, so a
		// cancelled context here is a resume abandoned server-side.
		select {
		case <-gate:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	name := in.GetActor().GetName()
	if err, ok := f.resumeFail[name]; ok {
		return nil, err
	}
	f.resumed = append(f.resumed, name)
	f.bootFlags = append(f.bootFlags, in.GetBoot())
	return &ateapipb.ResumeActorResponse{}, nil
}

func (f *fakeControl) SuspendActor(_ context.Context, in *ateapipb.SuspendActorRequest, _ ...grpc.CallOption) (*ateapipb.SuspendActorResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	name := in.GetActor().GetName()
	if f.refuse(name) {
		return nil, status.Error(codes.FailedPrecondition, "got: STATUS_RESUMING, want STATUS_RUNNING")
	}
	f.suspended = append(f.suspended, name)
	return &ateapipb.SuspendActorResponse{}, nil
}

func (f *fakeControl) DeleteActor(_ context.Context, in *ateapipb.DeleteActorRequest, _ ...grpc.CallOption) (*ateapipb.Actor, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	name := in.GetActor().GetName()
	if f.refuse(name) {
		return nil, status.Error(codes.FailedPrecondition, "not in a deletable status")
	}
	f.deleted = append(f.deleted, name)
	return &ateapipb.Actor{}, nil
}

func (f *fakeControl) counts() (created, resumed, suspended, deleted int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.created), len(f.resumed), len(f.suspended), len(f.deleted)
}

func newTestPool(api ateapipb.ControlClient) *ActorPool {
	return &ActorPool{API: api, Atespace: "routercap", Concurrency: 4}
}

func TestActorPoolWarmsEveryActorWithBoot(t *testing.T) {
	f := newFakeControl()
	p := newTestPool(f)

	if err := p.Warm(context.Background(), 8); err != nil {
		t.Fatalf("Warm: %v", err)
	}

	created, resumed, _, _ := f.counts()
	if created != 8 || resumed != 8 {
		t.Fatalf("created=%d resumed=%d, want 8 and 8", created, resumed)
	}
	// Boot must be set on the first resume, or the first ping pays the
	// cold-start cost the pre-warm exists to remove.
	for i, b := range f.bootFlags {
		if !b {
			t.Fatalf("resume %d had Boot=false", i)
		}
	}

	actors := p.Actors()
	if len(actors) != 8 {
		t.Fatalf("pool has %d actors, want 8", len(actors))
	}
	seen := map[string]bool{}
	for _, a := range actors {
		if seen[a.Name] {
			t.Fatalf("duplicate actor name %q: two actors would share one worker pod", a.Name)
		}
		seen[a.Name] = true
		want := a.Name + ".routercap." + actorDomain
		if a.Host != want {
			t.Errorf("Host = %q, want %q", a.Host, want)
		}
	}
}

func TestActorPoolTolerantOfAnExistingAtespace(t *testing.T) {
	// A re-run against a half-cleaned cluster must repair, not fail.
	f := newFakeControl()
	f.atespaces = []string{"routercap"}
	p := newTestPool(f)

	if err := p.Warm(context.Background(), 2); err != nil {
		t.Fatalf("Warm with an existing atespace: %v", err)
	}
}

func TestActorPoolWarmIsAllOrNothing(t *testing.T) {
	// A partial pool concentrates the ladder onto fewer worker pods than the
	// per-worker connection guard was sized for. That is a different
	// experiment, so it has to fail rather than shrink.
	f := newFakeControl()
	p := newTestPool(f)
	// Actor names are random, so fail the first one to arrive rather than a
	// named one.
	var once sync.Once
	p.API = &failingControl{fakeControl: f, failOn: func(string) error {
		var err error
		once.Do(func() { err = status.Error(codes.ResourceExhausted, "no worker capacity") })
		return err
	}}

	err := p.Warm(context.Background(), 6)
	if err == nil {
		t.Fatal("Warm succeeded despite an actor failing to come up")
	}
	if !strings.Contains(err.Error(), "no worker capacity") {
		t.Errorf("error = %q, want the underlying cause", err)
	}
	if got := p.Actors(); len(got) != 0 {
		t.Errorf("pool retained %d actors after a failed warm", len(got))
	}
	// Whatever did get created must be cleaned up, or the next attempt runs
	// against a cluster with orphaned actors holding worker pods.
	created, _, _, deleted := f.counts()
	if deleted < created {
		t.Errorf("created %d actors but deleted only %d after the failure", created, deleted)
	}
}

// failingControl injects a create failure for one actor.
type failingControl struct {
	*fakeControl
	failOn func(name string) error
}

func (f *failingControl) CreateActor(ctx context.Context, in *ateapipb.CreateActorRequest, opts ...grpc.CallOption) (*ateapipb.Actor, error) {
	if err := f.failOn(in.GetActor().GetMetadata().GetName()); err != nil {
		return nil, err
	}
	return f.fakeControl.CreateActor(ctx, in, opts...)
}

func TestActorPoolTeardownSuspendsThenDeletes(t *testing.T) {
	f := newFakeControl()
	p := newTestPool(f)
	if err := p.Warm(context.Background(), 5); err != nil {
		t.Fatalf("Warm: %v", err)
	}

	p.Teardown(context.Background())

	_, _, suspended, deleted := f.counts()
	if suspended != 5 || deleted != 5 {
		t.Fatalf("suspended=%d deleted=%d, want 5 and 5", suspended, deleted)
	}
	if got := p.Actors(); len(got) != 0 {
		t.Errorf("pool still holds %d actors after teardown", len(got))
	}
	// Idempotent: the run tears down on both the clean path and the signal
	// path, and those can overlap.
	p.Teardown(context.Background())
	_, _, suspended, deleted = f.counts()
	if suspended != 5 || deleted != 5 {
		t.Errorf("second teardown issued more calls: suspended=%d deleted=%d", suspended, deleted)
	}
}

func TestActorPoolTeardownContinuesPastFailures(t *testing.T) {
	// Best effort: one actor that refuses to suspend must not strand the other
	// ninety-nine, which the next arm needs released.
	f := newFakeControl()
	p := newTestPool(&stubbornControl{fakeControl: f})
	if err := p.Warm(context.Background(), 4); err != nil {
		t.Fatalf("Warm: %v", err)
	}

	p.Teardown(context.Background())

	if _, _, _, deleted := f.counts(); deleted != 4 {
		t.Errorf("deleted %d actors, want all 4 attempted despite the suspend failures", deleted)
	}
}

type stubbornControl struct{ *fakeControl }

func (s *stubbornControl) SuspendActor(context.Context, *ateapipb.SuspendActorRequest, ...grpc.CallOption) (*ateapipb.SuspendActorResponse, error) {
	return nil, status.Error(codes.Internal, "suspend wedged")
}

func TestActorPoolRejectsAnEmptyPool(t *testing.T) {
	if err := newTestPool(newFakeControl()).Warm(context.Background(), 0); err == nil {
		t.Fatal("Warm accepted zero actors")
	}
}

func TestActorPoolWarmLetsInFlightResumesFinishWhenAPeerFails(t *testing.T) {
	// An abandoned resume wedges the actor in STATUS_RESUMING and holds its
	// worker pod, so canceling a failed resume's siblings converts one lost
	// actor into n-1 lost worker slots for the next arm.
	f := newFakeControl()
	gate := make(chan struct{})
	f.resumeDelay = gate

	p := newTestPool(f)
	p.Concurrency = 8
	var once sync.Once
	p.API = &failingControl{fakeControl: f, failOn: func(string) error {
		var err error
		once.Do(func() { err = status.Error(codes.FailedPrecondition, "no free workers available") })
		return err
	}}

	done := make(chan error, 1)
	go func() { done <- p.Warm(context.Background(), 8) }()

	// Let the one failure land and propagate before releasing the rest, so the
	// siblings are unambiguously still in flight when Warm learns it failed.
	time.Sleep(50 * time.Millisecond)
	close(gate)

	err := <-done
	if err == nil {
		t.Fatal("Warm succeeded despite an actor failing to come up")
	}
	if !strings.Contains(err.Error(), "no free workers available") {
		t.Errorf("error = %q, want the underlying cause", err)
	}
	// One create failed; every other actor must have completed its resume
	// rather than been abandoned mid-flight.
	created, resumed, _, _ := f.counts()
	if created != 7 || resumed != 7 {
		t.Errorf("created %d and resumed %d, want 7 and 7: a resume was abandoned instead of finished", created, resumed)
	}
}

func TestActorPoolTeardownRetriesThroughATransitionalStatus(t *testing.T) {
	// Teardown routinely races the router's in-flight ResumeActor backlog, so
	// FailedPrecondition means "not yet". Treating it as final strands the
	// actor on its worker pod for the rest of the run.
	f := newFakeControl()
	p := newTestPool(f)
	if err := p.Warm(context.Background(), 3); err != nil {
		t.Fatalf("Warm: %v", err)
	}
	// Every actor refuses its first suspend and its first delete.
	f.mu.Lock()
	for _, a := range p.Actors() {
		f.refuseFor[a.Name] = 2
	}
	f.mu.Unlock()

	p.Teardown(context.Background())

	_, _, suspended, deleted := f.counts()
	if suspended != 3 || deleted != 3 {
		t.Errorf("suspended %d and deleted %d, want 3 and 3: a refusal was treated as final", suspended, deleted)
	}
}

func TestActorPoolWarmPurgesActorsLeftByAnEarlierArm(t *testing.T) {
	// The pool is one actor per worker pod, so n leftovers make the next Warm
	// fail with "no free workers available"; the purge must remove them.
	f := newFakeControl()
	f.existing = []string{"rc-stale-1", "rc-stale-2"}
	p := newTestPool(f)

	if err := p.Warm(context.Background(), 3); err != nil {
		t.Fatalf("Warm: %v", err)
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	for _, want := range f.existing {
		if !slices.Contains(f.deleted, want) {
			t.Errorf("stale actor %q survived the purge (deleted: %v)", want, f.deleted)
		}
	}
}

func TestActorPoolPurgeIsQuietWhenTheAtespaceIsClean(t *testing.T) {
	// The common case, and it must cost nothing: no list of actors to delete
	// means no suspend and no delete calls at all.
	f := newFakeControl()
	p := newTestPool(f)
	if err := p.Purge(context.Background()); err != nil {
		t.Fatalf("Purge: %v", err)
	}
	if _, _, suspended, deleted := f.counts(); suspended != 0 || deleted != 0 {
		t.Errorf("suspended %d and deleted %d against an empty atespace, want 0 and 0", suspended, deleted)
	}
}
