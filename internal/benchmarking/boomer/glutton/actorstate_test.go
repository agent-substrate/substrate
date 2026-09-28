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
	"testing"

	"github.com/agent-substrate/substrate/internal/benchmarking/boomer/boomerutil"
	bmetrics "github.com/agent-substrate/substrate/internal/benchmarking/boomer/metrics"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// onlyActor returns the single actor of the VU iterate() started on this
// goroutine.
func onlyActor(t *testing.T, rt *taskRuntime) *gluttonActor {
	t.Helper()
	val, ok := rt.users.Load(boomerutil.GoroutineID())
	if !ok {
		t.Fatal("iterate() did not start a user on this goroutine")
	}
	u := val.(*gluttonUser)
	if len(u.actors) != 1 {
		t.Fatalf("user has %d actors, want 1", len(u.actors))
	}
	return u.actors[0]
}

func TestActorStateFollowsLifecycle(t *testing.T) {
	a, _ := newResumeTestActor(t)
	ctx := context.Background()

	if err := a.create(ctx); err != nil {
		t.Fatalf("create: %v", err)
	}
	if a.state != bmetrics.ActorStateHibernated {
		t.Errorf("after create: state = %q, want %q", a.state, bmetrics.ActorStateHibernated)
	}
	if err := a.resume(ctx); err != nil {
		t.Fatalf("resume: %v", err)
	}
	if a.state != bmetrics.ActorStateRunning {
		t.Errorf("after resume: state = %q, want %q", a.state, bmetrics.ActorStateRunning)
	}
	if err := a.hibernate(ctx); err != nil {
		t.Fatalf("hibernate: %v", err)
	}
	if a.state != bmetrics.ActorStateHibernated {
		t.Errorf("after hibernate: state = %q, want %q", a.state, bmetrics.ActorStateHibernated)
	}
	a.delete(ctx)
	if a.state != bmetrics.ActorStateNone {
		t.Errorf("after delete: state = %q, want untracked", a.state)
	}
}

// A failed resume leaves the actor where it was: the client saw no
// transition.
func TestActorStateUnchangedOnResumeFailure(t *testing.T) {
	a, _ := newResumeTestActor(t, status.Error(codes.ResourceExhausted, "no free workers available"))
	ctx := context.Background()
	if err := a.create(ctx); err != nil {
		t.Fatalf("create: %v", err)
	}

	if err := a.resume(ctx); err == nil {
		t.Fatal("resume succeeded, want ResourceExhausted")
	}
	if a.state != bmetrics.ActorStateHibernated {
		t.Errorf("state = %q, want %q", a.state, bmetrics.ActorStateHibernated)
	}
}

func TestActorStateCrashed(t *testing.T) {
	a, _ := newResumeTestActor(t, status.Error(codes.Aborted, "actor crashed"))
	ctx := context.Background()
	if err := a.create(ctx); err != nil {
		t.Fatalf("create: %v", err)
	}

	if err := a.resume(ctx); err == nil {
		t.Fatal("resume succeeded, want crashed")
	}
	if a.state != bmetrics.ActorStateCrashed {
		t.Errorf("state = %q, want %q", a.state, bmetrics.ActorStateCrashed)
	}
}

func TestActorStateHibernatePending(t *testing.T) {
	rt, fakeCtrl := newReplacementRuntime(t)
	fakeCtrl.suspendErrs = []error{status.Error(codes.Unavailable, "ate-api-server restarting")}

	rt.iterate() // resume, ping, suspend → suspend fails
	a := onlyActor(t, rt)
	if a.state != bmetrics.ActorStateHibernatePending {
		t.Errorf("after failed suspend: state = %q, want %q", a.state, bmetrics.ActorStateHibernatePending)
	}

	rt.iterate() // re-driven suspend succeeds
	if a.state != bmetrics.ActorStateHibernated {
		t.Errorf("after retried suspend: state = %q, want %q", a.state, bmetrics.ActorStateHibernated)
	}
}

// A replaced actor stops counting and its replacement starts, so the gauge
// never counts the dead actor alongside the new one.
func TestActorStateOnReplacement(t *testing.T) {
	rt, _ := newReplacementRuntime(t, status.Error(codes.FailedPrecondition, "AssignWorker prerequisite not met (got: ACTOR_STATE_SUSPENDING)"))
	u, err := rt.startUser(context.Background())
	if err != nil {
		t.Fatalf("startUser: %v", err)
	}
	broken := u.actors[0]
	rt.users.Store(boomerutil.GoroutineID(), u)

	rt.iterate() // resume fails terminally → replaced

	if broken.state != bmetrics.ActorStateNone {
		t.Errorf("replaced actor: state = %q, want untracked", broken.state)
	}
	replacement := onlyActor(t, rt)
	if replacement == broken {
		t.Fatal("actor was not replaced")
	}
	if replacement.state != bmetrics.ActorStateHibernated {
		t.Errorf("replacement: state = %q, want %q", replacement.state, bmetrics.ActorStateHibernated)
	}
}

func TestActorStateClearedOnShutdown(t *testing.T) {
	rt, _ := newReplacementRuntime(t)
	rt.iterate()
	a := onlyActor(t, rt)

	rt.shutdown(context.Background())

	if a.state != bmetrics.ActorStateNone {
		t.Errorf("after shutdown: state = %q, want untracked", a.state)
	}
}
