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

package ingress

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/internal/resources"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestActorResumer_InProgressBudgetExtendsPastParkBudget(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		parkCfg := ParkedRequestConfig{
			Budget:           5 * time.Second,
			InProgressBudget: 30 * time.Second,
			Max:              64,
			RetryInterval:    600 * time.Millisecond,
			RetryFactor:      1.0,
			RetryJitter:      0,
		}

		// 1. Aborted (actor lease held during overlapping suspend),
		// FailedPrecondition (ACTOR_STATE_SUSPENDING), and a brief post-suspend
		// ResourceExhausted blip at t=12s all ride out cleanly beyond the 5s
		// capacity park budget because capacityDeadline arms at the first
		// non-in-progress error rather than flight start.
		start := time.Now()
		mock := &resumerMockClient{
			resumeFn: func(ctx context.Context, in *ateapi.ResumeActorRequest, opts ...grpc.CallOption) (*ateapi.ResumeActorResponse, error) {
				elapsed := time.Since(start)
				if elapsed < 6*time.Second {
					return nil, status.Error(codes.Aborted, "another operation is in progress for actor")
				}
				if elapsed < 12*time.Second {
					return nil, status.Error(codes.FailedPrecondition, "AssignWorker prerequisite not met for Actor: ate-env/a (got: ACTOR_STATE_SUSPENDING, want ACTOR_STATE_SUSPENDED)")
				}
				if elapsed < 13*time.Second {
					return nil, status.Error(codes.ResourceExhausted, "no worker has room for the actor")
				}
				return &ateapi.ResumeActorResponse{
					Actor: &ateapi.Actor{
						Status: &ateapi.ActorStatus{
							State: ateapi.ActorState_ACTOR_STATE_RUNNING,
							WorkerAssignment: &ateapi.WorkerAssignment{
								WorkerPodIps:   []string{"10.0.1.9"},
								WorkerSpiffeId: "spiffe://cluster.local/ns/ate-env/sa/worker",
							},
						},
					},
					Resumed: true,
				}, nil
			},
		}

		resumer := NewActorResumer(mock, withParking(parkCfg))
		actor, outcome, err := resumer.ResumeActor(t.Context(), resources.ActorRef{Atespace: "ate-env", Name: "rapid-step-actor"})
		if err != nil {
			t.Fatalf("ResumeActor failed during in-progress transition: %v", err)
		}
		if outcome != ResumeOutcomeTriggered {
			t.Fatalf("outcome = %q, want %q", outcome, ResumeOutcomeTriggered)
		}
		if got := actor.GetStatus().GetWorkerAssignment().GetWorkerPodIps()[0]; got != "10.0.1.9" {
			t.Fatalf("worker IP = %q, want 10.0.1.9", got)
		}
		if elapsed := time.Since(start); elapsed < 13*time.Second {
			t.Fatalf("elapsed = %v, want >= 13s", elapsed)
		}

		// 2. Non-suspending FailedPrecondition (such as ACTOR_STATE_CRASHED)
		// still fails immediately on the first call without retrying.
		var crashedCalls atomic.Int32
		mock.resumeFn = func(ctx context.Context, in *ateapi.ResumeActorRequest, opts ...grpc.CallOption) (*ateapi.ResumeActorResponse, error) {
			crashedCalls.Add(1)
			return nil, status.Error(codes.FailedPrecondition, "AssignWorker prerequisite not met for Actor: ate-env/crashed (got: ACTOR_STATE_CRASHED, want ACTOR_STATE_SUSPENDED)")
		}
		_, _, err = resumer.ResumeActor(t.Context(), resources.ActorRef{Atespace: "ate-env", Name: "crashed-actor"})
		if status.Code(err) != codes.FailedPrecondition || crashedCalls.Load() != 1 {
			t.Fatalf("crashed actor: err=%v calls=%d, want FailedPrecondition after 1 call", err, crashedCalls.Load())
		}

		// 3. ResourceExhausted (pool capacity full) remains bounded by the 5s
		// Budget even when InProgressBudget is 30s, including when RetryInterval
		// (600ms) does not divide 5s evenly and exercises rem < delay clamping.
		capStart := time.Now()
		mock.resumeFn = func(ctx context.Context, in *ateapi.ResumeActorRequest, opts ...grpc.CallOption) (*ateapi.ResumeActorResponse, error) {
			return nil, status.Error(codes.ResourceExhausted, "no worker has room for the actor")
		}
		_, _, err = resumer.ResumeActor(t.Context(), resources.ActorRef{Atespace: "ate-env", Name: "saturated-actor"})
		if status.Code(err) != codes.ResourceExhausted {
			t.Fatalf("saturated actor err = %v, want ResourceExhausted", err)
		}
		var budgetErr *budgetExhaustedError
		if !errors.As(err, &budgetErr) {
			t.Fatalf("expected budgetExhaustedError wrapper, got %T (%v)", err, err)
		}
		if elapsed := time.Since(capStart); elapsed != 5*time.Second {
			t.Fatalf("saturated actor elapsed = %v, want exact 5s Budget", elapsed)
		}
	})
}

func TestParkedRequestConfig_ExtProcHold(t *testing.T) {
	t.Parallel()
	if got := (ParkedRequestConfig{Max: 0, Budget: 5 * time.Second}).ExtProcHold(); got != 0 {
		t.Fatalf("disabled parking without in-progress budget: got %v, want 0", got)
	}
	if got := (ParkedRequestConfig{Max: 0, Budget: 5 * time.Second, InProgressBudget: 8 * time.Second}).ExtProcHold(); got != failFastResumeBudget {
		t.Fatalf("disabled parking with 8s in-progress budget: got %v, want %v", got, failFastResumeBudget)
	}
	if got := (ParkedRequestConfig{Max: 64, Budget: 5 * time.Second, InProgressBudget: 30 * time.Second}).ExtProcHold(); got != 30*time.Second {
		t.Fatalf("enabled parking with 30s in-progress budget: got %v, want 30s", got)
	}
}
