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

package ingress

import (
	"context"
	"errors"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type resumerMockClient struct {
	ateapipb.ControlClient
	resumeFn func(ctx context.Context, in *ateapipb.ResumeActorRequest, opts ...grpc.CallOption) (*ateapipb.ResumeActorResponse, error)
}

func (m *resumerMockClient) ResumeActor(ctx context.Context, in *ateapipb.ResumeActorRequest, opts ...grpc.CallOption) (*ateapipb.ResumeActorResponse, error) {
	if m.resumeFn != nil {
		return m.resumeFn(ctx, in, opts...)
	}
	return nil, status.Error(codes.Unimplemented, "unimplemented")
}

func TestActorResumer_ResumeActor(t *testing.T) {
	const testActorName = "actor-a"
	const testAtespace = "team-a"
	const expectedIP = "10.0.0.52"

	testActorRef := resources.ActorRef{Atespace: testAtespace, Name: testActorName}

	t.Run("SuspendedResumedSuccessfully", func(t *testing.T) {
		var resumeCalled int
		mock := &resumerMockClient{
			resumeFn: func(ctx context.Context, in *ateapipb.ResumeActorRequest, opts ...grpc.CallOption) (*ateapipb.ResumeActorResponse, error) {
				resumeCalled++
				return &ateapipb.ResumeActorResponse{
					Actor: &ateapipb.Actor{
						Status:           ateapipb.Actor_STATUS_RUNNING,
						WorkerAssignment: &ateapipb.WorkerAssignment{WorkerPodIp: expectedIP},
					},
					Resumed: true,
				}, nil
			},
		}

		resumer := NewActorResumer(mock)
		actor, outcome, err := resumer.ResumeActor(context.Background(), testActorRef)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if actor.GetWorkerAssignment().GetWorkerPodIp() != expectedIP {
			t.Errorf("expected IP %q, got %q", expectedIP, actor.GetWorkerAssignment().GetWorkerPodIp())
		}
		if outcome != ResumeOutcomeTriggered {
			t.Errorf("expected outcome %q, got %q", ResumeOutcomeTriggered, outcome)
		}
		if resumeCalled != 1 {
			t.Errorf("expected ResumeActor called 1 time, got %d", resumeCalled)
		}
	})

	t.Run("WarmRouting_Disambiguation", func(t *testing.T) {
		mock := &resumerMockClient{
			resumeFn: func(ctx context.Context, in *ateapipb.ResumeActorRequest, opts ...grpc.CallOption) (*ateapipb.ResumeActorResponse, error) {
				return &ateapipb.ResumeActorResponse{
					Actor: &ateapipb.Actor{
						Metadata:         &ateapipb.ResourceMetadata{Name: testActorName},
						Status:           ateapipb.Actor_STATUS_RUNNING,
						WorkerAssignment: &ateapipb.WorkerAssignment{WorkerPodIp: expectedIP},
					},
					Resumed: false,
				}, nil
			},
		}

		resumer := NewActorResumer(mock)
		_, outcome, err := resumer.ResumeActor(context.Background(), testActorRef)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if outcome != ResumeOutcomeNone {
			t.Errorf("expected outcome %q for warm routing, got %q", ResumeOutcomeNone, outcome)
		}
	})

	t.Run("RetryOnAbortedConflict", func(t *testing.T) {
		var resumeCalled int
		mock := &resumerMockClient{
			resumeFn: func(ctx context.Context, in *ateapipb.ResumeActorRequest, opts ...grpc.CallOption) (*ateapipb.ResumeActorResponse, error) {
				resumeCalled++
				if resumeCalled < 3 {
					return nil, status.Error(codes.Aborted, "concurrent update conflict")
				}
				return &ateapipb.ResumeActorResponse{
					Actor: &ateapipb.Actor{
						Status:           ateapipb.Actor_STATUS_RUNNING,
						WorkerAssignment: &ateapipb.WorkerAssignment{WorkerPodIp: expectedIP},
					},
					Resumed: true,
				}, nil
			},
		}

		resumer := NewActorResumer(mock)
		actor, outcome, err := resumer.ResumeActor(context.Background(), testActorRef)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if actor.GetWorkerAssignment().GetWorkerPodIp() != expectedIP {
			t.Errorf("expected IP %q, got %q", expectedIP, actor.GetWorkerAssignment().GetWorkerPodIp())
		}
		if outcome != ResumeOutcomeTriggered {
			t.Errorf("expected outcome %q, got %q", ResumeOutcomeTriggered, outcome)
		}
		if resumeCalled != 3 {
			t.Errorf("expected ResumeActor called 3 times, got %d", resumeCalled)
		}
	})

	t.Run("ActorNotFound", func(t *testing.T) {
		mock := &resumerMockClient{
			resumeFn: func(ctx context.Context, in *ateapipb.ResumeActorRequest, opts ...grpc.CallOption) (*ateapipb.ResumeActorResponse, error) {
				return nil, status.Error(codes.NotFound, "not found")
			},
		}

		resumer := NewActorResumer(mock)
		_, outcome, err := resumer.ResumeActor(context.Background(), testActorRef)
		if got := status.Code(err); got != codes.NotFound {
			t.Errorf("expected gRPC code NotFound, got %v (err=%v)", got, err)
		}
		if outcome != ResumeOutcomeNone {
			t.Errorf("expected outcome %q on error, got %q", ResumeOutcomeNone, outcome)
		}
	})

	t.Run("SingleflightDeduplication_Disambiguation", func(t *testing.T) {
		var resumeCalled int
		var mu sync.Mutex

		mock := &resumerMockClient{
			resumeFn: func(ctx context.Context, in *ateapipb.ResumeActorRequest, opts ...grpc.CallOption) (*ateapipb.ResumeActorResponse, error) {
				mu.Lock()
				resumeCalled++
				mu.Unlock()
				time.Sleep(20 * time.Millisecond)
				return &ateapipb.ResumeActorResponse{
					Actor: &ateapipb.Actor{
						Status:           ateapipb.Actor_STATUS_RUNNING,
						WorkerAssignment: &ateapipb.WorkerAssignment{WorkerPodIp: expectedIP},
					},
					Resumed: true,
				}, nil
			},
		}

		resumer := NewActorResumer(mock)

		var wg sync.WaitGroup
		const concurrentRequests = 10
		results := make([]*ateapipb.Actor, concurrentRequests)
		outcomes := make([]ResumeOutcome, concurrentRequests)
		errs := make([]error, concurrentRequests)

		wg.Add(concurrentRequests)
		for i := 0; i < concurrentRequests; i++ {
			go func(idx int) {
				defer wg.Done()
				results[idx], outcomes[idx], errs[idx] = resumer.ResumeActor(context.Background(), testActorRef)
			}(i)
		}
		wg.Wait()

		var triggeredCount, joinedCount int
		for i := 0; i < concurrentRequests; i++ {
			if errs[i] != nil {
				t.Fatalf("request %d failed: %v", i, errs[i])
			}
			if results[i].GetWorkerAssignment().GetWorkerPodIp() != expectedIP {
				t.Errorf("request %d expected IP %q, got %q", i, expectedIP, results[i].GetWorkerAssignment().GetWorkerPodIp())
			}
			switch outcomes[i] {
			case ResumeOutcomeTriggered:
				triggeredCount++
			case ResumeOutcomeJoined:
				joinedCount++
			default:
				t.Errorf("unexpected outcome for request %d: %q", i, outcomes[i])
			}
		}

		if triggeredCount != 1 {
			t.Errorf("expected exactly 1 request to have outcome 'triggered', got %d", triggeredCount)
		}
		if joinedCount != concurrentRequests-1 {
			t.Errorf("expected %d requests to have outcome 'joined', got %d", concurrentRequests-1, joinedCount)
		}

		mu.Lock()
		defer mu.Unlock()
		if resumeCalled != 1 {
			t.Errorf("expected ResumeActor called exactly once by singleflight, got %d", resumeCalled)
		}
	})
}

// TestActorResumer_Parking runs each case inside a synctest bubble, so the
// parked retry loop's waits are fake time.
func TestActorResumer_Parking(t *testing.T) {
	const (
		testActorName = "actor-park"
		testAtespace  = "team-a"
		expectedIP    = "10.0.0.77"
	)
	testActorRef := resources.ActorRef{Atespace: testAtespace, Name: testActorName}

	t.Run("ParksThenSucceedsOnCapacityError", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			var mu sync.Mutex
			var calls int
			mock := &resumerMockClient{
				resumeFn: func(ctx context.Context, in *ateapipb.ResumeActorRequest, opts ...grpc.CallOption) (*ateapipb.ResumeActorResponse, error) {
					mu.Lock()
					calls++
					n := calls
					mu.Unlock()
					if n < 3 {
						// Worker pool momentarily saturated.
						return nil, status.Error(codes.FailedPrecondition, "no free workers available")
					}
					return &ateapipb.ResumeActorResponse{
						Actor: &ateapipb.Actor{Metadata: &ateapipb.ResourceMetadata{Name: testActorName}, Status: ateapipb.Actor_STATUS_RUNNING, WorkerAssignment: &ateapipb.WorkerAssignment{WorkerPodIp: expectedIP}},
					}, nil
				},
			}

			resumer := NewActorResumer(mock, withParking(ParkedRequestConfig{Max: 1, Budget: 5 * time.Second}))
			actor, _, err := resumer.ResumeActor(context.Background(), testActorRef)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if actor.GetWorkerAssignment().GetWorkerPodIp() != expectedIP {
				t.Errorf("expected IP %q, got %q", expectedIP, actor.GetWorkerAssignment().GetWorkerPodIp())
			}
			mu.Lock()
			defer mu.Unlock()
			if calls != 3 {
				t.Errorf("expected 3 resume attempts (parked through 2 capacity errors), got %d", calls)
			}
		})
	})

	t.Run("BudgetExpiryReturnsUnderlyingCapacityError", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			var mu sync.Mutex
			var calls int
			mock := &resumerMockClient{
				resumeFn: func(ctx context.Context, in *ateapipb.ResumeActorRequest, opts ...grpc.CallOption) (*ateapipb.ResumeActorResponse, error) {
					mu.Lock()
					calls++
					mu.Unlock()
					return nil, status.Error(codes.FailedPrecondition, "no free workers available")
				},
			}

			// Budget large enough for a few ~100ms-spaced retries before it elapses;
			// the pool never frees up.
			resumer := NewActorResumer(mock, withParking(ParkedRequestConfig{Max: 1, Budget: 1500 * time.Millisecond}))
			_, _, err := resumer.ResumeActor(context.Background(), testActorRef)
			// The client must see the meaningful capacity error, not a generic
			// timeout: status.Code must unwrap through the budget-exhaustion marker.
			if got := status.Code(err); got != codes.FailedPrecondition {
				t.Errorf("expected FailedPrecondition after park budget elapsed, got %v (err=%v)", got, err)
			}
			var budget *budgetExhaustedError
			if !errors.As(err, &budget) {
				t.Errorf("expected the error to be marked as budget exhaustion, got %T (%v)", err, err)
			}
			mu.Lock()
			defer mu.Unlock()
			if calls < 2 {
				t.Errorf("expected the resume to be retried at least twice while parked, got %d", calls)
			}
		})
	})

	t.Run("ParksThroughUnavailableBlip", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			var mu sync.Mutex
			var calls int
			mock := &resumerMockClient{
				resumeFn: func(ctx context.Context, in *ateapipb.ResumeActorRequest, opts ...grpc.CallOption) (*ateapipb.ResumeActorResponse, error) {
					mu.Lock()
					calls++
					n := calls
					mu.Unlock()
					if n < 3 {
						// Control plane momentarily unreachable (e.g. rolling restart).
						return nil, status.Error(codes.Unavailable, "connection refused")
					}
					return &ateapipb.ResumeActorResponse{
						Actor: &ateapipb.Actor{Metadata: &ateapipb.ResourceMetadata{Name: testActorName}, Status: ateapipb.Actor_STATUS_RUNNING, WorkerAssignment: &ateapipb.WorkerAssignment{WorkerPodIp: expectedIP}},
					}, nil
				},
			}

			resumer := NewActorResumer(mock, withParking(ParkedRequestConfig{Max: 1, Budget: 5 * time.Second}))
			actor, _, err := resumer.ResumeActor(context.Background(), testActorRef)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if actor.GetWorkerAssignment().GetWorkerPodIp() != expectedIP {
				t.Errorf("expected IP %q, got %q", expectedIP, actor.GetWorkerAssignment().GetWorkerPodIp())
			}
			mu.Lock()
			defer mu.Unlock()
			if calls != 3 {
				t.Errorf("expected 3 resume attempts (parked through 2 Unavailable blips), got %d", calls)
			}
		})
	})

	t.Run("DisabledFailsFastOnUnavailable", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			var mu sync.Mutex
			var calls int
			mock := &resumerMockClient{
				resumeFn: func(ctx context.Context, in *ateapipb.ResumeActorRequest, opts ...grpc.CallOption) (*ateapipb.ResumeActorResponse, error) {
					mu.Lock()
					calls++
					mu.Unlock()
					return nil, status.Error(codes.Unavailable, "connection refused")
				},
			}

			resumer := NewActorResumer(mock)
			_, _, err := resumer.ResumeActor(context.Background(), testActorRef)
			if got := status.Code(err); got != codes.Unavailable {
				t.Errorf("expected Unavailable, got %v (err=%v)", got, err)
			}
			mu.Lock()
			defer mu.Unlock()
			if calls != 1 {
				t.Errorf("expected exactly 1 resume attempt when parking disabled, got %d", calls)
			}
		})
	})

	t.Run("BudgetExpiryDuringInFlightRPC", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			var mu sync.Mutex
			var calls int
			mock := &resumerMockClient{
				resumeFn: func(ctx context.Context, in *ateapipb.ResumeActorRequest, opts ...grpc.CallOption) (*ateapipb.ResumeActorResponse, error) {
					mu.Lock()
					calls++
					n := calls
					mu.Unlock()
					if n == 1 {
						// First attempt: transient saturation, remembered as the
						// last retryable error.
						return nil, status.Error(codes.FailedPrecondition, "no free workers available")
					}
					// Later attempt: block until the park budget cancels the RPC,
					// then return what a real gRPC client returns — a *status*
					// error with code DeadlineExceeded that does NOT satisfy
					// errors.Is(err, context.DeadlineExceeded).
					<-ctx.Done()
					return nil, status.FromContextError(ctx.Err()).Err()
				},
			}

			resumer := NewActorResumer(mock, withParking(ParkedRequestConfig{Max: 1, Budget: 300 * time.Millisecond}))
			_, _, err := resumer.ResumeActor(context.Background(), testActorRef)
			// The deadline landed mid-RPC; the client must still see the capacity
			// error (503 "no free workers available"), not a generic timeout (504).
			if got := status.Code(err); got != codes.FailedPrecondition {
				t.Errorf("expected FailedPrecondition when the budget lands mid-RPC, got %v (err=%v)", got, err)
			}
			var budget *budgetExhaustedError
			if !errors.As(err, &budget) {
				t.Errorf("expected the error to be marked as budget exhaustion, got %T (%v)", err, err)
			}
		})
	})

	t.Run("DisabledFailsFastOnCapacityError", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			var mu sync.Mutex
			var calls int
			mock := &resumerMockClient{
				resumeFn: func(ctx context.Context, in *ateapipb.ResumeActorRequest, opts ...grpc.CallOption) (*ateapipb.ResumeActorResponse, error) {
					mu.Lock()
					calls++
					mu.Unlock()
					return nil, status.Error(codes.FailedPrecondition, "no free workers available")
				},
			}

			// Default constructor => parking disabled => fail-fast.
			resumer := NewActorResumer(mock)
			_, _, err := resumer.ResumeActor(context.Background(), testActorRef)
			if got := status.Code(err); got != codes.FailedPrecondition {
				t.Errorf("expected FailedPrecondition, got %v (err=%v)", got, err)
			}
			mu.Lock()
			defer mu.Unlock()
			if calls != 1 {
				t.Errorf("expected exactly 1 resume attempt when parking disabled, got %d", calls)
			}
		})
	})
}

// TestActorResumer_CallerCancelDoesNotAbortFlight pins the detached-context
// contract from both sides: a caller that disconnects while parked gets
// context.Canceled (classified as the `canceled` outcome) WITHOUT aborting the
// shared in-flight resume, which keeps running and serves a later caller from
// the same single RPC.
func TestActorResumer_CallerCancelDoesNotAbortFlight(t *testing.T) {
	synctest.Test(t, testCallerCancelDoesNotAbortFlight)
}

func testCallerCancelDoesNotAbortFlight(t *testing.T) {
	const (
		testActorName = "actor-cancel"
		testAtespace  = "team-a"
		expectedIP    = "10.0.0.88"
	)
	testActorRef := resources.ActorRef{Atespace: testAtespace, Name: testActorName}

	var mu sync.Mutex
	var calls int
	started := make(chan struct{})
	proceed := make(chan struct{})
	mock := &resumerMockClient{
		resumeFn: func(ctx context.Context, in *ateapipb.ResumeActorRequest, opts ...grpc.CallOption) (*ateapipb.ResumeActorResponse, error) {
			mu.Lock()
			calls++
			n := calls
			mu.Unlock()
			if n == 1 {
				close(started)
			}
			// Hold the flight open until the test releases it.
			<-proceed
			return &ateapipb.ResumeActorResponse{
				Actor: &ateapipb.Actor{Metadata: &ateapipb.ResourceMetadata{Name: testActorName}, Status: ateapipb.Actor_STATUS_RUNNING, WorkerAssignment: &ateapipb.WorkerAssignment{WorkerPodIp: expectedIP}},
			}, nil
		},
	}

	resumer := NewActorResumer(mock, withParking(ParkedRequestConfig{Max: 2, Budget: 5 * time.Second}))

	// Caller 1 starts the flight, then disconnects while parked.
	ctx1, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		_, _, err := resumer.ResumeActor(ctx1, testActorRef)
		errCh <- err
	}()
	<-started
	cancel()

	err := <-errCh
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("disconnected caller: expected context.Canceled, got %v", err)
	}
	if got := parkOutcomeFor(err); got != parkOutcomeCanceled {
		t.Errorf("disconnected caller outcome = %q, want %q", got, parkOutcomeCanceled)
	}

	// Caller 2 arrives after caller 1 left; the flight is still in its first
	// RPC, so it must join that flight rather than start a new one.
	type result struct {
		actor *ateapipb.Actor
		err   error
	}
	resCh := make(chan result, 1)
	go func() {
		a, _, rerr := resumer.ResumeActor(context.Background(), testActorRef)
		resCh <- result{a, rerr}
	}()
	// Let caller 2 reach the flight before releasing it, so the call-count
	// assertion proves it shared the first RPC. Inside the bubble this is exact
	// rather than a hopeful sleep: Wait blocks until every other goroutine here
	// — caller 2 included — is durably blocked, i.e. parked on the flight.
	synctest.Wait()
	close(proceed)

	res := <-resCh
	if res.err != nil {
		t.Fatalf("second caller: unexpected error: %v", res.err)
	}
	if res.actor.GetWorkerAssignment().GetWorkerPodIp() != expectedIP {
		t.Errorf("second caller IP = %q, want %q", res.actor.GetWorkerAssignment().GetWorkerPodIp(), expectedIP)
	}
	mu.Lock()
	defer mu.Unlock()
	if calls != 1 {
		t.Errorf("expected the canceled caller's flight to be shared (1 RPC), got %d", calls)
	}
}

func TestResumeBackoffHasNoCap(t *testing.T) {
	// Regression: the resume backoff must NOT set wait.Backoff.Cap. delay() zeroes
	// Steps the moment the delay reaches Cap, which would end parking retries far
	// short of the budget (a 2s Cap stops the loop in ~7 steps / ~5s). The budget
	// context — not the step count or a cap — must bound how long a request parks.
	b := resumeBackoff(DefaultParkedRequestRetryInterval, DefaultParkedRequestRetryFactor, DefaultParkedRequestRetryJitter)
	if b.Cap != 0 {
		t.Errorf("resume backoff must not set Cap (it would stop retries at the cap); got %v", b.Cap)
	}
	if b.Steps < 1<<20 {
		t.Errorf("resume backoff Steps must be high so the budget bounds the wait; got %d", b.Steps)
	}
}
