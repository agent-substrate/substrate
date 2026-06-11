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

package router

import (
	"context"
	"sync"
	"testing"
	"time"

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
	const testActorID = "actor-a"
	const expectedIP = "10.0.0.52"

	t.Run("SuspendedResumedSuccessfully", func(t *testing.T) {
		var resumeCalled int
		mock := &resumerMockClient{
			resumeFn: func(ctx context.Context, in *ateapipb.ResumeActorRequest, opts ...grpc.CallOption) (*ateapipb.ResumeActorResponse, error) {
				resumeCalled++
				return &ateapipb.ResumeActorResponse{
					Actor: &ateapipb.Actor{
						ActorId:    testActorID,
						Status:     ateapipb.Actor_STATUS_RUNNING,
						AteomPodIp: expectedIP,
					},
				}, nil
			},
		}

		resumer := NewActorResumer(mock)
		actor, err := resumer.ResumeActor(context.Background(), testActorID)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if actor.GetAteomPodIp() != expectedIP {
			t.Errorf("expected IP %q, got %q", expectedIP, actor.GetAteomPodIp())
		}
		if resumeCalled != 1 {
			t.Errorf("expected ResumeActor called 1 time, got %d", resumeCalled)
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
						ActorId:    testActorID,
						Status:     ateapipb.Actor_STATUS_RUNNING,
						AteomPodIp: expectedIP,
					},
				}, nil
			},
		}

		resumer := NewActorResumer(mock)
		actor, err := resumer.ResumeActor(context.Background(), testActorID)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if actor.GetAteomPodIp() != expectedIP {
			t.Errorf("expected IP %q, got %q", expectedIP, actor.GetAteomPodIp())
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
		_, err := resumer.ResumeActor(context.Background(), testActorID)
		if got := status.Code(err); got != codes.NotFound {
			t.Errorf("expected gRPC code NotFound, got %v (err=%v)", got, err)
		}
	})

	t.Run("SingleflightDeduplication", func(t *testing.T) {
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
						ActorId:    testActorID,
						Status:     ateapipb.Actor_STATUS_RUNNING,
						AteomPodIp: expectedIP,
					},
				}, nil
			},
		}

		resumer := NewActorResumer(mock)

		var wg sync.WaitGroup
		const concurrentRequests = 10
		results := make([]*ateapipb.Actor, concurrentRequests)
		errs := make([]error, concurrentRequests)

		wg.Add(concurrentRequests)
		for i := 0; i < concurrentRequests; i++ {
			go func(idx int) {
				defer wg.Done()
				results[idx], errs[idx] = resumer.ResumeActor(context.Background(), testActorID)
			}(i)
		}
		wg.Wait()

		for i := 0; i < concurrentRequests; i++ {
			if errs[i] != nil {
				t.Fatalf("request %d failed: %v", i, errs[i])
			}
			if results[i].GetAteomPodIp() != expectedIP {
				t.Errorf("request %d expected IP %q, got %q", i, expectedIP, results[i].GetAteomPodIp())
			}
		}

		mu.Lock()
		defer mu.Unlock()
		if resumeCalled != 1 {
			t.Errorf("expected ResumeActor called exactly once by singleflight, got %d", resumeCalled)
		}
	})
}

func TestActorResumer_Parking(t *testing.T) {
	const testActorID = "actor-park"
	const expectedIP = "10.0.0.77"

	t.Run("ParksThenSucceedsOnCapacityError", func(t *testing.T) {
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
					Actor: &ateapipb.Actor{ActorId: testActorID, Status: ateapipb.Actor_STATUS_RUNNING, AteomPodIp: expectedIP},
				}, nil
			},
		}

		resumer := NewActorResumer(mock, withParking(true, 5*time.Second))
		actor, err := resumer.ResumeActor(context.Background(), testActorID)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if actor.GetAteomPodIp() != expectedIP {
			t.Errorf("expected IP %q, got %q", expectedIP, actor.GetAteomPodIp())
		}
		mu.Lock()
		defer mu.Unlock()
		if calls != 3 {
			t.Errorf("expected 3 resume attempts (parked through 2 capacity errors), got %d", calls)
		}
	})

	t.Run("BudgetExpiryReturnsUnderlyingCapacityError", func(t *testing.T) {
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

		// Short budget so the test is fast; the pool never frees up.
		resumer := NewActorResumer(mock, withParking(true, 250*time.Millisecond))
		_, err := resumer.ResumeActor(context.Background(), testActorID)
		// The client must see the meaningful capacity error, not a generic timeout.
		if got := status.Code(err); got != codes.FailedPrecondition {
			t.Errorf("expected FailedPrecondition after park budget elapsed, got %v (err=%v)", got, err)
		}
		mu.Lock()
		defer mu.Unlock()
		if calls < 2 {
			t.Errorf("expected the resume to be retried at least twice while parked, got %d", calls)
		}
	})

	t.Run("DisabledFailsFastOnCapacityError", func(t *testing.T) {
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

		// Default constructor => parking disabled => legacy fail-fast.
		resumer := NewActorResumer(mock)
		_, err := resumer.ResumeActor(context.Background(), testActorID)
		if got := status.Code(err); got != codes.FailedPrecondition {
			t.Errorf("expected FailedPrecondition, got %v (err=%v)", got, err)
		}
		mu.Lock()
		defer mu.Unlock()
		if calls != 1 {
			t.Errorf("expected exactly 1 resume attempt when parking disabled, got %d", calls)
		}
	})
}
