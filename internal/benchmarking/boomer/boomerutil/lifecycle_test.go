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

package boomerutil

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func conflictErr() error {
	return status.Error(codes.Aborted, ConcurrentUpdateMsg)
}

func TestClassifyLifecycleFailure(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want FailureAction
	}{
		{"actor gone", status.Error(codes.NotFound, "Actor not found"), ReplaceNow},
		{"snapshot unreadable", status.Error(codes.DataLoss, "external snapshot"), ReplaceNow},
		{"stuck state", status.Error(codes.FailedPrecondition, "MarkSuspending prerequisite not met"), ReplaceNow},
		{"crashed", status.Error(codes.Aborted, "actor bench/sb-1 crashed"), ReplaceNow},
		{"no capacity", status.Error(codes.ResourceExhausted, "no free workers available"), RetryLater},
		{"api server down", status.Error(codes.Unavailable, "connection refused"), RetryLater},
		{"timeout", status.Error(codes.DeadlineExceeded, "context deadline exceeded"), RetryLater},
		{"canceled", status.Error(codes.Canceled, "context canceled"), RetryLater},
		{"misconfigured run", status.Error(codes.InvalidArgument, "bad template"), RetryLater},
		{"unauthorized", status.Error(codes.PermissionDenied, "denied"), RetryLater},
		{"update conflict", conflictErr(), ReplaceIfPersistent},
		{"wrapped atelet error", status.Error(codes.Unknown, "while checkpointing workload"), ReplaceIfPersistent},
		{"internal", status.Error(codes.Internal, "boom"), ReplaceIfPersistent},
		{"not a status", errors.New("plain error"), ReplaceIfPersistent},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := ClassifyLifecycleFailure(tc.err); got != tc.want {
				t.Errorf("ClassifyLifecycleFailure(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

func TestIsCrashedVsConflict(t *testing.T) {
	crashed := status.Error(codes.Aborted, "actor bench/sb-1 crashed")
	if !IsCrashed(crashed) || IsConcurrentUpdateConflict(crashed) {
		t.Error("crashed verdict misclassified")
	}
	if IsCrashed(conflictErr()) || !IsConcurrentUpdateConflict(conflictErr()) {
		t.Error("update conflict misclassified")
	}
}

// queue returns a call that pops errs in order and succeeds once drained,
// counting the attempts. Safe for concurrent callers.
func queue(errs ...error) (call func() error, attempts *atomic.Int64) {
	var mu sync.Mutex
	attempts = new(atomic.Int64)
	return func() error {
		attempts.Add(1)
		mu.Lock()
		defer mu.Unlock()
		if len(errs) == 0 {
			return nil
		}
		err := errs[0]
		errs = errs[1:]
		return err
	}, attempts
}

func TestRetryOnConflictRetriesThenSucceeds(t *testing.T) {
	errs := []error{conflictErr(), conflictErr()}
	call, attempts := queue(errs...)
	start := time.Now()
	if err := RetryOnConflict(context.Background(), call); err != nil {
		t.Fatalf("RetryOnConflict = %v, want nil after conflicts clear", err)
	}
	if want := int64(len(errs) + 1); attempts.Load() != want {
		t.Errorf("attempts = %d, want %d (every conflict, then success)", attempts.Load(), want)
	}
	if elapsed := time.Since(start); elapsed < ConflictRetryBackoff {
		t.Errorf("elapsed = %v, want >= %v (second retry must back off)", elapsed, ConflictRetryBackoff)
	}
}

func TestRetryOnConflictPassesOtherErrorsThrough(t *testing.T) {
	want := status.Error(codes.Unavailable, "down")
	call, attempts := queue(want)
	if err := RetryOnConflict(context.Background(), call); !errors.Is(err, want) {
		t.Fatalf("RetryOnConflict = %v, want %v", err, want)
	}
	if attempts.Load() != 1 {
		t.Errorf("attempts = %d, want 1 (no retry for non-conflict errors)", attempts.Load())
	}
}

func TestRetryOnConflictGivesUp(t *testing.T) {
	errs := make([]error, ConflictRetryAttempts+1)
	for i := range errs {
		errs[i] = conflictErr()
	}
	call, attempts := queue(errs...)
	if err := RetryOnConflict(context.Background(), call); !IsConcurrentUpdateConflict(err) {
		t.Fatalf("RetryOnConflict = %v, want the last conflict", err)
	}
	if attempts.Load() != ConflictRetryAttempts {
		t.Errorf("attempts = %d, want %d", attempts.Load(), ConflictRetryAttempts)
	}
}

func TestRetryOnConflictHonorsContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	call, attempts := queue(conflictErr(), conflictErr(), conflictErr())
	if err := RetryOnConflict(ctx, call); !errors.Is(err, context.Canceled) {
		t.Fatalf("RetryOnConflict = %v, want context.Canceled", err)
	}
	// The first retry is immediate; the second sees the canceled context
	// instead of sleeping.
	if attempts.Load() != 2 {
		t.Errorf("attempts = %d, want 2 after cancellation", attempts.Load())
	}
}
