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

package controlapi

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/util/wait"
)

// stubStep records the order of engine callbacks and returns canned results.
type stubStep struct {
	name      string
	complete  bool
	prereqErr error
	// executeErrs is consumed one entry per Execute call; once exhausted,
	// executeErr is returned (nil by default).
	executeErrs []error
	executeErr  error
	backoff     *wait.Backoff
	calls       *[]string
}

func (s *stubStep) Name() string { return s.name }
func (s *stubStep) IsComplete(ctx context.Context, params struct{}, wCtx struct{}) (bool, error) {
	*s.calls = append(*s.calls, s.name+".IsComplete")
	return s.complete, nil
}
func (s *stubStep) CheckPrerequisite(ctx context.Context, params struct{}, wCtx struct{}) error {
	*s.calls = append(*s.calls, s.name+".CheckPrerequisite")
	return s.prereqErr
}
func (s *stubStep) Execute(ctx context.Context, params struct{}, wCtx struct{}) error {
	*s.calls = append(*s.calls, s.name+".Execute")
	if len(s.executeErrs) > 0 {
		err := s.executeErrs[0]
		s.executeErrs = s.executeErrs[1:]
		return err
	}
	return s.executeErr
}
func (s *stubStep) RetryBackoff() *wait.Backoff { return s.backoff }

func countCalls(calls []string, want string) int {
	n := 0
	for _, c := range calls {
		if c == want {
			n++
		}
	}
	return n
}

func TestRunWorkflow_CheckPrerequisiteOrdering(t *testing.T) {
	ctx := context.Background()

	t.Run("prerequisite checked after IsComplete and before Execute", func(t *testing.T) {
		var calls []string
		steps := []WorkflowStep[struct{}, struct{}]{
			&stubStep{name: "s1", calls: &calls},
		}
		if err := RunWorkflow(ctx, struct{}{}, struct{}{}, steps); err != nil {
			t.Fatalf("RunWorkflow: %v", err)
		}
		want := []string{"s1.IsComplete", "s1.CheckPrerequisite", "s1.Execute"}
		if len(calls) != len(want) {
			t.Fatalf("calls = %v, want %v", calls, want)
		}
		for i := range want {
			if calls[i] != want[i] {
				t.Fatalf("calls = %v, want %v", calls, want)
			}
		}
	})

	t.Run("prerequisite skipped when step is complete", func(t *testing.T) {
		var calls []string
		steps := []WorkflowStep[struct{}, struct{}]{
			&stubStep{name: "s1", complete: true, prereqErr: status.Error(codes.FailedPrecondition, "must not be called"), calls: &calls},
		}
		if err := RunWorkflow(ctx, struct{}{}, struct{}{}, steps); err != nil {
			t.Fatalf("RunWorkflow: %v", err)
		}
		for _, c := range calls {
			if c == "s1.CheckPrerequisite" || c == "s1.Execute" {
				t.Fatalf("unexpected call %q; calls = %v", c, calls)
			}
		}
	})

	t.Run("prerequisite error aborts before Execute and preserves status code", func(t *testing.T) {
		var calls []string
		steps := []WorkflowStep[struct{}, struct{}]{
			&stubStep{name: "s1", prereqErr: status.Error(codes.FailedPrecondition, "nope"), calls: &calls},
			&stubStep{name: "s2", calls: &calls},
		}
		err := RunWorkflow(ctx, struct{}{}, struct{}{}, steps)
		if err == nil {
			t.Fatal("RunWorkflow: expected error, got nil")
		}
		if got := status.Code(err); got != codes.FailedPrecondition {
			t.Fatalf("status.Code(err) = %v, want %v (err: %v)", got, codes.FailedPrecondition, err)
		}
		for _, c := range calls {
			if c == "s1.Execute" || c == "s2.IsComplete" {
				t.Fatalf("unexpected call %q after failed prerequisite; calls = %v", c, calls)
			}
		}
	})
}

// TestRunWorkflow_RetryOnPersistenceConflict verifies runStep's retry loop:
// with a RetryBackoff, Execute is retried on store.ErrPersistenceRetry without
// re-running IsComplete or CheckPrerequisite; any other error — or a nil
// backoff — fails the step on the first attempt.
func TestRunWorkflow_RetryOnPersistenceConflict(t *testing.T) {
	ctx := context.Background()
	// Small steps and duration keep retries fast; Steps also bounds the
	// exhaustion subtest to exactly 3 Execute attempts.
	backoff := &wait.Backoff{Steps: 3, Duration: time.Millisecond, Factor: 2.0}

	t.Run("retries on persistence conflict then succeeds", func(t *testing.T) {
		var calls []string
		steps := []WorkflowStep[struct{}, struct{}]{
			&stubStep{
				name:        "s1",
				executeErrs: []error{store.ErrPersistenceRetry, store.ErrPersistenceRetry},
				backoff:     backoff,
				calls:       &calls,
			},
		}
		if err := RunWorkflow(ctx, struct{}{}, struct{}{}, steps); err != nil {
			t.Fatalf("RunWorkflow: %v", err)
		}
		if got := countCalls(calls, "s1.Execute"); got != 3 {
			t.Errorf("Execute called %d times, want 3; calls = %v", got, calls)
		}
		// The retry loop lives inside runStep: the prerequisite must not be
		// re-validated between attempts.
		if got := countCalls(calls, "s1.CheckPrerequisite"); got != 1 {
			t.Errorf("CheckPrerequisite called %d times, want 1; calls = %v", got, calls)
		}
		if got := countCalls(calls, "s1.IsComplete"); got != 1 {
			t.Errorf("IsComplete called %d times, want 1; calls = %v", got, calls)
		}
	})

	t.Run("non-retryable error fails without retry", func(t *testing.T) {
		var calls []string
		fatal := errors.New("boom")
		steps := []WorkflowStep[struct{}, struct{}]{
			&stubStep{name: "s1", executeErrs: []error{fatal}, backoff: backoff, calls: &calls},
		}
		err := RunWorkflow(ctx, struct{}{}, struct{}{}, steps)
		if !errors.Is(err, fatal) {
			t.Fatalf("RunWorkflow error = %v, want wrapping %v", err, fatal)
		}
		if got := countCalls(calls, "s1.Execute"); got != 1 {
			t.Errorf("Execute called %d times, want 1; calls = %v", got, calls)
		}
	})

	t.Run("persistent conflict exhausts backoff", func(t *testing.T) {
		var calls []string
		steps := []WorkflowStep[struct{}, struct{}]{
			&stubStep{name: "s1", executeErr: store.ErrPersistenceRetry, backoff: backoff, calls: &calls},
		}
		if err := RunWorkflow(ctx, struct{}{}, struct{}{}, steps); err == nil {
			t.Fatal("RunWorkflow: expected error after exhausting retries, got nil")
		}
		if got := countCalls(calls, "s1.Execute"); got != backoff.Steps {
			t.Errorf("Execute called %d times, want %d; calls = %v", got, backoff.Steps, calls)
		}
	})

	t.Run("nil backoff means no retry", func(t *testing.T) {
		var calls []string
		steps := []WorkflowStep[struct{}, struct{}]{
			&stubStep{name: "s1", executeErrs: []error{store.ErrPersistenceRetry}, calls: &calls},
		}
		err := RunWorkflow(ctx, struct{}{}, struct{}{}, steps)
		if !errors.Is(err, store.ErrPersistenceRetry) {
			t.Fatalf("RunWorkflow error = %v, want wrapping store.ErrPersistenceRetry", err)
		}
		if got := countCalls(calls, "s1.Execute"); got != 1 {
			t.Errorf("Execute called %d times, want 1; calls = %v", got, calls)
		}
	})
}
