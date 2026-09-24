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
	"slices"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/benchmarking/boomer/userclass"
	"github.com/agent-substrate/substrate/internal/benchmarking/glutton/fake"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func conflictErr() error {
	return status.Error(codes.Aborted, concurrentUpdateMsg)
}

func newResumeTestActor(t *testing.T, resumeErrs ...error) (*gluttonActor, *fakeControlClient) {
	t.Helper()
	fakeCtrl := &fakeControlClient{resumeErrs: resumeErrs}
	cfg := newTestConfig(t, &fake.Server{}, &userclass.Config{APIStub: fakeCtrl})
	return &gluttonActor{cfg: cfg, actorName: "resumeactor"}, fakeCtrl
}

func resumeCalls(f *fakeControlClient) int {
	return len(slices.DeleteFunc(f.recordedCalls(), func(c string) bool { return c != "ResumeActor" }))
}

func TestResumeRetriesConcurrentUpdateConflict(t *testing.T) {
	u, fakeCtrl := newResumeTestActor(t, conflictErr(), conflictErr())

	start := time.Now()
	err := u.resume(context.Background())
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("resume = %v, want nil after conflicts clear", err)
	}
	if got := resumeCalls(fakeCtrl); got != 3 {
		t.Errorf("ResumeActor calls = %d, want 3 (two conflicts, then success)", got)
	}
	if elapsed < resumeMaxBackoff {
		t.Errorf("elapsed = %v, want >= %v (second retry must back off)", elapsed, resumeMaxBackoff)
	}
	if u.crashed {
		t.Error("crashed = true after a transient conflict")
	}
}

func TestResumeDoesNotRetryOtherErrors(t *testing.T) {
	u, fakeCtrl := newResumeTestActor(t, status.Error(codes.Unavailable, "down"))

	if err := u.resume(context.Background()); err == nil {
		t.Fatal("resume = nil, want an error on a non-conflict error")
	}
	if got := resumeCalls(fakeCtrl); got != 1 {
		t.Errorf("ResumeActor calls = %d, want 1 (no retry for non-conflict errors)", got)
	}
}

func TestResumeGivesUpAfterMaxAttempts(t *testing.T) {
	errs := make([]error, resumeMaxAttempts+1)
	for i := range errs {
		errs[i] = conflictErr()
	}
	u, fakeCtrl := newResumeTestActor(t, errs...)

	if err := u.resume(context.Background()); err == nil {
		t.Fatal("resume = nil, want an error when every attempt conflicts")
	}
	if got := resumeCalls(fakeCtrl); got != resumeMaxAttempts {
		t.Errorf("ResumeActor calls = %d, want %d", got, resumeMaxAttempts)
	}
}

func TestResumeRetryHonorsContextCancellation(t *testing.T) {
	u, fakeCtrl := newResumeTestActor(t, conflictErr(), conflictErr(), conflictErr())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := u.resume(ctx); err == nil {
		t.Fatal("resume = nil, want an error with a canceled context")
	}
	// The first retry is immediate; the second retry's select sees the
	// canceled context and stops instead of sleeping.
	if got := resumeCalls(fakeCtrl); got != 2 {
		t.Errorf("ResumeActor calls = %d, want 2 after cancellation", got)
	}
}
