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
	"errors"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/benchmarking/boomer/boomerutil"
	"github.com/agent-substrate/substrate/internal/benchmarking/boomer/dynconfig"
	"github.com/agent-substrate/substrate/internal/benchmarking/boomer/userclass"
	"github.com/agent-substrate/substrate/internal/benchmarking/glutton/fake"
)

// TestIteratePacesResumeFailures guards against a crashed actor's VU
// spinning on ResumeActor as fast as ateapi can reject it: a VU that never
// waits between failed resumes issues far more QPS than a healthy VU,
// distorting benchmark sampling.
func TestIteratePacesResumeFailures(t *testing.T) {
	srv := &fake.Server{}
	fakeCtrl := &fakeControlClient{resumeErr: errors.New("actor crashed")}
	const wait = 50 * time.Millisecond
	cfg := newTestConfig(t, srv, &userclass.Config{
		APIStub: fakeCtrl,
		Dyn: dynconfig.NewHolder(dynconfig.Config{
			MinWait: wait,
			MaxWait: wait,
		}),
	})

	rt := &taskRuntime{cfg: cfg}
	rt.users.Store(boomerutil.GoroutineID(), &gluttonUser{cfg: cfg, actorName: "sb-test"})

	start := time.Now()
	rt.iterate()
	elapsed := time.Since(start)

	if elapsed < wait {
		t.Errorf("iterate() returned after %v on a failed resume, want at least the configured wait %v", elapsed, wait)
	}

	calls := fakeCtrl.recordedCalls()
	if len(calls) != 1 || calls[0] != "ResumeActor" {
		t.Errorf("recordedCalls: got %v, want [ResumeActor]", calls)
	}
}
