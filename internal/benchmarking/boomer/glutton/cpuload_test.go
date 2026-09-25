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
	"net/http"
	"testing"

	"github.com/agent-substrate/substrate/internal/benchmarking/boomer/dynconfig"
	"github.com/agent-substrate/substrate/internal/benchmarking/glutton/fake"
)

func TestEnsureCPULoadRequestsConfiguredLoad(t *testing.T) {
	srv := &fake.Server{}
	u := newTestGluttonActor(t, srv, dynconfig.Config{CPUCores: 2, CPUDutyCycle: 0.1})

	u.ensureCPULoad(context.Background())

	if !u.cpuLoaded {
		t.Fatal("cpuLoaded = false after successful UseCPU")
	}
	reqs := srv.RecordedCPURequests()
	if len(reqs) != 1 {
		t.Fatalf("UseCPU calls = %d, want 1", len(reqs))
	}
	if reqs[0].GetNumCores() != 2 || reqs[0].GetDutyCycle() != 0.1 {
		t.Errorf("UseCPU request = (%d, %v), want (2, 0.1)", reqs[0].GetNumCores(), reqs[0].GetDutyCycle())
	}

	// Second call must be a no-op: the load persists in the actor.
	u.ensureCPULoad(context.Background())
	if got := len(srv.RecordedCPURequests()); got != 1 {
		t.Errorf("UseCPU calls after repeat = %d, want 1 (no new calls)", got)
	}
}

func TestEnsureCPULoadDisabledByDefault(t *testing.T) {
	srv := &fake.Server{}
	u := newTestGluttonActor(t, srv, dynconfig.Config{})

	u.ensureCPULoad(context.Background())

	if !u.cpuLoaded {
		t.Fatal("cpuLoaded = false with zero cores; want true (disabled = done)")
	}
	if got := len(srv.RecordedCPURequests()); got != 0 {
		t.Errorf("UseCPU calls with zero cores = %d, want 0", got)
	}
}

func TestEnsureCPULoadRetriesAfterFailure(t *testing.T) {
	srv := &fake.Server{Status: http.StatusServiceUnavailable}
	u := newTestGluttonActor(t, srv, dynconfig.Config{CPUCores: 1, CPUDutyCycle: 0.5})
	ctx := context.Background()

	u.ensureCPULoad(ctx)
	if u.cpuLoaded {
		t.Fatal("cpuLoaded = true after failed UseCPU; want false so the next iteration retries")
	}

	srv.Status = 0
	u.ensureCPULoad(ctx)
	if !u.cpuLoaded {
		t.Fatal("cpuLoaded = false after retry succeeded")
	}
	if got := len(srv.RecordedCPURequests()); got != 1 {
		t.Errorf("recorded UseCPU requests = %d, want 1 (the failed call is rejected before recording)", got)
	}
}
