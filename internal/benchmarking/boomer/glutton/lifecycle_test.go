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

	"github.com/agent-substrate/substrate/internal/benchmarking/boomer/boomerutil"
	"github.com/agent-substrate/substrate/internal/benchmarking/boomer/dynconfig"
	"github.com/agent-substrate/substrate/internal/benchmarking/boomer/userclass"
	"github.com/agent-substrate/substrate/internal/benchmarking/glutton/fake"
)

func TestGluttonIterate_SuspendMode(t *testing.T) {
	srv := &fake.Server{}
	fakeCtrl := &fakeControlClient{}
	cfg := newTestConfig(t, srv, &userclass.Config{
		APIStub:  fakeCtrl,
		Atespace: "bench-test",
		Dyn: dynconfig.NewHolder(dynconfig.Config{
			LifecycleMode: dynconfig.LifecycleModeSuspend,
		}),
	})

	rt := &taskRuntime{cfg: cfg}
	rt.iterate()

	calls := fakeCtrl.recordedCalls()
	if !slices.Contains(calls, "SuspendActor") {
		t.Errorf("expected SuspendActor in calls, got: %v", calls)
	}
	if slices.Contains(calls, "PauseActor") {
		t.Errorf("did not expect PauseActor in suspend mode calls, got: %v", calls)
	}
}

func TestGluttonIterate_PauseMode(t *testing.T) {
	srv := &fake.Server{}
	fakeCtrl := &fakeControlClient{}
	cfg := newTestConfig(t, srv, &userclass.Config{
		APIStub:  fakeCtrl,
		Atespace: "bench-test",
		Dyn: dynconfig.NewHolder(dynconfig.Config{
			LifecycleMode: dynconfig.LifecycleModePause,
		}),
	})

	rt := &taskRuntime{cfg: cfg}
	rt.iterate()

	calls := fakeCtrl.recordedCalls()
	if !slices.Contains(calls, "PauseActor") {
		t.Errorf("expected PauseActor in calls, got: %v", calls)
	}
	if slices.Contains(calls, "SuspendActor") {
		t.Errorf("did not expect SuspendActor in pause mode calls, got: %v", calls)
	}
}

func TestGluttonShutdown_PauseModeRunningActor(t *testing.T) {
	srv := &fake.Server{}
	fakeCtrl := &fakeControlClient{}
	cfg := newTestConfig(t, srv, &userclass.Config{
		APIStub:  fakeCtrl,
		Atespace: "bench-test",
		Dyn: dynconfig.NewHolder(dynconfig.Config{
			LifecycleMode: dynconfig.LifecycleModePause,
		}),
	})

	u := &gluttonUser{
		cfg:          cfg,
		actorName:    "running-actor",
		actorRunning: true,
	}

	rt := &taskRuntime{cfg: cfg}
	rt.users.Store(boomerutil.GoroutineID(), u)
	rt.shutdown(context.Background())

	calls := fakeCtrl.recordedCalls()
	if len(calls) < 2 || calls[len(calls)-2] != "PauseActor" || calls[len(calls)-1] != "DeleteActor" {
		t.Errorf("recordedCalls must end with [PauseActor, DeleteActor], got %v", calls)
	}
	reqs := fakeCtrl.recordedDeleteRequests()
	if len(reqs) == 0 || !reqs[0].GetAnyState() {
		t.Errorf("DeleteActor must set AnyState=true, got %v", reqs)
	}
}

func TestGluttonShutdown_DeleteSetsAnyState(t *testing.T) {
	srv := &fake.Server{}
	fakeCtrl := &fakeControlClient{}
	cfg := newTestConfig(t, srv, &userclass.Config{
		APIStub:  fakeCtrl,
		Atespace: "bench-test",
		Dyn: dynconfig.NewHolder(dynconfig.Config{
			LifecycleMode: dynconfig.LifecycleModeSuspend,
		}),
	})

	u := &gluttonUser{
		cfg:          cfg,
		actorName:    "stopped-actor",
		actorRunning: false,
	}

	rt := &taskRuntime{cfg: cfg}
	rt.users.Store(boomerutil.GoroutineID(), u)
	rt.shutdown(context.Background())

	calls := fakeCtrl.recordedCalls()
	if len(calls) != 1 || calls[0] != "DeleteActor" {
		t.Errorf("expected only DeleteActor call, got %v", calls)
	}
	reqs := fakeCtrl.recordedDeleteRequests()
	if len(reqs) == 0 || !reqs[0].GetAnyState() {
		t.Errorf("DeleteActor must set AnyState=true, got %v", reqs)
	}
}
