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
	"maps"
	"testing"

	"github.com/agent-substrate/substrate/internal/benchmarking/boomer/userclass"
	"github.com/agent-substrate/substrate/internal/benchmarking/glutton/fake"
)

// These tests cover the wiring from Config.Pools to the worker_selector on the
// wire. userclass/pools_test.go covers the picker itself; what can break here
// instead is an actor that never gets a selector, or one that redraws its pool
// and so asks to resume a snapshot on the wrong CPU.

// picker builds a PoolPicker over equally weighted names, failing the test
// rather than returning an error, since a malformed spec here is a test bug.
func picker(t *testing.T, names ...string) *userclass.PoolPicker {
	t.Helper()
	pools := make([]userclass.Pool, 0, len(names))
	for _, name := range names {
		pools = append(pools, userclass.Pool{Name: name, Weight: 1})
	}
	p, err := userclass.NewPoolPicker(pools)
	if err != nil {
		t.Fatalf("NewPoolPicker(%v) = %v", names, err)
	}
	return p
}

// createdSelectors is the match_labels of every CreateActor the fake saw, with
// nil for a request that carried no selector.
func createdSelectors(f *fakeControlClient) []map[string]string {
	reqs := f.recordedCreateRequests()
	out := make([]map[string]string, 0, len(reqs))
	for _, req := range reqs {
		out = append(out, req.GetActor().GetWorkerSelector().GetMatchLabels())
	}
	return out
}

func TestGluttonCreateSetsWorkerSelector(t *testing.T) {
	tests := []struct {
		name   string
		pools  *userclass.PoolPicker
		pool   string
		want   map[string]string
		wantNo bool // want no selector at all
	}{{
		// The single-pool default: placement is left to the ActorTemplate's
		// own workerSelector, exactly as before pools existed.
		name:   "no pools configured",
		pools:  nil,
		pool:   "",
		wantNo: true,
	}, {
		name:  "pool configured",
		pools: picker(t, "n4"),
		pool:  "n4",
		want:  map[string]string{userclass.PoolLabelKey: "n4"},
	}}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fakeCtrl := &fakeControlClient{}
			cfg := newTestConfig(t, &fake.Server{}, &userclass.Config{
				APIStub:  fakeCtrl,
				Atespace: "bench-test",
				Pools:    tc.pools,
			})
			u := &gluttonActor{cfg: cfg, actorName: "sb-test", pool: tc.pool}

			if err := u.create(context.Background()); err != nil {
				t.Fatalf("create() = %v", err)
			}

			got := createdSelectors(fakeCtrl)
			if len(got) != 1 {
				t.Fatalf("CreateActor calls = %d, want 1", len(got))
			}
			if tc.wantNo {
				if got[0] != nil {
					t.Errorf("worker_selector = %v, want none", got[0])
				}
				return
			}
			if !maps.Equal(got[0], tc.want) {
				t.Errorf("worker_selector = %v, want %v", got[0], tc.want)
			}
		})
	}
}

// An actor's pool is drawn once and held. Were a future change to move the
// draw into create, a recreated actor could ask for a different pool and then
// fail to restore its snapshot on a different CPU model.
func TestGluttonCreateKeepsTheSamePoolAcrossCalls(t *testing.T) {
	fakeCtrl := &fakeControlClient{}
	cfg := newTestConfig(t, &fake.Server{}, &userclass.Config{
		APIStub:  fakeCtrl,
		Atespace: "bench-test",
		// Many equally weighted pools, so a redraw would almost certainly
		// pick a different one.
		Pools: picker(t, "a", "b", "c", "d", "e", "f", "g", "h"),
	})
	u := &gluttonActor{cfg: cfg, actorName: "sb-test", pool: "d"}

	for i := 0; i < 20; i++ {
		if err := u.create(context.Background()); err != nil {
			t.Fatalf("create() #%d = %v", i, err)
		}
	}

	want := map[string]string{userclass.PoolLabelKey: "d"}
	for i, got := range createdSelectors(fakeCtrl) {
		if !maps.Equal(got, want) {
			t.Fatalf("create #%d worker_selector = %v, want %v", i, got, want)
		}
	}
}

// startUser draws per actor, not per VU, so a VU holding several actors
// spreads them. Every actor must still land on a configured pool.
func TestStartUserPinsEveryActorToAConfiguredPool(t *testing.T) {
	const actorsPerUser = 60
	names := []string{"n4", "c4", "c3"}

	fakeCtrl := &fakeControlClient{}
	cfg := newTestConfig(t, &fake.Server{}, &userclass.Config{
		APIStub:       fakeCtrl,
		Atespace:      "bench-test",
		ActorsPerUser: actorsPerUser,
		Pools:         picker(t, names...),
	})

	rt := &taskRuntime{cfg: cfg}
	if _, err := rt.startUser(context.Background()); err != nil {
		t.Fatalf("startUser() = %v", err)
	}

	selectors := createdSelectors(fakeCtrl)
	if len(selectors) != actorsPerUser {
		t.Fatalf("CreateActor calls = %d, want %d", len(selectors), actorsPerUser)
	}

	valid := make(map[string]bool, len(names))
	for _, name := range names {
		valid[name] = true
	}
	seen := make(map[string]int, len(names))
	for i, sel := range selectors {
		if len(sel) != 1 {
			t.Fatalf("actor %d worker_selector = %v, want exactly one label", i, sel)
		}
		value, ok := sel[userclass.PoolLabelKey]
		if !ok {
			t.Fatalf("actor %d worker_selector = %v, want key %q", i, sel, userclass.PoolLabelKey)
		}
		if !valid[value] {
			t.Fatalf("actor %d pinned to unknown pool %q, want one of %v", i, value, names)
		}
		seen[value]++
	}

	// With 60 actors over 3 equal pools, a pool missing entirely means the
	// draw is not spreading. P(some pool empty) < 3*(2/3)^60, far below any
	// flake threshold.
	for _, name := range names {
		if seen[name] == 0 {
			t.Errorf("pool %q got no actors; distribution = %v", name, seen)
		}
	}
}

func TestDurDirCreateSetsWorkerSelector(t *testing.T) {
	fakeCtrl := &fakeControlClient{}
	cfg := newTestConfig(t, &fake.Server{}, &userclass.Config{
		APIStub:  fakeCtrl,
		Atespace: "bench-test",
		Pools:    picker(t, "n4", "c4"),
	})
	u := &durDirUser{
		cfg:          cfg,
		actorName:    "duractor",
		templateName: defaultDurTemplate,
		userClass:    durDirUserClass,
		pool:         "c4",
	}

	if err := u.create(context.Background()); err != nil {
		t.Fatalf("create() = %v", err)
	}

	got := createdSelectors(fakeCtrl)
	want := map[string]string{userclass.PoolLabelKey: "c4"}
	if len(got) != 1 || !maps.Equal(got[0], want) {
		t.Errorf("worker_selector = %v, want [%v]", got, want)
	}
}

// The template reference must survive the create() refactor that hoisted the
// Actor literal into a local to attach the selector.
func TestCreateStillCarriesTemplateAndName(t *testing.T) {
	fakeCtrl := &fakeControlClient{}
	cfg := newTestConfig(t, &fake.Server{}, &userclass.Config{
		APIStub:  fakeCtrl,
		Atespace: "bench-test",
		Pools:    picker(t, "n4"),
	})
	u := &gluttonActor{cfg: cfg, actorName: "sb-test", pool: "n4"}

	if err := u.create(context.Background()); err != nil {
		t.Fatalf("create() = %v", err)
	}

	reqs := fakeCtrl.recordedCreateRequests()
	if len(reqs) != 1 {
		t.Fatalf("CreateActor calls = %d, want 1", len(reqs))
	}
	actor := reqs[0].GetActor()
	if got := actor.GetMetadata().GetName(); got != "sb-test" {
		t.Errorf("actor name = %q, want %q", got, "sb-test")
	}
	if got := actor.GetMetadata().GetAtespace(); got != "bench-test" {
		t.Errorf("actor atespace = %q, want %q", got, "bench-test")
	}
	if got := actor.GetActorTemplate().GetName(); got != templateName {
		t.Errorf("actor template = %q, want %q", got, templateName)
	}
	if got := actor.GetActorTemplate().GetAtespace(); got != templateAtespace {
		t.Errorf("template atespace = %q, want %q", got, templateAtespace)
	}
}
