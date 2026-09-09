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
	"testing"

	"github.com/agent-substrate/substrate/internal/benchmarking/boomer/dynconfig"
	"github.com/agent-substrate/substrate/internal/benchmarking/boomer/userclass"
	"github.com/agent-substrate/substrate/internal/benchmarking/glutton/fake"
	gluttonpb "github.com/agent-substrate/substrate/internal/proto/glutton"
)

func newTestGluttonUser(t *testing.T, srv *fake.Server, dyn dynconfig.Config) *gluttonUser {
	t.Helper()
	cfg := newTestConfig(t, srv, &userclass.Config{Dyn: dynconfig.NewHolder(dyn)})
	return &gluttonUser{
		cfg:        cfg,
		actorName:  "memactor",
		hostHeader: "memactor.benchmark." + actorDomain,
	}
}

func TestEnsureRAMFilledRequestsTarget(t *testing.T) {
	srv := &fake.Server{}
	u := newTestGluttonUser(t, srv, dynconfig.Config{MemTarget: "2Gi"})

	u.ensureRAMFilled(context.Background())

	if !u.ramFilled {
		t.Fatal("ramFilled = false after successful fill")
	}
	sizes := srv.RecordedRAMWriteSizes()
	if len(sizes) != 1 || sizes[0] != "2Gi" {
		t.Fatalf("WriteRAM size = %v, want [2Gi] (passed through verbatim)", sizes)
	}

	// Second call must be a no-op: the working set persists in the actor.
	u.ensureRAMFilled(context.Background())
	if got := len(srv.RecordedRAMWriteSizes()); got != 1 {
		t.Errorf("WriteRAM calls after repeat fill = %d, want 1 (no new calls)", got)
	}
}

func TestEnsureRAMFilledDisabledByDefault(t *testing.T) {
	srv := &fake.Server{}
	u := newTestGluttonUser(t, srv, dynconfig.Config{})

	u.ensureRAMFilled(context.Background())

	if !u.ramFilled {
		t.Fatal("ramFilled = false with zero target; want true (disabled = done)")
	}
	if got := len(srv.RecordedRAMWriteSizes()); got != 0 {
		t.Errorf("WriteRAM calls with zero target = %d, want 0", got)
	}
}

func TestChurnRAMOverwritesEachCycle(t *testing.T) {
	srv := &fake.Server{}
	u := newTestGluttonUser(t, srv, dynconfig.Config{MemTarget: "1Gi", MemChurn: "64Mi"})
	ctx := context.Background()

	// Churn before fill is a no-op: there is nothing to overwrite yet.
	u.churnRAM(ctx)
	if got := len(srv.RecordedRAMWriteSizes()); got != 0 {
		t.Fatalf("WriteRAM calls from churn before fill = %d, want 0", got)
	}

	// Fill, then two cycles of churn.
	u.ensureRAMFilled(ctx)
	u.churnRAM(ctx)
	u.churnRAM(ctx)

	sizes := srv.RecordedRAMWriteSizes()
	modes := srv.RecordedRAMWriteModes()
	wantSizes := []string{"1Gi", "64Mi", "64Mi"}
	wantModes := []gluttonpb.WriteMode{
		gluttonpb.WriteMode_WRITE_MODE_TRUNCATE,
		gluttonpb.WriteMode_WRITE_MODE_OVERWRITE_ROTATE,
		gluttonpb.WriteMode_WRITE_MODE_OVERWRITE_ROTATE,
	}
	if len(sizes) != len(wantSizes) {
		t.Fatalf("WriteRAM calls = %d (%v), want %d", len(sizes), sizes, len(wantSizes))
	}
	for i := range wantSizes {
		if sizes[i] != wantSizes[i] || modes[i] != wantModes[i] {
			t.Errorf("call %d = (%s, %v), want (%s, %v)", i, sizes[i], modes[i], wantSizes[i], wantModes[i])
		}
	}
}

func TestChurnRAMDisabledByDefault(t *testing.T) {
	srv := &fake.Server{}
	u := newTestGluttonUser(t, srv, dynconfig.Config{MemTarget: "1Gi"})
	ctx := context.Background()

	u.ensureRAMFilled(ctx)
	u.churnRAM(ctx)
	if got := len(srv.RecordedRAMWriteSizes()); got != 1 {
		t.Errorf("WriteRAM calls with churn unset = %d, want 1 (fill only)", got)
	}
}

func TestReadRAMWalksAfterFill(t *testing.T) {
	srv := &fake.Server{}
	u := newTestGluttonUser(t, srv, dynconfig.Config{MemTarget: "1Gi", MemRead: "all"})
	ctx := context.Background()

	// Read before fill is a no-op: there is nothing to walk yet.
	u.readRAM(ctx)
	if got := len(srv.RecordedRAMReadSizes()); got != 0 {
		t.Fatalf("ReadRAM calls before fill = %d, want 0", got)
	}

	u.ensureRAMFilled(ctx)
	u.readRAM(ctx)
	u.readRAM(ctx)

	sizes := srv.RecordedRAMReadSizes()
	// "all" maps to an empty size: ReadRAM walks the whole array.
	want := []string{"", ""}
	if len(sizes) != len(want) {
		t.Fatalf("ReadRAM calls = %d (%v), want %d", len(sizes), sizes, len(want))
	}
	for i := range want {
		if sizes[i] != want[i] {
			t.Errorf("call %d size = %q, want %q", i, sizes[i], want[i])
		}
	}
}

func TestReadRAMPassesSizeVerbatim(t *testing.T) {
	srv := &fake.Server{}
	u := newTestGluttonUser(t, srv, dynconfig.Config{MemTarget: "1Gi", MemRead: "512Mi"})
	ctx := context.Background()

	u.ensureRAMFilled(ctx)
	u.readRAM(ctx)

	sizes := srv.RecordedRAMReadSizes()
	if len(sizes) != 1 || sizes[0] != "512Mi" {
		t.Fatalf("ReadRAM sizes = %v, want [512Mi] (passed through verbatim)", sizes)
	}
}

func TestReadRAMDisabledByDefault(t *testing.T) {
	srv := &fake.Server{}
	u := newTestGluttonUser(t, srv, dynconfig.Config{MemTarget: "1Gi"})
	ctx := context.Background()

	u.ensureRAMFilled(ctx)
	u.readRAM(ctx)
	if got := len(srv.RecordedRAMReadSizes()); got != 0 {
		t.Errorf("ReadRAM calls with mem_read unset = %d, want 0", got)
	}
}

func TestEnsureRAMFilledRetriesAfterFailure(t *testing.T) {
	srv := &fake.Server{Status: 503}
	u := newTestGluttonUser(t, srv, dynconfig.Config{MemTarget: "1Mi"})

	u.ensureRAMFilled(context.Background())
	if u.ramFilled {
		t.Fatal("ramFilled = true after failed fill; want false so the next iteration retries")
	}

	srv.Status = 0
	u.ensureRAMFilled(context.Background())
	if !u.ramFilled {
		t.Fatal("ramFilled = false after recovery; want true")
	}
}
