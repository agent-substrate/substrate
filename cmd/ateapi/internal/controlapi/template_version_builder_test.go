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
	"strings"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

func newTestBuilder(tc *testContext) (*TemplateVersionBuilder, *time.Time) {
	builder := NewTemplateVersionBuilder(tc.service, tc.persistence)
	now := time.Now()
	builder.now = func() time.Time { return now }
	return builder, &now
}

func getVersionState(t *testing.T, tc *testContext, name string) *ateapipb.ActorTemplateVersion {
	t.Helper()
	atv, err := tc.persistence.GetActorTemplateVersion(context.Background(), name)
	if err != nil {
		t.Fatalf("GetActorTemplateVersion(%s) failed: %v", name, err)
	}
	return atv
}

func TestTemplateVersionBuilder_BuildsGoldenSnapshot(t *testing.T) {
	ns := namespaceForTest("ns-atv-build")
	tc := setupTest(t, ns)
	defer tc.cleanup()
	ctx := context.Background()
	ensureDefaultGvisorSandboxConfig(t, tc)
	createWorkerPool(t, tc, ns, "pool1", map[string]string{poolLabelKey: ns})
	createWorkerPod(t, tc, ns, "worker-1", "node1", "pool1")
	createTemplateForVersions(t, tc, "tmpl-a")
	// validTemplateVersionSpec puts readyz on every container, so the warmup
	// is zero and READY needs no clock manipulation.
	created := createTemplateVersion(t, tc, "tmpl-a-v1", "tmpl-a")

	builder, _ := newTestBuilder(tc)

	// Tick 1: INITIAL -> RESUME_GOLDEN_ACTOR, golden actor created.
	builder.runOnce(ctx)
	atv := getVersionState(t, tc, "tmpl-a-v1")
	if got := atv.GetStatus().GetState(); got != ateapipb.ActorTemplateVersionStatus_STATE_RESUME_GOLDEN_ACTOR {
		t.Fatalf("after tick 1, state = %s, want RESUME_GOLDEN_ACTOR", got)
	}
	goldenActor := atv.GetStatus().GetGoldenActor()
	if goldenActor.GetAtespace() != resources.GoldenActorAtespace || goldenActor.GetName() != created.GetMetadata().GetUid() {
		t.Fatalf("golden actor = %v, want %s/%s", goldenActor, resources.GoldenActorAtespace, created.GetMetadata().GetUid())
	}
	golden, err := tc.persistence.GetActor(ctx, resources.ActorRef{Atespace: goldenActor.GetAtespace(), Name: goldenActor.GetName()})
	if err != nil {
		t.Fatalf("golden actor not stored: %v", err)
	}
	if golden.GetActorTemplateVersion() != "tmpl-a-v1" {
		t.Errorf("golden actor pins version %q, want tmpl-a-v1", golden.GetActorTemplateVersion())
	}

	// Tick 2: RESUME_GOLDEN_ACTOR -> WAIT_GOLDEN_ACTOR, workload booted.
	builder.runOnce(ctx)
	atv = getVersionState(t, tc, "tmpl-a-v1")
	if got := atv.GetStatus().GetState(); got != ateapipb.ActorTemplateVersionStatus_STATE_WAIT_GOLDEN_ACTOR {
		t.Fatalf("after tick 2, state = %s, want WAIT_GOLDEN_ACTOR", got)
	}
	if !tc.fakeAtelet.RunCalled {
		t.Error("expected the golden actor to boot via Run")
	}

	// Tick 3: WAIT_GOLDEN_ACTOR -> READY (readyz everywhere, warmup 0).
	builder.runOnce(ctx)
	atv = getVersionState(t, tc, "tmpl-a-v1")
	if got := atv.GetStatus().GetState(); got != ateapipb.ActorTemplateVersionStatus_STATE_READY {
		t.Fatalf("after tick 3, state = %s (message %q), want READY", got, atv.GetStatus().GetMessage())
	}
	goldenSnapshot := atv.GetStatus().GetGoldenSnapshot()
	if goldenSnapshot.GetAtespace() != resources.GoldenActorAtespace {
		t.Errorf("golden snapshot atespace = %q, want %s", goldenSnapshot.GetAtespace(), resources.GoldenActorAtespace)
	}
	snapshot, err := tc.persistence.GetActorSnapshot(ctx, goldenSnapshot.GetAtespace(), goldenSnapshot.GetName())
	if err != nil {
		t.Fatalf("golden snapshot not resolvable: %v", err)
	}
	if snapshot.GetContentScope() != ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL {
		t.Errorf("golden snapshot scope = %s, want FULL", snapshot.GetContentScope())
	}
	if snapshot.GetActorTemplateUid() != created.GetMetadata().GetUid() {
		t.Errorf("golden snapshot template uid = %q, want the version uid %q", snapshot.GetActorTemplateUid(), created.GetMetadata().GetUid())
	}
	// The sandbox frozen at creation must survive every status write.
	if atv.GetStatus().GetResolvedSandbox() == nil {
		t.Error("resolved_sandbox lost during the build")
	}
	// The golden actor is deleted once READY; its snapshot record stays.
	if _, err := tc.persistence.GetActor(ctx, resources.ActorRef{Atespace: goldenActor.GetAtespace(), Name: goldenActor.GetName()}); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("golden actor after READY = %v, want ErrNotFound", err)
	}

	// A READY version is terminal: another tick must not touch it.
	version := atv.GetMetadata().GetVersion()
	builder.runOnce(ctx)
	if got := getVersionState(t, tc, "tmpl-a-v1").GetMetadata().GetVersion(); got != version {
		t.Errorf("READY version advanced from %d to %d on an extra tick", version, got)
	}
}

func TestTemplateVersionBuilder_WarmupWithoutReadyz(t *testing.T) {
	ns := namespaceForTest("ns-atv-warmup")
	tc := setupTest(t, ns)
	defer tc.cleanup()
	ctx := context.Background()
	ensureDefaultGvisorSandboxConfig(t, tc)
	createWorkerPool(t, tc, ns, "pool1", map[string]string{poolLabelKey: ns})
	createWorkerPod(t, tc, ns, "worker-1", "node1", "pool1")
	createTemplateForVersions(t, tc, "tmpl-a")
	if _, err := tc.client.CreateActorTemplateVersion(ctx, &ateapipb.CreateActorTemplateVersionRequest{
		ActorTemplateVersion: &ateapipb.ActorTemplateVersion{
			Metadata:      &ateapipb.ResourceMetadata{Name: "tmpl-a-v1"},
			ActorTemplate: &ateapipb.ObjectRef{Name: "tmpl-a"},
			Spec: validTemplateVersionSpec(func(spec *ateapipb.ActorTemplateVersionSpec) {
				spec.Containers[0].Readyz = nil
			}),
		},
	}); err != nil {
		t.Fatalf("CreateActorTemplateVersion failed: %v", err)
	}

	builder, now := newTestBuilder(tc)
	builder.runOnce(ctx) // INITIAL -> RESUME_GOLDEN_ACTOR
	builder.runOnce(ctx) // RESUME_GOLDEN_ACTOR -> WAIT_GOLDEN_ACTOR

	atv := getVersionState(t, tc, "tmpl-a-v1")
	wantAt := now.Add(templateVersionWarmup)
	if got := atv.GetStatus().GetTakeGoldenSnapshotAt().AsTime(); !got.Equal(wantAt) {
		t.Fatalf("take_golden_snapshot_at = %v, want %v", got, wantAt)
	}

	// Before the deadline the version idles in WAIT_GOLDEN_ACTOR.
	builder.runOnce(ctx)
	if got := getVersionState(t, tc, "tmpl-a-v1").GetStatus().GetState(); got != ateapipb.ActorTemplateVersionStatus_STATE_WAIT_GOLDEN_ACTOR {
		t.Fatalf("state before warmup deadline = %s, want WAIT_GOLDEN_ACTOR", got)
	}

	*now = now.Add(templateVersionWarmup + time.Second)
	builder.runOnce(ctx)
	if got := getVersionState(t, tc, "tmpl-a-v1").GetStatus().GetState(); got != ateapipb.ActorTemplateVersionStatus_STATE_READY {
		t.Fatalf("state after warmup deadline = %s, want READY", got)
	}
}

func TestTemplateVersionBuilder_LockConflictSkips(t *testing.T) {
	ns := namespaceForTest("ns-atv-lock")
	tc := setupTest(t, ns)
	defer tc.cleanup()
	ctx := context.Background()
	ensureDefaultGvisorSandboxConfig(t, tc)
	createTemplateForVersions(t, tc, "tmpl-a")
	createTemplateVersion(t, tc, "tmpl-a-v1", "tmpl-a")

	lock, err := tc.persistence.AcquireLock(ctx, actorTemplateVersionLockKey("tmpl-a-v1"))
	if err != nil {
		t.Fatalf("AcquireLock failed: %v", err)
	}
	defer lock.Close()

	builder, _ := newTestBuilder(tc)
	builder.runOnce(ctx)
	if got := getVersionState(t, tc, "tmpl-a-v1").GetStatus().GetState(); got != ateapipb.ActorTemplateVersionStatus_STATE_INITIAL {
		t.Fatalf("state under a held peer lock = %s, want INITIAL untouched", got)
	}
}

func TestTemplateVersionBuilder_FailsAfterBuildTimeout(t *testing.T) {
	ns := namespaceForTest("ns-atv-timeout")
	tc := setupTest(t, ns)
	defer tc.cleanup()
	ctx := context.Background()
	ensureDefaultGvisorSandboxConfig(t, tc)
	// No pool matches this selector (worker pods from sibling tests leak in
	// through the shared envtest cluster), so resuming the golden actor
	// keeps failing.
	if _, err := tc.client.CreateActorTemplate(ctx, &ateapipb.CreateActorTemplateRequest{
		ActorTemplate: &ateapipb.ActorTemplate{
			Metadata: &ateapipb.ResourceMetadata{Name: "tmpl-a"},
			Spec: &ateapipb.ActorTemplateSpec{
				WorkerSelector: &ateapipb.Selector{MatchLabels: map[string]string{"tier": "no-such-pool"}},
			},
		},
	}); err != nil {
		t.Fatalf("CreateActorTemplate failed: %v", err)
	}
	createTemplateVersion(t, tc, "tmpl-a-v1", "tmpl-a")

	builder, now := newTestBuilder(tc)
	builder.runOnce(ctx) // INITIAL -> RESUME_GOLDEN_ACTOR

	// Within the build timeout the resume failure only retries.
	builder.runOnce(ctx)
	if got := getVersionState(t, tc, "tmpl-a-v1").GetStatus().GetState(); got != ateapipb.ActorTemplateVersionStatus_STATE_RESUME_GOLDEN_ACTOR {
		t.Fatalf("state after transient failure = %s, want RESUME_GOLDEN_ACTOR retained", got)
	}

	*now = now.Add(builder.buildTimeout + time.Minute)
	builder.runOnce(ctx)
	atv := getVersionState(t, tc, "tmpl-a-v1")
	if got := atv.GetStatus().GetState(); got != ateapipb.ActorTemplateVersionStatus_STATE_FAILED {
		t.Fatalf("state after build timeout = %s, want FAILED", got)
	}
	if !strings.Contains(atv.GetStatus().GetMessage(), "while resuming golden actor") {
		t.Errorf("FAILED message = %q, want the last resume error", atv.GetStatus().GetMessage())
	}
}
