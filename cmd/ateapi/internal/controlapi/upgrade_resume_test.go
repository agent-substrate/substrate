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
	"testing"

	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

// upgradeTestVersion creates a version of template with the given spec
// mutations and marks it READY.
func upgradeTestVersion(t *testing.T, tc *testContext, name, template string, mutations ...func(*ateapipb.ActorTemplateVersionSpec)) *ateapipb.ActorTemplateVersion {
	t.Helper()
	if _, err := tc.client.CreateActorTemplateVersion(context.Background(), &ateapipb.CreateActorTemplateVersionRequest{
		ActorTemplateVersion: &ateapipb.ActorTemplateVersion{
			Metadata:      &ateapipb.ResourceMetadata{Name: name},
			ActorTemplate: &ateapipb.ObjectRef{Name: template},
			Spec:          validTemplateVersionSpec(mutations...),
		},
	}); err != nil {
		t.Fatalf("CreateActorTemplateVersion(%s) failed: %v", name, err)
	}
	return markTemplateVersionReady(t, tc, name)
}

func getActor(t *testing.T, tc *testContext, name string) *ateapipb.Actor {
	t.Helper()
	actor, err := tc.client.GetActor(context.Background(), &ateapipb.GetActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: testAtespace, Name: name},
	})
	if err != nil {
		t.Fatalf("GetActor(%s) failed: %v", name, err)
	}
	return actor
}

// TestResumeActor_UpgradeToNewVersion walks the whole upgrade primitive: a
// FULL snapshot taken under v1 restores as-is while the pin is v1, restores
// data-only with v2's spec and sandbox after the upgrade resume, and rolls
// back to v1 via UpdateActor + plain resume.
func TestResumeActor_UpgradeToNewVersion(t *testing.T) {
	ns := namespaceForTest("ns-upgrade-resume")
	tc := setupTest(t, ns)
	defer tc.cleanup()
	ctx := context.Background()
	ensureDefaultGvisorSandboxConfig(t, tc)
	createTemplateForVersions(t, tc, "tmpl-up")
	upgradeTestVersion(t, tc, "tmpl-up-v1", "tmpl-up")
	upgradeTestVersion(t, tc, "tmpl-up-v2", "tmpl-up", func(s *ateapipb.ActorTemplateVersionSpec) {
		s.Containers[0].Image = "app@sha256:fff"
	})
	createWorkerPool(t, tc, ns, "pool1", map[string]string{poolLabelKey: ns})
	createWorkerPod(t, tc, ns, "worker-1", "node1", "pool1")

	actorRef := &ateapipb.ObjectRef{Atespace: testAtespace, Name: "id-up"}
	if _, err := tc.client.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:             &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "id-up"},
		ActorTemplate:        "tmpl-up",
		ActorTemplateVersion: "tmpl-up-v1",
	}}); err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}
	if _, err := tc.client.ResumeActor(ctx, &ateapipb.ResumeActorRequest{Actor: actorRef}); err != nil {
		t.Fatalf("ResumeActor(boot) failed: %v", err)
	}
	if _, err := tc.client.SuspendActor(ctx, &ateapipb.SuspendActorRequest{Actor: actorRef}); err != nil {
		t.Fatalf("SuspendActor failed: %v", err)
	}

	// The suspend recorded the producing version on the snapshot.
	suspended := getActor(t, tc, "id-up")
	snapshot, err := tc.persistence.GetActorSnapshot(ctx, testAtespace, suspended.GetLatestSnapshot().GetName())
	if err != nil {
		t.Fatalf("GetActorSnapshot failed: %v", err)
	}
	if got := snapshot.GetActorTemplateVersion(); got != "tmpl-up-v1" {
		t.Errorf("snapshot actor_template_version = %q, want tmpl-up-v1", got)
	}
	if got := snapshot.GetContentScope(); got != ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL {
		t.Fatalf("snapshot content scope = %v, want FULL", got)
	}

	// Same-version resume restores the FULL snapshot as-is, manifest-described.
	tc.fakeAtelet.Reset()
	if _, err := tc.client.ResumeActor(ctx, &ateapipb.ResumeActorRequest{Actor: actorRef}); err != nil {
		t.Fatalf("ResumeActor(same version) failed: %v", err)
	}
	restore := tc.fakeAtelet.lastRestoreRequest()
	if got := restore.GetScope(); got != ateletpb.SnapshotScope_SNAPSHOT_SCOPE_FULL {
		t.Errorf("same-version restore scope = %v, want FULL", got)
	}
	if restore.GetSandboxAssets() != nil {
		t.Errorf("same-version restore carries sandbox_assets = %v, want none", restore.GetSandboxAssets())
	}
	if _, err := tc.client.SuspendActor(ctx, &ateapipb.SuspendActorRequest{Actor: actorRef}); err != nil {
		t.Fatalf("SuspendActor failed: %v", err)
	}

	// Upgrade: the resume re-pins to v2 and restores durable data only, with
	// v2's images and frozen sandbox.
	tc.fakeAtelet.Reset()
	if _, err := tc.client.ResumeActor(ctx, &ateapipb.ResumeActorRequest{
		Actor:                actorRef,
		ActorTemplateVersion: "tmpl-up-v2",
	}); err != nil {
		t.Fatalf("ResumeActor(upgrade to v2) failed: %v", err)
	}
	if got := getActor(t, tc, "id-up").GetActorTemplateVersion(); got != "tmpl-up-v2" {
		t.Errorf("actor pin after upgrade = %q, want tmpl-up-v2", got)
	}
	restore = tc.fakeAtelet.lastRestoreRequest()
	if got := restore.GetScope(); got != ateletpb.SnapshotScope_SNAPSHOT_SCOPE_DATA {
		t.Errorf("upgrade restore scope = %v, want DATA", got)
	}
	if got := restore.GetSpec().GetContainers()[0].GetImage(); got != "app@sha256:fff" {
		t.Errorf("upgrade restore image = %q, want v2's app@sha256:fff", got)
	}
	if got := restore.GetActorTemplateName(); got != "tmpl-up-v2" {
		t.Errorf("upgrade restore template identity = %q, want tmpl-up-v2", got)
	}
	if got := restore.GetSandboxAssets().GetSandboxClass(); got != "gvisor" {
		t.Errorf("upgrade restore sandbox class = %q, want the frozen gvisor sandbox", got)
	}

	// The next suspend pins the new version on its snapshot.
	if _, err := tc.client.SuspendActor(ctx, &ateapipb.SuspendActorRequest{Actor: actorRef}); err != nil {
		t.Fatalf("SuspendActor failed: %v", err)
	}
	suspended = getActor(t, tc, "id-up")
	snapshot, err = tc.persistence.GetActorSnapshot(ctx, testAtespace, suspended.GetLatestSnapshot().GetName())
	if err != nil {
		t.Fatalf("GetActorSnapshot failed: %v", err)
	}
	if got := snapshot.GetActorTemplateVersion(); got != "tmpl-up-v2" {
		t.Errorf("post-upgrade snapshot actor_template_version = %q, want tmpl-up-v2", got)
	}

	// Rollback: revert the pin via UpdateActor, then a plain resume restores
	// v1's spec. The latest snapshot was produced under v2, so this is again
	// a cross-version, data-only restore.
	if _, err := tc.client.UpdateActor(ctx, &ateapipb.UpdateActorRequest{
		Actor: &ateapipb.Actor{
			Metadata:             &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "id-up"},
			ActorTemplateVersion: "tmpl-up-v1",
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"actor_template_version"}},
	}); err != nil {
		t.Fatalf("UpdateActor(rollback pin) failed: %v", err)
	}
	tc.fakeAtelet.Reset()
	if _, err := tc.client.ResumeActor(ctx, &ateapipb.ResumeActorRequest{Actor: actorRef}); err != nil {
		t.Fatalf("ResumeActor(after rollback) failed: %v", err)
	}
	restore = tc.fakeAtelet.lastRestoreRequest()
	if got := restore.GetScope(); got != ateletpb.SnapshotScope_SNAPSHOT_SCOPE_DATA {
		t.Errorf("rollback restore scope = %v, want DATA", got)
	}
	if got := restore.GetSpec().GetContainers()[0].GetImage(); got != "app@sha256:def" {
		t.Errorf("rollback restore image = %q, want v1's app@sha256:def", got)
	}
}

// TestResumeActor_UpgradeRetryKeepsPin verifies the re-pin is durable before
// any restore attempt: after a failed upgrade resume, a plain retry without
// the version argument still restores the new version's shape.
func TestResumeActor_UpgradeRetryKeepsPin(t *testing.T) {
	ns := namespaceForTest("ns-upgrade-retry")
	tc := setupTest(t, ns)
	defer tc.cleanup()
	ctx := context.Background()
	ensureDefaultGvisorSandboxConfig(t, tc)
	createTemplateForVersions(t, tc, "tmpl-up")
	upgradeTestVersion(t, tc, "tmpl-up-v1", "tmpl-up")
	upgradeTestVersion(t, tc, "tmpl-up-v2", "tmpl-up", func(s *ateapipb.ActorTemplateVersionSpec) {
		s.Containers[0].Image = "app@sha256:fff"
	})
	createWorkerPool(t, tc, ns, "pool1", map[string]string{poolLabelKey: ns})
	createWorkerPod(t, tc, ns, "worker-1", "node1", "pool1")

	actorRef := &ateapipb.ObjectRef{Atespace: testAtespace, Name: "id-retry"}
	if _, err := tc.client.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:             &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "id-retry"},
		ActorTemplate:        "tmpl-up",
		ActorTemplateVersion: "tmpl-up-v1",
	}}); err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}
	if _, err := tc.client.ResumeActor(ctx, &ateapipb.ResumeActorRequest{Actor: actorRef}); err != nil {
		t.Fatalf("ResumeActor(boot) failed: %v", err)
	}
	if _, err := tc.client.SuspendActor(ctx, &ateapipb.SuspendActorRequest{Actor: actorRef}); err != nil {
		t.Fatalf("SuspendActor failed: %v", err)
	}

	tc.fakeAtelet.Reset()
	tc.fakeAtelet.FailRestore = status.Error(codes.Unavailable, "atelet down")
	if _, err := tc.client.ResumeActor(ctx, &ateapipb.ResumeActorRequest{
		Actor:                actorRef,
		ActorTemplateVersion: "tmpl-up-v2",
	}); err == nil {
		t.Fatal("ResumeActor(upgrade) should fail while atelet is down")
	}
	if got := getActor(t, tc, "id-retry").GetActorTemplateVersion(); got != "tmpl-up-v2" {
		t.Fatalf("pin after failed upgrade = %q, want tmpl-up-v2 (persisted before restore)", got)
	}

	tc.fakeAtelet.Reset()
	if _, err := tc.client.ResumeActor(ctx, &ateapipb.ResumeActorRequest{Actor: actorRef}); err != nil {
		t.Fatalf("plain retry after failed upgrade failed: %v", err)
	}
	restore := tc.fakeAtelet.lastRestoreRequest()
	if got := restore.GetSpec().GetContainers()[0].GetImage(); got != "app@sha256:fff" {
		t.Errorf("retried restore image = %q, want v2's app@sha256:fff", got)
	}
	if got := restore.GetScope(); got != ateletpb.SnapshotScope_SNAPSHOT_SCOPE_DATA {
		t.Errorf("retried restore scope = %v, want DATA", got)
	}
}

// TestResumeActor_FromCrashed covers the rollback flow the design prescribes
// for failed upgrades: a CRASHED actor can be resumed directly, including
// with an explicit version to revert to.
func TestResumeActor_FromCrashed(t *testing.T) {
	ns := namespaceForTest("ns-upgrade-crashed")
	tc := setupTest(t, ns)
	defer tc.cleanup()
	ctx := context.Background()
	ensureDefaultGvisorSandboxConfig(t, tc)
	createTemplateForVersions(t, tc, "tmpl-up")
	upgradeTestVersion(t, tc, "tmpl-up-v1", "tmpl-up")
	upgradeTestVersion(t, tc, "tmpl-up-v2", "tmpl-up", func(s *ateapipb.ActorTemplateVersionSpec) {
		s.Containers[0].Image = "app@sha256:fff"
	})
	createWorkerPool(t, tc, ns, "pool1", map[string]string{poolLabelKey: ns})
	createWorkerPod(t, tc, ns, "worker-1", "node1", "pool1")

	actorRef := &ateapipb.ObjectRef{Atespace: testAtespace, Name: "id-crash"}
	if _, err := tc.client.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:             &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "id-crash"},
		ActorTemplate:        "tmpl-up",
		ActorTemplateVersion: "tmpl-up-v1",
	}}); err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}
	if _, err := tc.client.ResumeActor(ctx, &ateapipb.ResumeActorRequest{Actor: actorRef}); err != nil {
		t.Fatalf("ResumeActor(boot) failed: %v", err)
	}
	if _, err := tc.client.SuspendActor(ctx, &ateapipb.SuspendActorRequest{Actor: actorRef}); err != nil {
		t.Fatalf("SuspendActor failed: %v", err)
	}

	// A failed upgrade leaves the actor CRASHED with the new pin.
	ref := resources.ActorRef{Atespace: testAtespace, Name: "id-crash"}
	if _, err := tc.client.ResumeActor(ctx, &ateapipb.ResumeActorRequest{
		Actor:                actorRef,
		ActorTemplateVersion: "tmpl-up-v2",
	}); err != nil {
		t.Fatalf("ResumeActor(upgrade) failed: %v", err)
	}
	if _, err := tc.client.SuspendActor(ctx, &ateapipb.SuspendActorRequest{Actor: actorRef}); err != nil {
		t.Fatalf("SuspendActor failed: %v", err)
	}
	if err := crashActor(ctx, tc.persistence, ref, ateattr.OperationResume, ateattr.ReasonCorruptedAssignment); err != nil {
		t.Fatalf("crashActor failed: %v", err)
	}

	// Rollback straight from CRASHED with an explicit version.
	tc.fakeAtelet.Reset()
	if _, err := tc.client.ResumeActor(ctx, &ateapipb.ResumeActorRequest{
		Actor:                actorRef,
		ActorTemplateVersion: "tmpl-up-v1",
	}); err != nil {
		t.Fatalf("ResumeActor(rollback from CRASHED) failed: %v", err)
	}
	actor := getActor(t, tc, "id-crash")
	if got := actor.GetStatus(); got != ateapipb.Actor_STATUS_RUNNING {
		t.Errorf("actor status after rollback = %v, want RUNNING", got)
	}
	if got := actor.GetActorTemplateVersion(); got != "tmpl-up-v1" {
		t.Errorf("actor pin after rollback = %q, want tmpl-up-v1", got)
	}
	restore := tc.fakeAtelet.lastRestoreRequest()
	if got := restore.GetSpec().GetContainers()[0].GetImage(); got != "app@sha256:def" {
		t.Errorf("rollback restore image = %q, want v1's app@sha256:def", got)
	}

	// A plain resume also works from CRASHED (retry-with-current-pin).
	if _, err := tc.client.SuspendActor(ctx, &ateapipb.SuspendActorRequest{Actor: actorRef}); err != nil {
		t.Fatalf("SuspendActor failed: %v", err)
	}
	if err := crashActor(ctx, tc.persistence, ref, ateattr.OperationResume, ateattr.ReasonCorruptedAssignment); err != nil {
		t.Fatalf("crashActor failed: %v", err)
	}
	if _, err := tc.client.ResumeActor(ctx, &ateapipb.ResumeActorRequest{Actor: actorRef}); err != nil {
		t.Fatalf("ResumeActor(plain, from CRASHED) failed: %v", err)
	}
	if got := getActor(t, tc, "id-crash").GetStatus(); got != ateapipb.Actor_STATUS_RUNNING {
		t.Errorf("actor status after plain resume = %v, want RUNNING", got)
	}
}

// TestResumeActor_RepinRejections exercises validateVersionRepin end to end
// through both ResumeActor and UpdateActor.
func TestResumeActor_RepinRejections(t *testing.T) {
	ns := namespaceForTest("ns-upgrade-reject")
	tc := setupTest(t, ns)
	defer tc.cleanup()
	ctx := context.Background()
	ensureDefaultGvisorSandboxConfig(t, tc)
	createTemplateForVersions(t, tc, "tmpl-up")
	upgradeTestVersion(t, tc, "tmpl-up-v1", "tmpl-up")
	createTemplateVersion(t, tc, "tmpl-up-v2-building", "tmpl-up") // stays INITIAL
	upgradeTestVersion(t, tc, "tmpl-up-v3-vols", "tmpl-up", func(s *ateapipb.ActorTemplateVersionSpec) {
		s.Volumes = nil
		s.Containers[0].VolumeMounts = nil
	})
	createTemplateForVersions(t, tc, "tmpl-other")
	upgradeTestVersion(t, tc, "tmpl-other-v1", "tmpl-other")
	// Creates pool1 alongside the CRD template tmpl1 (for the CRD-path case).
	createTemplate(t, tc, ns)
	createWorkerPod(t, tc, ns, "worker-1", "node1", "pool1")

	actorRef := &ateapipb.ObjectRef{Atespace: testAtespace, Name: "id-rej"}
	if _, err := tc.client.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:             &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "id-rej"},
		ActorTemplate:        "tmpl-up",
		ActorTemplateVersion: "tmpl-up-v1",
	}}); err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}
	if _, err := tc.client.ResumeActor(ctx, &ateapipb.ResumeActorRequest{Actor: actorRef}); err != nil {
		t.Fatalf("ResumeActor(boot) failed: %v", err)
	}

	// RUNNING actors cannot be re-pinned, via either RPC.
	const notSuspendedMsg = "actor id-rej is STATUS_RUNNING; re-pinning requires STATUS_SUSPENDED or STATUS_CRASHED"
	_, err := tc.client.ResumeActor(ctx, &ateapipb.ResumeActorRequest{Actor: actorRef, ActorTemplateVersion: "tmpl-up-v3-vols"})
	assertGrpcError(t, err, codes.FailedPrecondition, notSuspendedMsg)
	_, err = tc.client.UpdateActor(ctx, &ateapipb.UpdateActorRequest{
		Actor: &ateapipb.Actor{
			Metadata:             &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "id-rej"},
			ActorTemplateVersion: "tmpl-up-v3-vols",
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"actor_template_version"}},
	})
	assertGrpcError(t, err, codes.FailedPrecondition, notSuspendedMsg)

	if _, err := tc.client.SuspendActor(ctx, &ateapipb.SuspendActorRequest{Actor: actorRef}); err != nil {
		t.Fatalf("SuspendActor failed: %v", err)
	}

	_, err = tc.client.ResumeActor(ctx, &ateapipb.ResumeActorRequest{Actor: actorRef, ActorTemplateVersion: "no-such-version"})
	assertGrpcError(t, err, codes.FailedPrecondition, `ActorTemplateVersion "no-such-version" not found`)
	_, err = tc.client.ResumeActor(ctx, &ateapipb.ResumeActorRequest{Actor: actorRef, ActorTemplateVersion: "tmpl-up-v2-building"})
	assertGrpcError(t, err, codes.FailedPrecondition, `ActorTemplateVersion "tmpl-up-v2-building" is not READY (state STATE_INITIAL)`)
	_, err = tc.client.ResumeActor(ctx, &ateapipb.ResumeActorRequest{Actor: actorRef, ActorTemplateVersion: "tmpl-other-v1"})
	assertGrpcError(t, err, codes.FailedPrecondition, `ActorTemplateVersion "tmpl-other-v1" belongs to ActorTemplate "tmpl-other", not "tmpl-up"`)
	_, err = tc.client.ResumeActor(ctx, &ateapipb.ResumeActorRequest{Actor: actorRef, ActorTemplateVersion: "tmpl-up-v3-vols"})
	assertGrpcError(t, err, codes.FailedPrecondition, `ActorTemplateVersion "tmpl-up-v3-vols" removes volume "data"; volumes are immutable across versions`)

	// A rejected re-pin leaves the pin untouched.
	if got := getActor(t, tc, "id-rej").GetActorTemplateVersion(); got != "tmpl-up-v1" {
		t.Errorf("pin after rejected re-pins = %q, want tmpl-up-v1", got)
	}

	// CRD-path actors cannot be re-pinned at all.
	if _, err := tc.client.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:               &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "id-crd"},
		ActorTemplateNamespace: ns,
		ActorTemplateName:      "tmpl1",
	}}); err != nil {
		t.Fatalf("CreateActor(CRD) failed: %v", err)
	}
	_, err = tc.client.ResumeActor(ctx, &ateapipb.ResumeActorRequest{
		Actor:                &ateapipb.ObjectRef{Atespace: testAtespace, Name: "id-crd"},
		ActorTemplateVersion: "tmpl-up-v1",
	})
	assertGrpcError(t, err, codes.FailedPrecondition, "only actors created from a control-plane ActorTemplate can be re-pinned")
}

// TestResumeActor_UpgradeOnResumeGolden verifies that when the new version's
// onResume policy selects the golden snapshot, an upgrade restores as
// DATA_ON_GOLDEN combining the new version's golden with the actor's data.
func TestResumeActor_UpgradeOnResumeGolden(t *testing.T) {
	ns := namespaceForTest("ns-upgrade-golden")
	tc := setupTest(t, ns)
	defer tc.cleanup()
	ctx := context.Background()

	// Seed READY versions through the store: v2 uses onResume FromData=GOLDEN
	// (spec validation restricts that to microvm at create time; the resume
	// path is class-agnostic and this test drives it with gvisor workers).
	gvisorSandbox := &ateapipb.SandboxAssets{SandboxClass: ateapipb.SandboxClass_SANDBOX_CLASS_GVISOR}
	createTemplateForVersions(t, tc, "tmpl-up")
	if _, err := tc.persistence.CreateActorTemplateVersion(ctx, &ateapipb.ActorTemplateVersion{
		Metadata:      &ateapipb.ResourceMetadata{Name: "tmpl-up-v1"},
		ActorTemplate: &ateapipb.ObjectRef{Name: "tmpl-up"},
		Spec:          validTemplateVersionSpec(),
		Status: &ateapipb.ActorTemplateVersionStatus{
			State:           ateapipb.ActorTemplateVersionStatus_STATE_READY,
			ResolvedSandbox: gvisorSandbox,
		},
	}); err != nil {
		t.Fatalf("CreateActorTemplateVersion(v1, store) failed: %v", err)
	}
	goldenURI := "gs://bucket/snapshots/" + resources.GoldenActorAtespace + "/golden-v2"
	if _, err := tc.persistence.CreateActorSnapshot(ctx, &ateapipb.ActorSnapshot{
		Metadata:     &ateapipb.ResourceMetadata{Atespace: resources.GoldenActorAtespace, Name: "golden-v2"},
		ContentScope: ateapipb.SnapshotContentScope_SNAPSHOT_CONTENT_SCOPE_FULL,
		SnapshotUri:  goldenURI,
	}); err != nil {
		t.Fatalf("CreateActorSnapshot(golden) failed: %v", err)
	}
	if _, err := tc.persistence.CreateActorTemplateVersion(ctx, &ateapipb.ActorTemplateVersion{
		Metadata:      &ateapipb.ResourceMetadata{Name: "tmpl-up-v2"},
		ActorTemplate: &ateapipb.ObjectRef{Name: "tmpl-up"},
		Spec: validTemplateVersionSpec(func(s *ateapipb.ActorTemplateVersionSpec) {
			s.Containers[0].Image = "app@sha256:fff"
			s.SnapshotsConfig.OnResume = &ateapipb.OnResumeConfig{FromData: ateapipb.ResumeSource_RESUME_SOURCE_GOLDEN}
		}),
		Status: &ateapipb.ActorTemplateVersionStatus{
			State:           ateapipb.ActorTemplateVersionStatus_STATE_READY,
			ResolvedSandbox: gvisorSandbox,
			GoldenSnapshot:  &ateapipb.ObjectRef{Atespace: resources.GoldenActorAtespace, Name: "golden-v2"},
		},
	}); err != nil {
		t.Fatalf("CreateActorTemplateVersion(v2, store) failed: %v", err)
	}
	createWorkerPool(t, tc, ns, "pool1", map[string]string{poolLabelKey: ns})
	createWorkerPod(t, tc, ns, "worker-1", "node1", "pool1")

	actorRef := &ateapipb.ObjectRef{Atespace: testAtespace, Name: "id-golden"}
	if _, err := tc.client.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:             &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "id-golden"},
		ActorTemplate:        "tmpl-up",
		ActorTemplateVersion: "tmpl-up-v1",
	}}); err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}
	if _, err := tc.client.ResumeActor(ctx, &ateapipb.ResumeActorRequest{Actor: actorRef, Boot: true}); err != nil {
		t.Fatalf("ResumeActor(boot) failed: %v", err)
	}
	if _, err := tc.client.SuspendActor(ctx, &ateapipb.SuspendActorRequest{Actor: actorRef}); err != nil {
		t.Fatalf("SuspendActor failed: %v", err)
	}
	actorSnapshotURI := ""
	{
		suspended := getActor(t, tc, "id-golden")
		snapshot, err := tc.persistence.GetActorSnapshot(ctx, testAtespace, suspended.GetLatestSnapshot().GetName())
		if err != nil {
			t.Fatalf("GetActorSnapshot failed: %v", err)
		}
		actorSnapshotURI = snapshot.GetSnapshotUri()
	}

	tc.fakeAtelet.Reset()
	if _, err := tc.client.ResumeActor(ctx, &ateapipb.ResumeActorRequest{
		Actor:                actorRef,
		ActorTemplateVersion: "tmpl-up-v2",
	}); err != nil {
		t.Fatalf("ResumeActor(upgrade to v2) failed: %v", err)
	}
	restore := tc.fakeAtelet.lastRestoreRequest()
	if got := restore.GetScope(); got != ateletpb.SnapshotScope_SNAPSHOT_SCOPE_DATA_ON_GOLDEN {
		t.Errorf("upgrade restore scope = %v, want DATA_ON_GOLDEN", got)
	}
	if got := restore.GetGoldenSnapshotUri(); got != goldenURI {
		t.Errorf("upgrade restore golden URI = %q, want v2's %q", got, goldenURI)
	}
	if got := restore.GetExternalConfig().GetSnapshotUri(); got != actorSnapshotURI {
		t.Errorf("upgrade restore snapshot URI = %q, want the actor's %q", got, actorSnapshotURI)
	}
	if got := restore.GetSpec().GetContainers()[0].GetImage(); got != "app@sha256:fff" {
		t.Errorf("upgrade restore image = %q, want v2's app@sha256:fff", got)
	}
	if restore.GetSandboxAssets() != nil {
		t.Errorf("DATA_ON_GOLDEN restore carries sandbox_assets = %v, want none (golden manifest wins)", restore.GetSandboxAssets())
	}
}
