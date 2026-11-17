// Copyright 2026 The Agent Substrate Authors
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

package functionaltest

import (
	"context"
	"sync"
	"testing"

	"github.com/google/go-cmp/cmp"
	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/internal/preview"
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/internal/volume"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// restoringPlugin is the mock volume plugin, recording the snapshot each
// volume was created from, keyed by the volume's name in the storage system.
// It reports snapshots as not ready until setReady, and fails a restore from a
// snapshot it deleted with NotFound, as a CSI driver does.
type restoringPlugin struct {
	*volume.MockVolumePlugin

	mu       sync.Mutex
	notReady bool
	sources  map[string]string
	deleted  map[string]bool
}

func (p *restoringPlugin) CreateVolume(ctx context.Context, req volume.CreateVolumeRequest) (volume.CreateVolumeResponse, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.deleted[req.SourceSnapshotID] {
		return volume.CreateVolumeResponse{}, status.Errorf(codes.NotFound, "snapshot %q not found", req.SourceSnapshotID)
	}
	p.sources[req.Name] = req.SourceSnapshotID
	return p.MockVolumePlugin.CreateVolume(ctx, req)
}

func (p *restoringPlugin) CreateSnapshot(ctx context.Context, req volume.CreateSnapshotRequest) (volume.Snapshot, error) {
	snap, err := p.MockVolumePlugin.CreateSnapshot(ctx, req)
	p.mu.Lock()
	defer p.mu.Unlock()
	snap.ReadyToUse = !p.notReady
	return snap, err
}

func (p *restoringPlugin) GetSnapshot(_ context.Context, snapshotID string) (volume.Snapshot, bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.deleted[snapshotID] {
		return volume.Snapshot{}, false, nil
	}
	return volume.Snapshot{SnapshotID: snapshotID, ReadyToUse: !p.notReady}, true, nil
}

func (p *restoringPlugin) DeleteSnapshot(_ context.Context, snapshotID string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.deleted[snapshotID] = true
	return nil
}

// setReady sets whether the storage system reports snapshots ready.
func (p *restoringPlugin) setReady(ready bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.notReady = !ready
}

// restoreSources returns the snapshot each named volume of actor was created
// from, "" for one provisioned empty. It fails the test if a volume was never
// created.
func (p *restoringPlugin) restoreSources(t *testing.T, actor *ateapi.Actor, volNames ...string) map[string]string {
	t.Helper()
	p.mu.Lock()
	defer p.mu.Unlock()
	got := map[string]string{}
	for _, name := range volNames {
		src, ok := p.sources["substrate-"+actor.GetMetadata().GetUid()+"-"+name]
		if !ok {
			t.Fatalf("volume %q of Actor %s was never created", name, actor.GetMetadata().GetName())
		}
		got[name] = src
	}
	return got
}

// setupTagVolumesTest starts a server whose mock volume plugin records restore
// sources, and creates a template declaring the external volumes "data" and
// "cache" plus a worker to run its Actors on.
func setupTagVolumesTest(t *testing.T, ns string) (*testContext, *restoringPlugin, string) {
	t.Helper()
	preview.InitForTest(t, "ExternalVolumes")
	plugin := &restoringPlugin{MockVolumePlugin: volume.NewMockVolumePlugin(), sources: map[string]string{}, deleted: map[string]bool{}}
	tc := setupTestWithVolumePlugins(t, ns, map[string]volume.VolumePluginControlPlane{
		"substrate.io/mock": plugin,
	})
	var volumes []*ateapi.Volume
	var mounts []*ateapi.VolumeMount
	for _, name := range []string{"data", "cache"} {
		volumes = append(volumes, &ateapi.Volume{
			Name:     name,
			External: &ateapi.ExternalVolumeSource{Template: &ateapi.ExternalVolumeTemplate{StorageClassName: "standard", CapacityBytes: 1 << 30}},
		})
		mounts = append(mounts, &ateapi.VolumeMount{Name: name, MountPath: "/mnt/" + name})
	}
	createTemplateWithVolumes(t, tc, ns, volumes, mounts)
	workerName := createWorkerPod(t, tc, ns, "worker-1", "node1", "pool1")
	return tc, plugin, workerName
}

// createVolumeTag tags the suspended Actor "source" as tagName, capturing
// selection.
func createVolumeTag(t *testing.T, tc *testContext, tagName string, selection *ateapi.ExternalVolumeSelection) *ateapi.Tag {
	t.Helper()
	tag, err := tc.client.CreateTag(context.Background(), &ateapi.CreateTagRequest{
		Tag: &ateapi.Tag{
			Metadata:    &ateapi.ResourceMetadata{Atespace: testAtespace, Name: tagName},
			Scope:       ateapi.TagScope_TAG_SCOPE_ATESPACE,
			SourceActor: &ateapi.ObjectRef{Atespace: testAtespace, Name: "source"},
		},
		VolumeSelection: selection,
	})
	if err != nil {
		t.Fatalf("CreateTag(%s) failed: %v", tagName, err)
	}
	return tag
}

// createFromTag creates the Actor ref from the tag tagRef, with the template
// "tmpl1".
func createFromTag(tc *testContext, ref, tagRef *ateapi.ObjectRef) (*ateapi.Actor, error) {
	return tc.client.CreateActor(context.Background(), &ateapi.CreateActorRequest{Actor: &ateapi.Actor{
		Metadata:      &ateapi.ResourceMetadata{Atespace: ref.GetAtespace(), Name: ref.GetName()},
		ActorTemplate: &ateapi.ObjectRef{Atespace: testAtespace, Name: "tmpl1"},
		SourceTag:     tagRef,
	}})
}

// cloneAndResume creates the Actor ref from tagRef and resumes it, which
// provisions its volumes.
func cloneAndResume(t *testing.T, tc *testContext, workerName string, ref, tagRef *ateapi.ObjectRef) *ateapi.Actor {
	t.Helper()
	if _, err := createFromTag(tc, ref, tagRef); err != nil {
		t.Fatalf("CreateActor(%s) from tag %s failed: %v", ref.GetName(), tagRef.GetName(), err)
	}
	waitForWorkerAvailable(t, tc, workerName)
	resumed, err := tc.client.ResumeActor(context.Background(), &ateapi.ResumeActorRequest{Actor: ref})
	if err != nil {
		t.Fatalf("ResumeActor(%s) failed: %v", ref.GetName(), err)
	}
	return resumed.GetActor()
}

// tagVolumeHandles returns the snapshot handle of each ExternalVolumeSnapshot
// the tag owns, by source volume name, read through the API.
func tagVolumeHandles(t *testing.T, tc *testContext, tag *ateapi.Tag) map[string]string {
	t.Helper()
	resp, err := tc.client.ListExternalVolumeSnapshots(context.Background(), &ateapi.ListExternalVolumeSnapshotsRequest{Atespace: tag.GetMetadata().GetAtespace()})
	if err != nil {
		t.Fatalf("ListExternalVolumeSnapshots failed: %v", err)
	}
	handles := map[string]string{}
	for _, snap := range resp.GetExternalVolumeSnapshots() {
		origin := snap.GetOrigin().GetTagCapture()
		if origin.GetTag().GetName() != tag.GetMetadata().GetName() || origin.GetTagUid() != tag.GetMetadata().GetUid() {
			continue
		}
		if !snap.GetStatus().GetReadyToUse() {
			t.Errorf("snapshot of volume %q has status %v, want it ready to use", origin.GetSourceVolumeName(), snap.GetStatus())
		}
		handles[origin.GetSourceVolumeName()] = snap.GetStatus().GetSnapshotId()
	}
	return handles
}

// tagVolumesReady returns whether each ExternalVolumeSnapshot the tag owns is
// ready to use, by source volume name, read through the API.
func tagVolumesReady(t *testing.T, tc *testContext, tag *ateapi.Tag) map[string]bool {
	t.Helper()
	resp, err := tc.client.ListExternalVolumeSnapshots(context.Background(), &ateapi.ListExternalVolumeSnapshotsRequest{Atespace: tag.GetMetadata().GetAtespace()})
	if err != nil {
		t.Fatalf("ListExternalVolumeSnapshots failed: %v", err)
	}
	ready := map[string]bool{}
	for _, snap := range resp.GetExternalVolumeSnapshots() {
		if origin := snap.GetOrigin().GetTagCapture(); origin.GetTagUid() == tag.GetMetadata().GetUid() {
			ready[origin.GetSourceVolumeName()] = snap.GetStatus().GetReadyToUse()
		}
	}
	return ready
}

// TestCreateTag_CapturesVolumes verifies that a tag taken with external volumes
// snapshots each of them and records an ExternalVolumeSnapshot with the handle
// for each, that an Actor created from it restores each volume from its
// snapshot on its first resume, and that the tag can then be deleted along
// with its snapshots.
func TestCreateTag_CapturesVolumes(t *testing.T) {
	ns := namespaceForTest("ns-tag-capture-volumes")
	tc, plugin, workerName := setupTagVolumesTest(t, ns)
	defer tc.cleanup()
	ctx := context.Background()

	suspendActorForTest(t, tc, workerName, "source")
	tag := createVolumeTag(t, tc, "with-volumes", &ateapi.ExternalVolumeSelection{})
	handles := tagVolumeHandles(t, tc, tag)
	if len(handles) != 2 || handles["data"] == "" || handles["cache"] == "" {
		t.Fatalf("tag volume snapshot handles = %v, want one for each of data and cache", handles)
	}
	if got, want := tag.GetStatus().GetSnapshot().GetDurableSnapshot().GetState(), ateapi.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_COMPLETED; got != want {
		t.Errorf("tag snapshot status = %v, want %v", got, want)
	}

	tagRef := &ateapi.ObjectRef{Atespace: testAtespace, Name: "with-volumes"}
	clone := cloneAndResume(t, tc, workerName, &ateapi.ObjectRef{Atespace: testAtespace, Name: "clone"}, tagRef)
	if diff := cmp.Diff(handles, plugin.restoreSources(t, clone, "data", "cache")); diff != "" {
		t.Errorf("restore sources mismatch (-tag handles +restored from):\n%s", diff)
	}
	for _, vol := range clone.GetStatus().GetExternalVolumes().GetVolumes() {
		if vol.GetSourceSnapshotId() != "" {
			t.Errorf("provisioned volume %q keeps source_snapshot_id %q", vol.GetName(), vol.GetSourceSnapshotId())
		}
	}

	if _, err := tc.client.DeleteTag(ctx, &ateapi.DeleteTagRequest{Tag: tagRef}); err != nil {
		t.Fatalf("DeleteTag failed: %v", err)
	}
	_, err := tc.client.GetTag(ctx, &ateapi.GetTagRequest{Tag: tagRef})
	if status.Code(err) != codes.NotFound {
		t.Errorf("GetTag() after DeleteTag = %v, want NotFound", err)
	}
	if got := tagVolumeHandles(t, tc, tag); len(got) != 0 {
		t.Errorf("tag volume snapshots after DeleteTag = %v, want none", got)
	}
}

// TestCreateTag_CapturesNamedVolumes verifies that a tag taken with a named
// subset of the volumes captures only those, and that an Actor created from it
// restores them and provisions the rest empty.
func TestCreateTag_CapturesNamedVolumes(t *testing.T) {
	ns := namespaceForTest("ns-tag-capture-named-volumes")
	tc, plugin, workerName := setupTagVolumesTest(t, ns)
	defer tc.cleanup()

	suspendActorForTest(t, tc, workerName, "source")
	tag := createVolumeTag(t, tc, "data-only", &ateapi.ExternalVolumeSelection{Names: []string{"data"}})
	handles := tagVolumeHandles(t, tc, tag)
	if len(handles) != 1 || handles["data"] == "" {
		t.Fatalf("tag volume snapshot handles = %v, want one for data only", handles)
	}

	clone := cloneAndResume(t, tc, workerName, &ateapi.ObjectRef{Atespace: testAtespace, Name: "clone"}, &ateapi.ObjectRef{Atespace: testAtespace, Name: "data-only"})
	want := map[string]string{"data": handles["data"], "cache": ""}
	if diff := cmp.Diff(want, plugin.restoreSources(t, clone, "data", "cache")); diff != "" {
		t.Errorf("restore sources mismatch (-want +got):\n%s", diff)
	}
}

// TestCreateActor_TagVolumeSnapshotNotReady verifies that CreateActor refuses
// a tag whose volume snapshot the storage system has not finished, and accepts
// it once the snapshot is ready, recording it as ready to use.
func TestCreateActor_TagVolumeSnapshotNotReady(t *testing.T) {
	ns := namespaceForTest("ns-tag-volume-not-ready")
	tc, plugin, workerName := setupTagVolumesTest(t, ns)
	defer tc.cleanup()
	plugin.setReady(false)

	suspendActorForTest(t, tc, workerName, "source")
	tag := createVolumeTag(t, tc, "copying", &ateapi.ExternalVolumeSelection{Names: []string{"data"}})
	if got := tagVolumesReady(t, tc, tag); got["data"] {
		t.Fatalf("snapshots ready after CreateTag = %v, want data not ready", got)
	}

	tagRef := &ateapi.ObjectRef{Atespace: testAtespace, Name: "copying"}
	cloneRef := &ateapi.ObjectRef{Atespace: testAtespace, Name: "clone"}
	_, err := createFromTag(tc, cloneRef, tagRef)
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("CreateActor from a tag whose snapshot is copying = %v, want FailedPrecondition", err)
	}

	plugin.setReady(true)
	clone := cloneAndResume(t, tc, workerName, cloneRef, tagRef)
	if got := tagVolumesReady(t, tc, tag); !got["data"] {
		t.Errorf("snapshots ready after CreateActor = %v, want data ready", got)
	}
	if got := plugin.restoreSources(t, clone, "data"); got["data"] == "" {
		t.Errorf("restore sources = %v, want data restored", got)
	}
}

// TestResumeActor_TagDeletedBeforeFirstResume verifies that an Actor created
// from a tag that is deleted before its first resume fails that resume with
// FailedPrecondition, instead of coming up with an empty volume.
func TestResumeActor_TagDeletedBeforeFirstResume(t *testing.T) {
	ns := namespaceForTest("ns-tag-deleted-before-resume")
	tc, _, workerName := setupTagVolumesTest(t, ns)
	defer tc.cleanup()
	ctx := context.Background()

	suspendActorForTest(t, tc, workerName, "source")
	createVolumeTag(t, tc, "short-lived", &ateapi.ExternalVolumeSelection{})
	tagRef := &ateapi.ObjectRef{Atespace: testAtespace, Name: "short-lived"}
	cloneRef := &ateapi.ObjectRef{Atespace: testAtespace, Name: "clone"}
	if _, err := createFromTag(tc, cloneRef, tagRef); err != nil {
		t.Fatalf("CreateActor from the tag failed: %v", err)
	}
	if _, err := tc.client.DeleteTag(ctx, &ateapi.DeleteTagRequest{Tag: tagRef}); err != nil {
		t.Fatalf("DeleteTag failed: %v", err)
	}

	waitForWorkerAvailable(t, tc, workerName)
	_, err := tc.client.ResumeActor(ctx, &ateapi.ResumeActorRequest{Actor: cloneRef})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("ResumeActor after the tag was deleted = %v, want FailedPrecondition", err)
	}
}

// TestCreateActor_PublishedTagRestoresVolumes verifies that an Actor in
// another atespace, created from a published tag, restores the tag's volumes.
func TestCreateActor_PublishedTagRestoresVolumes(t *testing.T) {
	ns := namespaceForTest("ns-published-tag-volumes")
	tc, plugin, workerName := setupTagVolumesTest(t, ns)
	defer tc.cleanup()
	ctx := context.Background()

	suspendActorForTest(t, tc, workerName, "source")
	tag := createVolumeTag(t, tc, "published", &ateapi.ExternalVolumeSelection{})
	handles := tagVolumeHandles(t, tc, tag)
	tag.Scope = ateapi.TagScope_TAG_SCOPE_PUBLISHED
	if _, err := tc.client.UpdateTag(ctx, &ateapi.UpdateTagRequest{Tag: tag}); err != nil {
		t.Fatalf("UpdateTag to published failed: %v", err)
	}

	createAtespace(t, tc, "team-b")
	clone := cloneAndResume(t, tc, workerName, &ateapi.ObjectRef{Atespace: "team-b", Name: "clone"}, &ateapi.ObjectRef{Atespace: testAtespace, Name: "published"})
	if diff := cmp.Diff(handles, plugin.restoreSources(t, clone, "data", "cache")); diff != "" {
		t.Errorf("restore sources mismatch (-tag handles +restored from):\n%s", diff)
	}
}

// TestCreateTag_UnknownVolumeName verifies that naming a volume the source
// Actor does not have fails the create and leaves no tag behind.
func TestCreateTag_UnknownVolumeName(t *testing.T) {
	ns := namespaceForTest("ns-tag-unknown-volume")
	tc, _, workerName := setupTagVolumesTest(t, ns)
	defer tc.cleanup()
	ctx := context.Background()

	suspendActorForTest(t, tc, workerName, "source")
	_, err := tc.client.CreateTag(ctx, &ateapi.CreateTagRequest{
		Tag: &ateapi.Tag{
			Metadata:    &ateapi.ResourceMetadata{Atespace: testAtespace, Name: "typo"},
			Scope:       ateapi.TagScope_TAG_SCOPE_ATESPACE,
			SourceActor: &ateapi.ObjectRef{Atespace: testAtespace, Name: "source"},
		},
		VolumeSelection: &ateapi.ExternalVolumeSelection{Names: []string{"data", "dta"}},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("CreateTag() = %v, want InvalidArgument", err)
	}
	_, err = tc.client.GetTag(ctx, &ateapi.GetTagRequest{Tag: &ateapi.ObjectRef{Atespace: testAtespace, Name: "typo"}})
	if status.Code(err) != codes.NotFound {
		t.Errorf("GetTag() after a failed create = %v, want NotFound", err)
	}
}

// seedTagWithVolumeSnapshot seeds a finished tag, as seedTag does, owning a
// ready ExternalVolumeSnapshot of volName taken by driver.
func seedTagWithVolumeSnapshot(t *testing.T, tc *testContext, actorName, tagName string, template *ateapi.ActorTemplate, volName, driver string) *ateapi.Tag {
	t.Helper()
	ctx := context.Background()
	// Snapshots can only be recorded for a pending tag.
	tag := seedTag(t, tc, actorName, tagName, func(tag *ateapi.Tag) {
		tag.Status.ActorTemplateUid = template.GetMetadata().GetUid()
		tag.Status.Snapshot.DurableSnapshot.State = ateapi.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_PENDING
	})
	tagRef := resources.TagRefFromTag(tag)
	snap, err := tc.persistence.CreateExternalVolumeSnapshot(ctx, &ateapi.ExternalVolumeSnapshot{
		Metadata:   &ateapi.ResourceMetadata{Atespace: testAtespace, Name: "snap-" + tagName + "-" + volName},
		DriverName: driver,
		Origin: &ateapi.ExternalVolumeSnapshotOrigin{TagCapture: &ateapi.TagVolumeSnapshotOrigin{
			Tag:              tagRef.ToObjectRef(),
			TagUid:           tag.GetMetadata().GetUid(),
			SourceVolumeName: volName,
			SourceVolumeId:   "vol-" + volName,
		}},
		Status: &ateapi.ExternalVolumeSnapshotStatus{},
	})
	if err != nil {
		t.Fatalf("CreateExternalVolumeSnapshot: %v", err)
	}
	if _, err := tc.persistence.UpdateExternalVolumeSnapshot(ctx, resources.ExternalVolumeSnapshotRefFromExternalVolumeSnapshot(snap), store.PreconditionFrom(snap), func(toUpdate *ateapi.ExternalVolumeSnapshot) error {
		toUpdate.Status = &ateapi.ExternalVolumeSnapshotStatus{SnapshotId: "handle-" + volName, ReadyToUse: true}
		return nil
	}); err != nil {
		t.Fatalf("UpdateExternalVolumeSnapshot: %v", err)
	}
	tag, err = tc.persistence.UpdateTag(ctx, tagRef, store.PreconditionFrom(tag), func(toUpdate *ateapi.Tag) error {
		toUpdate.Status.Snapshot.DurableSnapshot.State = ateapi.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_COMPLETED
		return nil
	})
	if err != nil {
		t.Fatalf("finalizing tag %s: %v", tagName, err)
	}
	return tag
}
