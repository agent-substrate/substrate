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
	"fmt"
	"sync"
	"testing"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/storetest"
	"github.com/agent-substrate/substrate/internal/objectstore/objectstoretest"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/internal/volume"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const testVolumeDriver = "substrate.io/mock"

// fakeSnapshotPlugin is a volume plugin that records the snapshot calls made
// against it, so a test can assert on what the workflow asked the storage
// system to do rather than on the workflow's own bookkeeping.
type fakeSnapshotPlugin struct {
	caps volume.Capabilities

	// failSnapshotOfVolume makes CreateSnapshot fail for one source volume ID,
	// standing in for a driver that cannot capture a particular volume.
	failSnapshotOfVolume string
	// readyToUse is what CreateSnapshot reports. A driver that finishes the
	// copy in the background returns false here.
	readyToUse bool
	// missingSnapshots are handles GetSnapshot reports as gone, standing in for
	// a snapshot deleted behind the control plane's back.
	missingSnapshots map[string]bool
	// failDeleteSnapshot makes DeleteSnapshot fail, standing in for a driver
	// that cannot release snapshots.
	failDeleteSnapshot bool

	mu      sync.Mutex
	created []string
	deleted []string
}

func newFakeSnapshotPlugin() *fakeSnapshotPlugin {
	return &fakeSnapshotPlugin{
		caps:       volume.Capabilities{CreateDeleteSnapshot: true, ListSnapshots: true},
		readyToUse: true,
	}
}

func (f *fakeSnapshotPlugin) DriverName(context.Context) (string, error) {
	return testVolumeDriver, nil
}

func (f *fakeSnapshotPlugin) CreateVolume(_ context.Context, req volume.CreateVolumeRequest) (volume.CreateVolumeResponse, error) {
	return volume.CreateVolumeResponse{
		VolumeID:                "vol-" + req.Name,
		VolumeContext:           req.Parameters,
		ContentSourceSnapshotID: req.SourceSnapshotID,
	}, nil
}

func (f *fakeSnapshotPlugin) DeleteVolume(context.Context, string) error         { return nil }
func (f *fakeSnapshotPlugin) AttachVolume(context.Context, string, string) error { return nil }
func (f *fakeSnapshotPlugin) DetachVolume(context.Context, string, string) error { return nil }

func (f *fakeSnapshotPlugin) CreateSnapshot(_ context.Context, req volume.CreateSnapshotRequest) (volume.Snapshot, error) {
	if req.SourceVolumeID == f.failSnapshotOfVolume {
		return volume.Snapshot{}, fmt.Errorf("simulated snapshot failure for %q", req.SourceVolumeID)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	id := "snap-" + req.Name
	f.created = append(f.created, id)
	return volume.Snapshot{
		SnapshotID:     id,
		SourceVolumeID: req.SourceVolumeID,
		ReadyToUse:     f.readyToUse,
		SizeBytes:      1024,
	}, nil
}

func (f *fakeSnapshotPlugin) GetSnapshot(_ context.Context, snapshotID string) (volume.Snapshot, bool, error) {
	if f.missingSnapshots[snapshotID] {
		return volume.Snapshot{}, false, nil
	}
	return volume.Snapshot{SnapshotID: snapshotID, ReadyToUse: f.readyToUse}, true, nil
}

func (f *fakeSnapshotPlugin) DeleteSnapshot(_ context.Context, snapshotID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failDeleteSnapshot {
		return fmt.Errorf("simulated delete failure for %q", snapshotID)
	}
	f.deleted = append(f.deleted, snapshotID)
	return nil
}

func (f *fakeSnapshotPlugin) ControllerCapabilities(context.Context) (volume.Capabilities, error) {
	return f.caps, nil
}

func (f *fakeSnapshotPlugin) snapshotsCreated() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.created...)
}

func (f *fakeSnapshotPlugin) snapshotsDeleted() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.deleted...)
}

var _ volume.VolumePluginControlPlane = (*fakeSnapshotPlugin)(nil)

// newVolumeTagWorkflow builds a tag workflow wired to a single fake driver.
func newVolumeTagWorkflow(persistence store.Interface, plugin *fakeSnapshotPlugin) (*ActorWorkflow, *objectstoretest.Fake) {
	objects := objectstoretest.New()
	return &ActorWorkflow{
		store:       persistence,
		objectStore: objects,
		pluginRegistry: &mockPluginRegistry{
			plugins: map[string]volume.VolumePluginControlPlane{testVolumeDriver: plugin},
		},
	}, objects
}

// seedTagSourceWithVolumes is seedTagSource plus provisioned external volumes,
// the state an actor is in when it can be tagged with its volumes.
func seedTagSourceWithVolumes(t *testing.T, ctx context.Context, persistence store.Interface, objects *objectstoretest.Fake, template *ateapipb.ActorTemplate, name string, volumeNames ...string) *ateapipb.Actor {
	t.Helper()
	actor, _ := seedTagSource(t, ctx, persistence, objects, template, name, "manifest.json")
	return mustUpdateActorStatus(t, ctx, persistence, actor, func(s *ateapipb.ActorStatus) {
		for _, volName := range volumeNames {
			s.ActorVolumes = append(s.ActorVolumes, &ateapipb.ExternalVolume{
				VolumeName:      volName,
				StorageVolumeId: "vol-" + volName,
				VolumeType:      testVolumeDriver,
				Status:          ateapipb.ExternalVolume_STATUS_CREATED,
			})
		}
	})
}

// TestTagActorSnapshot_CapturesVolumes verifies that a tag asked to include
// external volumes snapshots every one of the actor's external volumes and
// publishes a handle for each on its own snapshot.
func TestTagActorSnapshot_CapturesVolumes(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)
	template := seedSubstrateTemplate(t, ctx, persistence, "sub-tmpl")
	plugin := newFakeSnapshotPlugin()
	w, objects := newVolumeTagWorkflow(persistence, plugin)

	actor := seedTagSourceWithVolumes(t, ctx, persistence, objects, template, "actor-1", "data", "cache")

	tag, err := w.TagActorSnapshot(ctx, tagToCreate(resources.ActorRefFromActor(actor), "v1"), true)
	if err != nil {
		t.Fatalf("TagActorSnapshot: %v", err)
	}

	snapshot := tag.GetStatus().GetSnapshot()

	var gotVolumes []string
	for _, snap := range snapshot.GetVolumeSnapshots() {
		gotVolumes = append(gotVolumes, snap.GetVolumeName())
		if snap.GetStorageSnapshotId() == "" {
			t.Errorf("volume %q recorded no snapshot handle", snap.GetVolumeName())
		}
		if got, want := snap.GetVolumeType(), testVolumeDriver; got != want {
			t.Errorf("volume %q snapshot driver = %q, want %q", snap.GetVolumeName(), got, want)
		}
		if !snap.GetReadyToUse() {
			t.Errorf("volume %q snapshot ready_to_use = false, want the driver's true", snap.GetVolumeName())
		}
	}
	if diff := cmp.Diff([]string{"data", "cache"}, gotVolumes); diff != "" {
		t.Errorf("captured volumes mismatch (-want +got):\n%s", diff)
	}

	wantCreated := []string{
		"snap-" + tagVolumeSnapshotID(tag.GetMetadata().GetUid(), "data"),
		"snap-" + tagVolumeSnapshotID(tag.GetMetadata().GetUid(), "cache"),
	}
	if diff := cmp.Diff(wantCreated, plugin.snapshotsCreated()); diff != "" {
		t.Errorf("snapshots taken mismatch (-want +got):\n%s", diff)
	}
	if got := plugin.snapshotsDeleted(); len(got) != 0 {
		t.Errorf("snapshots deleted = %v, want none on a successful create", got)
	}
}

// TestTagActorSnapshot_VolumesNotRequested verifies volumes are captured only
// when asked for: an actor with volumes tagged without including them produces
// a tag with no volume snapshot entries.
func TestTagActorSnapshot_VolumesNotRequested(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)
	template := seedSubstrateTemplate(t, ctx, persistence, "sub-tmpl")
	plugin := newFakeSnapshotPlugin()
	w, objects := newVolumeTagWorkflow(persistence, plugin)

	actor := seedTagSourceWithVolumes(t, ctx, persistence, objects, template, "actor-1", "data")

	tag, err := w.TagActorSnapshot(ctx, tagToCreate(resources.ActorRefFromActor(actor), "v1"), false)
	if err != nil {
		t.Fatalf("TagActorSnapshot: %v", err)
	}

	if got := tag.GetStatus().GetSnapshot().GetVolumeSnapshots(); len(got) != 0 {
		t.Errorf("volume snapshots = %v, want none", got)
	}
	if got := plugin.snapshotsCreated(); len(got) != 0 {
		t.Errorf("snapshots taken = %v, want none", got)
	}
}

// TestTagActorSnapshot_DriverCannotSnapshot verifies the capability check
// rejects the tag before any snapshot is attempted, rather than discovering the
// driver's limits partway through.
func TestTagActorSnapshot_DriverCannotSnapshot(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)
	template := seedSubstrateTemplate(t, ctx, persistence, "sub-tmpl")
	plugin := newFakeSnapshotPlugin()
	plugin.caps = volume.Capabilities{}
	w, objects := newVolumeTagWorkflow(persistence, plugin)

	actor := seedTagSourceWithVolumes(t, ctx, persistence, objects, template, "actor-1", "data")

	_, err := w.TagActorSnapshot(ctx, tagToCreate(resources.ActorRefFromActor(actor), "v1"), true)
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("TagActorSnapshot error = %v, want FailedPrecondition", err)
	}
	if got := plugin.snapshotsCreated(); len(got) != 0 {
		t.Errorf("snapshots taken = %v, want none when the driver cannot snapshot", got)
	}
	// The failed create takes the name with it.
	if _, getErr := persistence.GetTag(ctx, resources.TagRef{Atespace: "team-a", Name: "v1"}); !errors.Is(getErr, store.ErrNotFound) {
		t.Errorf("GetTag = %v, want ErrNotFound: the failed create left its tag behind", getErr)
	}
}

// TestTagActorSnapshot_PartialVolumeFailureRollsBack verifies capture is
// all-or-nothing: a driver that fails on the second volume fails the create,
// and the rollback releases the first volume's snapshot and the copied
// objects and drops the tag, so a retry under the same name succeeds.
func TestTagActorSnapshot_PartialVolumeFailureRollsBack(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)
	template := seedSubstrateTemplate(t, ctx, persistence, "sub-tmpl")
	plugin := newFakeSnapshotPlugin()
	plugin.failSnapshotOfVolume = "vol-cache"
	w, objects := newVolumeTagWorkflow(persistence, plugin)

	actor := seedTagSourceWithVolumes(t, ctx, persistence, objects, template, "actor-1", "data", "cache")
	tagRef := resources.TagRef{Atespace: "team-a", Name: "v1"}
	before := objects.Objects()

	_, err := w.TagActorSnapshot(ctx, tagToCreate(resources.ActorRefFromActor(actor), "v1"), true)
	if err == nil {
		t.Fatal("TagActorSnapshot succeeded, want the failed volume to fail the whole create")
	}

	if got := plugin.snapshotsCreated(); len(got) != 1 {
		t.Fatalf("snapshots taken = %v, want only the first volume's", got)
	}
	if diff := cmp.Diff(plugin.snapshotsCreated(), plugin.snapshotsDeleted()); diff != "" {
		t.Errorf("snapshots created but not deleted (-created +deleted):\n%s", diff)
	}
	if diff := cmp.Diff(before, objects.Objects()); diff != "" {
		t.Errorf("objects after the failed create mismatch, want the copy collected (-want +got):\n%s", diff)
	}
	if _, getErr := persistence.GetTag(ctx, tagRef); !errors.Is(getErr, store.ErrNotFound) {
		t.Errorf("GetTag = %v, want ErrNotFound: the failed create left its tag behind", getErr)
	}

	plugin.failSnapshotOfVolume = ""
	if _, err := w.TagActorSnapshot(ctx, tagToCreate(resources.ActorRefFromActor(actor), "v1"), true); err != nil {
		t.Fatalf("TagActorSnapshot retry: %v", err)
	}
}

// TestTagActorSnapshot_PartialVolumeFailureRollbackFails verifies that when
// the rollback cannot release a volume snapshot, the tag is left pending and
// still names it, so a later delete can collect it instead of it leaking. The
// failed volume is recorded without a handle.
func TestTagActorSnapshot_PartialVolumeFailureRollbackFails(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)
	template := seedSubstrateTemplate(t, ctx, persistence, "sub-tmpl")
	plugin := newFakeSnapshotPlugin()
	plugin.failSnapshotOfVolume = "vol-cache"
	plugin.failDeleteSnapshot = true
	w, objects := newVolumeTagWorkflow(persistence, plugin)

	actor := seedTagSourceWithVolumes(t, ctx, persistence, objects, template, "actor-1", "data", "cache")
	tagRef := resources.TagRef{Atespace: "team-a", Name: "v1"}

	if _, err := w.TagActorSnapshot(ctx, tagToCreate(resources.ActorRefFromActor(actor), "v1"), true); err == nil {
		t.Fatal("TagActorSnapshot succeeded, want the failed volume to fail the whole create")
	}

	stored, getErr := persistence.GetTag(ctx, tagRef)
	if getErr != nil {
		t.Fatalf("GetTag: %v", getErr)
	}
	if got := stored.GetStatus().GetSnapshot().GetSnapshotUri(); got != "" {
		t.Errorf("tag was published with snapshot URI %q, want it left pending", got)
	}
	handles := map[string]string{}
	for _, snap := range stored.GetStatus().GetSnapshot().GetVolumeSnapshots() {
		handles[snap.GetVolumeName()] = snap.GetStorageSnapshotId()
	}
	want := map[string]string{
		"data":  "snap-" + tagVolumeSnapshotID(stored.GetMetadata().GetUid(), "data"),
		"cache": "",
	}
	if diff := cmp.Diff(want, handles); diff != "" {
		t.Errorf("recorded volume snapshot handles mismatch (-want +got):\n%s", diff)
	}

	// Once the driver recovers, deleting the tag collects what it stranded.
	plugin.failDeleteSnapshot = false
	if _, err := w.DeleteTag(ctx, tagRef, store.DeletePreconditions{}); err != nil {
		t.Fatalf("DeleteTag: %v", err)
	}
	if diff := cmp.Diff([]string{want["data"]}, plugin.snapshotsDeleted()); diff != "" {
		t.Errorf("released snapshots mismatch (-want +got):\n%s", diff)
	}
}

// TestDeleteTag_ReleasesVolumeSnapshots verifies a delete collects the volume
// snapshots the tag owns, not just its object-storage prefix.
func TestDeleteTag_ReleasesVolumeSnapshots(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)
	template := seedSubstrateTemplate(t, ctx, persistence, "sub-tmpl")
	plugin := newFakeSnapshotPlugin()
	w, objects := newVolumeTagWorkflow(persistence, plugin)

	actor := seedTagSourceWithVolumes(t, ctx, persistence, objects, template, "actor-1", "data")
	tag, err := w.TagActorSnapshot(ctx, tagToCreate(resources.ActorRefFromActor(actor), "v1"), true)
	if err != nil {
		t.Fatalf("TagActorSnapshot: %v", err)
	}
	created := plugin.snapshotsCreated()

	if _, err := w.DeleteTag(ctx, resources.TagRefFromTag(tag), store.DeletePreconditions{}); err != nil {
		t.Fatalf("DeleteTag: %v", err)
	}
	if diff := cmp.Diff(created, plugin.snapshotsDeleted()); diff != "" {
		t.Errorf("released snapshots mismatch (-want +got):\n%s", diff)
	}
}

// TestDeleteTag_ReleasesPendingVolumeSnapshots verifies a tag left pending by a
// create that died still has its snapshots collected: they are reachable
// through status.snapshot.volume_snapshots even though snapshot_uri is unset,
// and entries without a handle are skipped.
func TestDeleteTag_ReleasesPendingVolumeSnapshots(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)
	plugin := newFakeSnapshotPlugin()
	w, _ := newVolumeTagWorkflow(persistence, plugin)

	tagRef := resources.TagRef{Atespace: "team-a", Name: "pending"}
	pending := storetest.MustCreateTag(t, ctx, persistence, &ateapipb.Tag{
		Metadata:    &ateapipb.ResourceMetadata{Atespace: tagRef.Atespace, Name: tagRef.Name},
		Scope:       ateapipb.TagScope_TAG_SCOPE_ATESPACE,
		SourceActor: &ateapipb.ObjectRef{Atespace: "team-a", Name: "actor-1"},
		Status: &ateapipb.TagStatus{
			StorageLocation: testStorageLocation,
			Snapshot: &ateapipb.ExternalSnapshot{
				VolumeSnapshots: []*ateapipb.ExternalVolumeSnapshot{
					{VolumeName: "data", StorageSnapshotId: "snap-stranded", VolumeType: testVolumeDriver},
					{VolumeName: "cache", VolumeType: testVolumeDriver},
				},
			},
		},
	})

	if _, err := w.DeleteTag(ctx, resources.TagRefFromTag(pending), store.DeletePreconditions{}); err != nil {
		t.Fatalf("DeleteTag: %v", err)
	}
	if diff := cmp.Diff([]string{"snap-stranded"}, plugin.snapshotsDeleted()); diff != "" {
		t.Errorf("released snapshots mismatch (-want +got):\n%s", diff)
	}
}

// TestValidateTagVolumeCompatibility covers the check that stops an Actor from
// being seeded from a tag that cannot fill its volumes.
func TestValidateTagVolumeCompatibility(t *testing.T) {
	externalVolumeTemplate := func(names ...string) *ateapipb.ActorTemplate {
		tmpl := &ateapipb.ActorTemplate{}
		for _, name := range names {
			tmpl.Volumes = append(tmpl.Volumes, &ateapipb.Volume{
				Name:                   name,
				ExternalVolumeTemplate: &ateapipb.ExternalVolumeTemplate{StorageClassName: "standard"},
			})
		}
		return tmpl
	}
	// tagWith builds a tag whose volume snapshots are named by the keys of
	// handles, each with the handle it maps to ("" for one that did not finish).
	tagWith := func(handles map[string]string) *ateapipb.Tag {
		snapshot := &ateapipb.ExternalSnapshot{SnapshotUri: "gs://bucket/tag"}
		for name, id := range handles {
			snapshot.VolumeSnapshots = append(snapshot.VolumeSnapshots, &ateapipb.ExternalVolumeSnapshot{VolumeName: name, StorageSnapshotId: id})
		}
		return &ateapipb.Tag{
			Metadata: &ateapipb.ResourceMetadata{Atespace: "team-a", Name: "v1"},
			Status:   &ateapipb.TagStatus{Snapshot: snapshot},
		}
	}

	tests := []struct {
		name     string
		tag      *ateapipb.Tag
		template *ateapipb.ActorTemplate
		wantCode codes.Code
	}{
		{
			name:     "template declares no external volumes",
			tag:      tagWith(nil),
			template: &ateapipb.ActorTemplate{Volumes: []*ateapipb.Volume{{Name: "scratch"}}},
			wantCode: codes.OK,
		},
		{
			name:     "every volume captured",
			tag:      tagWith(map[string]string{"data": "snap-data", "cache": "snap-cache"}),
			template: externalVolumeTemplate("data", "cache"),
			wantCode: codes.OK,
		},
		{
			name:     "tag captured no volumes",
			tag:      tagWith(nil),
			template: externalVolumeTemplate("data"),
			wantCode: codes.FailedPrecondition,
		},
		{
			name:     "volume snapshot did not finish",
			tag:      tagWith(map[string]string{"data": "snap-data", "cache": ""}),
			template: externalVolumeTemplate("data", "cache"),
			wantCode: codes.FailedPrecondition,
		},
		{
			name:     "one volume missing from the tag",
			tag:      tagWith(map[string]string{"data": "snap-data"}),
			template: externalVolumeTemplate("data", "cache"),
			wantCode: codes.FailedPrecondition,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateTagVolumeCompatibility(tt.tag, tt.template)
			if got := status.Code(err); got != tt.wantCode {
				t.Errorf("validateTagVolumeCompatibility() = %v (code %v), want code %v", err, got, tt.wantCode)
			}
		})
	}
}

// TestResolveVolumeSource covers picking the snapshot a restored volume is
// seeded from, and the readiness check deferred here from tag creation.
func TestResolveVolumeSource(t *testing.T) {
	snapshots := []*ateapipb.ExternalVolumeSnapshot{
		{VolumeName: "data", StorageSnapshotId: "snap-data", VolumeType: testVolumeDriver, ReadyToUse: true},
	}

	t.Run("volume with no snapshot is provisioned empty", func(t *testing.T) {
		got, err := resolveVolumeSource(context.Background(), newFakeSnapshotPlugin(), snapshots, "cache", testVolumeDriver)
		if err != nil {
			t.Fatalf("resolveVolumeSource: %v", err)
		}
		if got != "" {
			t.Errorf("source snapshot = %q, want empty", got)
		}
	})

	t.Run("volume with a snapshot is restored from it", func(t *testing.T) {
		got, err := resolveVolumeSource(context.Background(), newFakeSnapshotPlugin(), snapshots, "data", testVolumeDriver)
		if err != nil {
			t.Fatalf("resolveVolumeSource: %v", err)
		}
		if want := "snap-data"; got != want {
			t.Errorf("source snapshot = %q, want %q", got, want)
		}
	})

	t.Run("volume whose snapshot did not finish is rejected", func(t *testing.T) {
		unfinished := []*ateapipb.ExternalVolumeSnapshot{{VolumeName: "data", VolumeType: testVolumeDriver}}
		_, err := resolveVolumeSource(context.Background(), newFakeSnapshotPlugin(), unfinished, "data", testVolumeDriver)
		if status.Code(err) != codes.FailedPrecondition {
			t.Errorf("resolveVolumeSource() = %v, want FailedPrecondition", err)
		}
	})

	t.Run("snapshot from another driver is rejected", func(t *testing.T) {
		_, err := resolveVolumeSource(context.Background(), newFakeSnapshotPlugin(), snapshots, "data", "substrate.io/other")
		if status.Code(err) != codes.FailedPrecondition {
			t.Errorf("resolveVolumeSource() = %v, want FailedPrecondition", err)
		}
	})

	t.Run("snapshot still copying is rejected", func(t *testing.T) {
		plugin := newFakeSnapshotPlugin()
		plugin.readyToUse = false
		_, err := resolveVolumeSource(context.Background(), plugin, snapshots, "data", testVolumeDriver)
		if status.Code(err) != codes.FailedPrecondition {
			t.Errorf("resolveVolumeSource() = %v, want FailedPrecondition", err)
		}
	})

	t.Run("snapshot deleted behind our back is rejected", func(t *testing.T) {
		plugin := newFakeSnapshotPlugin()
		plugin.missingSnapshots = map[string]bool{"snap-data": true}
		_, err := resolveVolumeSource(context.Background(), plugin, snapshots, "data", testVolumeDriver)
		if status.Code(err) != codes.FailedPrecondition {
			t.Errorf("resolveVolumeSource() = %v, want FailedPrecondition", err)
		}
	})
}
