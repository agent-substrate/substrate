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

package controlapi

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/cmd/ateapi/internal/store/storetest"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/objectstore/objectstoretest"
	"go.ate.dev/substrate/internal/objectstoreplugin/objectstoreplugintest"
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/internal/volume"
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
	// failSnapshotCode is the gRPC code that failure carries. codes.OK leaves
	// the error without a status.
	failSnapshotCode codes.Code
	// goneVolumes are source volume IDs CreateSnapshot reports as NotFound,
	// standing in for a source volume deleted since the tag was created.
	goneVolumes map[string]bool
	// readyToUse is what CreateSnapshot reports. A driver that finishes the
	// copy in the background returns false here.
	readyToUse bool
	// failDeleteSnapshot makes DeleteSnapshot fail, standing in for a driver
	// that cannot release snapshots.
	failDeleteSnapshot bool
	// rendezvous, when set, makes each CreateSnapshot wait until that many
	// calls are in flight at once. A caller that issues the snapshots one at
	// a time blocks until the test times out.
	rendezvous int

	mu         sync.Mutex
	created    []string
	deleted    []string
	arrived    int
	allArrived chan struct{}
}

func newFakeSnapshotPlugin() *fakeSnapshotPlugin {
	return &fakeSnapshotPlugin{
		caps:       volume.Capabilities{CreateDeleteSnapshot: true, ListSnapshots: true},
		readyToUse: true,
		allArrived: make(chan struct{}),
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

func (f *fakeSnapshotPlugin) DeleteVolume(context.Context, string) error { return nil }
func (f *fakeSnapshotPlugin) AttachVolume(context.Context, volume.AttachVolumeRequest) (volume.AttachVolumeResponse, error) {
	return volume.AttachVolumeResponse{}, nil
}
func (f *fakeSnapshotPlugin) DetachVolume(context.Context, string, string) error { return nil }

func (f *fakeSnapshotPlugin) CreateSnapshot(ctx context.Context, req volume.CreateSnapshotRequest) (volume.Snapshot, error) {
	if f.rendezvous > 0 {
		f.mu.Lock()
		f.arrived++
		if f.arrived == f.rendezvous {
			close(f.allArrived)
		}
		f.mu.Unlock()
		select {
		case <-f.allArrived:
		case <-ctx.Done():
			return volume.Snapshot{}, ctx.Err()
		}
	}
	if req.SourceVolumeID == f.failSnapshotOfVolume {
		if f.failSnapshotCode != codes.OK {
			return volume.Snapshot{}, fmt.Errorf("CSI CreateSnapshot failed: %w", status.Errorf(f.failSnapshotCode, "simulated snapshot failure for %q", req.SourceVolumeID))
		}
		return volume.Snapshot{}, fmt.Errorf("simulated snapshot failure for %q", req.SourceVolumeID)
	}
	if f.goneVolumes[req.SourceVolumeID] {
		return volume.Snapshot{}, fmt.Errorf("CSI CreateSnapshot failed: %w", status.Errorf(codes.NotFound, "source volume %q not found", req.SourceVolumeID))
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
		store:             persistence,
		objectStorePlugin: objectstoreplugintest.ControlClient(objects),
		pluginRegistry: &mockPluginRegistry{
			plugins: map[string]volume.VolumePluginControlPlane{testVolumeDriver: plugin},
		},
	}, objects
}

// seedTagSourceWithVolumes is seedTagSource plus provisioned external volumes,
// the state an actor is in when it can be tagged with its volumes.
func seedTagSourceWithVolumes(t *testing.T, ctx context.Context, persistence store.Interface, objects *objectstoretest.Fake, template *ateapi.ActorTemplate, name string, volumeNames ...string) *ateapi.Actor {
	t.Helper()
	actor, _ := seedTagSource(t, ctx, persistence, objects, template, name, "manifest.json")
	return mustUpdateActorStatus(t, ctx, persistence, actor, func(s *ateapi.ActorStatus) {
		s.ExternalVolumes = &ateapi.ExternalVolumes{}
		for _, volName := range volumeNames {
			s.ExternalVolumes.Volumes = append(s.ExternalVolumes.Volumes, &ateapi.ExternalVolumeStatus{
				Name:            volName,
				StorageVolumeId: "vol-" + volName,
				VolumeType:      testVolumeDriver,
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

	tag, err := w.TagActorSnapshot(ctx, tagToCreate(resources.ActorRefFromActor(actor), "v1"), &ateapi.ExternalVolumeSelection{})
	if err != nil {
		t.Fatalf("TagActorSnapshot: %v", err)
	}

	snapshot := tag.GetStatus().GetSnapshot()

	var gotVolumes []string
	for _, snap := range snapshot.GetVolumeSnapshots() {
		gotVolumes = append(gotVolumes, snap.GetSourceVolumeName())
		if snap.GetStorageSnapshotId() == "" {
			t.Errorf("volume %q recorded no snapshot handle", snap.GetSourceVolumeName())
		}
		if got, want := snap.GetSourceVolumeId(), "vol-"+snap.GetSourceVolumeName(); got != want {
			t.Errorf("volume %q source volume id = %q, want %q", snap.GetSourceVolumeName(), got, want)
		}
		if got, want := snap.GetVolumeType(), testVolumeDriver; got != want {
			t.Errorf("volume %q snapshot driver = %q, want %q", snap.GetSourceVolumeName(), got, want)
		}
		if !snap.GetReadyToUse() {
			t.Errorf("volume %q snapshot ready_to_use = false, want the driver's true", snap.GetSourceVolumeName())
		}
	}
	if diff := cmp.Diff([]string{"data", "cache"}, gotVolumes); diff != "" {
		t.Errorf("captured volumes mismatch (-want +got):\n%s", diff)
	}
	if got, want := tag.GetStatus().GetSnapshot().GetDurableSnapshot().GetState(), ateapi.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_COMPLETED; got != want {
		t.Errorf("tag snapshot status = %v, want %v", got, want)
	}

	wantCreated := []string{
		"snap-" + tagVolumeSnapshotID(tag.GetMetadata().GetUid(), "data"),
		"snap-" + tagVolumeSnapshotID(tag.GetMetadata().GetUid(), "cache"),
	}
	if diff := cmp.Diff(wantCreated, plugin.snapshotsCreated(), cmpopts.SortSlices(func(a, b string) bool { return a < b })); diff != "" {
		t.Errorf("snapshots taken mismatch (-want +got):\n%s", diff)
	}
	if got := plugin.snapshotsDeleted(); len(got) != 0 {
		t.Errorf("snapshots deleted = %v, want none on a successful create", got)
	}
}

// TestTagActorSnapshot_SnapshotNotReadyCompletes verifies a tag whose volume
// snapshot the driver is still processing is still completed, recording
// ready_to_use false: creation does not wait for the copy.
func TestTagActorSnapshot_SnapshotNotReadyCompletes(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)
	template := seedSubstrateTemplate(t, ctx, persistence, "sub-tmpl")
	plugin := newFakeSnapshotPlugin()
	plugin.readyToUse = false
	w, objects := newVolumeTagWorkflow(persistence, plugin)

	actor := seedTagSourceWithVolumes(t, ctx, persistence, objects, template, "actor-1", "data")

	tag, err := w.TagActorSnapshot(ctx, tagToCreate(resources.ActorRefFromActor(actor), "v1"), &ateapi.ExternalVolumeSelection{})
	if err != nil {
		t.Fatalf("TagActorSnapshot: %v", err)
	}
	if got, want := tag.GetStatus().GetSnapshot().GetDurableSnapshot().GetState(), ateapi.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_COMPLETED; got != want {
		t.Errorf("tag snapshot status = %v, want %v", got, want)
	}
	for _, snap := range tag.GetStatus().GetSnapshot().GetVolumeSnapshots() {
		if snap.GetReadyToUse() {
			t.Errorf("volume %q snapshot ready_to_use = true, want the driver's false", snap.GetSourceVolumeName())
		}
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

	tag, err := w.TagActorSnapshot(ctx, tagToCreate(resources.ActorRefFromActor(actor), "v1"), nil)
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

// TestTagActorSnapshot_CapturesNamedVolumes verifies that naming volumes
// captures only those, in the actor's order, and leaves the rest off the tag.
func TestTagActorSnapshot_CapturesNamedVolumes(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)
	template := seedSubstrateTemplate(t, ctx, persistence, "sub-tmpl")
	plugin := newFakeSnapshotPlugin()
	w, objects := newVolumeTagWorkflow(persistence, plugin)

	actor := seedTagSourceWithVolumes(t, ctx, persistence, objects, template, "actor-1", "data", "cache", "scratch")

	tag, err := w.TagActorSnapshot(ctx, tagToCreate(resources.ActorRefFromActor(actor), "v1"), &ateapi.ExternalVolumeSelection{Names: []string{"scratch", "data"}})
	if err != nil {
		t.Fatalf("TagActorSnapshot: %v", err)
	}

	var gotVolumes []string
	for _, snap := range tag.GetStatus().GetSnapshot().GetVolumeSnapshots() {
		gotVolumes = append(gotVolumes, snap.GetSourceVolumeName())
		if snap.GetStorageSnapshotId() == "" {
			t.Errorf("volume %q recorded no snapshot handle", snap.GetSourceVolumeName())
		}
	}
	if diff := cmp.Diff([]string{"data", "scratch"}, gotVolumes); diff != "" {
		t.Errorf("captured volumes mismatch (-want +got):\n%s", diff)
	}
	wantCreated := []string{
		"snap-" + tagVolumeSnapshotID(tag.GetMetadata().GetUid(), "data"),
		"snap-" + tagVolumeSnapshotID(tag.GetMetadata().GetUid(), "scratch"),
	}
	if diff := cmp.Diff(wantCreated, plugin.snapshotsCreated(), cmpopts.SortSlices(func(a, b string) bool { return a < b })); diff != "" {
		t.Errorf("snapshots taken mismatch (-want +got):\n%s", diff)
	}
}

// TestTagActorSnapshot_UnknownVolumeName verifies a name the actor has no
// external volume for fails the call before the tag is reserved, so nothing is
// copied or snapshotted and the name stays free.
func TestTagActorSnapshot_UnknownVolumeName(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)
	template := seedSubstrateTemplate(t, ctx, persistence, "sub-tmpl")
	plugin := newFakeSnapshotPlugin()
	w, objects := newVolumeTagWorkflow(persistence, plugin)

	actor := seedTagSourceWithVolumes(t, ctx, persistence, objects, template, "actor-1", "data")
	before := objects.Objects()

	_, err := w.TagActorSnapshot(ctx, tagToCreate(resources.ActorRefFromActor(actor), "v1"), &ateapi.ExternalVolumeSelection{Names: []string{"data", "missing"}})
	if apierror.Code(err) != codes.InvalidArgument {
		t.Fatalf("TagActorSnapshot error = %v, want InvalidArgument", err)
	}
	if got := plugin.snapshotsCreated(); len(got) != 0 {
		t.Errorf("snapshots taken = %v, want none", got)
	}
	if diff := cmp.Diff(before, objects.Objects()); diff != "" {
		t.Errorf("objects changed by the rejected create (-want +got):\n%s", diff)
	}
	if _, getErr := persistence.GetTag(ctx, resources.TagRef{Atespace: "team-a", Name: "v1"}); !errors.Is(getErr, store.ErrNotFound) {
		t.Errorf("GetTag = %v, want ErrNotFound: the rejected create reserved its tag", getErr)
	}
}

// TestVolumeSnapshotError covers the code a failed capture returns. The
// failure leaves the tag pending, so no code may invite a plain retry, and the
// message says to delete the tag first.
func TestVolumeSnapshotError(t *testing.T) {
	tagRef := resources.TagRef{Atespace: "team-a", Name: "v1"}
	tests := []struct {
		name string
		err  error
		want codes.Code
	}{
		{name: "quota", err: status.Error(codes.ResourceExhausted, "quota"), want: codes.ResourceExhausted},
		{name: "unavailable", err: status.Error(codes.Unavailable, "down"), want: codes.Aborted},
		{name: "deadline", err: status.Error(codes.DeadlineExceeded, "slow"), want: codes.Aborted},
		{name: "aborted", err: status.Error(codes.Aborted, "conflict"), want: codes.Aborted},
		{name: "source volume gone", err: status.Error(codes.NotFound, "gone"), want: codes.FailedPrecondition},
		{name: "driver rejects", err: status.Error(codes.InvalidArgument, "bad"), want: codes.FailedPrecondition},
		{name: "no status", err: errors.New("boom"), want: codes.Internal},
		{
			name: "first code wins",
			err:  errors.Join(errors.New("boom"), fmt.Errorf("wrapped: %w", status.Error(codes.Unavailable, "down")), status.Error(codes.NotFound, "gone")),
			want: codes.Aborted,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := volumeSnapshotError(tagRef, tt.err)
			if code := apierror.Code(got); code != tt.want {
				t.Errorf("volumeSnapshotError() code = %v, want %v", code, tt.want)
			}
			if want := "delete Tag " + tagRef.String() + " and create it again"; !strings.Contains(got.Error(), want) {
				t.Errorf("volumeSnapshotError() = %q, want it to contain %q", got, want)
			}
		})
	}
}

// TestSelectTagVolumes covers which of the actor's volumes a tag captures.
func TestSelectTagVolumes(t *testing.T) {
	actor := &ateapi.Actor{
		Metadata: &ateapi.ResourceMetadata{Atespace: "team-a", Name: "actor-1"},
		Status: &ateapi.ActorStatus{ExternalVolumes: &ateapi.ExternalVolumes{Volumes: []*ateapi.ExternalVolumeStatus{
			{Name: "data"}, {Name: "cache"}, {Name: "scratch"},
		}}},
	}
	tests := []struct {
		name     string
		sel      *ateapi.ExternalVolumeSelection
		want     []string
		wantCode codes.Code
	}{
		{name: "not requested", sel: nil, want: nil},
		{name: "all", sel: &ateapi.ExternalVolumeSelection{}, want: []string{"data", "cache", "scratch"}},
		{name: "named keeps actor order", sel: &ateapi.ExternalVolumeSelection{Names: []string{"scratch", "data"}}, want: []string{"data", "scratch"}},
		{name: "unknown name", sel: &ateapi.ExternalVolumeSelection{Names: []string{"data", "missing"}}, wantCode: codes.InvalidArgument},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := selectTagVolumes(actor, tt.sel)
			if code := apierror.Code(err); code != tt.wantCode {
				t.Fatalf("selectTagVolumes() error = %v (code %v), want code %v", err, code, tt.wantCode)
			}
			var gotNames []string
			for _, vol := range got {
				gotNames = append(gotNames, vol.GetName())
			}
			if diff := cmp.Diff(tt.want, gotNames); diff != "" {
				t.Errorf("selected volumes mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// TestTagActorSnapshot_RejectedBeforeReserve verifies a volume that cannot be
// captured fails the tag before it is reserved, so the name stays free and the
// storage system is not touched.
func TestTagActorSnapshot_RejectedBeforeReserve(t *testing.T) {
	for _, tc := range []struct {
		name   string
		plugin func(*fakeSnapshotPlugin)
		status func(*ateapi.ActorStatus)
	}{
		{
			name:   "driver cannot snapshot",
			plugin: func(p *fakeSnapshotPlugin) { p.caps = volume.Capabilities{} },
		},
		{
			name:   "volume not provisioned",
			status: func(s *ateapi.ActorStatus) { s.ExternalVolumes.Volumes[0].StorageVolumeId = "" },
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			persistence := newTestPersistence(t)
			template := seedSubstrateTemplate(t, ctx, persistence, "sub-tmpl")
			plugin := newFakeSnapshotPlugin()
			if tc.plugin != nil {
				tc.plugin(plugin)
			}
			w, objects := newVolumeTagWorkflow(persistence, plugin)

			actor := seedTagSourceWithVolumes(t, ctx, persistence, objects, template, "actor-1", "data")
			if tc.status != nil {
				actor = mustUpdateActorStatus(t, ctx, persistence, actor, tc.status)
			}

			_, err := w.TagActorSnapshot(ctx, tagToCreate(resources.ActorRefFromActor(actor), "v1"), &ateapi.ExternalVolumeSelection{})
			if apierror.Code(err) != codes.FailedPrecondition {
				t.Fatalf("TagActorSnapshot error = %v, want FailedPrecondition", err)
			}
			if got := plugin.snapshotsCreated(); len(got) != 0 {
				t.Errorf("snapshots taken = %v, want none", got)
			}
			if _, getErr := persistence.GetTag(ctx, resources.TagRef{Atespace: "team-a", Name: "v1"}); !errors.Is(getErr, store.ErrNotFound) {
				t.Errorf("GetTag = %v, want ErrNotFound: the rejected create reserved its tag", getErr)
			}
		})
	}
}

// TestTagActorSnapshot_PartialVolumeFailureLeavesPendingTag verifies capture is
// all-or-nothing: a driver that fails on one volume fails the create with the
// driver's code, which leaves the tag pending. The volume that succeeded keeps
// its handle and the failed one has none. Deleting the tag then releases every
// snapshot and the copied objects, and frees the name for a retry.
func TestTagActorSnapshot_PartialVolumeFailureLeavesPendingTag(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)
	template := seedSubstrateTemplate(t, ctx, persistence, "sub-tmpl")
	plugin := newFakeSnapshotPlugin()
	plugin.failSnapshotOfVolume = "vol-cache"
	plugin.failSnapshotCode = codes.ResourceExhausted
	w, objects := newVolumeTagWorkflow(persistence, plugin)

	actor := seedTagSourceWithVolumes(t, ctx, persistence, objects, template, "actor-1", "data", "cache")
	tagRef := resources.TagRef{Atespace: "team-a", Name: "v1"}
	before := objects.Objects()

	_, err := w.TagActorSnapshot(ctx, tagToCreate(resources.ActorRefFromActor(actor), "v1"), &ateapi.ExternalVolumeSelection{})
	if apierror.Code(err) != codes.ResourceExhausted {
		t.Fatalf("TagActorSnapshot error = %v, want the driver's ResourceExhausted", err)
	}
	if got := plugin.snapshotsDeleted(); len(got) != 0 {
		t.Errorf("snapshots deleted by the failed create = %v, want none: cleanup is DeleteTag's", got)
	}

	stored, err := persistence.GetTag(ctx, tagRef)
	if err != nil {
		t.Fatalf("GetTag: %v", err)
	}
	if got, want := stored.GetStatus().GetSnapshot().GetDurableSnapshot().GetState(), ateapi.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_PENDING; got != want {
		t.Errorf("tag snapshot status = %v, want %v", got, want)
	}
	uid := stored.GetMetadata().GetUid()
	handles := map[string]string{}
	for _, snap := range stored.GetStatus().GetSnapshot().GetVolumeSnapshots() {
		handles[snap.GetSourceVolumeName()] = snap.GetStorageSnapshotId()
	}
	want := map[string]string{
		"data":  "snap-" + tagVolumeSnapshotID(uid, "data"),
		"cache": "",
	}
	if diff := cmp.Diff(want, handles); diff != "" {
		t.Errorf("recorded volume snapshot handles mismatch (-want +got):\n%s", diff)
	}

	// Once the driver recovers, deleting the tag releases the recorded
	// snapshot, and the one it re-requests for the entry without a handle.
	plugin.failSnapshotOfVolume = ""
	if _, err := w.DeleteTag(ctx, tagRef, store.DeletePreconditions{}); err != nil {
		t.Fatalf("DeleteTag: %v", err)
	}
	sortStrings := cmpopts.SortSlices(func(a, b string) bool { return a < b })
	wantDeleted := []string{want["data"], "snap-" + tagVolumeSnapshotID(uid, "cache")}
	if diff := cmp.Diff(wantDeleted, plugin.snapshotsDeleted(), sortStrings); diff != "" {
		t.Errorf("released snapshots mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(before, objects.Objects()); diff != "" {
		t.Errorf("objects after the delete mismatch, want the copy collected (-want +got):\n%s", diff)
	}

	if _, err := w.TagActorSnapshot(ctx, tagToCreate(resources.ActorRefFromActor(actor), "v1"), &ateapi.ExternalVolumeSelection{}); err != nil {
		t.Fatalf("TagActorSnapshot retry: %v", err)
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
	tag, err := w.TagActorSnapshot(ctx, tagToCreate(resources.ActorRefFromActor(actor), "v1"), &ateapi.ExternalVolumeSelection{})
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

// seedCreatingVolumeTag stores a tag the way a create that died midway leaves
// it: its snapshot PENDING, with the given volume snapshot entries recorded.
func seedCreatingVolumeTag(t *testing.T, ctx context.Context, persistence store.Interface, entries ...*ateapi.ExternalVolumeSnapshot) *ateapi.Tag {
	t.Helper()
	uri, err := resources.NewTagSnapshotURI(testStorageLocation, "team-a", "pending")
	if err != nil {
		t.Fatalf("NewTagSnapshotURI: %v", err)
	}
	snap := newDurableSnapshot(1, ateapi.SnapshotOwner_SNAPSHOT_OWNER_TAG, ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", uri.Name(), uri.String(), ateapi.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_PENDING)
	snap.VolumeSnapshots = entries
	return storetest.MustCreateTag(t, ctx, persistence, &ateapi.Tag{
		Metadata:    &ateapi.ResourceMetadata{Atespace: "team-a", Name: "pending"},
		Scope:       ateapi.TagScope_TAG_SCOPE_ATESPACE,
		SourceActor: &ateapi.ObjectRef{Atespace: "team-a", Name: "actor-1"},
		Status:      &ateapi.TagStatus{Snapshot: snap},
	})
}

// TestDeleteTag_RecoversUnrecordedVolumeSnapshots verifies a tag left behind
// by a create that died still has every snapshot collected: recorded handles
// are released directly, and an entry without one has its handle recovered by
// repeating the create's idempotent CreateSnapshot against the recorded source
// volume. The recovered handle is released even when writing it to the tag
// fails.
func TestDeleteTag_RecoversUnrecordedVolumeSnapshots(t *testing.T) {
	for _, tc := range []struct {
		name      string
		failWrite bool
	}{
		{name: "handle recorded"},
		{name: "handle write fails", failWrite: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			persistence := newTestPersistence(t)
			plugin := newFakeSnapshotPlugin()
			counting := &updateTagCountingStore{Interface: persistence}
			if tc.failWrite {
				counting.failCall = 1
			}
			w, _ := newVolumeTagWorkflow(counting, plugin)

			pending := seedCreatingVolumeTag(t, ctx, persistence,
				&ateapi.ExternalVolumeSnapshot{SourceVolumeName: "data", SourceVolumeId: "vol-data", StorageSnapshotId: "snap-stranded", VolumeType: testVolumeDriver},
				&ateapi.ExternalVolumeSnapshot{SourceVolumeName: "cache", SourceVolumeId: "vol-cache", VolumeType: testVolumeDriver},
			)

			if _, err := w.DeleteTag(ctx, resources.TagRefFromTag(pending), store.DeletePreconditions{}); err != nil {
				t.Fatalf("DeleteTag: %v", err)
			}
			recovered := "snap-" + tagVolumeSnapshotID(pending.GetMetadata().GetUid(), "cache")
			if diff := cmp.Diff([]string{recovered}, plugin.snapshotsCreated()); diff != "" {
				t.Errorf("snapshots re-requested mismatch (-want +got):\n%s", diff)
			}
			sortStrings := cmpopts.SortSlices(func(a, b string) bool { return a < b })
			if diff := cmp.Diff([]string{"snap-stranded", recovered}, plugin.snapshotsDeleted(), sortStrings); diff != "" {
				t.Errorf("released snapshots mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// TestDeleteTag_UnrecoverableSnapshotSkipped verifies a snapshot whose handle
// was never recorded does not block the delete when retrying cannot recover
// it: it is skipped and the tag is still removed.
func TestDeleteTag_UnrecoverableSnapshotSkipped(t *testing.T) {
	for _, tc := range []struct {
		name   string
		plugin func(*fakeSnapshotPlugin)
	}{
		{
			name:   "source volume gone",
			plugin: func(p *fakeSnapshotPlugin) { p.goneVolumes = map[string]bool{"vol-cache": true} },
		},
		{
			name: "terminal error",
			plugin: func(p *fakeSnapshotPlugin) {
				p.failSnapshotOfVolume = "vol-cache"
				p.failSnapshotCode = codes.InvalidArgument
			},
		},
		{
			name: "quota exhausted",
			plugin: func(p *fakeSnapshotPlugin) {
				p.failSnapshotOfVolume = "vol-cache"
				p.failSnapshotCode = codes.ResourceExhausted
			},
		},
		{
			name:   "driver without snapshots",
			plugin: func(p *fakeSnapshotPlugin) { p.caps.CreateDeleteSnapshot = false },
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			persistence := newTestPersistence(t)
			plugin := newFakeSnapshotPlugin()
			tc.plugin(plugin)
			w, _ := newVolumeTagWorkflow(persistence, plugin)

			pending := seedCreatingVolumeTag(t, ctx, persistence,
				&ateapi.ExternalVolumeSnapshot{SourceVolumeName: "cache", SourceVolumeId: "vol-cache", VolumeType: testVolumeDriver},
			)
			tagRef := resources.TagRefFromTag(pending)

			if _, err := w.DeleteTag(ctx, tagRef, store.DeletePreconditions{}); err != nil {
				t.Fatalf("DeleteTag: %v", err)
			}
			if got := plugin.snapshotsDeleted(); len(got) != 0 {
				t.Errorf("snapshots deleted = %v, want none: nothing could be recovered", got)
			}
			if _, err := persistence.GetTag(ctx, tagRef); !errors.Is(err, store.ErrNotFound) {
				t.Errorf("GetTag after the delete = %v, want ErrNotFound", err)
			}
		})
	}
}

// TestDeleteTag_TransientFailureKeepsTag verifies that when a transient error
// stops a handle from being recovered, or a snapshot from being deleted, the
// delete fails and keeps the row, so a retry can still release the snapshot.
func TestDeleteTag_TransientFailureKeepsTag(t *testing.T) {
	for _, tc := range []struct {
		name string
		fail func(*fakeSnapshotPlugin)
	}{
		{
			name: "recovery unavailable",
			fail: func(p *fakeSnapshotPlugin) {
				p.failSnapshotOfVolume = "vol-cache"
				p.failSnapshotCode = codes.Unavailable
			},
		},
		{
			name: "recovery error without status",
			fail: func(p *fakeSnapshotPlugin) { p.failSnapshotOfVolume = "vol-cache" },
		},
		{
			name: "delete fails",
			fail: func(p *fakeSnapshotPlugin) { p.failDeleteSnapshot = true },
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			persistence := newTestPersistence(t)
			plugin := newFakeSnapshotPlugin()
			tc.fail(plugin)
			w, _ := newVolumeTagWorkflow(persistence, plugin)

			pending := seedCreatingVolumeTag(t, ctx, persistence,
				&ateapi.ExternalVolumeSnapshot{SourceVolumeName: "cache", SourceVolumeId: "vol-cache", VolumeType: testVolumeDriver},
			)
			tagRef := resources.TagRefFromTag(pending)

			if _, err := w.DeleteTag(ctx, tagRef, store.DeletePreconditions{}); err == nil {
				t.Fatal("DeleteTag succeeded, want the failure to fail it")
			}
			stored, err := persistence.GetTag(ctx, tagRef)
			if err != nil {
				t.Fatalf("GetTag after the failed delete: %v", err)
			}
			if stored.GetMetadata().GetDeleteTime() == nil {
				t.Error("tag delete_time is unset, want the failed delete to have marked it")
			}

			plugin.failSnapshotOfVolume = ""
			plugin.failDeleteSnapshot = false
			if _, err := w.DeleteTag(ctx, tagRef, store.DeletePreconditions{}); err != nil {
				t.Fatalf("retried DeleteTag: %v", err)
			}
			want := []string{"snap-" + tagVolumeSnapshotID(pending.GetMetadata().GetUid(), "cache")}
			if diff := cmp.Diff(want, plugin.snapshotsDeleted()); diff != "" {
				t.Errorf("released snapshots mismatch (-want +got):\n%s", diff)
			}
			// A handle recovered by the failed attempt is recorded, so the
			// retry deletes it without cutting the snapshot again.
			if diff := cmp.Diff(want, plugin.snapshotsCreated()); diff != "" {
				t.Errorf("snapshots cut mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// updateTagCountingStore wraps a store, counting UpdateTag calls and failing
// the one numbered failCall (1-based; 0 never fails).
type updateTagCountingStore struct {
	store.Interface
	failCall int

	mu    sync.Mutex
	calls int
}

func (s *updateTagCountingStore) UpdateTag(ctx context.Context, tagRef resources.TagRef, precondition store.Precondition, mutate func(*ateapi.Tag) error) (*ateapi.Tag, error) {
	s.mu.Lock()
	s.calls++
	n := s.calls
	s.mu.Unlock()
	if n == s.failCall {
		return nil, fmt.Errorf("simulated UpdateTag failure on call %d", n)
	}
	return s.Interface.UpdateTag(ctx, tagRef, precondition, mutate)
}

func (s *updateTagCountingStore) updateTagCalls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

// TestTagActorSnapshot_SnapshotsVolumesConcurrently verifies every volume's
// snapshot is in flight at once, and that the handles are recorded with a
// fixed number of tag writes however many volumes there are.
func TestTagActorSnapshot_SnapshotsVolumesConcurrently(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)
	template := seedSubstrateTemplate(t, ctx, persistence, "sub-tmpl")
	plugin := newFakeSnapshotPlugin()
	plugin.rendezvous = 3
	counting := &updateTagCountingStore{Interface: persistence}
	w, objects := newVolumeTagWorkflow(counting, plugin)

	actor := seedTagSourceWithVolumes(t, ctx, persistence, objects, template, "actor-1", "data", "cache", "scratch")
	before := counting.updateTagCalls()

	tag, err := w.TagActorSnapshot(ctx, tagToCreate(resources.ActorRefFromActor(actor), "v1"), &ateapi.ExternalVolumeSelection{})
	if err != nil {
		t.Fatalf("TagActorSnapshot: %v", err)
	}
	for _, snap := range tag.GetStatus().GetSnapshot().GetVolumeSnapshots() {
		if snap.GetStorageSnapshotId() == "" {
			t.Errorf("volume %q recorded no snapshot handle", snap.GetSourceVolumeName())
		}
	}
	// One write records the snapshot, one the volumes, one every handle, and
	// one finalizes.
	if got, want := counting.updateTagCalls()-before, 4; got != want {
		t.Errorf("UpdateTag calls = %d, want %d", got, want)
	}
}

// TestTagActorSnapshot_HandleWriteFailureIsRecoveredOnDelete verifies that
// when the single write recording the handles fails, the snapshots already
// taken are not released inline: the tag stays pending with its entries still
// naming the source volumes, and deleting it recovers and releases them.
func TestTagActorSnapshot_HandleWriteFailureIsRecoveredOnDelete(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)
	template := seedSubstrateTemplate(t, ctx, persistence, "sub-tmpl")
	plugin := newFakeSnapshotPlugin()
	counting := &updateTagCountingStore{Interface: persistence}
	w, objects := newVolumeTagWorkflow(counting, plugin)
	actor := seedTagSourceWithVolumes(t, ctx, persistence, objects, template, "actor-1", "data", "cache")
	tagRef := resources.TagRef{Atespace: "team-a", Name: "v1"}
	// The first UpdateTag records the snapshot, the second the volumes, and the
	// third the handles.
	counting.failCall = counting.updateTagCalls() + 3

	if _, err := w.TagActorSnapshot(ctx, tagToCreate(resources.ActorRefFromActor(actor), "v1"), &ateapi.ExternalVolumeSelection{}); err == nil {
		t.Fatal("TagActorSnapshot succeeded, want the failed handle write to fail the create")
	}
	created := plugin.snapshotsCreated()
	if len(created) != 2 {
		t.Fatalf("snapshots taken = %v, want both volumes'", created)
	}
	if got := plugin.snapshotsDeleted(); len(got) != 0 {
		t.Errorf("snapshots deleted by the failed create = %v, want none: cleanup is DeleteTag's", got)
	}
	stored, err := persistence.GetTag(ctx, tagRef)
	if err != nil {
		t.Fatalf("GetTag: %v", err)
	}
	if got, want := stored.GetStatus().GetSnapshot().GetDurableSnapshot().GetState(), ateapi.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_PENDING; got != want {
		t.Errorf("tag snapshot status = %v, want %v", got, want)
	}

	if _, err := w.DeleteTag(ctx, tagRef, store.DeletePreconditions{}); err != nil {
		t.Fatalf("DeleteTag: %v", err)
	}
	sortStrings := cmpopts.SortSlices(func(a, b string) bool { return a < b })
	if diff := cmp.Diff(created, plugin.snapshotsDeleted(), sortStrings); diff != "" {
		t.Errorf("snapshots created but not deleted (-created +deleted):\n%s", diff)
	}
}

// TestTagVolumeSnapshotID verifies the snapshot name fits the 63 characters
// some drivers allow for any volume name, and differs per volume.
func TestTagVolumeSnapshotID(t *testing.T) {
	const tagUID = "0b8f4a52-6f53-4a2e-9d0c-3c1f7e5a9b21"
	long := strings.Repeat("v", 63)
	names := map[string]bool{}
	for _, vol := range []string{"a", "data", long} {
		name := tagVolumeSnapshotID(tagUID, vol)
		if len(name) > 63 {
			t.Errorf("tagVolumeSnapshotID(%q) = %q, %d characters, want at most 63", vol, name, len(name))
		}
		names[name] = true
	}
	if len(names) != 3 {
		t.Errorf("tagVolumeSnapshotID gave %d distinct names for 3 volumes", len(names))
	}
}
