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
	"unicode/utf8"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/ateapi/internal/apivalidation"
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/cmd/ateapi/internal/store/storetest"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/objectstore/objectstoretest"
	"go.ate.dev/substrate/internal/objectstoreplugin/objectstoreplugintest"
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/internal/volume"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
)

const testVolumeDriver = "substrate.io/mock"

// errorRecorded stands in for the driver's error in the statuses
// tagVolumeStatuses returns.
const errorRecorded = "<error>"

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
	// noHandle makes CreateSnapshot succeed without a handle, standing in for
	// a buggy driver.
	noHandle bool
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
	if f.noHandle {
		return volume.Snapshot{}, nil
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

// newVolumeTagWorkflow builds a tag workflow wired to a single fake driver. Its
// store is a ServiceImpl over persistence, so ExternalVolumeSnapshot writes are
// validated as in ate-api-server.
func newVolumeTagWorkflow(persistence store.Interface, plugin *fakeSnapshotPlugin) (*ActorWorkflow, *objectstoretest.Fake) {
	objects := objectstoretest.New()
	return &ActorWorkflow{
		store:             &ServiceImpl{store: persistence},
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

// tagVolumeSnapshots returns the ExternalVolumeSnapshots the tag owns, by
// source volume name.
func tagVolumeSnapshots(t *testing.T, ctx context.Context, persistence store.Interface, tag *ateapi.Tag) map[string]*ateapi.ExternalVolumeSnapshot {
	t.Helper()
	snaps, err := persistence.ListTagExternalVolumeSnapshots(ctx, resources.TagRefFromTag(tag), tag.GetMetadata().GetUid())
	if err != nil {
		t.Fatalf("ListTagExternalVolumeSnapshots: %v", err)
	}
	got := map[string]*ateapi.ExternalVolumeSnapshot{}
	for _, snap := range snaps {
		got[snap.GetOrigin().GetTagCapture().GetSourceVolumeName()] = snap
	}
	return got
}

// tagVolumeStatuses returns the status of each ExternalVolumeSnapshot the tag
// owns, by source volume name, with any error_message replaced by
// errorRecorded.
func tagVolumeStatuses(t *testing.T, ctx context.Context, persistence store.Interface, tag *ateapi.Tag) map[string]*ateapi.ExternalVolumeSnapshotStatus {
	t.Helper()
	got := map[string]*ateapi.ExternalVolumeSnapshotStatus{}
	for volName, snap := range tagVolumeSnapshots(t, ctx, persistence, tag) {
		s := proto.CloneOf(snap.GetStatus())
		if s.GetErrorMessage() != "" {
			s.ErrorMessage = errorRecorded
		}
		got[volName] = s
	}
	return got
}

var sortStrings = cmpopts.SortSlices(func(a, b string) bool { return a < b })

// TestTagActorSnapshot_CapturesVolumes verifies that a tag asked to include
// external volumes snapshots every one of the actor's external volumes and
// records an ExternalVolumeSnapshot with the handle for each.
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
	if got, want := tag.GetStatus().GetSnapshot().GetDurableSnapshot().GetState(), ateapi.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_COMPLETED; got != want {
		t.Errorf("tag snapshot status = %v, want %v", got, want)
	}

	uid := tag.GetMetadata().GetUid()
	var got []*ateapi.ExternalVolumeSnapshot
	for _, snap := range tagVolumeSnapshots(t, ctx, persistence, tag) {
		got = append(got, snap)
	}
	var want []*ateapi.ExternalVolumeSnapshot
	for _, volName := range []string{"data", "cache"} {
		name := tagVolumeSnapshotID(uid, volName)
		want = append(want, &ateapi.ExternalVolumeSnapshot{
			Metadata:   &ateapi.ResourceMetadata{Atespace: "team-a", Name: name},
			DriverName: testVolumeDriver,
			Origin: &ateapi.ExternalVolumeSnapshotOrigin{TagCapture: &ateapi.TagVolumeSnapshotOrigin{
				Tag:              &ateapi.ObjectRef{Atespace: "team-a", Name: "v1"},
				TagUid:           uid,
				SourceVolumeName: volName,
				SourceVolumeId:   "vol-" + volName,
			}},
			Status: &ateapi.ExternalVolumeSnapshotStatus{SnapshotId: "snap-" + name, ReadyToUse: true, RestoreSizeBytes: 1024},
		})
	}
	opts := []cmp.Option{
		protocmp.Transform(),
		protocmp.IgnoreFields(&ateapi.ResourceMetadata{}, "uid", "version", "create_time", "update_time"),
		cmpopts.SortSlices(func(a, b *ateapi.ExternalVolumeSnapshot) bool {
			return a.GetMetadata().GetName() < b.GetMetadata().GetName()
		}),
	}
	if diff := cmp.Diff(want, got, opts...); diff != "" {
		t.Errorf("recorded external volume snapshots mismatch (-want +got):\n%s", diff)
	}

	wantCreated := []string{
		"snap-" + tagVolumeSnapshotID(uid, "data"),
		"snap-" + tagVolumeSnapshotID(uid, "cache"),
	}
	if diff := cmp.Diff(wantCreated, plugin.snapshotsCreated(), sortStrings); diff != "" {
		t.Errorf("snapshots taken mismatch (-want +got):\n%s", diff)
	}
	if got := plugin.snapshotsDeleted(); len(got) != 0 {
		t.Errorf("snapshots deleted = %v, want none on a successful create", got)
	}
}

// TestTagActorSnapshot_SnapshotNotReadyCompletes verifies a tag whose volume
// snapshot the driver is still processing is still completed, recording the
// snapshot's handle with ready_to_use false: creation does not wait for the
// copy. The snapshot can become ready to use after the tag completes.
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
	snap := tagVolumeSnapshots(t, ctx, persistence, tag)["data"]
	if st := snap.GetStatus(); st.GetSnapshotId() == "" || st.GetReadyToUse() {
		t.Fatalf("volume snapshot status = %v, want a handle not ready to use", st)
	}

	ready, err := w.store.UpdateExternalVolumeSnapshot(ctx, resources.ExternalVolumeSnapshotRefFromExternalVolumeSnapshot(snap), store.PreconditionFrom(snap), func(toUpdate *ateapi.ExternalVolumeSnapshot) error {
		toUpdate.Status.ReadyToUse = true
		return nil
	})
	if err != nil {
		t.Fatalf("marking the snapshot of a completed tag ready to use: %v", err)
	}
	if _, err := w.store.UpdateExternalVolumeSnapshot(ctx, resources.ExternalVolumeSnapshotRefFromExternalVolumeSnapshot(ready), store.PreconditionFrom(ready), func(toUpdate *ateapi.ExternalVolumeSnapshot) error {
		toUpdate.Status.ReadyToUse = false
		return nil
	}); apierror.Code(err) != codes.Internal {
		t.Errorf("moving ready_to_use from true to false = %v, want Internal", err)
	}
}

// TestTagActorSnapshot_VolumesNotRequested verifies volumes are captured only
// when asked for: an actor with volumes tagged without including them produces
// a tag that owns no ExternalVolumeSnapshots.
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

	if got := tagVolumeSnapshots(t, ctx, persistence, tag); len(got) != 0 {
		t.Errorf("external volume snapshots = %v, want none", got)
	}
	if got := plugin.snapshotsCreated(); len(got) != 0 {
		t.Errorf("snapshots taken = %v, want none", got)
	}
}

// TestTagActorSnapshot_CapturesNamedVolumes verifies that naming volumes
// captures only those and leaves the rest out.
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

	uid := tag.GetMetadata().GetUid()
	want := map[string]*ateapi.ExternalVolumeSnapshotStatus{
		"data":    {SnapshotId: "snap-" + tagVolumeSnapshotID(uid, "data"), ReadyToUse: true, RestoreSizeBytes: 1024},
		"scratch": {SnapshotId: "snap-" + tagVolumeSnapshotID(uid, "scratch"), ReadyToUse: true, RestoreSizeBytes: 1024},
	}
	if diff := cmp.Diff(want, tagVolumeStatuses(t, ctx, persistence, tag), protocmp.Transform()); diff != "" {
		t.Errorf("captured volumes mismatch (-want +got):\n%s", diff)
	}
	wantCreated := []string{want["data"].GetSnapshotId(), want["scratch"].GetSnapshotId()}
	if diff := cmp.Diff(wantCreated, plugin.snapshotsCreated(), sortStrings); diff != "" {
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

// TestTagVolumeSnapshotFailed verifies a driver error too long for
// error_message, or not valid UTF-8, is still recorded as a valid status.
func TestTagVolumeSnapshotFailed(t *testing.T) {
	got := tagVolumeSnapshotFailed(errors.New(strings.Repeat("é", 3000) + "\xff"))
	if n := len(got.GetErrorMessage()); n == 0 || n > maxVolumeSnapshotErrorMessageLen {
		t.Errorf("error_message is %d bytes, want 1 to %d", n, maxVolumeSnapshotErrorMessageLen)
	}
	if !utf8.ValidString(got.GetErrorMessage()) {
		t.Errorf("error_message is not valid UTF-8")
	}
	oldSnap := &ateapi.ExternalVolumeSnapshot{Status: &ateapi.ExternalVolumeSnapshotStatus{}}
	newSnap := &ateapi.ExternalVolumeSnapshot{Status: got}
	if errs := apivalidation.ValidateExternalVolumeSnapshotUpdate(context.Background(), newSnap, oldSnap); len(errs) > 0 {
		t.Errorf("the failed status is not a valid update: %v", errs)
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
		actor    *ateapi.Actor
		sel      *ateapi.ExternalVolumeSelection
		want     []string
		wantCode codes.Code
	}{
		{name: "not requested", sel: nil, want: nil},
		{name: "all", sel: &ateapi.ExternalVolumeSelection{}, want: []string{"data", "cache", "scratch"}},
		{name: "named keeps actor order", sel: &ateapi.ExternalVolumeSelection{Names: []string{"scratch", "data"}}, want: []string{"data", "scratch"}},
		{name: "unknown name", sel: &ateapi.ExternalVolumeSelection{Names: []string{"data", "missing"}}, wantCode: codes.InvalidArgument},
		{name: "all with no volumes", actor: &ateapi.Actor{Metadata: &ateapi.ResourceMetadata{Atespace: "team-a", Name: "actor-2"}}, sel: &ateapi.ExternalVolumeSelection{}, wantCode: codes.InvalidArgument},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := actor
			if tt.actor != nil {
				a = tt.actor
			}
			got, err := selectTagVolumes(a, tt.sel)
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
// its handle and the failed one records the driver's error. Deleting
// the tag then releases every snapshot, their rows and the copied objects, and
// frees the name for a retry.
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
	snaps := tagVolumeSnapshots(t, ctx, persistence, stored)
	if msg := snaps["cache"].GetStatus().GetErrorMessage(); !strings.Contains(msg, "simulated snapshot failure") {
		t.Errorf("cache error_message = %q, want the driver's error", msg)
	}
	want := map[string]*ateapi.ExternalVolumeSnapshotStatus{
		"data":  {SnapshotId: "snap-" + tagVolumeSnapshotID(uid, "data"), ReadyToUse: true, RestoreSizeBytes: 1024},
		"cache": {ErrorMessage: errorRecorded},
	}
	if diff := cmp.Diff(want, tagVolumeStatuses(t, ctx, persistence, stored), protocmp.Transform()); diff != "" {
		t.Errorf("recorded volume snapshots mismatch (-want +got):\n%s", diff)
	}

	// Once the driver recovers, deleting the tag releases the recorded
	// snapshot, and the one it re-requests for the row without a handle.
	plugin.failSnapshotOfVolume = ""
	if _, err := w.DeleteTag(ctx, tagRef, store.DeletePreconditions{}); err != nil {
		t.Fatalf("DeleteTag: %v", err)
	}
	wantDeleted := []string{want["data"].GetSnapshotId(), "snap-" + tagVolumeSnapshotID(uid, "cache")}
	if diff := cmp.Diff(wantDeleted, plugin.snapshotsDeleted(), sortStrings); diff != "" {
		t.Errorf("released snapshots mismatch (-want +got):\n%s", diff)
	}
	if got := tagVolumeSnapshots(t, ctx, persistence, stored); len(got) != 0 {
		t.Errorf("external volume snapshots after the delete = %v, want none", got)
	}
	if diff := cmp.Diff(before, objects.Objects()); diff != "" {
		t.Errorf("objects after the delete mismatch, want the copy collected (-want +got):\n%s", diff)
	}

	if _, err := w.TagActorSnapshot(ctx, tagToCreate(resources.ActorRefFromActor(actor), "v1"), &ateapi.ExternalVolumeSelection{}); err != nil {
		t.Fatalf("TagActorSnapshot retry: %v", err)
	}
}

// TestDeleteTag_ReleasesVolumeSnapshots verifies a delete collects the volume
// snapshots the tag owns and their rows, not just its object-storage prefix.
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
	if got := tagVolumeSnapshots(t, ctx, persistence, tag); len(got) != 0 {
		t.Errorf("external volume snapshots after the delete = %v, want none", got)
	}
	if _, err := persistence.GetTag(ctx, resources.TagRefFromTag(tag)); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("GetTag after the delete = %v, want ErrNotFound", err)
	}
}

// seedCreatingVolumeTag stores a tag the way a create that died midway leaves
// it: its snapshot PENDING, owning an ExternalVolumeSnapshot with each of the
// given statuses, keyed by source volume name. Each volume's source volume ID
// is "vol-" and its name.
func seedCreatingVolumeTag(t *testing.T, ctx context.Context, persistence store.Interface, statuses map[string]*ateapi.ExternalVolumeSnapshotStatus) *ateapi.Tag {
	t.Helper()
	uri, err := resources.NewTagSnapshotURI(testStorageLocation, "team-a", "pending")
	if err != nil {
		t.Fatalf("NewTagSnapshotURI: %v", err)
	}
	snap := newDurableSnapshot(1, ateapi.SnapshotOwner_SNAPSHOT_OWNER_TAG, ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, "", uri.Name(), uri.String(), ateapi.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_PENDING)
	tag := storetest.MustCreateTag(t, ctx, persistence, &ateapi.Tag{
		Metadata:    &ateapi.ResourceMetadata{Atespace: "team-a", Name: "pending"},
		Scope:       ateapi.TagScope_TAG_SCOPE_ATESPACE,
		SourceActor: &ateapi.ObjectRef{Atespace: "team-a", Name: "actor-1"},
		Status:      &ateapi.TagStatus{Snapshot: snap},
	})
	svc := &ServiceImpl{store: persistence}
	for volName, status := range statuses {
		evs := newTagVolumeSnapshot(tag, &ateapi.ExternalVolumeStatus{Name: volName, StorageVolumeId: "vol-" + volName, VolumeType: testVolumeDriver})
		created, err := svc.CreateExternalVolumeSnapshot(ctx, evs)
		if err != nil {
			t.Fatalf("CreateExternalVolumeSnapshot(%s): %v", volName, err)
		}
		if proto.Equal(status, &ateapi.ExternalVolumeSnapshotStatus{}) {
			continue
		}
		if _, err := svc.UpdateExternalVolumeSnapshot(ctx, resources.ExternalVolumeSnapshotRefFromExternalVolumeSnapshot(created), store.PreconditionFrom(created), func(toUpdate *ateapi.ExternalVolumeSnapshot) error {
			toUpdate.Status = proto.CloneOf(status)
			return nil
		}); err != nil {
			t.Fatalf("UpdateExternalVolumeSnapshot(%s): %v", volName, err)
		}
	}
	return tag
}

// TestDeleteTag_RecoversUnrecordedVolumeSnapshots verifies a tag left behind
// by a create that died still has every snapshot collected: recorded handles
// are released directly, and a row without one has its handle recovered by
// repeating the create's idempotent CreateSnapshot against the recorded source
// volume.
func TestDeleteTag_RecoversUnrecordedVolumeSnapshots(t *testing.T) {
	for _, tc := range []struct {
		name string
	}{
		{name: "handle recovered"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			persistence := newTestPersistence(t)
			plugin := newFakeSnapshotPlugin()
			w, _ := newVolumeTagWorkflow(persistence, plugin)

			pending := seedCreatingVolumeTag(t, ctx, persistence, map[string]*ateapi.ExternalVolumeSnapshotStatus{
				"data":    {SnapshotId: "snap-stranded"},
				"cache":   {},
				"scratch": {ErrorMessage: "quota exceeded"},
			})

			if _, err := w.DeleteTag(ctx, resources.TagRefFromTag(pending), store.DeletePreconditions{}); err != nil {
				t.Fatalf("DeleteTag: %v", err)
			}
			uid := pending.GetMetadata().GetUid()
			recovered := []string{"snap-" + tagVolumeSnapshotID(uid, "cache"), "snap-" + tagVolumeSnapshotID(uid, "scratch")}
			if diff := cmp.Diff(recovered, plugin.snapshotsCreated(), sortStrings); diff != "" {
				t.Errorf("snapshots re-requested mismatch (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(append([]string{"snap-stranded"}, recovered...), plugin.snapshotsDeleted(), sortStrings); diff != "" {
				t.Errorf("released snapshots mismatch (-want +got):\n%s", diff)
			}
			if got := tagVolumeSnapshots(t, ctx, persistence, pending); len(got) != 0 {
				t.Errorf("external volume snapshots after the delete = %v, want none", got)
			}
		})
	}
}

// TestDeleteTag_UnrecoverableSnapshotSkipped verifies a snapshot whose handle
// was never recorded does not block the delete when retrying cannot recover
// it: it is skipped, and its row and the tag are still removed.
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

			pending := seedCreatingVolumeTag(t, ctx, persistence, map[string]*ateapi.ExternalVolumeSnapshotStatus{"cache": {}})
			tagRef := resources.TagRefFromTag(pending)

			if _, err := w.DeleteTag(ctx, tagRef, store.DeletePreconditions{}); err != nil {
				t.Fatalf("DeleteTag: %v", err)
			}
			if got := plugin.snapshotsDeleted(); len(got) != 0 {
				t.Errorf("snapshots deleted = %v, want none: nothing could be recovered", got)
			}
			if got := tagVolumeSnapshots(t, ctx, persistence, pending); len(got) != 0 {
				t.Errorf("external volume snapshots after the delete = %v, want none", got)
			}
			if _, err := persistence.GetTag(ctx, tagRef); !errors.Is(err, store.ErrNotFound) {
				t.Errorf("GetTag after the delete = %v, want ErrNotFound", err)
			}
		})
	}
}

// TestDeleteTag_TransientFailureKeepsTag verifies that when a transient error
// stops a handle from being recovered, or a snapshot or its row from being
// deleted, the delete fails and keeps the rows, so a retry can still release
// the snapshot. A failed recovery's error is recorded on the row.
func TestDeleteTag_TransientFailureKeepsTag(t *testing.T) {
	for _, tc := range []struct {
		name      string
		fail      func(*fakeSnapshotPlugin)
		failStore bool
		// wantErrorMessage is part of the row's error_message after the
		// failed delete. Empty wants none.
		wantErrorMessage string
	}{
		{
			name: "recovery unavailable",
			fail: func(p *fakeSnapshotPlugin) {
				p.failSnapshotOfVolume = "vol-cache"
				p.failSnapshotCode = codes.Unavailable
			},
			wantErrorMessage: "simulated snapshot failure",
		},
		{
			name:             "recovery error without status",
			fail:             func(p *fakeSnapshotPlugin) { p.failSnapshotOfVolume = "vol-cache" },
			wantErrorMessage: "simulated snapshot failure",
		},
		{
			name: "delete fails",
			fail: func(p *fakeSnapshotPlugin) { p.failDeleteSnapshot = true },
		},
		{
			name:      "row delete fails",
			failStore: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			persistence := newTestPersistence(t)
			plugin := newFakeSnapshotPlugin()
			if tc.fail != nil {
				tc.fail(plugin)
			}
			faulty := &faultyEVSStore{Interface: persistence, failDelete: tc.failStore}
			w, _ := newVolumeTagWorkflow(faulty, plugin)

			pending := seedCreatingVolumeTag(t, ctx, persistence, map[string]*ateapi.ExternalVolumeSnapshotStatus{"cache": {}})
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
			if got := tagVolumeSnapshots(t, ctx, persistence, pending); len(got) != 1 {
				t.Errorf("external volume snapshots after the failed delete = %v, want the one kept", got)
			} else if msg := got["cache"].GetStatus().GetErrorMessage(); tc.wantErrorMessage == "" && msg != "" || !strings.Contains(msg, tc.wantErrorMessage) {
				t.Errorf("error_message after the failed delete = %q, want %q in it, or none if empty", msg, tc.wantErrorMessage)
			}

			plugin.failSnapshotOfVolume = ""
			plugin.failDeleteSnapshot = false
			faulty.failDelete = false
			if _, err := w.DeleteTag(ctx, tagRef, store.DeletePreconditions{}); err != nil {
				t.Fatalf("retried DeleteTag: %v", err)
			}
			snapshot := "snap-" + tagVolumeSnapshotID(pending.GetMetadata().GetUid(), "cache")
			wantDeleted := []string{snapshot}
			if tc.failStore {
				// The first attempt deleted the storage snapshot before its row
				// failed to go, and the retry deletes it again by handle.
				wantDeleted = append(wantDeleted, snapshot)
			}
			if diff := cmp.Diff(wantDeleted, plugin.snapshotsDeleted()); diff != "" {
				t.Errorf("released snapshots mismatch (-want +got):\n%s", diff)
			}
			if _, err := persistence.GetTag(ctx, tagRef); !errors.Is(err, store.ErrNotFound) {
				t.Errorf("GetTag after the retried delete = %v, want ErrNotFound", err)
			}
		})
	}
}

// faultyEVSStore wraps a store, failing ExternalVolumeSnapshot writes on
// request and counting batch updates.
type faultyEVSStore struct {
	store.Interface
	// failBatch fails UpdateTagExternalVolumeSnapshots.
	failBatch bool
	// failDelete fails DeleteExternalVolumeSnapshot.
	failDelete bool

	mu           sync.Mutex
	batchUpdates int
}

func (s *faultyEVSStore) UpdateTagExternalVolumeSnapshots(ctx context.Context, tagRef resources.TagRef, tagUID string, mutate func(*ateapi.ExternalVolumeSnapshot) error) ([]*ateapi.ExternalVolumeSnapshot, error) {
	s.mu.Lock()
	s.batchUpdates++
	s.mu.Unlock()
	if s.failBatch {
		return nil, errors.New("simulated UpdateTagExternalVolumeSnapshots failure")
	}
	return s.Interface.UpdateTagExternalVolumeSnapshots(ctx, tagRef, tagUID, mutate)
}

func (s *faultyEVSStore) DeleteExternalVolumeSnapshot(ctx context.Context, ref resources.ExternalVolumeSnapshotRef, precondition store.DeletePreconditions) (*ateapi.ExternalVolumeSnapshot, error) {
	if s.failDelete {
		return nil, errors.New("simulated DeleteExternalVolumeSnapshot failure")
	}
	return s.Interface.DeleteExternalVolumeSnapshot(ctx, ref, precondition)
}

func (s *faultyEVSStore) batchUpdateCalls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.batchUpdates
}

// TestTagActorSnapshot_SnapshotsVolumesConcurrently verifies every volume's
// snapshot is in flight at once, and that the results are recorded with one
// batch write however many volumes there are.
func TestTagActorSnapshot_SnapshotsVolumesConcurrently(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)
	template := seedSubstrateTemplate(t, ctx, persistence, "sub-tmpl")
	plugin := newFakeSnapshotPlugin()
	plugin.rendezvous = 3
	faulty := &faultyEVSStore{Interface: persistence}
	w, objects := newVolumeTagWorkflow(faulty, plugin)

	actor := seedTagSourceWithVolumes(t, ctx, persistence, objects, template, "actor-1", "data", "cache", "scratch")

	tag, err := w.TagActorSnapshot(ctx, tagToCreate(resources.ActorRefFromActor(actor), "v1"), &ateapi.ExternalVolumeSelection{})
	if err != nil {
		t.Fatalf("TagActorSnapshot: %v", err)
	}
	for volName, status := range tagVolumeStatuses(t, ctx, persistence, tag) {
		if status.GetSnapshotId() == "" {
			t.Errorf("volume %q recorded no snapshot handle", volName)
		}
	}
	if got := faulty.batchUpdateCalls(); got != 1 {
		t.Errorf("UpdateTagExternalVolumeSnapshots calls = %d, want 1", got)
	}
}

// TestTagActorSnapshot_HandleWriteFailureIsRecoveredOnDelete verifies that
// when the write recording the handles fails, the snapshots already taken are
// not released inline: the tag stays pending with its rows' statuses still
// empty and naming the source volumes, and deleting it recovers and releases
// them.
func TestTagActorSnapshot_HandleWriteFailureIsRecoveredOnDelete(t *testing.T) {
	ctx := context.Background()
	persistence := newTestPersistence(t)
	template := seedSubstrateTemplate(t, ctx, persistence, "sub-tmpl")
	plugin := newFakeSnapshotPlugin()
	faulty := &faultyEVSStore{Interface: persistence, failBatch: true}
	w, objects := newVolumeTagWorkflow(faulty, plugin)
	actor := seedTagSourceWithVolumes(t, ctx, persistence, objects, template, "actor-1", "data", "cache")
	tagRef := resources.TagRef{Atespace: "team-a", Name: "v1"}

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
	want := map[string]*ateapi.ExternalVolumeSnapshotStatus{"data": {}, "cache": {}}
	if diff := cmp.Diff(want, tagVolumeStatuses(t, ctx, persistence, stored), protocmp.Transform()); diff != "" {
		t.Errorf("volume snapshots after the failed write mismatch (-want +got):\n%s", diff)
	}

	if _, err := w.DeleteTag(ctx, tagRef, store.DeletePreconditions{}); err != nil {
		t.Fatalf("DeleteTag: %v", err)
	}
	if diff := cmp.Diff(created, plugin.snapshotsDeleted(), sortStrings); diff != "" {
		t.Errorf("snapshots created but not deleted (-created +deleted):\n%s", diff)
	}
}
