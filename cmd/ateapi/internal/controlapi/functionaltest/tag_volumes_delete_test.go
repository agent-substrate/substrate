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
	"errors"
	"slices"
	"sync"
	"testing"

	"github.com/google/go-cmp/cmp"
	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/internal/objectstore"
	"go.ate.dev/substrate/internal/preview"
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/internal/volume"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// deleteSnapshotRecordingPlugin records the handles passed to DeleteSnapshot.
type deleteSnapshotRecordingPlugin struct {
	volume.VolumePluginControlPlane
	mu      sync.Mutex
	deleted []string
}

func (p *deleteSnapshotRecordingPlugin) DeleteSnapshot(ctx context.Context, snapshotID string) error {
	p.mu.Lock()
	p.deleted = append(p.deleted, snapshotID)
	p.mu.Unlock()
	return p.VolumePluginControlPlane.DeleteSnapshot(ctx, snapshotID)
}

// TestDeleteTag_FinishesDeleteStartedWithoutSnapshotCleanup verifies that
// DeleteTag finishes a delete that a release without ExternalVolumeSnapshot
// cleanup started. Such a release marks the tag and releases its external
// snapshot, then fails to drop the row because the tag still owns
// ExternalVolumeSnapshots. A retry served by this release must delete the
// storage snapshots, the objects and the tag.
func TestDeleteTag_FinishesDeleteStartedWithoutSnapshotCleanup(t *testing.T) {
	preview.InitForTest(t, "ExternalVolumes")
	ns := namespaceForTest("ns-tag-delete-resume")
	plugin := &deleteSnapshotRecordingPlugin{VolumePluginControlPlane: volume.NewMockVolumePlugin()}
	tc := setupTestWithVolumePlugins(t, ns, map[string]volume.VolumePluginControlPlane{"substrate.io/mock": plugin})
	defer tc.cleanup()
	ctx := context.Background()

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
	suspendActorForTest(t, tc, workerName, "source")

	tag, err := tc.client.CreateTag(ctx, &ateapi.CreateTagRequest{
		Tag: &ateapi.Tag{
			Metadata:    &ateapi.ResourceMetadata{Atespace: testAtespace, Name: "resume"},
			Scope:       ateapi.TagScope_TAG_SCOPE_ATESPACE,
			SourceActor: &ateapi.ObjectRef{Atespace: testAtespace, Name: "source"},
		},
		VolumeSelection: &ateapi.ExternalVolumeSelection{},
	})
	if err != nil {
		t.Fatalf("CreateTag failed: %v", err)
	}
	handles := tagVolumeHandles(t, tc, tag)
	if len(handles) != 2 {
		t.Fatalf("tag volume snapshot handles = %v, want one for each of data and cache", handles)
	}
	tagRef := resources.TagRefFromTag(tag)

	// Leave the tag as the older release's DeleteTag does.
	if _, err := tc.persistence.MarkTagForDeletion(ctx, tagRef, store.DeletePreconditions{}, nil); err != nil {
		t.Fatalf("MarkTagForDeletion failed: %v", err)
	}
	uri, err := resources.ParseSnapshotURI(tagSnapshotURI(tag))
	if err != nil {
		t.Fatalf("ParseSnapshotURI: %v", err)
	}
	bucket, prefix, err := objectstore.BucketPrefix(uri.Prefix())
	if err != nil {
		t.Fatalf("BucketPrefix: %v", err)
	}
	for _, name := range tc.objectStore.Snapshot(t, uri) {
		if err := tc.objectStore.Delete(ctx, bucket, prefix+name); err != nil {
			t.Fatalf("deleting external snapshot object %s: %v", name, err)
		}
	}
	if _, err := tc.persistence.DeleteTag(ctx, tagRef, store.DeletePreconditions{UID: tag.GetMetadata().GetUid()}); !errors.Is(err, store.ErrFailedPrecondition) {
		t.Fatalf("store DeleteTag of a tag that owns snapshots = %v, want ErrFailedPrecondition", err)
	}

	if _, err := tc.client.DeleteTag(ctx, &ateapi.DeleteTagRequest{Tag: tagRef.ToObjectRef()}); err != nil {
		t.Fatalf("DeleteTag failed: %v", err)
	}
	_, err = tc.client.GetTag(ctx, &ateapi.GetTagRequest{Tag: tagRef.ToObjectRef()})
	if status.Code(err) != codes.NotFound {
		t.Errorf("GetTag() after DeleteTag = %v, want NotFound", err)
	}
	if got := tagVolumeHandles(t, tc, tag); len(got) != 0 {
		t.Errorf("tag volume snapshots after DeleteTag = %v, want none", got)
	}
	want := []string{handles["data"], handles["cache"]}
	slices.Sort(want)
	plugin.mu.Lock()
	got := slices.Sorted(slices.Values(plugin.deleted))
	plugin.mu.Unlock()
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("deleted storage snapshots mismatch (-want +got):\n%s", diff)
	}
}
