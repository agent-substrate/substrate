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
	"testing"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/internal/preview"
	"go.ate.dev/substrate/internal/volume"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// setupTagVolumesTest starts a server with the mock volume plugin, and creates
// a template declaring the external volumes "data" and "cache" plus a worker to
// run its Actors on.
func setupTagVolumesTest(t *testing.T, ns string) (*testContext, string) {
	t.Helper()
	preview.InitForTest(t, "ExternalVolumes")
	tc := setupTestWithVolumePlugins(t, ns, map[string]volume.VolumePluginControlPlane{
		"substrate.io/mock": volume.NewMockVolumePlugin(),
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
	return tc, workerName
}

// tagVolumeHandles returns the snapshot handle a tag records per volume.
func tagVolumeHandles(tag *ateapi.Tag) map[string]string {
	handles := map[string]string{}
	for _, snap := range tag.GetStatus().GetSnapshot().GetVolumeSnapshots() {
		handles[snap.GetSourceVolumeName()] = snap.GetStorageSnapshotId()
	}
	return handles
}

// TestCreateTag_CapturesVolumes verifies that a tag taken with external volumes
// snapshots each of them and records the handles, and that the tag can then be
// deleted.
func TestCreateTag_CapturesVolumes(t *testing.T) {
	ns := namespaceForTest("ns-tag-capture-volumes")
	tc, workerName := setupTagVolumesTest(t, ns)
	defer tc.cleanup()
	ctx := context.Background()

	suspendActorForTest(t, tc, workerName, "source")
	tag, err := tc.client.CreateTag(ctx, &ateapi.CreateTagRequest{
		Tag: &ateapi.Tag{
			Metadata:    &ateapi.ResourceMetadata{Atespace: testAtespace, Name: "with-volumes"},
			Scope:       ateapi.TagScope_TAG_SCOPE_ATESPACE,
			SourceActor: &ateapi.ObjectRef{Atespace: testAtespace, Name: "source"},
		},
		VolumeSelection: &ateapi.ExternalVolumeSelection{},
	})
	if err != nil {
		t.Fatalf("CreateTag failed: %v", err)
	}
	handles := tagVolumeHandles(tag)
	if len(handles) != 2 || handles["data"] == "" || handles["cache"] == "" {
		t.Fatalf("tag volume snapshots = %v, want a handle for each of data and cache", tag.GetStatus().GetSnapshot().GetVolumeSnapshots())
	}
	if got, want := tag.GetStatus().GetSnapshot().GetDurableSnapshot().GetState(), ateapi.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_COMPLETED; got != want {
		t.Errorf("tag snapshot status = %v, want %v", got, want)
	}

	tagRef := &ateapi.ObjectRef{Atespace: testAtespace, Name: "with-volumes"}
	if _, err := tc.client.DeleteTag(ctx, &ateapi.DeleteTagRequest{Tag: tagRef}); err != nil {
		t.Fatalf("DeleteTag failed: %v", err)
	}
	_, err = tc.client.GetTag(ctx, &ateapi.GetTagRequest{Tag: tagRef})
	if status.Code(err) != codes.NotFound {
		t.Errorf("GetTag() after DeleteTag = %v, want NotFound", err)
	}
}

// TestCreateTag_CapturesNamedVolumes verifies that a tag taken with a named
// subset of the volumes captures only those.
func TestCreateTag_CapturesNamedVolumes(t *testing.T) {
	ns := namespaceForTest("ns-tag-capture-named-volumes")
	tc, workerName := setupTagVolumesTest(t, ns)
	defer tc.cleanup()
	ctx := context.Background()

	suspendActorForTest(t, tc, workerName, "source")
	tag, err := tc.client.CreateTag(ctx, &ateapi.CreateTagRequest{
		Tag: &ateapi.Tag{
			Metadata:    &ateapi.ResourceMetadata{Atespace: testAtespace, Name: "data-only"},
			Scope:       ateapi.TagScope_TAG_SCOPE_ATESPACE,
			SourceActor: &ateapi.ObjectRef{Atespace: testAtespace, Name: "source"},
		},
		VolumeSelection: &ateapi.ExternalVolumeSelection{Names: []string{"data"}},
	})
	if err != nil {
		t.Fatalf("CreateTag failed: %v", err)
	}
	handles := tagVolumeHandles(tag)
	if len(handles) != 1 || handles["data"] == "" {
		t.Fatalf("tag volume snapshots = %v, want a handle for data only", tag.GetStatus().GetSnapshot().GetVolumeSnapshots())
	}
}

// TestCreateTag_UnknownVolumeName verifies that naming a volume the source
// Actor does not have fails the create and leaves no tag behind.
func TestCreateTag_UnknownVolumeName(t *testing.T) {
	ns := namespaceForTest("ns-tag-unknown-volume")
	tc, workerName := setupTagVolumesTest(t, ns)
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
