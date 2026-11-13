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
	"go.ate.dev/substrate/internal/preview"
	"go.ate.dev/substrate/internal/volume"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// restoreRecordingPlugin is the mock volume plugin, recording the snapshot
// each volume was created from, keyed by the volume's name in the storage
// system.
type restoreRecordingPlugin struct {
	*volume.MockVolumePlugin

	mu      sync.Mutex
	sources map[string]string
}

func (p *restoreRecordingPlugin) CreateVolume(ctx context.Context, req volume.CreateVolumeRequest) (volume.CreateVolumeResponse, error) {
	p.mu.Lock()
	p.sources[req.Name] = req.SourceSnapshotID
	p.mu.Unlock()
	return p.MockVolumePlugin.CreateVolume(ctx, req)
}

// restoreSources returns the snapshot each named volume of actor was created
// from, "" for one provisioned empty. It fails the test if a volume was never
// created.
func (p *restoreRecordingPlugin) restoreSources(t *testing.T, actor *ateapi.Actor, volNames ...string) map[string]string {
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
func setupTagVolumesTest(t *testing.T, ns string) (*testContext, *restoreRecordingPlugin, string) {
	t.Helper()
	preview.InitForTest(t, "ExternalVolumes")
	plugin := &restoreRecordingPlugin{MockVolumePlugin: volume.NewMockVolumePlugin(), sources: map[string]string{}}
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

// cloneAndResume creates an Actor from tagName and resumes it, which
// provisions its volumes.
func cloneAndResume(t *testing.T, tc *testContext, workerName, tagName, name string) *ateapi.Actor {
	t.Helper()
	ctx := context.Background()
	if _, err := tc.client.CreateActor(ctx, &ateapi.CreateActorRequest{Actor: &ateapi.Actor{
		Metadata:      &ateapi.ResourceMetadata{Atespace: testAtespace, Name: name},
		ActorTemplate: &ateapi.ObjectRef{Atespace: testAtespace, Name: "tmpl1"},
		SourceTag:     &ateapi.ObjectRef{Atespace: testAtespace, Name: tagName},
	}}); err != nil {
		t.Fatalf("CreateActor(%s) from tag %s failed: %v", name, tagName, err)
	}
	waitForWorkerAvailable(t, tc, workerName)
	resumed, err := tc.client.ResumeActor(ctx, &ateapi.ResumeActorRequest{
		Actor: &ateapi.ObjectRef{Atespace: testAtespace, Name: name},
	})
	if err != nil {
		t.Fatalf("ResumeActor(%s) failed: %v", name, err)
	}
	return resumed.GetActor()
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
// snapshots each of them and records the handles, that an Actor created from it
// restores each volume from its snapshot on its first resume, and that the tag
// can then be deleted.
func TestCreateTag_CapturesVolumes(t *testing.T) {
	ns := namespaceForTest("ns-tag-capture-volumes")
	tc, plugin, workerName := setupTagVolumesTest(t, ns)
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

	clone := cloneAndResume(t, tc, workerName, "with-volumes", "clone")
	if diff := cmp.Diff(handles, plugin.restoreSources(t, clone, "data", "cache")); diff != "" {
		t.Errorf("restore sources mismatch (-tag handles +restored from):\n%s", diff)
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
// subset of the volumes captures only those, and that an Actor created from it
// restores them and provisions the rest empty.
func TestCreateTag_CapturesNamedVolumes(t *testing.T) {
	ns := namespaceForTest("ns-tag-capture-named-volumes")
	tc, plugin, workerName := setupTagVolumesTest(t, ns)
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

	clone := cloneAndResume(t, tc, workerName, "data-only", "clone")
	want := map[string]string{"data": handles["data"], "cache": ""}
	if diff := cmp.Diff(want, plugin.restoreSources(t, clone, "data", "cache")); diff != "" {
		t.Errorf("restore sources mismatch (-want +got):\n%s", diff)
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
