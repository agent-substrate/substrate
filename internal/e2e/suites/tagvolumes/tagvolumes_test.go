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

// Package tagvolumes tests tags that capture an Actor's external volumes, and
// Actors created from them, against a live cluster.
package tagvolumes

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/internal/e2e"
	"go.ate.dev/substrate/internal/preview"
	"go.ate.dev/substrate/internal/resources"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	atespace = "tagvolumes"

	dataVolume  = "data"
	cacheVolume = "cache"
	dataPath    = "/mnt/data"
	cachePath   = "/mnt/cache"
	extCapacity = 1 << 30

	// sourceFile is written by the source Actor before it is tagged.
	sourceFile = "source.txt"
	// cloneFile is written by an Actor created from the tag.
	cloneFile = "clone.txt"

	// probeWrittenContent is the fixed string the probe's /writefile writes.
	probeWrittenContent = "written by probe"

	// nfsDriver is the provisioner of the csi-nfs-sc StorageClass CI uses.
	nfsDriver = "nfs.csi.k8s.io"
)

type suite struct {
	ctx     context.Context
	clients *e2e.Clients
	ns      *e2e.Namespace
	tmpl    *ateapi.ActorTemplate
	router  *e2e.RouterClient
}

func TestTagVolumes(t *testing.T) {
	e2e.RequirePreview(t, preview.GateExternalVolumes)

	ctx := context.Background()
	clients := e2e.GetClients()
	if _, err := clients.K8s.StorageV1().StorageClasses().Get(ctx, e2e.StorageClass, metav1.GetOptions{}); err != nil {
		t.Fatalf("StorageClass %q (install with: go run ./cmd/ate-setup setup csi nfs): %v", e2e.StorageClass, err)
	}
	ns := e2e.CreateNamespace(t)
	router, err := e2e.NewRouterClient(t, ctx)
	if err != nil {
		t.Fatalf("NewRouterClient: %v", err)
	}
	defer router.Close()
	s := &suite{ctx: ctx, clients: clients, ns: ns, tmpl: createTemplate(ctx, t, clients, ns), router: router}

	// A tag of every volume seeds an Actor whose volumes start as copies of
	// the source's, and the copies outlive the tag.
	t.Run("RestoresEveryVolume", func(t *testing.T) {
		src := s.newSource(t, "all-src")
		tagRef := s.createTag(t, "all", src, &ateapi.ExternalVolumeSelection{}, dataVolume, cacheVolume)
		clone := s.newClone(t, "all-clone", tagRef)

		s.requireFile(t, clone, dataPath+"/"+sourceFile)
		s.requireFile(t, clone, cachePath+"/"+sourceFile)

		// The clone's volume is its own disk: a write to it does not reach
		// the source.
		s.write(t, clone, dataPath+"/"+cloneFile)
		s.resume(t, src)
		s.requireNoFile(t, src, dataPath+"/"+cloneFile)

		// The clone's volumes no longer depend on the tag's snapshots.
		s.deleteTag(t, tagRef)
		s.suspend(t, clone)
		s.resume(t, clone)
		s.requireFile(t, clone, dataPath+"/"+sourceFile)
		s.requireFile(t, clone, dataPath+"/"+cloneFile)
	})

	// A tag of named volumes restores those, and the clone's other volumes
	// start empty.
	t.Run("RestoresNamedVolumes", func(t *testing.T) {
		src := s.newSource(t, "named-src")
		tagRef := s.createTag(t, "named", src, &ateapi.ExternalVolumeSelection{Names: []string{dataVolume}}, dataVolume)
		clone := s.newClone(t, "named-clone", tagRef)

		s.requireFile(t, clone, dataPath+"/"+sourceFile)
		s.requireNoFile(t, clone, cachePath+"/"+sourceFile)
	})

	// A tag taken without volume_selection captures no volumes, so a clone's
	// volumes start empty.
	t.Run("WithoutVolumeSelectionRestoresNothing", func(t *testing.T) {
		src := s.newSource(t, "none-src")
		tagRef := s.createTag(t, "none", src, nil)
		clone := s.newClone(t, "none-clone", tagRef)

		s.requireNoFile(t, clone, dataPath+"/"+sourceFile)
		s.requireNoFile(t, clone, cachePath+"/"+sourceFile)
	})

	// Each Actor created from one tag gets its own copy of the volume.
	t.Run("ClonesAreIndependent", func(t *testing.T) {
		src := s.newSource(t, "multi-src")
		tagRef := s.createTag(t, "multi", src, &ateapi.ExternalVolumeSelection{}, dataVolume, cacheVolume)
		cloneA := s.newClone(t, "multi-a", tagRef)
		cloneB := s.newClone(t, "multi-b", tagRef)

		s.requireFile(t, cloneA, dataPath+"/"+sourceFile)
		s.requireFile(t, cloneB, dataPath+"/"+sourceFile)
		s.write(t, cloneA, dataPath+"/"+cloneFile)
		s.requireNoFile(t, cloneB, dataPath+"/"+cloneFile)
	})

	// The tag holds its own volume snapshots, so it restores them after the
	// source Actor and its volumes are deleted.
	t.Run("RestoresAfterSourceDeleted", func(t *testing.T) {
		src := s.newSource(t, "gone-src")
		tagRef := s.createTag(t, "gone", src, &ateapi.ExternalVolumeSelection{}, dataVolume, cacheVolume)
		if _, err := e2e.RetryAborted(t, ctx, clients.SubstrateAPI.DeleteActor, &ateapi.DeleteActorRequest{Actor: src.ToObjectRef()}); err != nil {
			t.Fatalf("DeleteActor %s: %v", src.Name, err)
		}
		clone := s.newClone(t, "gone-clone", tagRef)

		s.requireFile(t, clone, dataPath+"/"+sourceFile)
		s.requireFile(t, clone, cachePath+"/"+sourceFile)
	})

	// DeleteTag deletes the volume snapshots from the storage system. Only
	// checked for the NFS driver, whose snapshots can be listed on its
	// server.
	t.Run("DeleteTagReleasesSnapshots", func(t *testing.T) {
		src := s.newSource(t, "release-src")
		tagRef := s.createTag(t, "release", src, &ateapi.ExternalVolumeSelection{}, dataVolume, cacheVolume)
		tag, err := clients.SubstrateAPI.GetTag(ctx, &ateapi.GetTagRequest{Tag: tagRef})
		if err != nil {
			t.Fatalf("GetTag: %v", err)
		}
		var dirs []nfsSnapshotDir
		for _, snap := range tag.GetStatus().GetSnapshot().GetVolumeSnapshots() {
			if snap.GetVolumeType() != nfsDriver {
				t.Skipf("volume %q was snapshotted by %q; only %q snapshots can be listed", snap.GetSourceVolumeName(), snap.GetVolumeType(), nfsDriver)
			}
			dirs = append(dirs, parseNFSSnapshotID(t, snap.GetStorageSnapshotId()))
		}

		for _, dir := range dirs {
			if !nfsExportHas(t, dir) {
				t.Fatalf("NFS export has no snapshot directory %+v after CreateTag", dir)
			}
		}
		s.deleteTag(t, tagRef)
		for _, dir := range dirs {
			if nfsExportHas(t, dir) {
				t.Errorf("NFS export still has snapshot directory %+v after DeleteTag", dir)
			}
		}
	})
}

// createTemplate creates a probe ActorTemplate with the external volumes
// "data" and "cache", and a WorkerPool of two workers for its Actors.
func createTemplate(ctx context.Context, t *testing.T, clients *e2e.Clients, ns *e2e.Namespace) *ateapi.ActorTemplate {
	t.Helper()

	probeAtespace, _ := e2e.DeployProbe(t, "tagvolumes")
	src := e2e.SubstrateFixture{
		Atespace:      probeAtespace,
		Name:          e2e.ProbeName,
		PoolNamespace: probeAtespace,
		PoolName:      e2e.ProbeName,
		DeployWith:    "the tagvolumes suite's own DeployProbe",
	}
	return e2e.CreateSubstrateTemplateFrom(ctx, t, clients, ns.Name, src, e2e.SubstrateTemplateOptions{
		Atespace:     atespace,
		Name:         "probe-" + ns.Name,
		PoolName:     e2e.ProbeName,
		PoolReplicas: 2,
		// The pool is labeled uniquely to this namespace so the cluster-wide
		// scheduler cannot hand its workers to another suite's actors.
		Labels: map[string]string{"tagvolumes": ns.Name},
		SnapshotConfig: &ateapi.SnapshotConfig{
			StorageLocation: e2e.SnapshotLocation(t, ns.Name),
		},
		Modify: func(tmpl *ateapi.ActorTemplate) {
			for name, path := range map[string]string{dataVolume: dataPath, cacheVolume: cachePath} {
				tmpl.Containers[0].VolumeMounts = append(tmpl.Containers[0].VolumeMounts, &ateapi.VolumeMount{Name: name, MountPath: path})
				tmpl.Volumes = append(tmpl.Volumes, &ateapi.Volume{
					Name: name,
					External: &ateapi.ExternalVolumeSource{Template: &ateapi.ExternalVolumeTemplate{
						CapacityBytes:    extCapacity,
						StorageClassName: e2e.StorageClass,
					}},
				})
			}
		},
	})
}

// actorRef names an Actor of this test run in the suite's atespace.
func (s *suite) actorRef(name string) resources.ActorRef {
	return resources.ActorRef{Atespace: atespace, Name: name + "-" + s.ns.Name}
}

// createActor creates an Actor, from tagRef when it is set, and deletes it
// when the test ends.
func (s *suite) createActor(t *testing.T, ref resources.ActorRef, tagRef *ateapi.ObjectRef) {
	t.Helper()

	if _, err := s.clients.SubstrateAPI.CreateActor(s.ctx, &ateapi.CreateActorRequest{
		Actor: &ateapi.Actor{
			Metadata:      &ateapi.ResourceMetadata{Atespace: ref.Atespace, Name: ref.Name},
			ActorTemplate: e2e.TemplateRef(s.tmpl),
			SourceTag:     tagRef,
		},
	}); err != nil {
		t.Fatalf("CreateActor %s: %v", ref.Name, err)
	}
	t.Cleanup(func() {
		cleanupCtx := context.Background()
		_, _ = s.clients.SubstrateAPI.SuspendActor(cleanupCtx, &ateapi.SuspendActorRequest{Actor: ref.ToObjectRef()})
		_, _ = s.clients.SubstrateAPI.DeleteActor(cleanupCtx, &ateapi.DeleteActorRequest{Actor: ref.ToObjectRef()})
	})
}

// newSource creates and resumes an Actor, writes sourceFile to both of its
// volumes, and suspends it so it can be tagged.
func (s *suite) newSource(t *testing.T, name string) resources.ActorRef {
	t.Helper()

	ref := s.actorRef(name)
	s.createActor(t, ref, nil)
	s.resume(t, ref)
	s.write(t, ref, dataPath+"/"+sourceFile)
	s.write(t, ref, cachePath+"/"+sourceFile)
	s.suspend(t, ref)
	return ref
}

// newClone creates an Actor from tagRef and resumes it.
func (s *suite) newClone(t *testing.T, name string, tagRef *ateapi.ObjectRef) resources.ActorRef {
	t.Helper()

	ref := s.actorRef(name)
	s.createActor(t, ref, tagRef)
	s.resumeFromTag(t, ref)
	return ref
}

// createTag tags the suspended source Actor with selection, and deletes the
// tag when the test ends. It fails unless the tag recorded a snapshot handle
// for each of wantVolumes and no others.
func (s *suite) createTag(t *testing.T, name string, source resources.ActorRef, selection *ateapi.ExternalVolumeSelection, wantVolumes ...string) *ateapi.ObjectRef {
	t.Helper()

	tagRef := &ateapi.ObjectRef{Atespace: atespace, Name: name + "-" + s.ns.Name}
	t.Cleanup(func() {
		_, _ = s.clients.SubstrateAPI.DeleteTag(context.Background(), &ateapi.DeleteTagRequest{Tag: tagRef})
	})
	tag, err := e2e.RetryAborted(t, s.ctx, s.clients.SubstrateAPI.CreateTag, &ateapi.CreateTagRequest{
		Tag: &ateapi.Tag{
			Metadata:    &ateapi.ResourceMetadata{Atespace: tagRef.GetAtespace(), Name: tagRef.GetName()},
			Scope:       ateapi.TagScope_TAG_SCOPE_ATESPACE,
			SourceActor: source.ToObjectRef(),
		},
		VolumeSelection: selection,
	})
	if err != nil {
		t.Fatalf("CreateTag %s: %v", tagRef.GetName(), err)
	}

	got := map[string]bool{}
	for _, snap := range tag.GetStatus().GetSnapshot().GetVolumeSnapshots() {
		if snap.GetStorageSnapshotId() == "" {
			t.Errorf("tag %s volume %q has no snapshot handle", tagRef.GetName(), snap.GetSourceVolumeName())
		}
		got[snap.GetSourceVolumeName()] = true
	}
	if len(got) != len(wantVolumes) {
		t.Fatalf("tag %s captured volumes %v, want %v", tagRef.GetName(), got, wantVolumes)
	}
	for _, v := range wantVolumes {
		if !got[v] {
			t.Fatalf("tag %s captured volumes %v, want %v", tagRef.GetName(), got, wantVolumes)
		}
	}
	return tagRef
}

func (s *suite) deleteTag(t *testing.T, tagRef *ateapi.ObjectRef) {
	t.Helper()

	if _, err := s.clients.SubstrateAPI.DeleteTag(s.ctx, &ateapi.DeleteTagRequest{Tag: tagRef}); err != nil {
		t.Fatalf("DeleteTag %s: %v", tagRef.GetName(), err)
	}
}

func (s *suite) resume(t *testing.T, ref resources.ActorRef) {
	t.Helper()

	if _, err := e2e.ResumeActorAwaitCapacity(t, s.ctx, s.clients, &ateapi.ResumeActorRequest{Actor: ref.ToObjectRef()}); err != nil {
		t.Fatalf("ResumeActor %s: %v", ref.Name, err)
	}
}

// resumeFromTag resumes an Actor created from a tag with volume snapshots. A
// driver may return a snapshot before it is ready, and resume then fails until
// it is, so the resume is retried for that error only.
func (s *suite) resumeFromTag(t *testing.T, ref resources.ActorRef) {
	t.Helper()

	ctx, cancel := context.WithTimeout(s.ctx, 3*time.Minute)
	defer cancel()
	for {
		_, err := e2e.ResumeActorAwaitCapacity(t, ctx, s.clients, &ateapi.ResumeActorRequest{Actor: ref.ToObjectRef()})
		if err == nil {
			return
		}
		if status.Code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), "is not ready to use yet") {
			t.Fatalf("ResumeActor %s from tag: %v", ref.Name, err)
		}
		t.Logf("ResumeActor %s from tag: %v; retrying", ref.Name, err)
		select {
		case <-ctx.Done():
			t.Fatalf("ResumeActor %s from tag: snapshot not ready: %v", ref.Name, ctx.Err())
		case <-time.After(2 * time.Second):
		}
	}
}

// suspend suspends the Actor synchronously, so its volumes are detached when
// it returns.
func (s *suite) suspend(t *testing.T, ref resources.ActorRef) {
	t.Helper()

	if _, err := s.clients.SubstrateAPI.SuspendActor(s.ctx, &ateapi.SuspendActorRequest{
		Actor:   ref.ToObjectRef(),
		Options: &ateapi.SuspendOptions{Mode: ateapi.DurabilityMode_DURABILITY_MODE_SYNCHRONOUS},
	}); err != nil {
		t.Fatalf("SuspendActor %s: %v", ref.Name, err)
	}
}

// probe calls a probe endpoint through the router and decodes its reply.
func (s *suite) probe(t *testing.T, ref resources.ActorRef, path string) map[string]string {
	t.Helper()

	resp, err := s.router.Get(s.ctx, ref, path)
	if err != nil {
		t.Fatalf("GET %s on %s: %v", path, ref.Name, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("GET %s on %s: status %d: %s", path, ref.Name, resp.StatusCode, body)
	}
	var out map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decoding %s on %s: %v", path, ref.Name, err)
	}
	return out
}

func (s *suite) write(t *testing.T, ref resources.ActorRef, path string) {
	t.Helper()

	if got := s.probe(t, ref, "/writefile?path="+path); got["error"] != "" {
		t.Fatalf("writing %s on %s: %s", path, ref.Name, got["error"])
	}
}

func (s *suite) requireFile(t *testing.T, ref resources.ActorRef, path string) {
	t.Helper()

	got := s.probe(t, ref, "/readfile?path="+path)
	if got["error"] != "" {
		t.Fatalf("reading %s on %s: %s", path, ref.Name, got["error"])
	}
	if got["content"] != probeWrittenContent {
		t.Errorf("content of %s on %s = %q, want %q", path, ref.Name, got["content"], probeWrittenContent)
	}
}

func (s *suite) requireNoFile(t *testing.T, ref resources.ActorRef, path string) {
	t.Helper()

	if got := s.probe(t, ref, "/readfile?path="+path); got["error"] == "" {
		t.Errorf("%s on %s is readable (%q), want it absent", path, ref.Name, got["content"])
	}
}

// nfsSnapshotDir is where the NFS CSI driver stores a snapshot: the archive
// directory name under baseDir, relative to the export.
type nfsSnapshotDir struct {
	baseDir string
	name    string
}

// parseNFSSnapshotID splits an NFS CSI driver snapshot ID, which has the form
// server#baseDir#name#archiveDir#sourceVolume. The driver writes the archive
// under baseDir/archiveDir.
func parseNFSSnapshotID(t *testing.T, id string) nfsSnapshotDir {
	t.Helper()

	parts := strings.Split(id, "#")
	if len(parts) != 5 || parts[3] == "" {
		t.Fatalf("snapshot ID %q is not an NFS CSI driver snapshot ID", id)
	}
	return nfsSnapshotDir{baseDir: parts[1], name: parts[3]}
}

// nfsExportHas reports whether dir exists on the NFS server csi-nfs-sc
// provisions from, which `ate-setup setup csi nfs` deploys as
// default/nfs-server exporting /exports.
func nfsExportHas(t *testing.T, dir nfsSnapshotDir) bool {
	t.Helper()

	out := e2e.RunCmdOutput(t, nil, "kubectl", "exec", "-n", "default", "deploy/nfs-server", "--",
		"ls", "-1", "/exports/"+dir.baseDir)
	for _, entry := range strings.Fields(string(out)) {
		if entry == dir.name {
			return true
		}
	}
	return false
}
