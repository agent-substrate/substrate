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

package functionaltest

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/ateapi/internal/functionaltest/harness"
	"go.ate.dev/substrate/internal/preview"
	"go.ate.dev/substrate/internal/proto/ateletpb"
	"go.ate.dev/substrate/internal/resources"
)

// certificateSigningRequest is a valid CSR, for minting certificates.
func certificateSigningRequest(t *testing.T) []byte {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generating a key: %v", err)
	}
	csr, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{Subject: pkix.Name{CommonName: "actor"}}, key)
	if err != nil {
		t.Fatalf("creating a CSR: %v", err)
	}
	return csr
}

// resumingActor creates an actor from tmpl and leaves it RESUMING on w: its
// Restore fails with UNAVAILABLE, which ate-api leaves for a retry.
func resumingActor(t *testing.T, h *harness.Harness, as string, tmpl *ateapi.ActorTemplate, w *harness.Worker) *ateapi.Actor {
	t.Helper()
	actor := h.NewActor(as, tmpl)
	w.Node.Atelet.On(harness.AteletRestore).FailNext(harness.ErrUnavailable)
	if _, err := h.ResumeActor(t.Context(), &ateapi.ResumeActorRequest{Actor: harness.ActorRef(actor)}); err == nil {
		t.Fatal("ResumeActor succeeded despite an unavailable atelet")
	}
	resuming := h.GetActor(harness.ActorRef(actor))
	if got := resuming.GetStatus().GetState(); got != ateapi.ActorState_ACTOR_STATE_RESUMING {
		t.Fatalf("actor is %v after an unavailable atelet, want RESUMING", got)
	}
	return resuming
}

// workerAssignment is the assignment ate-api records on an actor placed on w.
func workerAssignment(w *harness.Worker) *ateapi.WorkerAssignment {
	return &ateapi.WorkerAssignment{
		Worker:          w.Ref(),
		WorkerNamespace: w.Namespace,
		WorkerPool:      w.Pool.Name,
		WorkerPod:       w.Pod,
		WorkerPodUid:    w.Name,
		WorkerPodIps:    []string{w.IP},
		NodeName:        w.Node.Name,
		WorkerEpoch:     w.Epoch,
		SandboxRuntime:  harness.SandboxRuntime(),
	}
}

// templateResources is the size of the actors of harness templates.
func templateResources() *ateapi.Resources {
	return resources.CPUMemory(harness.TemplateCPUMillicores, harness.TemplateMemoryBytes)
}

// latestSnapshot is the actor's snapshot with the highest generation.
func latestSnapshot(t *testing.T, actor *ateapi.Actor) *ateapi.Snapshot {
	t.Helper()
	var latest *ateapi.Snapshot
	for _, s := range actor.GetStatus().GetSnapshots() {
		if s.GetGeneration() >= latest.GetGeneration() {
			latest = s
		}
	}
	if latest == nil {
		t.Fatalf("actor %s has no snapshot", actor.GetMetadata().GetName())
	}
	return latest
}

// actorSnapshot is the snapshot a suspend of actor records on node, given the
// UUID ate-api picked for it.
func actorSnapshot(actor *ateapi.Actor, uuid string, generation int32, node string, state ateapi.SnapshotStorageState) *ateapi.Snapshot {
	return &ateapi.Snapshot{
		Uuid:             uuid,
		Generation:       generation,
		Owner:            ateapi.SnapshotOwner_SNAPSHOT_OWNER_ACTOR,
		Fidelity:         ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
		ActorTemplateUid: actor.GetStatus().GetSnapshots()[0].GetActorTemplateUid(),
		Locality:         node,
		DurableSnapshot: &ateapi.SnapshotStorage{
			State:  state,
			Object: &ateapi.ObjectSnapshot{SnapshotUri: snapshotURI(actor, uuid)},
		},
		SandboxRuntime: harness.SandboxRuntime(),
	}
}

// snapshotURI is where ate-api stores actor's snapshot uuid.
func snapshotURI(actor *ateapi.Actor, uuid string) string {
	return harness.StorageLocation + "/atespaces/" + actor.GetMetadata().GetAtespace() + "/actors/" + actor.GetMetadata().GetUid() + "/snapshots/" + uuid
}

// snapshotObjects are the objects the fake atelet uploads for the snapshot at
// uri.
func snapshotObjects(uri string) []string {
	prefix := strings.TrimPrefix(uri, "gs://")
	return []string{prefix + "/manifest.json", prefix + "/memory.zst"}
}

// assertRestoreOf checks that req restores snapshot from its durable copy.
func assertRestoreOf(t *testing.T, req *ateletpb.RestoreRequest, snapshot *ateapi.Snapshot) {
	t.Helper()
	if got, want := req.GetSnapshot().GetSnapshotUuid(), snapshot.GetUuid(); got != want {
		t.Errorf("Restore snapshot = %q, want %q", got, want)
	}
	if got, want := req.GetSnapshot().GetObject().GetSnapshotUri(), snapshot.GetDurableSnapshot().GetObject().GetSnapshotUri(); got != want {
		t.Errorf("Restore snapshot URI = %q, want %q", got, want)
	}
}

// Creates an actor and verifies that the whole resource has the expected
// shape.
func TestCreateActor(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	h.AddWorker(pool, h.Node("a"))
	tmpl := h.NewTemplate(as, pool)
	golden, err := h.Control.GetTag(t.Context(), &ateapi.GetTagRequest{Tag: tmpl.GetStatus().GetGoldenSnapshotStatus().GetGoldenTag()})
	if err != nil {
		t.Fatalf("GetTag: %v", err)
	}
	templateRef := &ateapi.ObjectRef{Atespace: as, Name: tmpl.GetMetadata().GetName()}
	goldenSnapshot := proto.CloneOf(golden.GetStatus().GetSnapshot())
	goldenSnapshot.ActorTemplateUid = tmpl.GetMetadata().GetUid()

	created, err := h.Control.CreateActor(t.Context(), &ateapi.CreateActorRequest{Actor: &ateapi.Actor{
		Metadata:      &ateapi.ResourceMetadata{Atespace: as, Name: "actor-a"},
		ActorTemplate: templateRef,
	}})
	if err != nil {
		t.Fatalf("CreateActor: %v", err)
	}
	want := &ateapi.Actor{
		Metadata:      &ateapi.ResourceMetadata{Atespace: as, Name: "actor-a"},
		ActorTemplate: templateRef,
		Status: &ateapi.ActorStatus{
			State:                  ateapi.ActorState_ACTOR_STATE_SUSPENDED,
			Snapshots:              []*ateapi.Snapshot{goldenSnapshot},
			LastAssignedGeneration: goldenSnapshot.GetGeneration(),
		},
	}
	if diff := cmp.Diff(want, created, protocmp.Transform(), ignoreServerMetadata); diff != "" {
		t.Errorf("created actor (-want +got):\n%s", diff)
	}
}

// Creating an actor with the name of an existing one fails with
// ALREADY_EXISTS.
func TestCreateActor_AlreadyExists(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	h.AddWorker(pool, h.Node("a"))
	tmpl := h.NewTemplate(as, pool)
	templateRef := &ateapi.ObjectRef{Atespace: as, Name: tmpl.GetMetadata().GetName()}
	created, err := h.Control.CreateActor(t.Context(), &ateapi.CreateActorRequest{Actor: &ateapi.Actor{
		Metadata:      &ateapi.ResourceMetadata{Atespace: as, Name: "actor-a"},
		ActorTemplate: templateRef,
	}})
	if err != nil {
		t.Fatalf("CreateActor: %v", err)
	}

	_, err = h.Control.CreateActor(t.Context(), &ateapi.CreateActorRequest{Actor: &ateapi.Actor{
		Metadata:      &ateapi.ResourceMetadata{Atespace: as, Name: "actor-a"},
		ActorTemplate: templateRef,
	}})
	if status.Code(err) != codes.AlreadyExists {
		t.Errorf("CreateActor of an existing name = %v, want AlreadyExists", err)
	}
	got, err := h.Control.GetActor(t.Context(), &ateapi.GetActorRequest{Actor: &ateapi.ObjectRef{Atespace: as, Name: "actor-a"}})
	if err != nil {
		t.Fatalf("GetActor: %v", err)
	}
	if diff := cmp.Diff(created, got, protocmp.Transform()); diff != "" {
		t.Errorf("actor after a refused create (-want +got):\n%s", diff)
	}
}

// Creating an actor with an invalid name fails with INVALID_ARGUMENT and the
// error names the invalid field.
func TestCreateActor_ValidationError(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	h.AddWorker(pool, h.Node("a"))
	tmpl := h.NewTemplate(as, pool)

	_, err := h.Control.CreateActor(t.Context(), &ateapi.CreateActorRequest{Actor: &ateapi.Actor{
		Metadata:      &ateapi.ResourceMetadata{Atespace: as, Name: "Not_A_Valid_Name"},
		ActorTemplate: &ateapi.ObjectRef{Atespace: as, Name: tmpl.GetMetadata().GetName()},
	}})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("CreateActor with an invalid name = %v, want InvalidArgument", err)
	}
	if msg := status.Convert(err).Message(); !strings.Contains(msg, "actor.metadata.name") {
		t.Errorf("CreateActor error %q does not name the invalid field actor.metadata.name", msg)
	}
	listed, err := h.Control.ListActors(t.Context(), &ateapi.ListActorsRequest{Atespace: as})
	if err != nil {
		t.Fatalf("ListActors: %v", err)
	}
	if diff := cmp.Diff(&ateapi.ListActorsResponse{}, listed, protocmp.Transform()); diff != "" {
		t.Errorf("actors after a refused create (-want +got):\n%s", diff)
	}
}

// Creating an actor that references an actor template that doesn't exist
// fails with FAILED_PRECONDITION.
func TestCreateActor_TemplateNotFound(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")

	_, err := h.Control.CreateActor(t.Context(), &ateapi.CreateActorRequest{Actor: &ateapi.Actor{
		Metadata:      &ateapi.ResourceMetadata{Atespace: as, Name: "actor-a"},
		ActorTemplate: &ateapi.ObjectRef{Atespace: as, Name: "tmpl-missing"},
	}})
	if status.Code(err) != codes.FailedPrecondition {
		t.Errorf("CreateActor with a missing template = %v, want FailedPrecondition", err)
	}
	listed, err := h.Control.ListActors(t.Context(), &ateapi.ListActorsRequest{Atespace: as})
	if err != nil {
		t.Fatalf("ListActors: %v", err)
	}
	if diff := cmp.Diff(&ateapi.ListActorsResponse{}, listed, protocmp.Transform()); diff != "" {
		t.Errorf("actors after a refused create (-want +got):\n%s", diff)
	}
}

// Creating an actor with an external volume whose storage class doesn't
// exist fails with FAILED_PRECONDITION.
func TestCreateActor_StorageClassNotFound(t *testing.T) {
	t.Parallel()
	h := harness.New(t, harness.WithPreviewClient())
	as := h.CreateAtespace("team-a")
	if _, err := h.Control.CreateActorTemplate(t.Context(), &ateapi.CreateActorTemplateRequest{ActorTemplate: &ateapi.ActorTemplate{
		Metadata: &ateapi.ResourceMetadata{Atespace: as, Name: "tmpl-a"},
		Containers: []*ateapi.Container{{
			Name:         "main",
			Image:        harness.Image,
			Command:      []string{"/main"},
			VolumeMounts: []*ateapi.VolumeMount{{Name: "data", MountPath: "/data"}},
		}},
		Volumes: []*ateapi.Volume{{
			Name: "data",
			External: &ateapi.ExternalVolumeSource{Template: &ateapi.ExternalVolumeTemplate{
				StorageClassName: "storage-class-missing",
				CapacityBytes:    1 << 30,
			}},
		}},
		WorkerSelector: &ateapi.Selector{MatchLabels: map[string]string{"pool": "pool1"}},
		Resources:      &ateapi.ResourceRequirements{Limits: templateResources()},
	}}); err != nil {
		t.Fatalf("CreateActorTemplate: %v", err)
	}

	_, err := h.Control.CreateActor(t.Context(), &ateapi.CreateActorRequest{Actor: &ateapi.Actor{
		Metadata:      &ateapi.ResourceMetadata{Atespace: as, Name: "actor-a"},
		ActorTemplate: &ateapi.ObjectRef{Atespace: as, Name: "tmpl-a"},
	}})
	if status.Code(err) != codes.FailedPrecondition {
		t.Errorf("CreateActor with a missing storage class = %v, want FailedPrecondition", err)
	}
	listed, err := h.Control.ListActors(t.Context(), &ateapi.ListActorsRequest{Atespace: as})
	if err != nil {
		t.Fatalf("ListActors: %v", err)
	}
	if diff := cmp.Diff(&ateapi.ListActorsResponse{}, listed, protocmp.Transform()); diff != "" {
		t.Errorf("actors after a refused create (-want +got):\n%s", diff)
	}
}

// Creating an actor in an atespace that doesn't exist fails with
// FAILED_PRECONDITION.
func TestCreateActor_AtespaceNotFound(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	h.AddWorker(pool, h.Node("a"))
	tmpl := h.NewTemplate(as, pool)

	_, err := h.Control.CreateActor(t.Context(), &ateapi.CreateActorRequest{Actor: &ateapi.Actor{
		Metadata:      &ateapi.ResourceMetadata{Atespace: "team-missing", Name: "actor-a"},
		ActorTemplate: &ateapi.ObjectRef{Atespace: as, Name: tmpl.GetMetadata().GetName()},
	}})
	if status.Code(err) != codes.FailedPrecondition {
		t.Errorf("CreateActor in a missing atespace = %v, want FailedPrecondition", err)
	}
	listed, err := h.Control.ListActors(t.Context(), &ateapi.ListActorsRequest{})
	if err != nil {
		t.Fatalf("ListActors: %v", err)
	}
	if diff := cmp.Diff(&ateapi.ListActorsResponse{}, listed, protocmp.Transform()); diff != "" {
		t.Errorf("actors after a refused create (-want +got):\n%s", diff)
	}
}

// Creating an actor that references a tag that doesn't exist fails with
// FAILED_PRECONDITION.
func TestCreateActor_TagNotFound(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	h.AddWorker(pool, h.Node("a"))
	tmpl := h.NewTemplate(as, pool)

	_, err := h.Control.CreateActor(t.Context(), &ateapi.CreateActorRequest{Actor: &ateapi.Actor{
		Metadata:      &ateapi.ResourceMetadata{Atespace: as, Name: "actor-a"},
		ActorTemplate: &ateapi.ObjectRef{Atespace: as, Name: tmpl.GetMetadata().GetName()},
		SourceTag:     &ateapi.ObjectRef{Atespace: as, Name: "tag-missing"},
	}})
	if status.Code(err) != codes.FailedPrecondition {
		t.Errorf("CreateActor with a missing source tag = %v, want FailedPrecondition", err)
	}
	listed, err := h.Control.ListActors(t.Context(), &ateapi.ListActorsRequest{Atespace: as})
	if err != nil {
		t.Fatalf("ListActors: %v", err)
	}
	if diff := cmp.Diff(&ateapi.ListActorsResponse{}, listed, protocmp.Transform()); diff != "" {
		t.Errorf("actors after a refused create (-want +got):\n%s", diff)
	}
}

// Getting an existing actor returns exactly the resource CreateActor
// returned.
func TestGetActor(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	h.AddWorker(pool, h.Node("a"))
	actor := h.NewActor(as, h.NewTemplate(as, pool))

	got, err := h.Control.GetActor(t.Context(), &ateapi.GetActorRequest{Actor: harness.ActorRef(actor)})
	if err != nil {
		t.Fatalf("GetActor: %v", err)
	}
	if diff := cmp.Diff(actor, got, protocmp.Transform()); diff != "" {
		t.Errorf("GetActor (-created +got):\n%s", diff)
	}
}

// Getting an actor that doesn't exist fails with NOT_FOUND.
func TestGetActor_NotFound(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")

	_, err := h.Control.GetActor(t.Context(), &ateapi.GetActorRequest{Actor: &ateapi.ObjectRef{Atespace: as, Name: "actor-missing"}})
	if status.Code(err) != codes.NotFound {
		t.Errorf("GetActor of a missing actor = %v, want NotFound", err)
	}
}

// Getting an actor with an invalid name fails with INVALID_ARGUMENT and the
// error names the invalid field.
func TestGetActor_ValidationError(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")

	_, err := h.Control.GetActor(t.Context(), &ateapi.GetActorRequest{Actor: &ateapi.ObjectRef{Atespace: as, Name: "Not_A_Valid_Name"}})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("GetActor with an invalid name = %v, want InvalidArgument", err)
	}
	if msg := status.Convert(err).Message(); !strings.Contains(msg, "actor.name") {
		t.Errorf("GetActor error %q does not name the invalid field actor.name", msg)
	}
}

// Creates N actors in one atespace and one in another, then lists the first
// atespace with pages smaller than N. The pages together hold exactly its N
// actors.
func TestListActors(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	other := h.CreateAtespace("team-b")
	pool := h.CreateWorkerPool("pool1", nil)
	h.AddWorker(pool, h.Node("a"))
	tmpl := h.NewTemplate(as, pool)
	otherTmpl := h.NewTemplate(other, pool)
	const n, pageSize = 5, 2
	var created []*ateapi.Actor
	for range n {
		created = append(created, h.NewActor(as, tmpl))
	}
	h.NewActor(other, otherTmpl)

	var listed []*ateapi.Actor
	req := &ateapi.ListActorsRequest{Atespace: as, Options: &ateapi.ListOptions{PageSize: pageSize}}
	for pages := 1; ; pages++ {
		if pages > n {
			t.Fatalf("listing took more than %d pages", n)
		}
		resp, err := h.Control.ListActors(t.Context(), req)
		if err != nil {
			t.Fatalf("ListActors page %d: %v", pages, err)
		}
		if got := len(resp.GetActors()); got > pageSize {
			t.Errorf("page %d holds %d actors, want at most %d", pages, got, pageSize)
		}
		listed = append(listed, resp.GetActors()...)
		if resp.GetNextPageToken() == "" {
			break
		}
		req.Options.PageToken = resp.GetNextPageToken()
	}
	byName := cmpopts.SortSlices(func(a, b *ateapi.Actor) bool { return a.GetMetadata().GetName() < b.GetMetadata().GetName() })
	if diff := cmp.Diff(created, listed, protocmp.Transform(), byName); diff != "" {
		t.Errorf("listed actors (-want +got):\n%s", diff)
	}
}

// Listing actors with a negative page size fails with INVALID_ARGUMENT and
// the error names the invalid field.
func TestListActors_ValidationError(t *testing.T) {
	t.Parallel()
	h := harness.New(t)

	_, err := h.Control.ListActors(t.Context(), &ateapi.ListActorsRequest{Options: &ateapi.ListOptions{PageSize: -1}})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("ListActors with a negative page size = %v, want InvalidArgument", err)
	}
	if msg := status.Convert(err).Message(); !strings.Contains(msg, "page_size") {
		t.Errorf("ListActors error %q does not name the invalid field page_size", msg)
	}
}

// Listing actors with a malformed page token fails with INVALID_ARGUMENT.
func TestListActors_InvalidPageToken(t *testing.T) {
	t.Parallel()
	h := harness.New(t)

	_, err := h.Control.ListActors(t.Context(), &ateapi.ListActorsRequest{Options: &ateapi.ListOptions{PageToken: "not-a-real-token"}})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("ListActors with a malformed page token = %v, want InvalidArgument", err)
	}
}

// Listing actors in an atespace that doesn't exist succeeds with an empty
// list.
func TestListActors_AtespaceNotFound(t *testing.T) {
	t.Parallel()
	h := harness.New(t)

	listed, err := h.Control.ListActors(t.Context(), &ateapi.ListActorsRequest{Atespace: "team-missing"})
	if err != nil {
		t.Fatalf("ListActors in a missing atespace: %v", err)
	}
	if diff := cmp.Diff(&ateapi.ListActorsResponse{}, listed, protocmp.Transform()); diff != "" {
		t.Errorf("ListActors in a missing atespace (-want +got):\n%s", diff)
	}
}

// Updates the worker selector of an existing actor, sending it as read. The
// result, and a later Get, is the resource as read with that field changed
// and a higher version.
func TestUpdateActor(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	h.AddWorker(pool, h.Node("a"))
	actor := h.NewActor(as, h.NewTemplate(as, pool))
	ref := harness.ActorRef(actor)

	before := h.GetActor(ref)
	toUpdate := proto.Clone(before).(*ateapi.Actor)
	toUpdate.WorkerSelector = &ateapi.Selector{MatchLabels: map[string]string{"tier": "1"}}
	updated, err := h.Control.UpdateActor(t.Context(), &ateapi.UpdateActorRequest{Actor: toUpdate})
	if err != nil {
		t.Fatalf("UpdateActor: %v", err)
	}

	want := proto.Clone(before).(*ateapi.Actor)
	want.WorkerSelector = &ateapi.Selector{MatchLabels: map[string]string{"tier": "1"}}
	if diff := cmp.Diff(want, updated, protocmp.Transform(), ignoreVersion, ignoreTimestamps); diff != "" {
		t.Errorf("updated actor (-want +got):\n%s", diff)
	}
	if got, was := updated.GetMetadata().GetVersion(), before.GetMetadata().GetVersion(); got <= was {
		t.Errorf("updated actor has version %d, want higher than %d", got, was)
	}
	if diff := cmp.Diff(updated, h.GetActor(ref), protocmp.Transform()); diff != "" {
		t.Errorf("GetActor after update (-updated +got):\n%s", diff)
	}
}

// Updating an actor that doesn't exist fails with NOT_FOUND.
func TestUpdateActor_NotFound(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	h.AddWorker(pool, h.Node("a"))
	tmpl := h.NewTemplate(as, pool)

	_, err := h.Control.UpdateActor(t.Context(), &ateapi.UpdateActorRequest{Actor: &ateapi.Actor{
		Metadata:      &ateapi.ResourceMetadata{Atespace: as, Name: "actor-missing", Uid: foreignUID, Version: 1},
		ActorTemplate: &ateapi.ObjectRef{Atespace: as, Name: tmpl.GetMetadata().GetName()},
	}})
	if status.Code(err) != codes.NotFound {
		t.Errorf("UpdateActor of a missing actor = %v, want NotFound", err)
	}
}

// Updating an actor with the wrong UID fails with ABORTED, and with the
// right UID but the wrong version fails with ABORTED; both leave it
// unchanged. Updating it without a UID or a version fails with
// INVALID_ARGUMENT.
func TestUpdateActor_WithPreconditions(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	h.AddWorker(pool, h.Node("a"))
	actor := h.NewActor(as, h.NewTemplate(as, pool))
	ref := harness.ActorRef(actor)
	before := h.GetActor(ref)
	update := func(uid string, version int64) error {
		toUpdate := proto.Clone(before).(*ateapi.Actor)
		toUpdate.Metadata.Uid, toUpdate.Metadata.Version = uid, version
		toUpdate.WorkerSelector = &ateapi.Selector{MatchLabels: map[string]string{"tier": "1"}}
		_, err := h.Control.UpdateActor(t.Context(), &ateapi.UpdateActorRequest{Actor: toUpdate})
		return err
	}

	if err := update(foreignUID, before.GetMetadata().GetVersion()); status.Code(err) != codes.Aborted {
		t.Errorf("UpdateActor with the wrong uid = %v, want Aborted", err)
	}
	if err := update(before.GetMetadata().GetUid(), before.GetMetadata().GetVersion()+1); status.Code(err) != codes.Aborted {
		t.Errorf("UpdateActor with the right uid and the wrong version = %v, want Aborted", err)
	}
	if diff := cmp.Diff(before, h.GetActor(ref), protocmp.Transform()); diff != "" {
		t.Errorf("actor after refused updates (-want +got):\n%s", diff)
	}
	if err := update("", 0); status.Code(err) != codes.InvalidArgument {
		t.Errorf("UpdateActor without a uid or version = %v, want InvalidArgument", err)
	}
}

// Updating an actor with an invalid name fails with INVALID_ARGUMENT and the
// error names the invalid field.
func TestUpdateActor_ValidationError(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	h.AddWorker(pool, h.Node("a"))
	tmpl := h.NewTemplate(as, pool)

	_, err := h.Control.UpdateActor(t.Context(), &ateapi.UpdateActorRequest{Actor: &ateapi.Actor{
		Metadata:      &ateapi.ResourceMetadata{Atespace: as, Name: "Not_A_Valid_Name", Uid: foreignUID, Version: 1},
		ActorTemplate: &ateapi.ObjectRef{Atespace: as, Name: tmpl.GetMetadata().GetName()},
	}})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("UpdateActor with an invalid name = %v, want InvalidArgument", err)
	}
	if msg := status.Convert(err).Message(); !strings.Contains(msg, "actor.metadata.name") {
		t.Errorf("UpdateActor error %q does not name the invalid field actor.metadata.name", msg)
	}
}

// Deletes a suspended actor. The response is the actor as read before the
// delete, marked DELETING, and a later Get fails with NOT_FOUND.
func TestDeleteActor(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	h.AddWorker(pool, h.Node("a"))
	actor := h.SuspendedActor(as, h.NewTemplate(as, pool))
	ref := harness.ActorRef(actor)
	before := h.GetActor(ref)

	deleted, err := h.Control.DeleteActor(t.Context(), &ateapi.DeleteActorRequest{Actor: ref})
	if err != nil {
		t.Fatalf("DeleteActor: %v", err)
	}
	want := proto.Clone(before).(*ateapi.Actor)
	want.Status.State = ateapi.ActorState_ACTOR_STATE_DELETING
	if diff := cmp.Diff(want, deleted, protocmp.Transform(), ignoreVersion, ignoreTimestamps, ignoreDeleteTime); diff != "" {
		t.Errorf("DeleteActor response (-want +got):\n%s", diff)
	}
	if _, err := h.Control.GetActor(t.Context(), &ateapi.GetActorRequest{Actor: ref}); status.Code(err) != codes.NotFound {
		t.Errorf("GetActor after delete = %v, want NotFound", err)
	}
}

// Deleting an actor that doesn't exist fails with NOT_FOUND.
func TestDeleteActor_NotFound(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")

	_, err := h.Control.DeleteActor(t.Context(), &ateapi.DeleteActorRequest{Actor: &ateapi.ObjectRef{Atespace: as, Name: "actor-missing"}})
	if status.Code(err) != codes.NotFound {
		t.Errorf("DeleteActor of a missing actor = %v, want NotFound", err)
	}
}

// Deleting an actor with the wrong UID fails with ABORTED, and with the
// right UID but the wrong version fails with ABORTED; both leave it
// unchanged. With the right UID and version the delete succeeds.
func TestDeleteActor_WithPreconditions(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	h.AddWorker(pool, h.Node("a"))
	actor := h.NewActor(as, h.NewTemplate(as, pool))
	ref := harness.ActorRef(actor)
	before := h.GetActor(ref)
	uid, version := before.GetMetadata().GetUid(), before.GetMetadata().GetVersion()

	_, err := h.Control.DeleteActor(t.Context(), &ateapi.DeleteActorRequest{Actor: ref, Options: &ateapi.DeleteOptions{Uid: foreignUID}})
	if status.Code(err) != codes.Aborted {
		t.Errorf("DeleteActor with the wrong uid = %v, want Aborted", err)
	}
	_, err = h.Control.DeleteActor(t.Context(), &ateapi.DeleteActorRequest{Actor: ref, Options: &ateapi.DeleteOptions{Uid: uid, Version: version + 1}})
	if status.Code(err) != codes.Aborted {
		t.Errorf("DeleteActor with the right uid and the wrong version = %v, want Aborted", err)
	}
	if diff := cmp.Diff(before, h.GetActor(ref), protocmp.Transform()); diff != "" {
		t.Errorf("actor after refused deletes (-want +got):\n%s", diff)
	}

	if _, err := h.Control.DeleteActor(t.Context(), &ateapi.DeleteActorRequest{Actor: ref, Options: &ateapi.DeleteOptions{Uid: uid, Version: version}}); err != nil {
		t.Errorf("DeleteActor with the right uid and version: %v", err)
	}
}

// Deleting an actor with an invalid name fails with INVALID_ARGUMENT and the
// error names the invalid field.
func TestDeleteActor_ValidationError(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")

	_, err := h.Control.DeleteActor(t.Context(), &ateapi.DeleteActorRequest{Actor: &ateapi.ObjectRef{Atespace: as, Name: "Not_A_Valid_Name"}})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("DeleteActor with an invalid name = %v, want InvalidArgument", err)
	}
	if msg := status.Convert(err).Message(); !strings.Contains(msg, "actor.name") {
		t.Errorf("DeleteActor error %q does not name the invalid field actor.name", msg)
	}
}

// Deletes a running actor that holds a snapshot of its own: it was suspended
// once and resumed again. The response is the actor as read before the
// delete, marked DELETING, with no worker assignment, no assigned node, and
// its snapshot no longer local to the node. A later Get fails with NOT_FOUND. The atelet
// received exactly one Terminate and runs no sandbox. The worker is left with
// no allocation and no assignments. Object storage no longer holds the actor's
// snapshot.
func TestDeleteActor_Running(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	node := h.Node("a")
	w := h.AddWorker(pool, node)
	suspended := h.SuspendedActor(as, h.NewTemplate(as, pool))
	ref := harness.ActorRef(suspended)
	objectsBeforeSuspend := slices.DeleteFunc(h.ObjectStore.Objects(), func(o string) bool {
		return strings.Contains(o, "/actors/"+suspended.GetMetadata().GetUid()+"/")
	})
	if _, err := h.ResumeActor(t.Context(), &ateapi.ResumeActorRequest{Actor: ref}); err != nil {
		t.Fatalf("ResumeActor: %v", err)
	}

	before := h.GetActor(ref)
	workerBefore := h.GetWorker(w)
	callsBefore := len(node.Atelet.Calls())
	deleted, err := h.Control.DeleteActor(t.Context(), &ateapi.DeleteActorRequest{Actor: ref})
	if err != nil {
		t.Fatalf("DeleteActor: %v", err)
	}
	want := proto.Clone(before).(*ateapi.Actor)
	want.Status.State = ateapi.ActorState_ACTOR_STATE_DELETING
	want.Status.WorkerAssignment = nil
	want.Status.AssignedNode = ""
	for _, snap := range want.Status.Snapshots {
		snap.Locality = ""
	}
	if diff := cmp.Diff(want, deleted, protocmp.Transform(), ignoreVersion, ignoreTimestamps, ignoreDeleteTime); diff != "" {
		t.Errorf("DeleteActor response (-want +got):\n%s", diff)
	}
	if _, err := h.Control.GetActor(t.Context(), &ateapi.GetActorRequest{Actor: ref}); status.Code(err) != codes.NotFound {
		t.Errorf("GetActor after delete = %v, want NotFound", err)
	}
	if calls := node.Atelet.Calls()[callsBefore:]; len(calls) != 1 || calls[0].Op != harness.AteletTerminate {
		t.Errorf("atelet received %v, want exactly one Terminate", calls)
	}
	if diff := cmp.Diff([]harness.Sandbox{}, node.Atelet.Sandboxes()); diff != "" {
		t.Errorf("sandboxes after delete (-want +got):\n%s", diff)
	}
	wantWorker := proto.Clone(workerBefore).(*ateapi.Worker)
	wantWorker.Status.Allocated = nil
	if diff := cmp.Diff(wantWorker, h.GetWorker(w), protocmp.Transform(), ignoreVersion, ignoreTimestamps); diff != "" {
		t.Errorf("worker after delete (-want +got):\n%s", diff)
	}
	assignments, err := h.Control.ListWorkerActorAssignments(t.Context(), &ateapi.ListWorkerActorAssignmentsRequest{Worker: w.Ref()})
	if err != nil {
		t.Fatalf("ListWorkerActorAssignments: %v", err)
	}
	if diff := cmp.Diff(&ateapi.ListWorkerActorAssignmentsResponse{}, assignments, protocmp.Transform()); diff != "" {
		t.Errorf("worker assignments after delete (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(objectsBeforeSuspend, h.ObjectStore.Objects()); diff != "" {
		t.Errorf("object storage after delete (-want +got):\n%s", diff)
	}
}

// Deletes a crashed actor. The response is the actor as read before the
// delete, marked DELETING, and a later Get fails with NOT_FOUND.
func TestDeleteActor_Crashed(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	h.AddWorker(pool, h.Node("a"))
	ref := harness.ActorRef(h.CrashedActor(as, h.NewTemplate(as, pool)))
	before := h.GetActor(ref)

	deleted, err := h.Control.DeleteActor(t.Context(), &ateapi.DeleteActorRequest{Actor: ref})
	if err != nil {
		t.Fatalf("DeleteActor: %v", err)
	}
	want := proto.Clone(before).(*ateapi.Actor)
	want.Status.State = ateapi.ActorState_ACTOR_STATE_DELETING
	if diff := cmp.Diff(want, deleted, protocmp.Transform(), ignoreVersion, ignoreTimestamps, ignoreDeleteTime); diff != "" {
		t.Errorf("DeleteActor response (-want +got):\n%s", diff)
	}
	if _, err := h.Control.GetActor(t.Context(), &ateapi.GetActorRequest{Actor: ref}); status.Code(err) != codes.NotFound {
		t.Errorf("GetActor after delete = %v, want NotFound", err)
	}
}

// Deletes an actor suspended asynchronously, whose latest snapshot is local to
// its node. The request succeeds (OK), and a later Get fails with NOT_FOUND.
// The atelet received exactly one Terminate and no longer holds the local
// snapshot.
func TestDeleteActor_AsyncSuspended(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	node := h.Node("a")
	h.AddWorker(pool, node)
	actor := h.AsyncSuspendedActor(as, h.NewTemplate(as, pool))
	ref := harness.ActorRef(actor)
	callsBefore := len(node.Atelet.Calls())

	if _, err := h.Control.DeleteActor(t.Context(), &ateapi.DeleteActorRequest{Actor: ref}); err != nil {
		t.Fatalf("DeleteActor: %v", err)
	}
	if _, err := h.Control.GetActor(t.Context(), &ateapi.GetActorRequest{Actor: ref}); status.Code(err) != codes.NotFound {
		t.Errorf("GetActor after delete = %v, want NotFound", err)
	}
	if calls := node.Atelet.Calls()[callsBefore:]; len(calls) != 1 || calls[0].Op != harness.AteletTerminate {
		t.Errorf("atelet received %v, want exactly one Terminate", calls)
	}
	if got := node.Atelet.LocalSnapshot(actor.GetMetadata().GetUid()); got != "" {
		t.Errorf("local snapshot on node a = %q, want none", got)
	}
}

// Deletes a running actor while the atelet fails the Terminate. The request
// fails with INTERNAL, and the actor becomes DELETING on its worker. Resuming
// it fails with FAILED_PRECONDITION and leaves it unchanged. Deleting it again
// succeeds (OK), and a later Get fails with NOT_FOUND.
func TestDeleteActor_TerminateFails(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	node := h.Node("a")
	h.AddWorker(pool, node)
	actor := h.RunningActor(as, h.NewTemplate(as, pool))
	ref := harness.ActorRef(actor)
	before := h.GetActor(ref)
	node.Atelet.On(harness.AteletTerminate).FailNext(harness.ErrSandboxFailed)

	_, err := h.Control.DeleteActor(t.Context(), &ateapi.DeleteActorRequest{Actor: ref})
	if status.Code(err) != codes.Internal {
		t.Fatalf("DeleteActor with a failing Terminate = %v, want Internal", err)
	}
	deleting := h.GetActor(ref)
	want := proto.Clone(before).(*ateapi.Actor)
	want.Status.State = ateapi.ActorState_ACTOR_STATE_DELETING
	if diff := cmp.Diff(want, deleting, protocmp.Transform(), ignoreVersion, ignoreTimestamps, ignoreDeleteTime); diff != "" {
		t.Errorf("actor after a failed Terminate (-want +got):\n%s", diff)
	}

	_, err = h.Control.ResumeActor(t.Context(), &ateapi.ResumeActorRequest{Actor: ref})
	if status.Code(err) != codes.FailedPrecondition {
		t.Errorf("ResumeActor of the DELETING actor = %v, want FailedPrecondition", err)
	}
	if diff := cmp.Diff(deleting, h.GetActor(ref), protocmp.Transform()); diff != "" {
		t.Errorf("actor after a refused resume (-want +got):\n%s", diff)
	}

	if _, err := h.Control.DeleteActor(t.Context(), &ateapi.DeleteActorRequest{Actor: ref}); err != nil {
		t.Fatalf("DeleteActor retry: %v", err)
	}
	if _, err := h.Control.GetActor(t.Context(), &ateapi.GetActorRequest{Actor: ref}); status.Code(err) != codes.NotFound {
		t.Errorf("GetActor after delete = %v, want NotFound", err)
	}
}

// Deletes an actor with an external volume left SUSPENDING by a suspend whose
// DetachVolume failed. The request succeeds (OK), a later Get fails with
// NOT_FOUND, and the volume plugin holds no volumes.
func TestDeleteActor_AfterVolumeDetachFails(t *testing.T) {
	t.Parallel()
	h := harness.New(t, harness.WithPreviewClient())
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	h.AddWorker(pool, h.Node("a"))
	ref := harness.ActorRef(h.RunningActor(as, h.NewTemplate(as, pool, harness.WithExternalVolume("data", "/data"))))
	h.Volumes.On(harness.VolumeDetach).FailNext(harness.ErrUnavailable)
	if _, err := h.Control.SuspendActor(t.Context(), &ateapi.SuspendActorRequest{Actor: ref}); err == nil {
		t.Fatal("SuspendActor succeeded despite a failing DetachVolume")
	}

	if _, err := h.Control.DeleteActor(t.Context(), &ateapi.DeleteActorRequest{Actor: ref}); err != nil {
		t.Fatalf("DeleteActor: %v", err)
	}
	if _, err := h.Control.GetActor(t.Context(), &ateapi.GetActorRequest{Actor: ref}); status.Code(err) != codes.NotFound {
		t.Errorf("GetActor after delete = %v, want NotFound", err)
	}
	if diff := cmp.Diff([]harness.Volume{}, h.Volumes.List()); diff != "" {
		t.Errorf("volumes after delete (-want +got):\n%s", diff)
	}
}

// Deletes a suspended actor with an external volume while the volume plugin
// fails DeleteVolume. The request fails with INTERNAL, and the actor becomes
// DELETING, keeping its volume. Deleting it again succeeds (OK), a later Get
// fails with NOT_FOUND, and the volume plugin holds no volumes.
func TestDeleteActor_VolumeDeleteFails(t *testing.T) {
	t.Parallel()
	h := harness.New(t, harness.WithPreviewClient())
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	h.AddWorker(pool, h.Node("a"))
	actor := h.SuspendedActor(as, h.NewTemplate(as, pool, harness.WithExternalVolume("data", "/data")))
	ref := harness.ActorRef(actor)
	h.Volumes.On(harness.VolumeDelete).FailNext(harness.ErrUnavailable)

	_, err := h.Control.DeleteActor(t.Context(), &ateapi.DeleteActorRequest{Actor: ref})
	if status.Code(err) != codes.Internal {
		t.Fatalf("DeleteActor with a failing DeleteVolume = %v, want Internal", err)
	}
	deleting := h.GetActor(ref)
	if got := deleting.GetStatus().GetState(); got != ateapi.ActorState_ACTOR_STATE_DELETING {
		t.Errorf("actor after a failed DeleteVolume is %v, want DELETING", got)
	}
	if diff := cmp.Diff(actor.GetStatus().GetExternalVolumes(), deleting.GetStatus().GetExternalVolumes(), protocmp.Transform()); diff != "" {
		t.Errorf("external volumes after a failed DeleteVolume (-want +got):\n%s", diff)
	}

	if _, err := h.Control.DeleteActor(t.Context(), &ateapi.DeleteActorRequest{Actor: ref}); err != nil {
		t.Fatalf("DeleteActor retry: %v", err)
	}
	if _, err := h.Control.GetActor(t.Context(), &ateapi.GetActorRequest{Actor: ref}); status.Code(err) != codes.NotFound {
		t.Errorf("GetActor after delete = %v, want NotFound", err)
	}
	if diff := cmp.Diff([]harness.Volume{}, h.Volumes.List()); diff != "" {
		t.Errorf("volumes after delete (-want +got):\n%s", diff)
	}
}

// Deletes an actor with an external volume crashed by the removal of its
// worker. The request succeeds (OK): the volume is detached from the node the
// actor was assigned to and deleted, so the volume plugin holds no volumes.
func TestDeleteActor_WorkerRemovedDetachesVolume(t *testing.T) {
	t.Parallel()
	h := harness.New(t, harness.WithPreviewClient())
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	w := h.AddWorker(pool, h.Node("a"))
	ref := harness.ActorRef(h.RunningActor(as, h.NewTemplate(as, pool, harness.WithExternalVolume("data", "/data"))))
	w.Remove()
	if got := h.GetActor(ref).GetStatus().GetState(); got != ateapi.ActorState_ACTOR_STATE_CRASHED {
		t.Fatalf("actor after its worker was removed is %v, want CRASHED", got)
	}

	if _, err := h.Control.DeleteActor(t.Context(), &ateapi.DeleteActorRequest{Actor: ref}); err != nil {
		t.Fatalf("DeleteActor: %v", err)
	}
	if diff := cmp.Diff([]harness.Volume{}, h.Volumes.List()); diff != "" {
		t.Errorf("volumes after delete (-want +got):\n%s", diff)
	}
}

// Deletes an actor with an external volume crashed by a Restore the atelet
// failed after the volume was attached. The crashed actor stays assigned to
// the node. Deleting it succeeds (OK), and the volume plugin holds no volumes.
// The resume retries while ate-api catches up.
func TestDeleteActor_RestoreFailedDetachesVolume(t *testing.T) {
	t.Parallel()
	h := harness.New(t, harness.WithPreviewClient())
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	node := h.Node("a")
	h.AddWorker(pool, node)
	ref := harness.ActorRef(h.NewActor(as, h.NewTemplate(as, pool, harness.WithExternalVolume("data", "/data"))))
	node.Atelet.On(harness.AteletRestore).FailNext(harness.ErrSandboxFailed)
	if _, err := h.ResumeActor(t.Context(), &ateapi.ResumeActorRequest{Actor: ref}); err == nil {
		t.Fatal("ResumeActor succeeded despite a failing Restore")
	}
	if got := h.GetActor(ref).GetStatus(); got.GetState() != ateapi.ActorState_ACTOR_STATE_CRASHED || got.GetAssignedNode() != node.Name {
		t.Fatalf("actor after a failed Restore is %v assigned to %q, want CRASHED assigned to %q", got.GetState(), got.GetAssignedNode(), node.Name)
	}

	if _, err := h.Control.DeleteActor(t.Context(), &ateapi.DeleteActorRequest{Actor: ref}); err != nil {
		t.Fatalf("DeleteActor: %v", err)
	}
	if diff := cmp.Diff([]harness.Volume{}, h.Volumes.List()); diff != "" {
		t.Errorf("volumes after delete (-want +got):\n%s", diff)
	}
}

// A running actor is left DELETING by a Terminate the atelet failed, and its
// worker is then removed. The actor stays DELETING, not CRASHED, and deleting
// it again succeeds (OK); a later Get fails with NOT_FOUND.
func TestDeleteActor_RetryAfterWorkerRemoved(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	node := h.Node("a")
	w := h.AddWorker(pool, node)
	ref := harness.ActorRef(h.RunningActor(as, h.NewTemplate(as, pool)))
	node.Atelet.On(harness.AteletTerminate).FailNext(harness.ErrSandboxFailed)
	if _, err := h.Control.DeleteActor(t.Context(), &ateapi.DeleteActorRequest{Actor: ref}); err == nil {
		t.Fatal("DeleteActor succeeded despite a failing Terminate")
	}

	w.Remove()
	if got := h.GetActor(ref).GetStatus().GetState(); got != ateapi.ActorState_ACTOR_STATE_DELETING {
		t.Errorf("actor after its worker was removed is %v, want DELETING", got)
	}
	if _, err := h.Control.DeleteActor(t.Context(), &ateapi.DeleteActorRequest{Actor: ref}); err != nil {
		t.Fatalf("DeleteActor retry: %v", err)
	}
	if _, err := h.Control.GetActor(t.Context(), &ateapi.GetActorRequest{Actor: ref}); status.Code(err) != codes.NotFound {
		t.Errorf("GetActor after delete = %v, want NotFound", err)
	}
}

// Resumes a new actor on a pool with one worker on node a, and an unrelated
// worker on node b. The response says it resumed and matches GetActor. The
// actor becomes RUNNING with a worker assignment to the node a worker and is
// otherwise unchanged. That worker's allocation goes to one actor, with one
// assignment for this actor. The node a atelet received exactly one Restore,
// from the template's golden snapshot, and runs exactly this actor's
// sandbox. Node b's atelet received no calls, and object storage is
// unchanged. The resume retries while ate-api catches up.
func TestResumeActor(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	nodeA := h.Node("a")
	w := h.AddWorker(pool, nodeA)
	nodeB := h.Node("b")
	other := h.AddWorker(h.CreateWorkerPool("pool2", nil), nodeB)
	tmpl := h.NewTemplate(as, pool)
	golden, err := h.Control.GetTag(t.Context(), &ateapi.GetTagRequest{Tag: tmpl.GetStatus().GetGoldenSnapshotStatus().GetGoldenTag()})
	if err != nil {
		t.Fatalf("GetTag: %v", err)
	}
	actor := h.NewActor(as, tmpl)
	ref := harness.ActorRef(actor)

	actorBefore := h.GetActor(ref)
	workerBefore := h.GetWorker(w)
	otherBefore := h.GetWorker(other)
	objectsBefore := h.ObjectStore.Objects()
	callsBefore := len(nodeA.Atelet.Calls())
	resp, err := h.ResumeActor(t.Context(), &ateapi.ResumeActorRequest{Actor: ref})
	if err != nil {
		t.Fatalf("ResumeActor: %v", err)
	}

	if !resp.GetResumed() {
		t.Errorf("ResumeActor reported resumed = false, want true")
	}
	got := h.GetActor(ref)
	if diff := cmp.Diff(got, resp.GetActor(), protocmp.Transform()); diff != "" {
		t.Errorf("ResumeActor response against GetActor (-get +response):\n%s", diff)
	}
	wantActor := proto.Clone(actorBefore).(*ateapi.Actor)
	wantActor.Status.State = ateapi.ActorState_ACTOR_STATE_RUNNING
	wantActor.Status.WorkerAssignment = workerAssignment(w)
	wantActor.Status.AssignedNode = w.Node.Name
	if diff := cmp.Diff(wantActor, got, protocmp.Transform(), ignoreVersion, ignoreTimestamps); diff != "" {
		t.Errorf("resumed actor (-want +got):\n%s", diff)
	}

	wantWorker := proto.Clone(workerBefore).(*ateapi.Worker)
	wantWorker.Status.Allocated = &ateapi.WorkerResources{Actors: 1, CpuMillicores: harness.TemplateCPUMillicores, MemoryBytes: harness.TemplateMemoryBytes}
	if diff := cmp.Diff(wantWorker, h.GetWorker(w), protocmp.Transform(), ignoreVersion, ignoreTimestamps); diff != "" {
		t.Errorf("worker after resume (-want +got):\n%s", diff)
	}
	assignments, err := h.Control.ListWorkerActorAssignments(t.Context(), &ateapi.ListWorkerActorAssignmentsRequest{Worker: w.Ref()})
	if err != nil {
		t.Fatalf("ListWorkerActorAssignments: %v", err)
	}
	wantAssignments := &ateapi.ListWorkerActorAssignmentsResponse{ActorAssignments: []*ateapi.ActorAssignment{{
		Metadata:  &ateapi.ResourceMetadata{Name: actor.GetMetadata().GetUid()},
		Actor:     ref,
		ActorUid:  actor.GetMetadata().GetUid(),
		Resources: templateResources(),
	}}}
	if diff := cmp.Diff(wantAssignments, assignments, protocmp.Transform(), ignoreServerMetadata); diff != "" {
		t.Errorf("worker assignments after resume (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(otherBefore, h.GetWorker(other), protocmp.Transform()); diff != "" {
		t.Errorf("unrelated worker after resume (-want +got):\n%s", diff)
	}

	calls := nodeA.Atelet.Calls()[callsBefore:]
	if len(calls) != 1 || calls[0].Op != harness.AteletRestore {
		t.Fatalf("node a atelet received %v, want exactly one Restore", calls)
	}
	assertRestoreOf(t, calls[0].Request.(*ateletpb.RestoreRequest), golden.GetStatus().GetSnapshot())
	wantSandboxes := []harness.Sandbox{{
		ActorUID:  actor.GetMetadata().GetUid(),
		Atespace:  as,
		ActorName: actor.GetMetadata().GetName(),
		WorkerUID: w.Name,
	}}
	if diff := cmp.Diff(wantSandboxes, nodeA.Atelet.Sandboxes()); diff != "" {
		t.Errorf("node a sandboxes after resume (-want +got):\n%s", diff)
	}
	if calls := nodeB.Atelet.Calls(); len(calls) != 0 {
		t.Errorf("node b atelet received %v, want no calls", calls)
	}
	if diff := cmp.Diff(objectsBefore, h.ObjectStore.Objects()); diff != "" {
		t.Errorf("object storage after resume (-want +got):\n%s", diff)
	}
}

// Resuming an actor that doesn't exist fails with NOT_FOUND.
func TestResumeActor_NotFound(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")

	_, err := h.Control.ResumeActor(t.Context(), &ateapi.ResumeActorRequest{Actor: &ateapi.ObjectRef{Atespace: as, Name: "actor-missing"}})
	if status.Code(err) != codes.NotFound {
		t.Errorf("ResumeActor of a missing actor = %v, want NotFound", err)
	}
}

// Resuming an actor with an invalid name fails with INVALID_ARGUMENT and the
// error names the invalid field.
func TestResumeActor_ValidationError(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")

	_, err := h.Control.ResumeActor(t.Context(), &ateapi.ResumeActorRequest{Actor: &ateapi.ObjectRef{Atespace: as, Name: "Not_A_Valid_Name"}})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("ResumeActor with an invalid name = %v, want InvalidArgument", err)
	}
	if msg := status.Convert(err).Message(); !strings.Contains(msg, "actor.name") {
		t.Errorf("ResumeActor error %q does not name the invalid field actor.name", msg)
	}
}

// Resumes a new actor while the atelet fails the Restore with INTERNAL. The
// request fails with INTERNAL, and the actor becomes CRASHED with no worker
// assignment, still assigned to the worker's node. Its status.crash is set: a
// message naming the atelet Restore and the atelet's error, and a crash time.
// It is otherwise unchanged. The worker is left with no allocation and no
// assignments, and the atelet runs no sandbox. The resume retries while
// ate-api catches up.
func TestResumeActor_RestoreFails(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	node := h.Node("a")
	w := h.AddWorker(pool, node)
	tmpl := h.NewTemplate(as, pool)
	actor := h.NewActor(as, tmpl)
	ref := harness.ActorRef(actor)
	node.Atelet.On(harness.AteletRestore).FailNext(status.Error(codes.Internal, "injected restore failure"))

	actorBefore := h.GetActor(ref)
	workerBefore := h.GetWorker(w)
	callsBefore := len(node.Atelet.Calls())
	_, err := h.ResumeActor(t.Context(), &ateapi.ResumeActorRequest{Actor: ref})
	if status.Code(err) != codes.Internal {
		t.Fatalf("ResumeActor with a failing Restore = %v, want Internal", err)
	}

	wantActor := proto.Clone(actorBefore).(*ateapi.Actor)
	wantActor.Status.State = ateapi.ActorState_ACTOR_STATE_CRASHED
	wantActor.Status.AssignedNode = node.Name
	wantActor.Status.Crash = &ateapi.ActorCrash{Message: "resume failed: atelet Restore: injected restore failure"}
	ignoreCrashTime := protocmp.IgnoreFields(&ateapi.ActorCrash{}, "crash_time")
	crashed := h.GetActor(ref)
	if diff := cmp.Diff(wantActor, crashed, protocmp.Transform(), ignoreVersion, ignoreTimestamps, ignoreCrashTime); diff != "" {
		t.Errorf("actor after a failed Restore (-want +got):\n%s", diff)
	}
	if crashed.GetStatus().GetCrash().GetCrashTime() == nil {
		t.Errorf("status.crash of the crashed actor has no crash time")
	}
	if diff := cmp.Diff(workerBefore, h.GetWorker(w), protocmp.Transform(), ignoreVersion, ignoreTimestamps); diff != "" {
		t.Errorf("worker after a failed Restore (-want +got):\n%s", diff)
	}
	assignments, err := h.Control.ListWorkerActorAssignments(t.Context(), &ateapi.ListWorkerActorAssignmentsRequest{Worker: w.Ref()})
	if err != nil {
		t.Fatalf("ListWorkerActorAssignments: %v", err)
	}
	if diff := cmp.Diff(&ateapi.ListWorkerActorAssignmentsResponse{}, assignments, protocmp.Transform()); diff != "" {
		t.Errorf("worker assignments after a failed Restore (-want +got):\n%s", diff)
	}
	if calls := node.Atelet.Calls()[callsBefore:]; len(calls) != 1 || calls[0].Op != harness.AteletRestore {
		t.Errorf("atelet received %v, want exactly one Restore", calls)
	}
	if diff := cmp.Diff([]harness.Sandbox{}, node.Atelet.Sandboxes()); diff != "" {
		t.Errorf("sandboxes after a failed Restore (-want +got):\n%s", diff)
	}
}

// Resumes a new actor while the atelet fails the Restore with UNAVAILABLE.
// The request fails with UNAVAILABLE, and the actor stays RESUMING with its
// worker assignment, so a later resume can retry. The worker keeps its
// allocation of one actor and its assignment for this actor, and the atelet
// runs no sandbox. The resume retries while ate-api catches up.
func TestResumeActor_RestoreUnavailable(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	node := h.Node("a")
	w := h.AddWorker(pool, node)
	tmpl := h.NewTemplate(as, pool)
	actor := h.NewActor(as, tmpl)
	ref := harness.ActorRef(actor)
	node.Atelet.On(harness.AteletRestore).FailNext(harness.ErrUnavailable)

	actorBefore := h.GetActor(ref)
	workerBefore := h.GetWorker(w)
	callsBefore := len(node.Atelet.Calls())
	_, err := h.ResumeActor(t.Context(), &ateapi.ResumeActorRequest{Actor: ref})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("ResumeActor with an unavailable atelet = %v, want Unavailable", err)
	}

	wantActor := proto.Clone(actorBefore).(*ateapi.Actor)
	wantActor.Status.State = ateapi.ActorState_ACTOR_STATE_RESUMING
	wantActor.Status.WorkerAssignment = workerAssignment(w)
	// ate-api records the worker epoch once a Restore succeeds.
	wantActor.Status.WorkerAssignment.WorkerEpoch = 0
	wantActor.Status.AssignedNode = w.Node.Name
	if diff := cmp.Diff(wantActor, h.GetActor(ref), protocmp.Transform(), ignoreVersion, ignoreTimestamps); diff != "" {
		t.Errorf("actor after an unavailable atelet (-want +got):\n%s", diff)
	}
	wantWorker := proto.Clone(workerBefore).(*ateapi.Worker)
	wantWorker.Status.Allocated = &ateapi.WorkerResources{Actors: 1, CpuMillicores: harness.TemplateCPUMillicores, MemoryBytes: harness.TemplateMemoryBytes}
	if diff := cmp.Diff(wantWorker, h.GetWorker(w), protocmp.Transform(), ignoreVersion, ignoreTimestamps); diff != "" {
		t.Errorf("worker after an unavailable atelet (-want +got):\n%s", diff)
	}
	assignments, err := h.Control.ListWorkerActorAssignments(t.Context(), &ateapi.ListWorkerActorAssignmentsRequest{Worker: w.Ref()})
	if err != nil {
		t.Fatalf("ListWorkerActorAssignments: %v", err)
	}
	wantAssignments := &ateapi.ListWorkerActorAssignmentsResponse{ActorAssignments: []*ateapi.ActorAssignment{{
		Metadata:  &ateapi.ResourceMetadata{Name: actor.GetMetadata().GetUid()},
		Actor:     ref,
		ActorUid:  actor.GetMetadata().GetUid(),
		Resources: templateResources(),
	}}}
	if diff := cmp.Diff(wantAssignments, assignments, protocmp.Transform(), ignoreServerMetadata); diff != "" {
		t.Errorf("worker assignments after an unavailable atelet (-want +got):\n%s", diff)
	}
	if calls := node.Atelet.Calls()[callsBefore:]; len(calls) != 1 || calls[0].Op != harness.AteletRestore {
		t.Errorf("atelet received %v, want exactly one Restore", calls)
	}
	if diff := cmp.Diff([]harness.Sandbox{}, node.Atelet.Sandboxes()); diff != "" {
		t.Errorf("sandboxes after an unavailable atelet (-want +got):\n%s", diff)
	}
}

// Resuming a crashed actor fails with FAILED_PRECONDITION and leaves the
// actor unchanged.
func TestResumeActor_WrongState(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	h.AddWorker(pool, h.Node("a"))
	crashed := h.CrashedActor(as, h.NewTemplate(as, pool))
	ref := harness.ActorRef(crashed)
	before := h.GetActor(ref)

	_, err := h.Control.ResumeActor(t.Context(), &ateapi.ResumeActorRequest{Actor: ref})
	if status.Code(err) != codes.FailedPrecondition {
		t.Errorf("ResumeActor of a crashed actor = %v, want FailedPrecondition", err)
	}
	if diff := cmp.Diff(before, h.GetActor(ref), protocmp.Transform()); diff != "" {
		t.Errorf("actor after a refused resume (-want +got):\n%s", diff)
	}
}

// Resuming a running actor succeeds (OK) and reports that it did not resume
// it. The actor is unchanged and the atelet receives no call.
func TestResumeActor_AlreadyRunning(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	node := h.Node("a")
	h.AddWorker(pool, node)
	running := h.RunningActor(as, h.NewTemplate(as, pool))
	ref := harness.ActorRef(running)
	before := h.GetActor(ref)
	callsBefore := len(node.Atelet.Calls())

	resp, err := h.Control.ResumeActor(t.Context(), &ateapi.ResumeActorRequest{Actor: ref})
	if status.Code(err) != codes.OK {
		t.Fatalf("ResumeActor of a running actor = %v, want OK", err)
	}
	if resp.GetResumed() {
		t.Error("ResumeActor of a running actor reported resumed = true, want false")
	}
	if diff := cmp.Diff(before, resp.GetActor(), protocmp.Transform()); diff != "" {
		t.Errorf("ResumeActor response (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(before, h.GetActor(ref), protocmp.Transform()); diff != "" {
		t.Errorf("actor after resuming it while running (-want +got):\n%s", diff)
	}
	if calls := node.Atelet.Calls()[callsBefore:]; len(calls) != 0 {
		t.Errorf("atelet received %v, want no calls", calls)
	}
}

// Resumes an actor that was suspended. The request succeeds (OK), the actor
// becomes RUNNING, and the atelet receives a Restore of the actor's own
// external snapshot, not the golden one. The resume retries while ate-api
// catches up.
func TestResumeActor_FromSuspended(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	node := h.Node("a")
	w := h.AddWorker(pool, node)
	suspended := h.SuspendedActor(as, h.NewTemplate(as, pool))
	ref := harness.ActorRef(suspended)
	before := h.GetActor(ref)
	callsBefore := len(node.Atelet.Calls())

	resp, err := h.ResumeActor(t.Context(), &ateapi.ResumeActorRequest{Actor: ref})
	if status.Code(err) != codes.OK {
		t.Fatalf("ResumeActor of a suspended actor = %v, want OK", err)
	}
	want := proto.Clone(before).(*ateapi.Actor)
	want.Status.State = ateapi.ActorState_ACTOR_STATE_RUNNING
	want.Status.WorkerAssignment = workerAssignment(w)
	want.Status.AssignedNode = w.Node.Name
	if diff := cmp.Diff(want, resp.GetActor(), protocmp.Transform(), ignoreVersion, ignoreTimestamps); diff != "" {
		t.Errorf("resumed actor (-want +got):\n%s", diff)
	}
	calls := node.Atelet.Calls()[callsBefore:]
	if len(calls) != 1 || calls[0].Op != harness.AteletRestore {
		t.Fatalf("atelet received %v, want exactly one Restore", calls)
	}
	own := latestSnapshot(t, before)
	if own.GetOwner() != ateapi.SnapshotOwner_SNAPSHOT_OWNER_ACTOR {
		t.Fatalf("suspended actor's latest snapshot is owned by %v, want the actor", own.GetOwner())
	}
	assertRestoreOf(t, calls[0].Request.(*ateletpb.RestoreRequest), own)
}

// Resumes an actor suspended asynchronously, whose latest snapshot is local to
// node a and not uploaded, on a pool with a worker on node a and one on node
// b. The request succeeds (OK). The actor becomes RUNNING on the node a worker,
// and is otherwise unchanged. The node a atelet received exactly one Restore,
// of the local snapshot, and node b's atelet received no calls. Object storage
// is unchanged.
func TestResumeActor_FromAsyncSuspended(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	nodeA := h.Node("a")
	w := h.AddWorker(pool, nodeA)
	tmpl := h.NewTemplate(as, pool)
	actor := h.AsyncSuspendedActor(as, tmpl)
	ref := harness.ActorRef(actor)
	nodeB := h.Node("b")
	h.AddWorker(pool, nodeB)

	before := h.GetActor(ref)
	objectsBefore := h.ObjectStore.Objects()
	callsBefore := len(nodeA.Atelet.Calls())
	resp, err := h.ResumeActor(t.Context(), &ateapi.ResumeActorRequest{Actor: ref})
	if err != nil {
		t.Fatalf("ResumeActor: %v", err)
	}
	want := proto.Clone(before).(*ateapi.Actor)
	want.Status.State = ateapi.ActorState_ACTOR_STATE_RUNNING
	want.Status.WorkerAssignment = workerAssignment(w)
	if diff := cmp.Diff(want, resp.GetActor(), protocmp.Transform(), ignoreVersion, ignoreTimestamps); diff != "" {
		t.Errorf("resumed actor (-want +got):\n%s", diff)
	}
	calls := nodeA.Atelet.Calls()[callsBefore:]
	if len(calls) != 1 || calls[0].Op != harness.AteletRestore {
		t.Fatalf("node a atelet received %v, want exactly one Restore", calls)
	}
	if got, want := calls[0].Request.(*ateletpb.RestoreRequest).GetSnapshot().GetSnapshotUuid(), latestSnapshot(t, before).GetUuid(); got != want {
		t.Errorf("Restore snapshot = %q, want the local snapshot %q", got, want)
	}
	if calls := nodeB.Atelet.Calls(); len(calls) != 0 {
		t.Errorf("node b atelet received %v, want no calls", calls)
	}
	if diff := cmp.Diff(objectsBefore, h.ObjectStore.Objects()); diff != "" {
		t.Errorf("object storage after resume (-want +got):\n%s", diff)
	}
}

// Resumes an actor suspended asynchronously while the atelet fails the
// Restore of its local snapshot. The request fails with INTERNAL, and the
// actor becomes CRASHED with no worker assignment, still assigned to its node.
// Its status.crash names the atelet Restore and the atelet's error. The worker
// is left with no allocation.
func TestResumeActor_LocalRestoreFails(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	node := h.Node("a")
	w := h.AddWorker(pool, node)
	ref := harness.ActorRef(h.AsyncSuspendedActor(as, h.NewTemplate(as, pool)))
	before := h.GetActor(ref)
	workerBefore := h.GetWorker(w)
	node.Atelet.On(harness.AteletRestore).FailNext(harness.ErrSandboxFailed)

	_, err := h.ResumeActor(t.Context(), &ateapi.ResumeActorRequest{Actor: ref})
	if status.Code(err) != codes.Internal {
		t.Fatalf("ResumeActor with a failing Restore = %v, want Internal", err)
	}
	want := proto.Clone(before).(*ateapi.Actor)
	want.Status.State = ateapi.ActorState_ACTOR_STATE_CRASHED
	want.Status.Crash = &ateapi.ActorCrash{Message: "resume failed: atelet Restore: harness: sandbox operation failed"}
	ignoreCrashTime := protocmp.IgnoreFields(&ateapi.ActorCrash{}, "crash_time")
	if diff := cmp.Diff(want, h.GetActor(ref), protocmp.Transform(), ignoreVersion, ignoreTimestamps, ignoreCrashTime); diff != "" {
		t.Errorf("actor after a failed Restore (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(workerBefore, h.GetWorker(w), protocmp.Transform(), ignoreVersion, ignoreTimestamps); diff != "" {
		t.Errorf("worker after a failed Restore (-want +got):\n%s", diff)
	}
}

// Resumes an actor suspended asynchronously on node a after another actor
// took node a's only worker, while a worker on node b has room. The request
// succeeds (OK). The node a atelet received exactly one DetachActor, which
// uploaded the local snapshot and dropped it from the node. The actor becomes
// RUNNING on the node b worker, whose atelet received exactly one Restore of
// that snapshot. Object storage gains the snapshot's objects. The resume
// retries while ate-api catches up.
func TestResumeActor_DetachesFromFullNode(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	nodeA := h.Node("a")
	h.AddWorker(pool, nodeA)
	tmpl := h.NewTemplate(as, pool)
	actor := h.AsyncSuspendedActor(as, tmpl)
	ref := harness.ActorRef(actor)
	h.RunningActor(as, tmpl)
	nodeB := h.Node("b")
	wB := h.AddWorker(pool, nodeB)

	before := h.GetActor(ref)
	local := latestSnapshot(t, before)
	objectsBefore := h.ObjectStore.Objects()
	callsBeforeA := len(nodeA.Atelet.Calls())
	resp, err := h.ResumeActor(t.Context(), &ateapi.ResumeActorRequest{Actor: ref})
	if err != nil {
		t.Fatalf("ResumeActor: %v", err)
	}
	if got := resp.GetActor().GetStatus(); got.GetState() != ateapi.ActorState_ACTOR_STATE_RUNNING || got.GetWorkerAssignment().GetWorker().GetName() != wB.Name {
		t.Errorf("actor after resume is %v on worker %q, want RUNNING on the node b worker %q", got.GetState(), got.GetWorkerAssignment().GetWorker().GetName(), wB.Name)
	}
	if calls := nodeA.Atelet.Calls()[callsBeforeA:]; len(calls) != 1 || calls[0].Op != harness.AteletDetachActor {
		t.Errorf("node a atelet received %v, want exactly one DetachActor", calls)
	}
	if got := nodeA.Atelet.LocalSnapshot(actor.GetMetadata().GetUid()); got != "" {
		t.Errorf("local snapshot on node a = %q, want none", got)
	}
	calls := nodeB.Atelet.Calls()
	if len(calls) != 1 || calls[0].Op != harness.AteletRestore {
		t.Fatalf("node b atelet received %v, want exactly one Restore", calls)
	}
	if got := calls[0].Request.(*ateletpb.RestoreRequest).GetSnapshot().GetSnapshotUuid(); got != local.GetUuid() {
		t.Errorf("Restore snapshot = %q, want %q", got, local.GetUuid())
	}
	wantObjects := append(slices.Clone(objectsBefore), snapshotObjects(local.GetDurableSnapshot().GetObject().GetSnapshotUri())...)
	if diff := cmp.Diff(wantObjects, h.ObjectStore.Objects(), cmpopts.SortSlices(func(a, b string) bool { return a < b })); diff != "" {
		t.Errorf("object storage after resume (-want +got):\n%s", diff)
	}
}

// Resuming an actor while the pool's only worker hosts another actor fails
// with RESOURCE_EXHAUSTED, leaves the actor unchanged, and makes no atelet
// call. After the other actor is suspended, resuming succeeds (OK) on that
// worker. The resume after the suspend retries while ate-api catches up.
func TestResumeActor_WorkerFull(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	node := h.Node("a")
	w := h.AddWorker(pool, node)
	tmpl := h.NewTemplate(as, pool)
	other := h.RunningActor(as, tmpl)
	actor := h.NewActor(as, tmpl)
	ref := harness.ActorRef(actor)

	before := h.GetActor(ref)
	callsBefore := len(node.Atelet.Calls())
	_, err := h.Control.ResumeActor(t.Context(), &ateapi.ResumeActorRequest{Actor: ref})
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("ResumeActor with the only worker full = %v, want ResourceExhausted", err)
	}
	if diff := cmp.Diff(before, h.GetActor(ref), protocmp.Transform()); diff != "" {
		t.Errorf("actor after a refused resume (-want +got):\n%s", diff)
	}
	if calls := node.Atelet.Calls()[callsBefore:]; len(calls) != 0 {
		t.Errorf("atelet received %v after a refused resume, want no calls", calls)
	}

	if _, err := h.Control.SuspendActor(t.Context(), &ateapi.SuspendActorRequest{Actor: harness.ActorRef(other)}); err != nil {
		t.Fatalf("SuspendActor: %v", err)
	}
	before = h.GetActor(ref)
	resp, err := h.ResumeActor(t.Context(), &ateapi.ResumeActorRequest{Actor: ref})
	if status.Code(err) != codes.OK {
		t.Fatalf("ResumeActor after freeing the worker = %v, want OK", err)
	}
	want := proto.Clone(before).(*ateapi.Actor)
	want.Status.State = ateapi.ActorState_ACTOR_STATE_RUNNING
	want.Status.WorkerAssignment = workerAssignment(w)
	want.Status.AssignedNode = w.Node.Name
	if diff := cmp.Diff(want, resp.GetActor(), protocmp.Transform(), ignoreVersion, ignoreTimestamps); diff != "" {
		t.Errorf("resumed actor (-want +got):\n%s", diff)
	}
}

// Resuming an actor while the pool's only worker is draining fails with
// RESOURCE_EXHAUSTED, leaves the actor and the draining worker unchanged,
// and makes no atelet call. After a new worker is added, resuming succeeds
// (OK) on the new worker. The resume after the new worker is added retries
// while ate-api catches up.
func TestResumeActor_WorkerDraining(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	node := h.Node("a")
	w := h.AddWorker(pool, node)
	tmpl := h.NewTemplate(as, pool)
	actor := h.NewActor(as, tmpl)
	ref := harness.ActorRef(actor)
	w.Drain()

	before := h.GetActor(ref)
	drainingBefore := h.GetWorker(w)
	callsBefore := len(node.Atelet.Calls())
	_, err := h.Control.ResumeActor(t.Context(), &ateapi.ResumeActorRequest{Actor: ref})
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("ResumeActor with the only worker draining = %v, want ResourceExhausted", err)
	}
	if diff := cmp.Diff(before, h.GetActor(ref), protocmp.Transform()); diff != "" {
		t.Errorf("actor after a refused resume (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(drainingBefore, h.GetWorker(w), protocmp.Transform()); diff != "" {
		t.Errorf("draining worker after a refused resume (-want +got):\n%s", diff)
	}
	if calls := node.Atelet.Calls()[callsBefore:]; len(calls) != 0 {
		t.Errorf("atelet received %v after a refused resume, want no calls", calls)
	}

	fresh := h.AddWorker(pool, node)
	before = h.GetActor(ref)
	resp, err := h.ResumeActor(t.Context(), &ateapi.ResumeActorRequest{Actor: ref})
	if status.Code(err) != codes.OK {
		t.Fatalf("ResumeActor after adding a worker = %v, want OK", err)
	}
	want := proto.Clone(before).(*ateapi.Actor)
	want.Status.State = ateapi.ActorState_ACTOR_STATE_RUNNING
	want.Status.WorkerAssignment = workerAssignment(fresh)
	want.Status.AssignedNode = fresh.Node.Name
	if diff := cmp.Diff(want, resp.GetActor(), protocmp.Transform(), ignoreVersion, ignoreTimestamps); diff != "" {
		t.Errorf("resumed actor (-want +got):\n%s", diff)
	}
}

// Resuming an actor after the pool's only worker was removed fails with
// RESOURCE_EXHAUSTED, leaves the actor unchanged, and makes no atelet call.
// After a new worker is added, resuming succeeds (OK) on the new worker. The
// resume after the new worker is added retries while ate-api catches up.
func TestResumeActor_WorkerRemoved(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	node := h.Node("a")
	w := h.AddWorker(pool, node)
	tmpl := h.NewTemplate(as, pool)
	actor := h.NewActor(as, tmpl)
	ref := harness.ActorRef(actor)
	w.Remove()

	before := h.GetActor(ref)
	callsBefore := len(node.Atelet.Calls())
	_, err := h.Control.ResumeActor(t.Context(), &ateapi.ResumeActorRequest{Actor: ref})
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("ResumeActor with the only worker removed = %v, want ResourceExhausted", err)
	}
	if diff := cmp.Diff(before, h.GetActor(ref), protocmp.Transform()); diff != "" {
		t.Errorf("actor after a refused resume (-want +got):\n%s", diff)
	}
	if calls := node.Atelet.Calls()[callsBefore:]; len(calls) != 0 {
		t.Errorf("atelet received %v after a refused resume, want no calls", calls)
	}

	fresh := h.AddWorker(pool, node)
	before = h.GetActor(ref)
	resp, err := h.ResumeActor(t.Context(), &ateapi.ResumeActorRequest{Actor: ref})
	if status.Code(err) != codes.OK {
		t.Fatalf("ResumeActor after adding a worker = %v, want OK", err)
	}
	want := proto.Clone(before).(*ateapi.Actor)
	want.Status.State = ateapi.ActorState_ACTOR_STATE_RUNNING
	want.Status.WorkerAssignment = workerAssignment(fresh)
	want.Status.AssignedNode = fresh.Node.Name
	if diff := cmp.Diff(want, resp.GetActor(), protocmp.Transform(), ignoreVersion, ignoreTimestamps); diff != "" {
		t.Errorf("resumed actor (-want +got):\n%s", diff)
	}
}

// An actor left RESUMING by a Restore that failed with UNAVAILABLE is
// resumed again. The request succeeds (OK), the actor becomes RUNNING on the
// same worker, and that worker still holds exactly one assignment, for this
// actor. Resuming it again retries while ate-api catches up.
func TestResumeActor_Retry(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	w := h.AddWorker(pool, h.Node("a"))
	tmpl := h.NewTemplate(as, pool)
	before := resumingActor(t, h, as, tmpl, w)
	ref := harness.ActorRef(before)
	workerBefore := h.GetWorker(w)

	resp, err := h.ResumeActor(t.Context(), &ateapi.ResumeActorRequest{Actor: ref})
	if status.Code(err) != codes.OK {
		t.Fatalf("ResumeActor retry = %v, want OK", err)
	}
	want := proto.Clone(before).(*ateapi.Actor)
	want.Status.State = ateapi.ActorState_ACTOR_STATE_RUNNING
	want.Status.WorkerAssignment.WorkerEpoch = w.Epoch
	if diff := cmp.Diff(want, resp.GetActor(), protocmp.Transform(), ignoreVersion, ignoreTimestamps); diff != "" {
		t.Errorf("resumed actor (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(workerBefore, h.GetWorker(w), protocmp.Transform(), ignoreVersion, ignoreTimestamps); diff != "" {
		t.Errorf("worker after the retry (-want +got):\n%s", diff)
	}
	assignments, err := h.Control.ListWorkerActorAssignments(t.Context(), &ateapi.ListWorkerActorAssignmentsRequest{Worker: w.Ref()})
	if err != nil {
		t.Fatalf("ListWorkerActorAssignments: %v", err)
	}
	wantAssignments := &ateapi.ListWorkerActorAssignmentsResponse{ActorAssignments: []*ateapi.ActorAssignment{{
		Metadata:  &ateapi.ResourceMetadata{Name: before.GetMetadata().GetUid()},
		Actor:     ref,
		ActorUid:  before.GetMetadata().GetUid(),
		Resources: templateResources(),
	}}}
	if diff := cmp.Diff(wantAssignments, assignments, protocmp.Transform(), ignoreServerMetadata); diff != "" {
		t.Errorf("worker assignments after the retry (-want +got):\n%s", diff)
	}
}

// An actor left RESUMING by a Restore that failed with UNAVAILABLE has its
// worker drained, and is resumed again. The request fails with ABORTED, and
// the actor becomes CRASHED with no worker assignment and a crash message
// saying its worker is draining.
//
// TODO(): The request should fail with INTERNAL, as other resumes that crash
// the actor do. ABORTED makes callers like the atenet router retry a resume
// that cannot succeed.
func TestResumeActor_RetryWorkerDrained(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	w := h.AddWorker(pool, h.Node("a"))
	before := resumingActor(t, h, as, h.NewTemplate(as, pool), w)
	ref := harness.ActorRef(before)
	w.Drain()

	_, err := h.Control.ResumeActor(t.Context(), &ateapi.ResumeActorRequest{Actor: ref})
	if status.Code(err) != codes.Aborted {
		t.Errorf("ResumeActor retry on a draining worker = %v, want Aborted", err)
	}
	want := proto.Clone(before).(*ateapi.Actor)
	want.Status.State = ateapi.ActorState_ACTOR_STATE_CRASHED
	want.Status.WorkerAssignment = nil
	want.Status.Crash = &ateapi.ActorCrash{Message: "resume failed: assigned worker is draining"}
	ignoreCrashTime := protocmp.IgnoreFields(&ateapi.ActorCrash{}, "crash_time")
	if diff := cmp.Diff(want, h.GetActor(ref), protocmp.Transform(), ignoreVersion, ignoreTimestamps, ignoreCrashTime); diff != "" {
		t.Errorf("actor after a retry on a draining worker (-want +got):\n%s", diff)
	}
}

// An actor left RESUMING by a Restore that failed with UNAVAILABLE has its
// worker removed. Removing the worker crashes the actor, with no worker
// assignment and a crash message saying the worker pod went away, and
// resuming it again fails with FAILED_PRECONDITION.
func TestResumeActor_RetryWorkerRemoved(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	w := h.AddWorker(pool, h.Node("a"))
	before := resumingActor(t, h, as, h.NewTemplate(as, pool), w)
	ref := harness.ActorRef(before)
	w.Remove()

	want := proto.Clone(before).(*ateapi.Actor)
	want.Status.State = ateapi.ActorState_ACTOR_STATE_CRASHED
	want.Status.WorkerAssignment = nil
	want.Status.Crash = &ateapi.ActorCrash{Message: "resume failed: worker pod went away while hosting the actor"}
	ignoreCrashTime := protocmp.IgnoreFields(&ateapi.ActorCrash{}, "crash_time")
	if diff := cmp.Diff(want, h.GetActor(ref), protocmp.Transform(), ignoreVersion, ignoreTimestamps, ignoreCrashTime); diff != "" {
		t.Errorf("actor after its worker was removed (-want +got):\n%s", diff)
	}
	_, err := h.Control.ResumeActor(t.Context(), &ateapi.ResumeActorRequest{Actor: ref})
	if status.Code(err) != codes.FailedPrecondition {
		t.Errorf("ResumeActor of the crashed actor = %v, want FailedPrecondition", err)
	}
}

// An actor left RESUMING by a Restore that failed with UNAVAILABLE has its
// worker relabeled so it no longer matches the template, and is resumed
// again. The request fails with ABORTED, the actor becomes CRASHED with no
// worker assignment and a crash message saying the worker no longer
// satisfies its placement constraints, and the worker is released.
//
// TODO(): The request should fail with INTERNAL, as other resumes that crash
// the actor do. ABORTED makes callers like the atenet router retry a resume
// that cannot succeed.
func TestResumeActor_RetryWorkerIneligible(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	w := h.AddWorker(pool, h.Node("a"))
	before := resumingActor(t, h, as, h.NewTemplate(as, pool), w)
	ref := harness.ActorRef(before)
	relabeled := proto.Clone(h.GetWorker(w)).(*ateapi.Worker)
	relabeled.Labels = map[string]string{"pool": "other"}
	relabeled, err := h.Control.UpdateWorker(t.Context(), &ateapi.UpdateWorkerRequest{Worker: relabeled})
	if err != nil {
		t.Fatalf("UpdateWorker: %v", err)
	}

	_, err = h.Control.ResumeActor(t.Context(), &ateapi.ResumeActorRequest{Actor: ref})
	if status.Code(err) != codes.Aborted {
		t.Errorf("ResumeActor retry on an ineligible worker = %v, want Aborted", err)
	}
	want := proto.Clone(before).(*ateapi.Actor)
	want.Status.State = ateapi.ActorState_ACTOR_STATE_CRASHED
	want.Status.WorkerAssignment = nil
	want.Status.Crash = &ateapi.ActorCrash{Message: "resume failed: assigned worker no longer satisfies the actor's placement constraints"}
	ignoreCrashTime := protocmp.IgnoreFields(&ateapi.ActorCrash{}, "crash_time")
	if diff := cmp.Diff(want, h.GetActor(ref), protocmp.Transform(), ignoreVersion, ignoreTimestamps, ignoreCrashTime); diff != "" {
		t.Errorf("actor after a retry on an ineligible worker (-want +got):\n%s", diff)
	}
	wantWorker := proto.Clone(relabeled).(*ateapi.Worker)
	wantWorker.Status.Allocated = nil
	if diff := cmp.Diff(wantWorker, h.GetWorker(w), protocmp.Transform(), ignoreVersion, ignoreTimestamps); diff != "" {
		t.Errorf("worker after a retry on it while ineligible (-want +got):\n%s", diff)
	}
}

// Resumes an actor whose template has an external volume. The request
// succeeds (OK), the actor's volume is CREATED, and the volume plugin holds
// that volume attached to the worker's node. The resume retries while
// ate-api catches up.
func TestResumeActor_WithVolume(t *testing.T) {
	t.Parallel()
	h := harness.New(t, harness.WithPreviewClient())
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	node := h.Node("a")
	w := h.AddWorker(pool, node)
	actor := h.NewActor(as, h.NewTemplate(as, pool, harness.WithExternalVolume("data", "/data")))
	ref := harness.ActorRef(actor)
	before := h.GetActor(ref)

	resp, err := h.ResumeActor(t.Context(), &ateapi.ResumeActorRequest{Actor: ref})
	if status.Code(err) != codes.OK {
		t.Fatalf("ResumeActor = %v, want OK", err)
	}
	volumeName := "substrate-" + actor.GetMetadata().GetUid() + "-data"
	want := proto.Clone(before).(*ateapi.Actor)
	want.Status.State = ateapi.ActorState_ACTOR_STATE_RUNNING
	want.Status.WorkerAssignment = workerAssignment(w)
	want.Status.AssignedNode = w.Node.Name
	want.Status.ExternalVolumes = &ateapi.ExternalVolumes{Volumes: []*ateapi.ExternalVolumeStatus{{
		Name:            "data",
		StorageVolumeId: "vol-" + volumeName,
		VolumeType:      harness.VolumeDriver,
	}}}
	if diff := cmp.Diff(want, resp.GetActor(), protocmp.Transform(), ignoreVersion, ignoreTimestamps); diff != "" {
		t.Errorf("resumed actor (-want +got):\n%s", diff)
	}
	wantVolumes := []harness.Volume{{ID: "vol-" + volumeName, Name: volumeName, AttachedTo: []string{node.Name}}}
	if diff := cmp.Diff(wantVolumes, h.Volumes.List()); diff != "" {
		t.Errorf("volumes after resume (-want +got):\n%s", diff)
	}
}

// Resumes an actor whose template has an external volume while the volume
// plugin fails CreateVolume. The request fails with INTERNAL and the actor
// stays SUSPENDED. A later resume creates and attaches the volume and
// succeeds (OK). Both resumes retry while ate-api catches up.
//
// TODO(): The request should fail with UNAVAILABLE when the volume plugin is
// unavailable, so callers like the atenet router retry it.
func TestResumeActor_VolumeCreateFails(t *testing.T) {
	t.Parallel()
	h := harness.New(t, harness.WithPreviewClient())
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	node := h.Node("a")
	h.AddWorker(pool, node)
	actor := h.NewActor(as, h.NewTemplate(as, pool, harness.WithExternalVolume("data", "/data")))
	ref := harness.ActorRef(actor)
	before := h.GetActor(ref)
	h.Volumes.On(harness.VolumeCreate).FailNext(harness.ErrUnavailable)

	_, err := h.ResumeActor(t.Context(), &ateapi.ResumeActorRequest{Actor: ref})
	if status.Code(err) != codes.Internal {
		t.Fatalf("ResumeActor with a failing CreateVolume = %v, want Internal", err)
	}
	if diff := cmp.Diff(before, h.GetActor(ref), protocmp.Transform(), ignoreVersion, ignoreTimestamps); diff != "" {
		t.Errorf("actor after a failed CreateVolume (-want +got):\n%s", diff)
	}

	if _, err := h.ResumeActor(t.Context(), &ateapi.ResumeActorRequest{Actor: ref}); status.Code(err) != codes.OK {
		t.Fatalf("ResumeActor retry = %v, want OK", err)
	}
	volumeName := "substrate-" + actor.GetMetadata().GetUid() + "-data"
	wantVolumes := []harness.Volume{{ID: "vol-" + volumeName, Name: volumeName, AttachedTo: []string{node.Name}}}
	if diff := cmp.Diff(wantVolumes, h.Volumes.List()); diff != "" {
		t.Errorf("volumes after the retry (-want +got):\n%s", diff)
	}
}

// Resumes an actor whose template has an external volume while the volume
// plugin fails AttachVolume once. The request fails with INTERNAL and the
// actor stays RESUMING with its worker. A later resume attaches the volume
// and succeeds (OK). Both resumes retry while ate-api catches up.
//
// TODO(): The request should fail with UNAVAILABLE when the volume plugin is
// unavailable, so callers like the atenet router retry it.
func TestResumeActor_VolumeAttachFails(t *testing.T) {
	t.Parallel()
	h := harness.New(t, harness.WithPreviewClient())
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	node := h.Node("a")
	w := h.AddWorker(pool, node)
	actor := h.NewActor(as, h.NewTemplate(as, pool, harness.WithExternalVolume("data", "/data")))
	ref := harness.ActorRef(actor)
	before := h.GetActor(ref)
	h.Volumes.On(harness.VolumeAttach).FailNext(harness.ErrUnavailable)

	_, err := h.ResumeActor(t.Context(), &ateapi.ResumeActorRequest{Actor: ref})
	if status.Code(err) != codes.Internal {
		t.Fatalf("ResumeActor with a failing AttachVolume = %v, want Internal", err)
	}
	volumeName := "substrate-" + actor.GetMetadata().GetUid() + "-data"
	createdVolume := &ateapi.ExternalVolumes{Volumes: []*ateapi.ExternalVolumeStatus{{
		Name:            "data",
		StorageVolumeId: "vol-" + volumeName,
		VolumeType:      harness.VolumeDriver,
	}}}
	want := proto.Clone(before).(*ateapi.Actor)
	want.Status.State = ateapi.ActorState_ACTOR_STATE_RESUMING
	want.Status.WorkerAssignment = workerAssignment(w)
	// ate-api records the worker epoch once a Restore succeeds.
	want.Status.WorkerAssignment.WorkerEpoch = 0
	want.Status.AssignedNode = w.Node.Name
	want.Status.ExternalVolumes = createdVolume
	if diff := cmp.Diff(want, h.GetActor(ref), protocmp.Transform(), ignoreVersion, ignoreTimestamps); diff != "" {
		t.Errorf("actor after a failed AttachVolume (-want +got):\n%s", diff)
	}

	resp, err := h.ResumeActor(t.Context(), &ateapi.ResumeActorRequest{Actor: ref})
	if status.Code(err) != codes.OK {
		t.Fatalf("ResumeActor retry = %v, want OK", err)
	}
	want.Status.State = ateapi.ActorState_ACTOR_STATE_RUNNING
	want.Status.WorkerAssignment.WorkerEpoch = w.Epoch
	if diff := cmp.Diff(want, resp.GetActor(), protocmp.Transform(), ignoreVersion, ignoreTimestamps); diff != "" {
		t.Errorf("actor after the retry (-want +got):\n%s", diff)
	}
	wantVolumes := []harness.Volume{{ID: "vol-" + volumeName, Name: volumeName, AttachedTo: []string{node.Name}}}
	if diff := cmp.Diff(wantVolumes, h.Volumes.List()); diff != "" {
		t.Errorf("volumes after the retry (-want +got):\n%s", diff)
	}
}

// Resumes an actor whose template has two external volumes while the volume
// plugin fails the second CreateVolume. The request fails with INTERNAL, and
// the actor stays SUSPENDED, recording the volume that was created. Deleting
// the actor succeeds (OK) and leaves the volume plugin with no volumes. The
// resume retries while ate-api catches up.
func TestResumeActor_VolumeCreateFailsThenDelete(t *testing.T) {
	t.Parallel()
	h := harness.New(t, harness.WithPreviewClient())
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	h.AddWorker(pool, h.Node("a"))
	tmpl := h.NewTemplate(as, pool, harness.WithExternalVolume("data", "/data"), harness.WithExternalVolume("logs", "/logs"))
	actor := h.NewActor(as, tmpl)
	ref := harness.ActorRef(actor)
	gate := h.Volumes.On(harness.VolumeCreate).Block()
	h.Volumes.On(harness.VolumeCreate).FailNext(harness.ErrUnavailable)

	resumed := make(chan error, 1)
	go func() {
		_, err := h.ResumeActor(t.Context(), &ateapi.ResumeActorRequest{Actor: ref})
		resumed <- err
	}()
	gate.Wait(t)
	gate.Release()
	if err := <-resumed; status.Code(err) != codes.Internal {
		t.Fatalf("ResumeActor with a failing CreateVolume = %v, want Internal", err)
	}
	dataVolume := "substrate-" + actor.GetMetadata().GetUid() + "-data"
	got := h.GetActor(ref)
	if got.GetStatus().GetState() != ateapi.ActorState_ACTOR_STATE_SUSPENDED {
		t.Errorf("actor after a failed CreateVolume is %v, want SUSPENDED", got.GetStatus().GetState())
	}
	wantVolumes := []harness.Volume{{ID: "vol-" + dataVolume, Name: dataVolume}}
	if diff := cmp.Diff(wantVolumes, h.Volumes.List(), cmpopts.EquateEmpty()); diff != "" {
		t.Errorf("volumes after a failed CreateVolume (-want +got):\n%s", diff)
	}

	if _, err := h.Control.DeleteActor(t.Context(), &ateapi.DeleteActorRequest{Actor: ref}); err != nil {
		t.Fatalf("DeleteActor: %v", err)
	}
	if diff := cmp.Diff([]harness.Volume{}, h.Volumes.List()); diff != "" {
		t.Errorf("volumes after delete (-want +got):\n%s", diff)
	}
}

// Resumes an actor whose template has an external volume while the volume
// plugin fails AttachVolume, leaving the actor RESUMING on its worker. Deleting
// the actor succeeds (OK): the volume plugin holds no volumes, and the worker
// is left with no allocation. The resume retries while ate-api catches up.
func TestResumeActor_VolumeAttachFailsThenDelete(t *testing.T) {
	t.Parallel()
	h := harness.New(t, harness.WithPreviewClient())
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	w := h.AddWorker(pool, h.Node("a"))
	ref := harness.ActorRef(h.NewActor(as, h.NewTemplate(as, pool, harness.WithExternalVolume("data", "/data"))))
	workerBefore := h.GetWorker(w)
	h.Volumes.On(harness.VolumeAttach).FailNext(harness.ErrUnavailable)
	if _, err := h.ResumeActor(t.Context(), &ateapi.ResumeActorRequest{Actor: ref}); err == nil {
		t.Fatal("ResumeActor succeeded despite a failing AttachVolume")
	}

	if _, err := h.Control.DeleteActor(t.Context(), &ateapi.DeleteActorRequest{Actor: ref}); err != nil {
		t.Fatalf("DeleteActor: %v", err)
	}
	if diff := cmp.Diff([]harness.Volume{}, h.Volumes.List()); diff != "" {
		t.Errorf("volumes after delete (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(workerBefore, h.GetWorker(w), protocmp.Transform(), ignoreVersion, ignoreTimestamps); diff != "" {
		t.Errorf("worker after delete (-want +got):\n%s", diff)
	}
}

// Resumes an actor whose template has two external volumes while the volume
// plugin fails the second AttachVolume. The request fails with INTERNAL, the
// actor stays RESUMING, and only the first volume is attached to the node. A
// later resume succeeds (OK) and both volumes are attached to the node. Both
// resumes retry while ate-api catches up.
func TestResumeActor_PartialVolumeAttachFails(t *testing.T) {
	t.Parallel()
	h := harness.New(t, harness.WithPreviewClient())
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	node := h.Node("a")
	h.AddWorker(pool, node)
	actor := h.NewActor(as, h.NewTemplate(as, pool, harness.WithExternalVolume("data", "/data"), harness.WithExternalVolume("logs", "/logs")))
	ref := harness.ActorRef(actor)
	gate := h.Volumes.On(harness.VolumeAttach).Block()
	h.Volumes.On(harness.VolumeAttach).FailNext(harness.ErrUnavailable)

	resumed := make(chan error, 1)
	go func() {
		_, err := h.ResumeActor(t.Context(), &ateapi.ResumeActorRequest{Actor: ref})
		resumed <- err
	}()
	gate.Wait(t)
	gate.Release()
	if err := <-resumed; status.Code(err) != codes.Internal {
		t.Fatalf("ResumeActor with a failing AttachVolume = %v, want Internal", err)
	}
	if got := h.GetActor(ref).GetStatus().GetState(); got != ateapi.ActorState_ACTOR_STATE_RESUMING {
		t.Errorf("actor after a failed AttachVolume is %v, want RESUMING", got)
	}
	dataVolume := "substrate-" + actor.GetMetadata().GetUid() + "-data"
	logsVolume := "substrate-" + actor.GetMetadata().GetUid() + "-logs"
	wantVolumes := []harness.Volume{
		{ID: "vol-" + dataVolume, Name: dataVolume, AttachedTo: []string{node.Name}},
		{ID: "vol-" + logsVolume, Name: logsVolume},
	}
	if diff := cmp.Diff(wantVolumes, h.Volumes.List(), cmpopts.EquateEmpty()); diff != "" {
		t.Errorf("volumes after a failed AttachVolume (-want +got):\n%s", diff)
	}

	if _, err := h.ResumeActor(t.Context(), &ateapi.ResumeActorRequest{Actor: ref}); err != nil {
		t.Fatalf("ResumeActor retry: %v", err)
	}
	wantVolumes[1].AttachedTo = []string{node.Name}
	if diff := cmp.Diff(wantVolumes, h.Volumes.List()); diff != "" {
		t.Errorf("volumes after the retry (-want +got):\n%s", diff)
	}
}

// Resumes an actor with an external volume that was suspended asynchronously
// on node a, after another actor took node a's only worker, while a worker on
// node b has room. The request succeeds (OK), the actor runs on node b, and
// its volume is attached to node b only. The resume retries while ate-api
// catches up.
func TestResumeActor_WithVolumeOnAnotherNode(t *testing.T) {
	t.Parallel()
	h := harness.New(t, harness.WithPreviewClient())
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	h.AddWorker(pool, h.Node("a"))
	actor := h.AsyncSuspendedActor(as, h.NewTemplate(as, pool, harness.WithExternalVolume("data", "/data")))
	ref := harness.ActorRef(actor)
	h.RunningActor(as, h.NewTemplate(as, pool))
	nodeB := h.Node("b")
	wB := h.AddWorker(pool, nodeB)

	resp, err := h.ResumeActor(t.Context(), &ateapi.ResumeActorRequest{Actor: ref})
	if err != nil {
		t.Fatalf("ResumeActor: %v", err)
	}
	if got := resp.GetActor().GetStatus().GetWorkerAssignment().GetWorker().GetName(); got != wB.Name {
		t.Errorf("actor runs on worker %q, want the node b worker %q", got, wB.Name)
	}
	volumeName := "substrate-" + actor.GetMetadata().GetUid() + "-data"
	wantVolumes := []harness.Volume{{ID: "vol-" + volumeName, Name: volumeName, AttachedTo: []string{nodeB.Name}}}
	if diff := cmp.Diff(wantVolumes, h.Volumes.List()); diff != "" {
		t.Errorf("volumes after resume (-want +got):\n%s", diff)
	}
}

// Resumes an actor whose template limits it to 500m of CPU and 1Gi of
// memory, on a worker reporting room for them. The request succeeds (OK),
// the atelet's Restore carries those limits, and the worker's allocation
// includes them. The resume retries while ate-api catches up.
func TestResumeActor_WithResourceLimits(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	node := h.Node("a")
	w := h.AddWorker(pool, node, harness.WithResourceCapacity("2", "4Gi"))
	limits := resources.CPUMemory(500, 1<<30)
	tmpl := h.NewTemplate(as, pool, func(tmpl *ateapi.ActorTemplate) {
		tmpl.Resources = &ateapi.ResourceRequirements{Limits: limits}
	})
	actor := h.NewActor(as, tmpl)
	ref := harness.ActorRef(actor)
	workerBefore := h.GetWorker(w)
	callsBefore := len(node.Atelet.Calls())

	if _, err := h.ResumeActor(t.Context(), &ateapi.ResumeActorRequest{Actor: ref}); status.Code(err) != codes.OK {
		t.Fatalf("ResumeActor = %v, want OK", err)
	}
	calls := node.Atelet.Calls()[callsBefore:]
	if len(calls) != 1 || calls[0].Op != harness.AteletRestore {
		t.Fatalf("atelet received %v, want exactly one Restore", calls)
	}
	restore := calls[0].Request.(*ateletpb.RestoreRequest)
	wantLimits := &ateletpb.ResourceRequirements{Limits: &ateletpb.Resources{CpuMillicores: 500, MemoryBytes: 1 << 30}}
	if diff := cmp.Diff(wantLimits, restore.GetResources(), protocmp.Transform()); diff != "" {
		t.Errorf("Restore resources (-want +got):\n%s", diff)
	}
	wantWorker := proto.Clone(workerBefore).(*ateapi.Worker)
	wantWorker.Status.Allocated = &ateapi.WorkerResources{Actors: 1, CpuMillicores: 500, MemoryBytes: 1 << 30}
	if diff := cmp.Diff(wantWorker, h.GetWorker(w), protocmp.Transform(), ignoreVersion, ignoreTimestamps); diff != "" {
		t.Errorf("worker after resume (-want +got):\n%s", diff)
	}
}

// Resumes an actor while a first resume of it is held at the atelet's
// Restore. The second request fails with ABORTED. Once the first is released
// it succeeds (OK), and the node runs exactly one sandbox for the actor. The
// first resume retries while ate-api catches up.
func TestResumeActor_Concurrent(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	node := h.Node("a")
	w := h.AddWorker(pool, node)
	actor := h.NewActor(as, h.NewTemplate(as, pool))
	ref := harness.ActorRef(actor)

	gate := node.Atelet.On(harness.AteletRestore).Block()
	first := make(chan error, 1)
	go func() {
		_, err := h.ResumeActor(t.Context(), &ateapi.ResumeActorRequest{Actor: ref})
		first <- err
	}()
	gate.Wait(t)
	_, err := h.Control.ResumeActor(t.Context(), &ateapi.ResumeActorRequest{Actor: ref})
	if status.Code(err) != codes.Aborted {
		t.Errorf("second ResumeActor while the first is held = %v, want Aborted", err)
	}
	gate.Release()
	if err := <-first; status.Code(err) != codes.OK {
		t.Fatalf("first ResumeActor = %v, want OK", err)
	}
	wantSandboxes := []harness.Sandbox{{
		ActorUID:  actor.GetMetadata().GetUid(),
		Atespace:  as,
		ActorName: actor.GetMetadata().GetName(),
		WorkerUID: w.Name,
	}}
	if diff := cmp.Diff(wantSandboxes, node.Atelet.Sandboxes()); diff != "" {
		t.Errorf("sandboxes after both resumes (-want +got):\n%s", diff)
	}
}

// Resumes a new actor after its template's golden snapshot was deleted from
// object storage. The request fails with INTERNAL, and the actor becomes
// CRASHED with a crash message naming the atelet Restore. The resume retries
// while ate-api catches up.
func TestResumeActor_GoldenSnapshotMissing(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	node := h.Node("a")
	h.AddWorker(pool, node)
	tmpl := h.NewTemplate(as, pool)
	golden, err := h.Control.GetTag(t.Context(), &ateapi.GetTagRequest{Tag: tmpl.GetStatus().GetGoldenSnapshotStatus().GetGoldenTag()})
	if err != nil {
		t.Fatalf("GetTag: %v", err)
	}
	goldenURI := golden.GetStatus().GetSnapshot().GetDurableSnapshot().GetObject().GetSnapshotUri()
	if err := h.ObjectStore.DeleteSnapshot(goldenURI); err != nil {
		t.Fatalf("DeleteSnapshot: %v", err)
	}
	actor := h.NewActor(as, tmpl)
	ref := harness.ActorRef(actor)
	before := h.GetActor(ref)

	_, err = h.ResumeActor(t.Context(), &ateapi.ResumeActorRequest{Actor: ref})
	if status.Code(err) != codes.Internal {
		t.Fatalf("ResumeActor with the golden snapshot missing = %v, want Internal", err)
	}
	want := proto.Clone(before).(*ateapi.Actor)
	want.Status.State = ateapi.ActorState_ACTOR_STATE_CRASHED
	want.Status.AssignedNode = node.Name
	want.Status.Crash = &ateapi.ActorCrash{Message: "resume failed: atelet Restore: while fetching the snapshot manifest: " + goldenURI + " not found"}
	ignoreCrashTime := protocmp.IgnoreFields(&ateapi.ActorCrash{}, "crash_time")
	if diff := cmp.Diff(want, h.GetActor(ref), protocmp.Transform(), ignoreVersion, ignoreTimestamps, ignoreCrashTime); diff != "" {
		t.Errorf("actor after a missing golden snapshot (-want +got):\n%s", diff)
	}
}

// Suspending an actor that doesn't exist fails with NOT_FOUND.
func TestSuspendActor_NotFound(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")

	_, err := h.Control.SuspendActor(t.Context(), &ateapi.SuspendActorRequest{Actor: &ateapi.ObjectRef{Atespace: as, Name: "actor-missing"}})
	if status.Code(err) != codes.NotFound {
		t.Errorf("SuspendActor of a missing actor = %v, want NotFound", err)
	}
}

// Suspending an actor with an invalid name fails with INVALID_ARGUMENT and
// the error names the invalid field.
func TestSuspendActor_ValidationError(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")

	_, err := h.Control.SuspendActor(t.Context(), &ateapi.SuspendActorRequest{Actor: &ateapi.ObjectRef{Atespace: as, Name: "Not_A_Valid_Name"}})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("SuspendActor with an invalid name = %v, want InvalidArgument", err)
	}
	if msg := status.Convert(err).Message(); !strings.Contains(msg, "actor.name") {
		t.Errorf("SuspendActor error %q does not name the invalid field actor.name", msg)
	}
}

// Suspending a crashed actor fails with FAILED_PRECONDITION and leaves the
// actor unchanged.
func TestSuspendActor_WrongState(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	h.AddWorker(pool, h.Node("a"))
	actor := h.CrashedActor(as, h.NewTemplate(as, pool))
	ref := harness.ActorRef(actor)
	before := h.GetActor(ref)

	_, err := h.Control.SuspendActor(t.Context(), &ateapi.SuspendActorRequest{Actor: ref})
	if status.Code(err) != codes.FailedPrecondition {
		t.Errorf("SuspendActor of a crashed actor = %v, want FailedPrecondition", err)
	}
	if diff := cmp.Diff(before, h.GetActor(ref), protocmp.Transform()); diff != "" {
		t.Errorf("actor after a refused SuspendActor (-want +got):\n%s", diff)
	}
}

// Suspends a running actor synchronously. The response matches GetActor. The
// actor becomes SUSPENDED with no worker assignment, still assigned to its
// node, and holds one snapshot of its own: the next generation, local to that
// node, with a completed upload. The worker is left with no allocation and no
// assignments. The atelet received exactly one Checkpoint, uploading
// immediately, runs no sandbox, and keeps the snapshot locally. Object storage
// gains the snapshot's objects.
func TestSuspendActor(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	node := h.Node("a")
	w := h.AddWorker(pool, node)
	actor := h.RunningActor(as, h.NewTemplate(as, pool))
	ref := harness.ActorRef(actor)

	before := h.GetActor(ref)
	workerBefore := h.GetWorker(w)
	objectsBefore := h.ObjectStore.Objects()
	callsBefore := len(node.Atelet.Calls())
	resp, err := h.Control.SuspendActor(t.Context(), &ateapi.SuspendActorRequest{
		Actor:   ref,
		Options: &ateapi.SuspendOptions{Mode: ateapi.DurabilityMode_DURABILITY_MODE_SYNCHRONOUS},
	})
	if err != nil {
		t.Fatalf("SuspendActor: %v", err)
	}

	got := h.GetActor(ref)
	if diff := cmp.Diff(got, resp.GetActor(), protocmp.Transform()); diff != "" {
		t.Errorf("SuspendActor response against GetActor (-get +response):\n%s", diff)
	}
	uuid := latestSnapshot(t, got).GetUuid()
	want := proto.Clone(before).(*ateapi.Actor)
	want.Status.State = ateapi.ActorState_ACTOR_STATE_SUSPENDED
	want.Status.WorkerAssignment = nil
	want.Status.Snapshots = []*ateapi.Snapshot{actorSnapshot(before, uuid, 2, node.Name, ateapi.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_COMPLETED)}
	want.Status.LastAssignedGeneration = 2
	if diff := cmp.Diff(want, got, protocmp.Transform(), ignoreVersion, ignoreTimestamps); diff != "" {
		t.Errorf("suspended actor (-want +got):\n%s", diff)
	}

	wantWorker := proto.Clone(workerBefore).(*ateapi.Worker)
	wantWorker.Status.Allocated = nil
	if diff := cmp.Diff(wantWorker, h.GetWorker(w), protocmp.Transform(), ignoreVersion, ignoreTimestamps); diff != "" {
		t.Errorf("worker after suspend (-want +got):\n%s", diff)
	}
	assignments, err := h.Control.ListWorkerActorAssignments(t.Context(), &ateapi.ListWorkerActorAssignmentsRequest{Worker: w.Ref()})
	if err != nil {
		t.Fatalf("ListWorkerActorAssignments: %v", err)
	}
	if diff := cmp.Diff(&ateapi.ListWorkerActorAssignmentsResponse{}, assignments, protocmp.Transform()); diff != "" {
		t.Errorf("worker assignments after suspend (-want +got):\n%s", diff)
	}

	calls := node.Atelet.Calls()[callsBefore:]
	if len(calls) != 1 || calls[0].Op != harness.AteletCheckpoint {
		t.Fatalf("atelet received %v, want exactly one Checkpoint", calls)
	}
	if got := calls[0].Request.(*ateletpb.CheckpointRequest).GetUploadConfig().GetMode(); got != ateletpb.UploadMode_UPLOAD_MODE_IMMEDIATE {
		t.Errorf("Checkpoint upload mode = %v, want IMMEDIATE", got)
	}
	if diff := cmp.Diff([]harness.Sandbox{}, node.Atelet.Sandboxes()); diff != "" {
		t.Errorf("sandboxes after suspend (-want +got):\n%s", diff)
	}
	if got := node.Atelet.LocalSnapshot(actor.GetMetadata().GetUid()); got != uuid {
		t.Errorf("local snapshot on node a = %q, want %q", got, uuid)
	}
	wantObjects := append(slices.Clone(objectsBefore), snapshotObjects(snapshotURI(before, uuid))...)
	if diff := cmp.Diff(wantObjects, h.ObjectStore.Objects(), cmpopts.SortSlices(func(a, b string) bool { return a < b })); diff != "" {
		t.Errorf("object storage after suspend (-want +got):\n%s", diff)
	}
}

// Suspends a running actor without a mode, which suspends it asynchronously.
// The actor becomes SUSPENDED with no worker assignment, still assigned to its
// node. It keeps the template's golden snapshot and gains one of its own: the
// next generation, local to that node, with a pending upload. The atelet
// received exactly one Checkpoint, deferring the upload, runs no sandbox, and
// keeps the snapshot locally. Object storage is unchanged.
func TestSuspendActor_Async(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	node := h.Node("a")
	h.AddWorker(pool, node)
	actor := h.RunningActor(as, h.NewTemplate(as, pool))
	ref := harness.ActorRef(actor)

	before := h.GetActor(ref)
	objectsBefore := h.ObjectStore.Objects()
	callsBefore := len(node.Atelet.Calls())
	resp, err := h.Control.SuspendActor(t.Context(), &ateapi.SuspendActorRequest{Actor: ref})
	if err != nil {
		t.Fatalf("SuspendActor: %v", err)
	}

	uuid := latestSnapshot(t, resp.GetActor()).GetUuid()
	want := proto.Clone(before).(*ateapi.Actor)
	want.Status.State = ateapi.ActorState_ACTOR_STATE_SUSPENDED
	want.Status.WorkerAssignment = nil
	want.Status.Snapshots = append(want.Status.Snapshots, actorSnapshot(before, uuid, 2, node.Name, ateapi.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_PENDING))
	want.Status.LastAssignedGeneration = 2
	if diff := cmp.Diff(want, resp.GetActor(), protocmp.Transform(), ignoreVersion, ignoreTimestamps); diff != "" {
		t.Errorf("suspended actor (-want +got):\n%s", diff)
	}

	calls := node.Atelet.Calls()[callsBefore:]
	if len(calls) != 1 || calls[0].Op != harness.AteletCheckpoint {
		t.Fatalf("atelet received %v, want exactly one Checkpoint", calls)
	}
	if got := calls[0].Request.(*ateletpb.CheckpointRequest).GetUploadConfig().GetMode(); got != ateletpb.UploadMode_UPLOAD_MODE_DEFERRED {
		t.Errorf("Checkpoint upload mode = %v, want DEFERRED", got)
	}
	if diff := cmp.Diff([]harness.Sandbox{}, node.Atelet.Sandboxes()); diff != "" {
		t.Errorf("sandboxes after suspend (-want +got):\n%s", diff)
	}
	if got := node.Atelet.LocalSnapshot(actor.GetMetadata().GetUid()); got != uuid {
		t.Errorf("local snapshot on node a = %q, want %q", got, uuid)
	}
	if diff := cmp.Diff(objectsBefore, h.ObjectStore.Objects()); diff != "" {
		t.Errorf("object storage after suspend (-want +got):\n%s", diff)
	}
}

// Suspends an actor that is already suspended with a node-local snapshot,
// once synchronously and once without a mode. Both succeed (OK) and return the
// actor unchanged. The atelet receives no call and keeps the local snapshot.
func TestSuspendActor_AlreadySuspended(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	node := h.Node("a")
	h.AddWorker(pool, node)
	actor := h.AsyncSuspendedActor(as, h.NewTemplate(as, pool))
	ref := harness.ActorRef(actor)
	before := h.GetActor(ref)
	callsBefore := len(node.Atelet.Calls())

	for _, mode := range []ateapi.DurabilityMode{ateapi.DurabilityMode_DURABILITY_MODE_SYNCHRONOUS, ateapi.DurabilityMode_DURABILITY_MODE_UNSPECIFIED} {
		resp, err := h.Control.SuspendActor(t.Context(), &ateapi.SuspendActorRequest{Actor: ref, Options: &ateapi.SuspendOptions{Mode: mode}})
		if err != nil {
			t.Fatalf("SuspendActor in mode %v: %v", mode, err)
		}
		if diff := cmp.Diff(before, resp.GetActor(), protocmp.Transform()); diff != "" {
			t.Errorf("SuspendActor in mode %v response (-want +got):\n%s", mode, diff)
		}
	}
	if diff := cmp.Diff(before, h.GetActor(ref), protocmp.Transform()); diff != "" {
		t.Errorf("actor after suspending it again (-want +got):\n%s", diff)
	}
	if calls := node.Atelet.Calls()[callsBefore:]; len(calls) != 0 {
		t.Errorf("atelet received %v, want no calls", calls)
	}
	if got, want := node.Atelet.LocalSnapshot(actor.GetMetadata().GetUid()), latestSnapshot(t, before).GetUuid(); got != want {
		t.Errorf("local snapshot on node a = %q, want %q", got, want)
	}
}

// Suspends a running actor while the atelet fails the Checkpoint. The request
// fails with INTERNAL, and the actor becomes CRASHED with no worker
// assignment, still assigned to its node. It keeps the pending snapshot the
// suspend started, the next generation with no locality. Its status.crash is
// set: a message naming the atelet Checkpoint and the atelet's error, and a
// crash time. It is otherwise unchanged. The worker is left with no
// allocation.
func TestSuspendActor_CheckpointFails(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	node := h.Node("a")
	w := h.AddWorker(pool, node)
	actor := h.RunningActor(as, h.NewTemplate(as, pool))
	ref := harness.ActorRef(actor)
	before := h.GetActor(ref)
	workerBefore := h.GetWorker(w)
	node.Atelet.On(harness.AteletCheckpoint).FailNext(harness.ErrSandboxFailed)

	_, err := h.Control.SuspendActor(t.Context(), &ateapi.SuspendActorRequest{Actor: ref})
	if status.Code(err) != codes.Internal {
		t.Errorf("SuspendActor with a failing Checkpoint = %v, want Internal", err)
	}
	crashed := h.GetActor(ref)
	want := proto.Clone(before).(*ateapi.Actor)
	want.Status.State = ateapi.ActorState_ACTOR_STATE_CRASHED
	want.Status.WorkerAssignment = nil
	want.Status.Snapshots = append(want.Status.Snapshots, actorSnapshot(before, latestSnapshot(t, crashed).GetUuid(), 2, "", ateapi.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_PENDING))
	want.Status.LastAssignedGeneration = 2
	want.Status.Crash = &ateapi.ActorCrash{Message: "suspend failed: atelet Checkpoint: harness: sandbox operation failed"}
	ignoreCrashTime := protocmp.IgnoreFields(&ateapi.ActorCrash{}, "crash_time")
	if diff := cmp.Diff(want, crashed, protocmp.Transform(), ignoreVersion, ignoreTimestamps, ignoreCrashTime); diff != "" {
		t.Errorf("actor after a failed Checkpoint (-want +got):\n%s", diff)
	}
	if crashed.GetStatus().GetCrash().GetCrashTime() == nil {
		t.Errorf("status.crash of the crashed actor has no crash time")
	}
	wantWorker := proto.Clone(workerBefore).(*ateapi.Worker)
	wantWorker.Status.Allocated = nil
	if diff := cmp.Diff(wantWorker, h.GetWorker(w), protocmp.Transform(), ignoreVersion, ignoreTimestamps); diff != "" {
		t.Errorf("worker after a failed Checkpoint (-want +got):\n%s", diff)
	}
}

// Suspends a running actor while the atelet reports a snapshot conflict
// (ABORTED) on the Checkpoint. The request fails with ABORTED, and the actor
// becomes SUSPENDING on its worker, keeping its last completed snapshot and
// gaining a pending one. Suspending it again sends the same pending snapshot,
// succeeds (OK), and the actor is SUSPENDED.
func TestSuspendActor_CheckpointConflict(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	node := h.Node("a")
	h.AddWorker(pool, node)
	actor := h.RunningActor(as, h.NewTemplate(as, pool))
	ref := harness.ActorRef(actor)
	before := h.GetActor(ref)
	node.Atelet.On(harness.AteletCheckpoint).FailNext(status.Error(codes.Aborted, "injected snapshot conflict"))
	callsBefore := len(node.Atelet.Calls())

	_, err := h.Control.SuspendActor(t.Context(), &ateapi.SuspendActorRequest{Actor: ref})
	if status.Code(err) != codes.Aborted {
		t.Fatalf("SuspendActor with a conflicting Checkpoint = %v, want Aborted", err)
	}
	suspending := h.GetActor(ref)
	pending := latestSnapshot(t, suspending)
	want := proto.Clone(before).(*ateapi.Actor)
	want.Status.State = ateapi.ActorState_ACTOR_STATE_SUSPENDING
	want.Status.Snapshots = append(want.Status.Snapshots, actorSnapshot(before, pending.GetUuid(), 2, "", ateapi.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_PENDING))
	want.Status.LastAssignedGeneration = 2
	if diff := cmp.Diff(want, suspending, protocmp.Transform(), ignoreVersion, ignoreTimestamps); diff != "" {
		t.Errorf("actor after a conflicting Checkpoint (-want +got):\n%s", diff)
	}

	resp, err := h.Control.SuspendActor(t.Context(), &ateapi.SuspendActorRequest{Actor: ref})
	if err != nil {
		t.Fatalf("SuspendActor retry: %v", err)
	}
	if got := resp.GetActor().GetStatus().GetState(); got != ateapi.ActorState_ACTOR_STATE_SUSPENDED {
		t.Errorf("actor after the retry is %v, want SUSPENDED", got)
	}
	calls := node.Atelet.Calls()[callsBefore:]
	if len(calls) != 2 {
		t.Fatalf("atelet received %v, want two Checkpoints", calls)
	}
	for i, c := range calls {
		if got := c.Request.(*ateletpb.CheckpointRequest).GetSnapshot().GetSnapshotUuid(); c.Op != harness.AteletCheckpoint || got != pending.GetUuid() {
			t.Errorf("atelet call %d = %s of snapshot %q, want a Checkpoint of %q", i, c.Op, got, pending.GetUuid())
		}
	}
}

// Suspends a running actor while the atelet is unavailable for the
// Checkpoint. The request fails with UNAVAILABLE, and the actor becomes
// SUSPENDING on its worker. Resuming it fails with FAILED_PRECONDITION and
// leaves it unchanged. Suspending it again succeeds (OK), and the actor is
// SUSPENDED.
func TestSuspendActor_CheckpointUnavailable(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	node := h.Node("a")
	h.AddWorker(pool, node)
	actor := h.RunningActor(as, h.NewTemplate(as, pool))
	ref := harness.ActorRef(actor)
	node.Atelet.On(harness.AteletCheckpoint).FailNext(harness.ErrUnavailable)

	_, err := h.Control.SuspendActor(t.Context(), &ateapi.SuspendActorRequest{Actor: ref})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("SuspendActor with an unavailable atelet = %v, want Unavailable", err)
	}
	suspending := h.GetActor(ref)
	if got := suspending.GetStatus().GetState(); got != ateapi.ActorState_ACTOR_STATE_SUSPENDING {
		t.Fatalf("actor after an unavailable atelet is %v, want SUSPENDING", got)
	}
	if diff := cmp.Diff(actor.GetStatus().GetWorkerAssignment(), suspending.GetStatus().GetWorkerAssignment(), protocmp.Transform()); diff != "" {
		t.Errorf("worker assignment after an unavailable atelet (-want +got):\n%s", diff)
	}

	_, err = h.Control.ResumeActor(t.Context(), &ateapi.ResumeActorRequest{Actor: ref})
	if status.Code(err) != codes.FailedPrecondition {
		t.Errorf("ResumeActor of the SUSPENDING actor = %v, want FailedPrecondition", err)
	}
	if diff := cmp.Diff(suspending, h.GetActor(ref), protocmp.Transform()); diff != "" {
		t.Errorf("actor after a refused resume (-want +got):\n%s", diff)
	}

	resp, err := h.Control.SuspendActor(t.Context(), &ateapi.SuspendActorRequest{Actor: ref})
	if err != nil {
		t.Fatalf("SuspendActor retry: %v", err)
	}
	if got := resp.GetActor().GetStatus().GetState(); got != ateapi.ActorState_ACTOR_STATE_SUSPENDED {
		t.Errorf("actor after the retry is %v, want SUSPENDED", got)
	}
}

// Suspends a running actor with an external volume synchronously. The request
// succeeds (OK), the actor keeps its volume in its status, and the volume
// plugin holds the volume attached to no node.
func TestSuspendActor_WithVolume(t *testing.T) {
	t.Parallel()
	h := harness.New(t, harness.WithPreviewClient())
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	h.AddWorker(pool, h.Node("a"))
	actor := h.RunningActor(as, h.NewTemplate(as, pool, harness.WithExternalVolume("data", "/data")))
	ref := harness.ActorRef(actor)

	resp, err := h.Control.SuspendActor(t.Context(), &ateapi.SuspendActorRequest{
		Actor:   ref,
		Options: &ateapi.SuspendOptions{Mode: ateapi.DurabilityMode_DURABILITY_MODE_SYNCHRONOUS},
	})
	if err != nil {
		t.Fatalf("SuspendActor: %v", err)
	}
	if diff := cmp.Diff(actor.GetStatus().GetExternalVolumes(), resp.GetActor().GetStatus().GetExternalVolumes(), protocmp.Transform()); diff != "" {
		t.Errorf("external volumes of the suspended actor (-want +got):\n%s", diff)
	}
	volumeName := "substrate-" + actor.GetMetadata().GetUid() + "-data"
	if diff := cmp.Diff([]harness.Volume{{ID: "vol-" + volumeName, Name: volumeName}}, h.Volumes.List(), cmpopts.EquateEmpty()); diff != "" {
		t.Errorf("volumes after suspend (-want +got):\n%s", diff)
	}
}

// Suspends a running actor with an external volume while the volume plugin
// fails DetachVolume. The request fails with INTERNAL, and the actor becomes
// SUSPENDING on its worker, with its volume still attached to the node.
// Suspending it again succeeds (OK): the actor is SUSPENDED and its volume is
// attached to no node.
func TestSuspendActor_VolumeDetachFails(t *testing.T) {
	t.Parallel()
	h := harness.New(t, harness.WithPreviewClient())
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	node := h.Node("a")
	h.AddWorker(pool, node)
	actor := h.RunningActor(as, h.NewTemplate(as, pool, harness.WithExternalVolume("data", "/data")))
	ref := harness.ActorRef(actor)
	h.Volumes.On(harness.VolumeDetach).FailNext(harness.ErrUnavailable)

	_, err := h.Control.SuspendActor(t.Context(), &ateapi.SuspendActorRequest{Actor: ref})
	if status.Code(err) != codes.Internal {
		t.Fatalf("SuspendActor with a failing DetachVolume = %v, want Internal", err)
	}
	suspending := h.GetActor(ref)
	if got := suspending.GetStatus().GetState(); got != ateapi.ActorState_ACTOR_STATE_SUSPENDING {
		t.Errorf("actor after a failed DetachVolume is %v, want SUSPENDING", got)
	}
	if diff := cmp.Diff(actor.GetStatus().GetWorkerAssignment(), suspending.GetStatus().GetWorkerAssignment(), protocmp.Transform()); diff != "" {
		t.Errorf("worker assignment after a failed DetachVolume (-want +got):\n%s", diff)
	}
	volumeName := "substrate-" + actor.GetMetadata().GetUid() + "-data"
	wantVolumes := []harness.Volume{{ID: "vol-" + volumeName, Name: volumeName, AttachedTo: []string{node.Name}}}
	if diff := cmp.Diff(wantVolumes, h.Volumes.List()); diff != "" {
		t.Errorf("volumes after a failed DetachVolume (-want +got):\n%s", diff)
	}

	resp, err := h.Control.SuspendActor(t.Context(), &ateapi.SuspendActorRequest{Actor: ref})
	if err != nil {
		t.Fatalf("SuspendActor retry: %v", err)
	}
	if got := resp.GetActor().GetStatus().GetState(); got != ateapi.ActorState_ACTOR_STATE_SUSPENDED {
		t.Errorf("actor after the retry is %v, want SUSPENDED", got)
	}
	wantVolumes[0].AttachedTo = nil
	if diff := cmp.Diff(wantVolumes, h.Volumes.List(), cmpopts.EquateEmpty()); diff != "" {
		t.Errorf("volumes after the retry (-want +got):\n%s", diff)
	}
}

// Resumes an actor while a suspend of it is held at the atelet's Checkpoint.
// The resume fails with ABORTED and leaves the actor unchanged. Once the
// Checkpoint is released, the suspend succeeds (OK).
func TestResumeActor_SuspendInProgress(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	node := h.Node("a")
	h.AddWorker(pool, node)
	actor := h.RunningActor(as, h.NewTemplate(as, pool))
	ref := harness.ActorRef(actor)

	gate := node.Atelet.On(harness.AteletCheckpoint).Block()
	suspended := make(chan error, 1)
	go func() {
		_, err := h.Control.SuspendActor(t.Context(), &ateapi.SuspendActorRequest{Actor: ref})
		suspended <- err
	}()
	gate.Wait(t)
	before := h.GetActor(ref)
	_, err := h.Control.ResumeActor(t.Context(), &ateapi.ResumeActorRequest{Actor: ref})
	if status.Code(err) != codes.Aborted {
		t.Errorf("ResumeActor while a suspend is held = %v, want Aborted", err)
	}
	if diff := cmp.Diff(before, h.GetActor(ref), protocmp.Transform()); diff != "" {
		t.Errorf("actor after a refused resume (-want +got):\n%s", diff)
	}
	gate.Release()
	if err := <-suspended; err != nil {
		t.Errorf("SuspendActor: %v", err)
	}
}

// Reverts a running actor that was suspended once and resumed again. The
// response matches GetActor. The actor becomes SUSPENDED with no worker
// assignment and no assigned node, keeping the snapshot its suspend wrote,
// which is no longer local to the node. It is otherwise unchanged. The atelet received exactly one Terminate and no Checkpoint, and
// runs no sandbox. The worker is left with no allocation, and object storage
// is unchanged.
func TestRevertActor(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	node := h.Node("a")
	w := h.AddWorker(pool, node)
	ref := harness.ActorRef(h.SuspendedActor(as, h.NewTemplate(as, pool)))
	if _, err := h.ResumeActor(t.Context(), &ateapi.ResumeActorRequest{Actor: ref}); err != nil {
		t.Fatalf("ResumeActor: %v", err)
	}

	before := h.GetActor(ref)
	workerBefore := h.GetWorker(w)
	objectsBefore := h.ObjectStore.Objects()
	callsBefore := len(node.Atelet.Calls())
	resp, err := h.Control.RevertActor(t.Context(), &ateapi.RevertActorRequest{Actor: ref})
	if err != nil {
		t.Fatalf("RevertActor: %v", err)
	}

	got := h.GetActor(ref)
	if diff := cmp.Diff(got, resp.GetActor(), protocmp.Transform()); diff != "" {
		t.Errorf("RevertActor response against GetActor (-get +response):\n%s", diff)
	}
	want := proto.Clone(before).(*ateapi.Actor)
	want.Status.State = ateapi.ActorState_ACTOR_STATE_SUSPENDED
	want.Status.WorkerAssignment = nil
	want.Status.AssignedNode = ""
	for _, snap := range want.Status.Snapshots {
		snap.Locality = ""
	}
	if diff := cmp.Diff(want, got, protocmp.Transform(), ignoreVersion, ignoreTimestamps); diff != "" {
		t.Errorf("reverted actor (-want +got):\n%s", diff)
	}
	if calls := node.Atelet.Calls()[callsBefore:]; len(calls) != 1 || calls[0].Op != harness.AteletTerminate {
		t.Errorf("atelet received %v, want exactly one Terminate", calls)
	}
	if diff := cmp.Diff([]harness.Sandbox{}, node.Atelet.Sandboxes()); diff != "" {
		t.Errorf("sandboxes after revert (-want +got):\n%s", diff)
	}
	wantWorker := proto.Clone(workerBefore).(*ateapi.Worker)
	wantWorker.Status.Allocated = nil
	if diff := cmp.Diff(wantWorker, h.GetWorker(w), protocmp.Transform(), ignoreVersion, ignoreTimestamps); diff != "" {
		t.Errorf("worker after revert (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(objectsBefore, h.ObjectStore.Objects()); diff != "" {
		t.Errorf("object storage after revert (-want +got):\n%s", diff)
	}
}

// Reverts an actor suspended asynchronously, whose latest snapshot is local to
// its node and not uploaded. The request succeeds (OK). The actor is
// SUSPENDED with no assigned node, holding only the template's golden
// snapshot, and is otherwise unchanged. The atelet received exactly one Terminate and no longer holds the
// local snapshot.
func TestRevertActor_FromAsyncSuspended(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	node := h.Node("a")
	h.AddWorker(pool, node)
	actor := h.AsyncSuspendedActor(as, h.NewTemplate(as, pool))
	ref := harness.ActorRef(actor)
	before := h.GetActor(ref)
	callsBefore := len(node.Atelet.Calls())

	resp, err := h.Control.RevertActor(t.Context(), &ateapi.RevertActorRequest{Actor: ref})
	if err != nil {
		t.Fatalf("RevertActor: %v", err)
	}
	want := proto.Clone(before).(*ateapi.Actor)
	want.Status.AssignedNode = ""
	want.Status.Snapshots = want.Status.Snapshots[:1]
	if diff := cmp.Diff(want, resp.GetActor(), protocmp.Transform(), ignoreVersion, ignoreTimestamps); diff != "" {
		t.Errorf("reverted actor (-want +got):\n%s", diff)
	}
	if calls := node.Atelet.Calls()[callsBefore:]; len(calls) != 1 || calls[0].Op != harness.AteletTerminate {
		t.Errorf("atelet received %v, want exactly one Terminate", calls)
	}
	if got := node.Atelet.LocalSnapshot(actor.GetMetadata().GetUid()); got != "" {
		t.Errorf("local snapshot on node a = %q, want none", got)
	}
}

// Reverts an actor crashed by a failed suspend Checkpoint. The request
// succeeds (OK). The actor is SUSPENDED with no crash and no assigned node,
// holding only the template's golden snapshot: the pending snapshot of the
// failed suspend is discarded. It is otherwise unchanged. A later resume succeeds (OK).
func TestRevertActor_FromCrashed(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	h.AddWorker(pool, h.Node("a"))
	ref := harness.ActorRef(h.CrashedActor(as, h.NewTemplate(as, pool)))
	before := h.GetActor(ref)

	resp, err := h.Control.RevertActor(t.Context(), &ateapi.RevertActorRequest{Actor: ref})
	if err != nil {
		t.Fatalf("RevertActor: %v", err)
	}
	want := proto.Clone(before).(*ateapi.Actor)
	want.Status.State = ateapi.ActorState_ACTOR_STATE_SUSPENDED
	want.Status.Crash = nil
	want.Status.AssignedNode = ""
	want.Status.Snapshots = want.Status.Snapshots[:1]
	if diff := cmp.Diff(want, resp.GetActor(), protocmp.Transform(), ignoreVersion, ignoreTimestamps); diff != "" {
		t.Errorf("reverted actor (-want +got):\n%s", diff)
	}
	if _, err := h.ResumeActor(t.Context(), &ateapi.ResumeActorRequest{Actor: ref}); err != nil {
		t.Errorf("ResumeActor after revert: %v", err)
	}
}

// Reverts an actor with an external volume crashed by the removal of its
// worker. The request succeeds (OK). The actor is SUSPENDED with no assigned
// node, and its volume is attached to no node.
func TestRevertActor_WorkerRemovedDetachesVolume(t *testing.T) {
	t.Parallel()
	h := harness.New(t, harness.WithPreviewClient())
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	w := h.AddWorker(pool, h.Node("a"))
	actor := h.RunningActor(as, h.NewTemplate(as, pool, harness.WithExternalVolume("data", "/data")))
	ref := harness.ActorRef(actor)
	w.Remove()
	if got := h.GetActor(ref).GetStatus().GetState(); got != ateapi.ActorState_ACTOR_STATE_CRASHED {
		t.Fatalf("actor after its worker was removed is %v, want CRASHED", got)
	}

	resp, err := h.Control.RevertActor(t.Context(), &ateapi.RevertActorRequest{Actor: ref})
	if err != nil {
		t.Fatalf("RevertActor: %v", err)
	}
	if got := resp.GetActor().GetStatus(); got.GetState() != ateapi.ActorState_ACTOR_STATE_SUSPENDED || got.GetAssignedNode() != "" {
		t.Errorf("reverted actor is %v assigned to %q, want SUSPENDED with no assigned node", got.GetState(), got.GetAssignedNode())
	}
	volumeName := "substrate-" + actor.GetMetadata().GetUid() + "-data"
	if diff := cmp.Diff([]harness.Volume{{ID: "vol-" + volumeName, Name: volumeName}}, h.Volumes.List(), cmpopts.EquateEmpty()); diff != "" {
		t.Errorf("volumes after revert (-want +got):\n%s", diff)
	}
}

// Reverts a running actor while the atelet fails the Terminate. The request
// fails with INTERNAL, and the actor becomes REVERTING on its worker. Resuming
// it fails with FAILED_PRECONDITION and leaves it unchanged. Reverting it again
// succeeds (OK), and the actor is SUSPENDED with no worker assignment.
func TestRevertActor_TerminateFails(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	node := h.Node("a")
	h.AddWorker(pool, node)
	actor := h.RunningActor(as, h.NewTemplate(as, pool))
	ref := harness.ActorRef(actor)
	before := h.GetActor(ref)
	node.Atelet.On(harness.AteletTerminate).FailNext(harness.ErrSandboxFailed)

	_, err := h.Control.RevertActor(t.Context(), &ateapi.RevertActorRequest{Actor: ref})
	if status.Code(err) != codes.Internal {
		t.Fatalf("RevertActor with a failing Terminate = %v, want Internal", err)
	}
	reverting := h.GetActor(ref)
	want := proto.Clone(before).(*ateapi.Actor)
	want.Status.State = ateapi.ActorState_ACTOR_STATE_REVERTING
	if diff := cmp.Diff(want, reverting, protocmp.Transform(), ignoreVersion, ignoreTimestamps); diff != "" {
		t.Errorf("actor after a failed Terminate (-want +got):\n%s", diff)
	}

	_, err = h.Control.ResumeActor(t.Context(), &ateapi.ResumeActorRequest{Actor: ref})
	if status.Code(err) != codes.FailedPrecondition {
		t.Errorf("ResumeActor of the REVERTING actor = %v, want FailedPrecondition", err)
	}
	if diff := cmp.Diff(reverting, h.GetActor(ref), protocmp.Transform()); diff != "" {
		t.Errorf("actor after a refused resume (-want +got):\n%s", diff)
	}

	resp, err := h.Control.RevertActor(t.Context(), &ateapi.RevertActorRequest{Actor: ref})
	if err != nil {
		t.Fatalf("RevertActor retry: %v", err)
	}
	if got := resp.GetActor().GetStatus(); got.GetState() != ateapi.ActorState_ACTOR_STATE_SUSPENDED || got.GetWorkerAssignment() != nil {
		t.Errorf("actor after the retry is %v with assignment %v, want SUSPENDED with none", got.GetState(), got.GetWorkerAssignment())
	}
}

// Reverting an actor that doesn't exist fails with NOT_FOUND.
func TestRevertActor_NotFound(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")

	_, err := h.Control.RevertActor(t.Context(), &ateapi.RevertActorRequest{Actor: &ateapi.ObjectRef{Atespace: as, Name: "actor-missing"}})
	if status.Code(err) != codes.NotFound {
		t.Errorf("RevertActor of a missing actor = %v, want NotFound", err)
	}
}

// Reverting an actor with an invalid name fails with INVALID_ARGUMENT and
// the error names the invalid field.
func TestRevertActor_ValidationError(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")

	_, err := h.Control.RevertActor(t.Context(), &ateapi.RevertActorRequest{Actor: &ateapi.ObjectRef{Atespace: as, Name: "Not_A_Valid_Name"}})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("RevertActor with an invalid name = %v, want InvalidArgument", err)
	}
	if msg := status.Convert(err).Message(); !strings.Contains(msg, "actor.name") {
		t.Errorf("RevertActor error %q does not name the invalid field actor.name", msg)
	}
}

// Reverting a suspended actor fails with FAILED_PRECONDITION and leaves the
// actor unchanged.
func TestRevertActor_WrongState(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	h.AddWorker(pool, h.Node("a"))
	actor := h.NewActor(as, h.NewTemplate(as, pool))
	ref := harness.ActorRef(actor)
	before := h.GetActor(ref)

	_, err := h.Control.RevertActor(t.Context(), &ateapi.RevertActorRequest{Actor: ref})
	if status.Code(err) != codes.FailedPrecondition {
		t.Errorf("RevertActor of a suspended actor = %v, want FailedPrecondition", err)
	}
	if diff := cmp.Diff(before, h.GetActor(ref), protocmp.Transform()); diff != "" {
		t.Errorf("actor after a refused RevertActor (-want +got):\n%s", diff)
	}
}

// Minting a JWT for an actor that doesn't exist fails with NOT_FOUND.
func TestMintActorJwt_NotFound(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")

	_, err := h.EgressGateway().MintActorJwt(t.Context(), &ateapi.MintActorJwtRequest{
		Actor:             &ateapi.ObjectRef{Atespace: as, Name: "actor-missing"},
		Audiences:         []string{"https://audience.example"},
		ExpirationSeconds: 600,
	})
	if status.Code(err) != codes.NotFound {
		t.Errorf("MintActorJwt for a missing actor = %v, want NotFound", err)
	}
}

// Minting a JWT for an actor with an invalid name fails with
// INVALID_ARGUMENT and the error names the invalid field.
func TestMintActorJwt_ValidationError(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")

	_, err := h.Control.MintActorJwt(t.Context(), &ateapi.MintActorJwtRequest{
		Actor:             &ateapi.ObjectRef{Atespace: as, Name: "Not_A_Valid_Name"},
		Audiences:         []string{"https://audience.example"},
		ExpirationSeconds: 600,
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("MintActorJwt with an invalid name = %v, want InvalidArgument", err)
	}
	if msg := status.Convert(err).Message(); !strings.Contains(msg, "actor.name") {
		t.Errorf("MintActorJwt error %q does not name the invalid field actor.name", msg)
	}
}

// Minting a worker certificate, as a node's atelet, for an actor that
// doesn't exist fails with NOT_FOUND.
func TestMintWorkerActorCertificate_NotFound(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	node := h.Node("a")
	w := h.AddWorker(h.CreateWorkerPool("pool1", nil), node)

	_, err := node.WorkerService().MintWorkerActorCertificate(t.Context(), &ateapi.MintWorkerActorCertificateRequest{
		Worker:                    w.Ref(),
		Actor:                     &ateapi.ObjectRef{Atespace: as, Name: "actor-missing"},
		ActorUid:                  foreignUID,
		CertificateSigningRequest: certificateSigningRequest(t),
	})
	if status.Code(err) != codes.NotFound {
		t.Errorf("MintWorkerActorCertificate for a missing actor = %v, want NotFound", err)
	}
}

// Minting a worker certificate, as a node's atelet, for an actor with an
// invalid name fails with INVALID_ARGUMENT and the error names the invalid
// field.
func TestMintWorkerActorCertificate_ValidationError(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	node := h.Node("a")
	w := h.AddWorker(h.CreateWorkerPool("pool1", nil), node)

	_, err := node.WorkerService().MintWorkerActorCertificate(t.Context(), &ateapi.MintWorkerActorCertificateRequest{
		Worker:                    w.Ref(),
		Actor:                     &ateapi.ObjectRef{Atespace: as, Name: "Not_A_Valid_Name"},
		ActorUid:                  foreignUID,
		CertificateSigningRequest: certificateSigningRequest(t),
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("MintWorkerActorCertificate with an invalid name = %v, want InvalidArgument", err)
	}
	if msg := status.Convert(err).Message(); !strings.Contains(msg, "actor.name") {
		t.Errorf("MintWorkerActorCertificate error %q does not name the invalid field actor.name", msg)
	}
}

// Requesting, as a node's atelet, the suspend of an actor that doesn't exist
// fails with NOT_FOUND.
func TestRequestActorSuspend_NotFound(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	node := h.Node("a")
	w := h.AddWorker(h.CreateWorkerPool("pool1", nil), node)

	_, err := node.WorkerService().RequestActorSuspend(t.Context(), &ateapi.RequestActorSuspendRequest{
		Worker:   w.Ref(),
		Actor:    &ateapi.ObjectRef{Atespace: as, Name: "actor-missing"},
		ActorUid: foreignUID,
	})
	if status.Code(err) != codes.NotFound {
		t.Errorf("RequestActorSuspend of a missing actor = %v, want NotFound", err)
	}
}

// Requesting, as a node's atelet, the suspend of an actor with an invalid
// name fails with INVALID_ARGUMENT and the error names the invalid field.
func TestRequestActorSuspend_ValidationError(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	node := h.Node("a")
	w := h.AddWorker(h.CreateWorkerPool("pool1", nil), node)

	_, err := node.WorkerService().RequestActorSuspend(t.Context(), &ateapi.RequestActorSuspendRequest{
		Worker:   w.Ref(),
		Actor:    &ateapi.ObjectRef{Atespace: as, Name: "Not_A_Valid_Name"},
		ActorUid: foreignUID,
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("RequestActorSuspend with an invalid name = %v, want InvalidArgument", err)
	}
	if msg := status.Convert(err).Message(); !strings.Contains(msg, "actor.name") {
		t.Errorf("RequestActorSuspend error %q does not name the invalid field actor.name", msg)
	}
}

// createTag tags the snapshot of source, a suspended actor, as name in its
// atespace.
func createTag(t *testing.T, h *harness.Harness, source *ateapi.Actor, name string) *ateapi.Tag {
	t.Helper()
	tag, err := h.Control.CreateTag(t.Context(), &ateapi.CreateTagRequest{Tag: &ateapi.Tag{
		Metadata:    &ateapi.ResourceMetadata{Atespace: source.GetMetadata().GetAtespace(), Name: name},
		Scope:       ateapi.TagScope_TAG_SCOPE_ATESPACE,
		SourceActor: harness.ActorRef(source),
	}})
	if err != nil {
		t.Fatalf("CreateTag: %v", err)
	}
	return tag
}

// withDataFidelity makes the template's actors snapshot only their volumes.
func withDataFidelity(tmpl *ateapi.ActorTemplate) {
	tmpl.SnapshotConfig = &ateapi.SnapshotConfig{PreferredFidelity: ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES}
}

// Creates an actor from a tag of a suspended actor and verifies that the whole
// resource has the expected shape: SUSPENDED, naming the tag as its source,
// and holding the tag's snapshot.
func TestCreateActor_FromTag(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	h.AddWorker(pool, h.Node("a"))
	tmpl := h.NewTemplate(as, pool)
	tag := createTag(t, h, h.SuspendedActor(as, tmpl), "tag-a")
	tagRef := &ateapi.ObjectRef{Atespace: as, Name: "tag-a"}
	templateRef := &ateapi.ObjectRef{Atespace: as, Name: tmpl.GetMetadata().GetName()}

	created, err := h.Control.CreateActor(t.Context(), &ateapi.CreateActorRequest{Actor: &ateapi.Actor{
		Metadata:      &ateapi.ResourceMetadata{Atespace: as, Name: "clone"},
		ActorTemplate: templateRef,
		SourceTag:     tagRef,
	}})
	if err != nil {
		t.Fatalf("CreateActor: %v", err)
	}
	snapshot := proto.CloneOf(tag.GetStatus().GetSnapshot())
	snapshot.ActorTemplateUid = tmpl.GetMetadata().GetUid()
	want := &ateapi.Actor{
		Metadata:      &ateapi.ResourceMetadata{Atespace: as, Name: "clone"},
		ActorTemplate: templateRef,
		SourceTag:     tagRef,
		Status: &ateapi.ActorStatus{
			State:                  ateapi.ActorState_ACTOR_STATE_SUSPENDED,
			Snapshots:              []*ateapi.Snapshot{snapshot},
			LastAssignedGeneration: snapshot.GetGeneration(),
		},
	}
	if diff := cmp.Diff(want, created, protocmp.Transform(), ignoreServerMetadata); diff != "" {
		t.Errorf("created actor (-want +got):\n%s", diff)
	}
}

// Resumes an actor created from a tag. The request succeeds (OK), and the
// atelet received exactly one Restore, of the tag's snapshot with memory
// fidelity. The resume retries while ate-api catches up.
func TestResumeActor_FromTag(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	node := h.Node("a")
	h.AddWorker(pool, node)
	tmpl := h.NewTemplate(as, pool)
	tag := createTag(t, h, h.SuspendedActor(as, tmpl), "tag-a")
	clone := h.NewActor(as, tmpl, func(a *ateapi.Actor) { a.SourceTag = &ateapi.ObjectRef{Atespace: as, Name: "tag-a"} })
	callsBefore := len(node.Atelet.Calls())

	if _, err := h.ResumeActor(t.Context(), &ateapi.ResumeActorRequest{Actor: harness.ActorRef(clone)}); err != nil {
		t.Fatalf("ResumeActor: %v", err)
	}
	calls := node.Atelet.Calls()[callsBefore:]
	if len(calls) != 1 || calls[0].Op != harness.AteletRestore {
		t.Fatalf("atelet received %v, want exactly one Restore", calls)
	}
	restore := calls[0].Request.(*ateletpb.RestoreRequest)
	assertRestoreOf(t, restore, tag.GetStatus().GetSnapshot())
	if got := restore.GetSnapshot().GetFidelity(); got != ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY {
		t.Errorf("Restore fidelity = %v, want MEMORY", got)
	}
}

// Resumes an actor created from a tag after updating it to another template
// with the same volumes. The request succeeds (OK), and the atelet received
// exactly one Restore, of the tag's snapshot with only volume fidelity: the
// new template's image boots fresh. The resume retries while ate-api catches
// up.
func TestResumeActor_FromTagAfterTemplateChange(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	node := h.Node("a")
	h.AddWorker(pool, node)
	tmpl := h.NewTemplate(as, pool)
	other := h.NewTemplate(as, pool)
	tag := createTag(t, h, h.SuspendedActor(as, tmpl), "tag-a")
	clone := h.NewActor(as, tmpl, func(a *ateapi.Actor) { a.SourceTag = &ateapi.ObjectRef{Atespace: as, Name: "tag-a"} })
	toUpdate := proto.Clone(clone).(*ateapi.Actor)
	toUpdate.ActorTemplate = &ateapi.ObjectRef{Atespace: as, Name: other.GetMetadata().GetName()}
	if _, err := h.Control.UpdateActor(t.Context(), &ateapi.UpdateActorRequest{Actor: toUpdate}); err != nil {
		t.Fatalf("UpdateActor: %v", err)
	}
	callsBefore := len(node.Atelet.Calls())

	if _, err := h.ResumeActor(t.Context(), &ateapi.ResumeActorRequest{Actor: harness.ActorRef(clone)}); err != nil {
		t.Fatalf("ResumeActor: %v", err)
	}
	calls := node.Atelet.Calls()[callsBefore:]
	if len(calls) != 1 || calls[0].Op != harness.AteletRestore {
		t.Fatalf("atelet received %v, want exactly one Restore", calls)
	}
	restore := calls[0].Request.(*ateletpb.RestoreRequest)
	assertRestoreOf(t, restore, tag.GetStatus().GetSnapshot())
	if got := restore.GetSnapshot().GetFidelity(); got != ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES {
		t.Errorf("Restore fidelity = %v, want VOLUMES", got)
	}
}

// Creating an actor from a tag holding a volume-fidelity snapshot, with a
// template other than the one the tag was taken under, fails with
// FAILED_PRECONDITION, and no actor is created.
func TestCreateActor_FromDataTagOfAnotherTemplate(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	h.AddWorker(pool, h.Node("a"))
	tmpl := h.NewTemplate(as, pool, withDataFidelity)
	other := h.NewTemplate(as, pool)
	source := h.SuspendedActor(as, tmpl)
	createTag(t, h, source, "tag-a")
	before, err := h.Control.ListActors(t.Context(), &ateapi.ListActorsRequest{Atespace: as})
	if err != nil {
		t.Fatalf("ListActors: %v", err)
	}

	_, err = h.Control.CreateActor(t.Context(), &ateapi.CreateActorRequest{Actor: &ateapi.Actor{
		Metadata:      &ateapi.ResourceMetadata{Atespace: as, Name: "clone"},
		ActorTemplate: &ateapi.ObjectRef{Atespace: as, Name: other.GetMetadata().GetName()},
		SourceTag:     &ateapi.ObjectRef{Atespace: as, Name: "tag-a"},
	}})
	if status.Code(err) != codes.FailedPrecondition {
		t.Errorf("CreateActor from a data tag of another template = %v, want FailedPrecondition", err)
	}
	after, err := h.Control.ListActors(t.Context(), &ateapi.ListActorsRequest{Atespace: as})
	if err != nil {
		t.Fatalf("ListActors: %v", err)
	}
	if diff := cmp.Diff(before, after, protocmp.Transform()); diff != "" {
		t.Errorf("actors after a refused create (-want +got):\n%s", diff)
	}
}

// Creating an actor from a tag with a template that has an external volume
// fails with FAILED_PRECONDITION.
func TestCreateActor_FromTagWithExternalVolumes(t *testing.T) {
	t.Parallel()
	h := harness.New(t, harness.WithPreviewClient())
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	h.AddWorker(pool, h.Node("a"))
	tmpl := h.NewTemplate(as, pool, harness.WithExternalVolume("data", "/data"))
	createTag(t, h, h.SuspendedActor(as, tmpl), "tag-a")

	_, err := h.Control.CreateActor(t.Context(), &ateapi.CreateActorRequest{Actor: &ateapi.Actor{
		Metadata:      &ateapi.ResourceMetadata{Atespace: as, Name: "clone"},
		ActorTemplate: &ateapi.ObjectRef{Atespace: as, Name: tmpl.GetMetadata().GetName()},
		SourceTag:     &ateapi.ObjectRef{Atespace: as, Name: "tag-a"},
	}})
	if status.Code(err) != codes.FailedPrecondition {
		t.Errorf("CreateActor from a tag with external volumes = %v, want FailedPrecondition", err)
	}
}

// Creates an actor from a tag while the tag's create is held at the atelet's
// PromoteSnapshot. The actor create fails with FAILED_PRECONDITION, and once
// the PromoteSnapshot is released, the tag create succeeds (OK).
func TestCreateActor_FromPendingTag(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	node := h.Node("a")
	h.AddWorker(pool, node)
	tmpl := h.NewTemplate(as, pool)
	source := h.AsyncSuspendedActor(as, tmpl)

	gate := node.Atelet.On(harness.AteletPromoteSnapshot).Block()
	tagged := make(chan error, 1)
	go func() {
		_, err := h.Control.CreateTag(t.Context(), &ateapi.CreateTagRequest{Tag: &ateapi.Tag{
			Metadata:    &ateapi.ResourceMetadata{Atespace: as, Name: "tag-a"},
			Scope:       ateapi.TagScope_TAG_SCOPE_ATESPACE,
			SourceActor: harness.ActorRef(source),
		}})
		tagged <- err
	}()
	gate.Wait(t)
	_, err := h.Control.CreateActor(t.Context(), &ateapi.CreateActorRequest{Actor: &ateapi.Actor{
		Metadata:      &ateapi.ResourceMetadata{Atespace: as, Name: "clone"},
		ActorTemplate: &ateapi.ObjectRef{Atespace: as, Name: tmpl.GetMetadata().GetName()},
		SourceTag:     &ateapi.ObjectRef{Atespace: as, Name: "tag-a"},
	}})
	if status.Code(err) != codes.FailedPrecondition {
		t.Errorf("CreateActor from a tag still being created = %v, want FailedPrecondition", err)
	}
	gate.Release()
	if err := <-tagged; err != nil {
		t.Errorf("CreateTag: %v", err)
	}
}

// Resumes a suspended actor whose template snapshots only volumes. The
// request succeeds (OK), and the atelet received exactly one Restore: of the
// actor's own snapshot, not the golden one, with volume fidelity and private
// sharing. The resume retries while ate-api catches up.
func TestResumeActor_DataSnapshot(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	node := h.Node("a")
	h.AddWorker(pool, node)
	suspended := h.SuspendedActor(as, h.NewTemplate(as, pool, withDataFidelity))
	callsBefore := len(node.Atelet.Calls())

	if _, err := h.ResumeActor(t.Context(), &ateapi.ResumeActorRequest{Actor: harness.ActorRef(suspended)}); err != nil {
		t.Fatalf("ResumeActor: %v", err)
	}
	calls := node.Atelet.Calls()[callsBefore:]
	if len(calls) != 1 || calls[0].Op != harness.AteletRestore {
		t.Fatalf("atelet received %v, want exactly one Restore", calls)
	}
	restore := calls[0].Request.(*ateletpb.RestoreRequest)
	assertRestoreOf(t, restore, latestSnapshot(t, suspended))
	if got := restore.GetSnapshot().GetFidelity(); got != ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES {
		t.Errorf("Restore fidelity = %v, want VOLUMES", got)
	}
	if got := restore.GetSnapshot().GetObject().GetSharing(); got != ateletpb.SnapshotSharing_SNAPSHOT_SHARING_PRIVATE {
		t.Errorf("Restore sharing = %v, want PRIVATE", got)
	}
}

// Updates the template of an existing actor to another template with the same
// volumes, sending it as read. The result, and a later Get, is the resource as
// read with that field changed and a higher version.
func TestUpdateActor_ActorTemplate(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	h.AddWorker(pool, h.Node("a"))
	actor := h.NewActor(as, h.NewTemplate(as, pool))
	other := h.NewTemplate(as, pool)
	ref := harness.ActorRef(actor)
	otherRef := &ateapi.ObjectRef{Atespace: as, Name: other.GetMetadata().GetName()}

	before := h.GetActor(ref)
	toUpdate := proto.Clone(before).(*ateapi.Actor)
	toUpdate.ActorTemplate = otherRef
	updated, err := h.Control.UpdateActor(t.Context(), &ateapi.UpdateActorRequest{Actor: toUpdate})
	if err != nil {
		t.Fatalf("UpdateActor: %v", err)
	}
	want := proto.Clone(before).(*ateapi.Actor)
	want.ActorTemplate = otherRef
	if diff := cmp.Diff(want, updated, protocmp.Transform(), ignoreVersion, ignoreTimestamps); diff != "" {
		t.Errorf("updated actor (-want +got):\n%s", diff)
	}
	if got, was := updated.GetMetadata().GetVersion(), before.GetMetadata().GetVersion(); got <= was {
		t.Errorf("updated actor has version %d, want higher than %d", got, was)
	}
	if diff := cmp.Diff(updated, h.GetActor(ref), protocmp.Transform()); diff != "" {
		t.Errorf("GetActor after update (-updated +got):\n%s", diff)
	}
}

// Updating an actor's template to one that doesn't exist fails with
// FAILED_PRECONDITION and leaves the actor unchanged.
func TestUpdateActor_ActorTemplateNotFound(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	h.AddWorker(pool, h.Node("a"))
	ref := harness.ActorRef(h.NewActor(as, h.NewTemplate(as, pool)))
	before := h.GetActor(ref)

	toUpdate := proto.Clone(before).(*ateapi.Actor)
	toUpdate.ActorTemplate = &ateapi.ObjectRef{Atespace: as, Name: "tmpl-missing"}
	_, err := h.Control.UpdateActor(t.Context(), &ateapi.UpdateActorRequest{Actor: toUpdate})
	if status.Code(err) != codes.FailedPrecondition {
		t.Errorf("UpdateActor to a missing template = %v, want FailedPrecondition", err)
	}
	if diff := cmp.Diff(before, h.GetActor(ref), protocmp.Transform()); diff != "" {
		t.Errorf("actor after a refused update (-want +got):\n%s", diff)
	}
}

// Updating an actor's template to one with different volumes, and to one that
// mounts the same volume at a different path, each fails with
// FAILED_PRECONDITION and leaves the actor unchanged.
func TestUpdateActor_ActorTemplateIncompatibleVolumes(t *testing.T) {
	t.Parallel()
	h := harness.New(t, harness.WithPreviewClient())
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	h.AddWorker(pool, h.Node("a"))
	ref := harness.ActorRef(h.NewActor(as, h.NewTemplate(as, pool, harness.WithExternalVolume("data", "/data"))))
	otherVolumes := h.NewTemplate(as, pool, harness.WithExternalVolume("logs", "/data"))
	otherMounts := h.NewTemplate(as, pool, harness.WithExternalVolume("data", "/mnt"))
	before := h.GetActor(ref)

	for _, other := range []*ateapi.ActorTemplate{otherVolumes, otherMounts} {
		toUpdate := proto.Clone(before).(*ateapi.Actor)
		toUpdate.ActorTemplate = &ateapi.ObjectRef{Atespace: as, Name: other.GetMetadata().GetName()}
		_, err := h.Control.UpdateActor(t.Context(), &ateapi.UpdateActorRequest{Actor: toUpdate})
		if status.Code(err) != codes.FailedPrecondition {
			t.Errorf("UpdateActor to template %s = %v, want FailedPrecondition", other.GetMetadata().GetName(), err)
		}
	}
	if diff := cmp.Diff(before, h.GetActor(ref), protocmp.Transform()); diff != "" {
		t.Errorf("actor after refused updates (-want +got):\n%s", diff)
	}
}

// Updating an actor left DELETING by a Terminate the atelet failed fails with
// FAILED_PRECONDITION and leaves the actor unchanged.
func TestUpdateActor_Deleting(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	node := h.Node("a")
	h.AddWorker(pool, node)
	ref := harness.ActorRef(h.RunningActor(as, h.NewTemplate(as, pool)))
	node.Atelet.On(harness.AteletTerminate).FailNext(harness.ErrSandboxFailed)
	if _, err := h.Control.DeleteActor(t.Context(), &ateapi.DeleteActorRequest{Actor: ref}); err == nil {
		t.Fatal("DeleteActor succeeded despite a failing Terminate")
	}
	before := h.GetActor(ref)

	toUpdate := proto.Clone(before).(*ateapi.Actor)
	toUpdate.WorkerSelector = &ateapi.Selector{MatchLabels: map[string]string{"tier": "1"}}
	_, err := h.Control.UpdateActor(t.Context(), &ateapi.UpdateActorRequest{Actor: toUpdate})
	if status.Code(err) != codes.FailedPrecondition {
		t.Errorf("UpdateActor of a DELETING actor = %v, want FailedPrecondition", err)
	}
	if diff := cmp.Diff(before, h.GetActor(ref), protocmp.Transform()); diff != "" {
		t.Errorf("actor after a refused update (-want +got):\n%s", diff)
	}
}

// Resumes an actor whose worker selector matches only pool p2, from a template
// whose selector matches pools p1 and p2, with a worker with room in each. The
// request succeeds (OK) and the actor runs on the p2 worker. The resume
// retries while ate-api catches up.
func TestResumeActor_ActorWorkerSelector(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	p1 := h.CreateWorkerPool("p1", map[string]string{"app": "x", "pool": "p1"})
	p2 := h.CreateWorkerPool("p2", map[string]string{"app": "x", "pool": "p2"})
	h.AddWorker(p1, h.Node("a"))
	w2 := h.AddWorker(p2, h.Node("b"))
	tmpl := h.NewTemplate(as, h.CreateWorkerPool("any", map[string]string{"app": "x"}))
	actor := h.NewActor(as, tmpl, func(a *ateapi.Actor) {
		a.WorkerSelector = &ateapi.Selector{MatchLabels: map[string]string{"pool": "p2"}}
	})

	resp, err := h.ResumeActor(t.Context(), &ateapi.ResumeActorRequest{Actor: harness.ActorRef(actor)})
	if err != nil {
		t.Fatalf("ResumeActor: %v", err)
	}
	if got := resp.GetActor().GetStatus().GetWorkerAssignment().GetWorker().GetName(); got != w2.Name {
		t.Errorf("actor runs on worker %q, want the p2 worker %q", got, w2.Name)
	}
}

// Resumes an actor whose worker selector matches pool p2, from a template
// whose selector matches pool p1, with a worker in each. The request fails
// with RESOURCE_EXHAUSTED and leaves the actor unchanged: no worker matches
// both selectors. After a worker is added to a pool matching both, resuming
// succeeds (OK) on that worker. The resume after the worker is added retries
// while ate-api catches up.
func TestResumeActor_BothSelectorsMustMatch(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	p1 := h.CreateWorkerPool("p1", map[string]string{"app": "x"})
	p2 := h.CreateWorkerPool("p2", map[string]string{"tier": "2"})
	h.AddWorker(p1, h.Node("a"))
	h.AddWorker(p2, h.Node("b"))
	tmpl := h.NewTemplate(as, p1)
	actor := h.NewActor(as, tmpl, func(a *ateapi.Actor) {
		a.WorkerSelector = &ateapi.Selector{MatchLabels: map[string]string{"tier": "2"}}
	})
	ref := harness.ActorRef(actor)
	before := h.GetActor(ref)

	_, err := h.Control.ResumeActor(t.Context(), &ateapi.ResumeActorRequest{Actor: ref})
	if status.Code(err) != codes.ResourceExhausted {
		t.Errorf("ResumeActor with no worker matching both selectors = %v, want ResourceExhausted", err)
	}
	if diff := cmp.Diff(before, h.GetActor(ref), protocmp.Transform()); diff != "" {
		t.Errorf("actor after a refused resume (-want +got):\n%s", diff)
	}

	both := h.AddWorker(h.CreateWorkerPool("p3", map[string]string{"app": "x", "tier": "2"}), h.Node("c"))
	resp, err := h.ResumeActor(t.Context(), &ateapi.ResumeActorRequest{Actor: ref})
	if err != nil {
		t.Fatalf("ResumeActor after adding a matching worker: %v", err)
	}
	if got := resp.GetActor().GetStatus().GetWorkerAssignment().GetWorker().GetName(); got != both.Name {
		t.Errorf("actor runs on worker %q, want %q", got, both.Name)
	}
}

// Updates the worker selector of an actor running on pool p1 to match only
// pool p2, then suspends and resumes it. The resume succeeds (OK) and the
// actor runs on the p2 worker. The resume retries while ate-api catches up.
func TestUpdateActor_WorkerSelectorMovesActor(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	p1 := h.CreateWorkerPool("p1", map[string]string{"app": "x", "pool": "p1"})
	p2 := h.CreateWorkerPool("p2", map[string]string{"app": "x", "pool": "p2"})
	h.AddWorker(p1, h.Node("a"))
	w2 := h.AddWorker(p2, h.Node("b"))
	tmpl := h.NewTemplate(as, h.CreateWorkerPool("any", map[string]string{"app": "x"}))
	actor := h.RunningActor(as, tmpl, func(a *ateapi.Actor) {
		a.WorkerSelector = &ateapi.Selector{MatchLabels: map[string]string{"pool": "p1"}}
	})
	ref := harness.ActorRef(actor)

	toUpdate := proto.Clone(h.GetActor(ref)).(*ateapi.Actor)
	toUpdate.WorkerSelector = &ateapi.Selector{MatchLabels: map[string]string{"pool": "p2"}}
	if _, err := h.Control.UpdateActor(t.Context(), &ateapi.UpdateActorRequest{Actor: toUpdate}); err != nil {
		t.Fatalf("UpdateActor: %v", err)
	}
	if _, err := h.Control.SuspendActor(t.Context(), &ateapi.SuspendActorRequest{
		Actor:   ref,
		Options: &ateapi.SuspendOptions{Mode: ateapi.DurabilityMode_DURABILITY_MODE_SYNCHRONOUS},
	}); err != nil {
		t.Fatalf("SuspendActor: %v", err)
	}
	resp, err := h.ResumeActor(t.Context(), &ateapi.ResumeActorRequest{Actor: ref})
	if err != nil {
		t.Fatalf("ResumeActor: %v", err)
	}
	if got := resp.GetActor().GetStatus().GetWorkerAssignment().GetWorker().GetName(); got != w2.Name {
		t.Errorf("actor runs on worker %q, want the p2 worker %q", got, w2.Name)
	}
}

// An actor left RESUMING on the pool p1 worker by a Restore that failed with
// UNAVAILABLE has its worker selector updated to match only pool p2, and is
// resumed again. The request fails with ABORTED, the actor becomes CRASHED
// with no worker assignment and a crash message saying the worker no longer
// satisfies its placement constraints, and the p1 worker is left with no
// allocation.
//
// TODO(): The request should fail with INTERNAL, as other resumes that crash
// the actor do. ABORTED makes callers like the atenet router retry a resume
// that cannot succeed.
func TestResumeActor_RetryAfterWorkerSelectorChange(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	p1 := h.CreateWorkerPool("p1", map[string]string{"app": "x", "pool": "p1"})
	p2 := h.CreateWorkerPool("p2", map[string]string{"app": "x", "pool": "p2"})
	w1 := h.AddWorker(p1, h.Node("a"))
	h.AddWorker(p2, h.Node("b"))
	tmpl := h.NewTemplate(as, h.CreateWorkerPool("any", map[string]string{"app": "x"}))
	idle := h.GetWorker(w1)
	actor := h.NewActor(as, tmpl, func(a *ateapi.Actor) {
		a.WorkerSelector = &ateapi.Selector{MatchLabels: map[string]string{"pool": "p1"}}
	})
	ref := harness.ActorRef(actor)
	w1.Node.Atelet.On(harness.AteletRestore).FailNext(harness.ErrUnavailable)
	if _, err := h.ResumeActor(t.Context(), &ateapi.ResumeActorRequest{Actor: ref}); status.Code(err) != codes.Unavailable {
		t.Fatalf("ResumeActor with an unavailable atelet = %v, want Unavailable", err)
	}
	toUpdate := proto.Clone(h.GetActor(ref)).(*ateapi.Actor)
	toUpdate.WorkerSelector = &ateapi.Selector{MatchLabels: map[string]string{"pool": "p2"}}
	updated, err := h.Control.UpdateActor(t.Context(), &ateapi.UpdateActorRequest{Actor: toUpdate})
	if err != nil {
		t.Fatalf("UpdateActor: %v", err)
	}

	_, err = h.Control.ResumeActor(t.Context(), &ateapi.ResumeActorRequest{Actor: ref})
	if status.Code(err) != codes.Aborted {
		t.Errorf("ResumeActor retry after the selector change = %v, want Aborted", err)
	}
	want := proto.Clone(updated).(*ateapi.Actor)
	want.Status.State = ateapi.ActorState_ACTOR_STATE_CRASHED
	want.Status.WorkerAssignment = nil
	want.Status.Crash = &ateapi.ActorCrash{Message: "resume failed: assigned worker no longer satisfies the actor's placement constraints"}
	ignoreCrashTime := protocmp.IgnoreFields(&ateapi.ActorCrash{}, "crash_time")
	if diff := cmp.Diff(want, h.GetActor(ref), protocmp.Transform(), ignoreVersion, ignoreTimestamps, ignoreCrashTime); diff != "" {
		t.Errorf("actor after the retry (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(idle, h.GetWorker(w1), protocmp.Transform(), ignoreVersion, ignoreTimestamps); diff != "" {
		t.Errorf("p1 worker after the retry (-want +got):\n%s", diff)
	}
}

// Resumes three actors at once onto a pool whose only worker has room for
// three. Every resume succeeds (OK), and the worker's allocation goes to three
// actors. Each resume retries while ate-api catches up.
func TestResumeActor_ConcurrentOntoOneWorker(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	w := h.AddWorker(pool, h.Node("a"), harness.WithActorCapacity(3))
	tmpl := h.NewTemplate(as, pool)
	var refs []*ateapi.ObjectRef
	for range 3 {
		refs = append(refs, harness.ActorRef(h.NewActor(as, tmpl)))
	}

	errs := make(chan error, len(refs))
	for _, ref := range refs {
		go func() {
			_, err := h.ResumeActor(t.Context(), &ateapi.ResumeActorRequest{Actor: ref})
			errs <- err
		}()
	}
	for range refs {
		if err := <-errs; err != nil {
			t.Errorf("ResumeActor: %v", err)
		}
	}
	if got := h.GetWorker(w).GetStatus().GetAllocated().GetActors(); got != 3 {
		t.Errorf("worker allocation is %d actors, want 3", got)
	}
}

// Creates actors in two atespaces and lists actors with no atespace. The list
// holds exactly the actors of both atespaces.
func TestListActors_AllAtespaces(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	other := h.CreateAtespace("team-b")
	pool := h.CreateWorkerPool("pool1", nil)
	h.AddWorker(pool, h.Node("a"))
	created := []*ateapi.Actor{
		h.NewActor(as, h.NewTemplate(as, pool)),
		h.NewActor(other, h.NewTemplate(other, pool)),
	}

	listed, err := h.Control.ListActors(t.Context(), &ateapi.ListActorsRequest{})
	if err != nil {
		t.Fatalf("ListActors: %v", err)
	}
	byName := cmpopts.SortSlices(func(a, b *ateapi.Actor) bool { return a.GetMetadata().GetName() < b.GetMetadata().GetName() })
	if diff := cmp.Diff(created, listed.GetActors(), protocmp.Transform(), byName); diff != "" {
		t.Errorf("listed actors (-want +got):\n%s", diff)
	}
}

// Resumes a new actor while the atelet fails the Restore with CANCELED, then
// with DEADLINE_EXCEEDED. Each request fails with INTERNAL and leaves the
// actor RESUMING on its worker. A third resume succeeds (OK). Each resume
// retries while ate-api catches up.
func TestResumeActor_RestoreCanceled(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	node := h.Node("a")
	w := h.AddWorker(pool, node)
	ref := harness.ActorRef(h.NewActor(as, h.NewTemplate(as, pool)))

	for _, failure := range []error{status.Error(codes.Canceled, "injected cancel"), status.Error(codes.DeadlineExceeded, "injected deadline")} {
		node.Atelet.On(harness.AteletRestore).FailNext(failure)
		_, err := h.ResumeActor(t.Context(), &ateapi.ResumeActorRequest{Actor: ref})
		if status.Code(err) != codes.Internal {
			t.Errorf("ResumeActor with a Restore failing with %v = %v, want Internal", status.Code(failure), err)
		}
		got := h.GetActor(ref).GetStatus()
		if got.GetState() != ateapi.ActorState_ACTOR_STATE_RESUMING || got.GetWorkerAssignment().GetWorker().GetName() != w.Name {
			t.Errorf("actor after a Restore failing with %v is %v on worker %q, want RESUMING on %q", status.Code(failure), got.GetState(), got.GetWorkerAssignment().GetWorker().GetName(), w.Name)
		}
	}
	if _, err := h.ResumeActor(t.Context(), &ateapi.ResumeActorRequest{Actor: ref}); err != nil {
		t.Errorf("ResumeActor retry: %v", err)
	}
}

// Resumes a new actor with a deadline that passes while the atelet's Restore
// is held. The request fails with DEADLINE_EXCEEDED and leaves the actor
// RESUMING on its worker. A later resume, retrying while it fails with
// ABORTED, succeeds (OK). Both resumes retry while ate-api catches up.
func TestResumeActor_CallerDeadline(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	node := h.Node("a")
	w := h.AddWorker(pool, node)
	ref := harness.ActorRef(h.NewActor(as, h.NewTemplate(as, pool)))
	node.Atelet.On(harness.AteletRestore).Block()

	ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
	defer cancel()
	_, err := h.ResumeActor(ctx, &ateapi.ResumeActorRequest{Actor: ref})
	if status.Code(err) != codes.DeadlineExceeded {
		t.Errorf("ResumeActor past its deadline = %v, want DeadlineExceeded", err)
	}
	if got := h.GetActor(ref).GetStatus(); got.GetState() != ateapi.ActorState_ACTOR_STATE_RESUMING || got.GetWorkerAssignment().GetWorker().GetName() != w.Name {
		t.Errorf("actor after the deadline is %v on worker %q, want RESUMING on %q", got.GetState(), got.GetWorkerAssignment().GetWorker().GetName(), w.Name)
	}
	if err := harness.RetryAborted(t.Context(), func() error {
		_, err := h.ResumeActor(t.Context(), &ateapi.ResumeActorRequest{Actor: ref})
		return err
	}); err != nil {
		t.Errorf("ResumeActor retry: %v", err)
	}
}

// Mints a JWT, as the egress gateway, for an existing actor. The request
// succeeds (OK) with a JWT whose header names type JWT, and whose claims name
// ate-api's issuer and the requested audience.
func TestMintActorJwt(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	h.AddWorker(pool, h.Node("a"))
	ref := harness.ActorRef(h.NewActor(as, h.NewTemplate(as, pool)))

	resp, err := h.EgressGateway().MintActorJwt(t.Context(), &ateapi.MintActorJwtRequest{
		Actor:             ref,
		Audiences:         []string{"https://audience.example"},
		ExpirationSeconds: 600,
	})
	if err != nil {
		t.Fatalf("MintActorJwt: %v", err)
	}
	segments := strings.Split(resp.GetActorJwt(), ".")
	if len(segments) != 3 {
		t.Fatalf("actor JWT has %d segments, want 3", len(segments))
	}
	var header struct {
		Type string `json:"typ"`
	}
	decodeJWTSegment(t, segments[0], &header)
	if header.Type != "JWT" {
		t.Errorf("JWT typ = %q, want JWT", header.Type)
	}
	var claims struct {
		Issuer   string `json:"iss"`
		Audience any    `json:"aud"`
	}
	decodeJWTSegment(t, segments[1], &claims)
	if claims.Issuer != harness.ActorJWTIssuer {
		t.Errorf("JWT iss = %q, want %q", claims.Issuer, harness.ActorJWTIssuer)
	}
	if !strings.Contains(fmt.Sprint(claims.Audience), "https://audience.example") {
		t.Errorf("JWT aud = %v, want it to hold https://audience.example", claims.Audience)
	}
}

// decodeJWTSegment decodes one base64url segment of a JWT into v.
func decodeJWTSegment(t *testing.T, segment string, v any) {
	t.Helper()
	raw, err := base64.RawURLEncoding.DecodeString(segment)
	if err != nil {
		t.Fatalf("decoding JWT segment: %v", err)
	}
	if err := json.Unmarshal(raw, v); err != nil {
		t.Fatalf("unmarshaling JWT segment: %v", err)
	}
}

// Resumes a new actor whose template sets a literal environment variable. The
// request succeeds (OK), and the atelet's Restore carries the variable in the
// container's spec. The resume retries while ate-api catches up.
func TestResumeActor_LiteralEnv(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	node := h.Node("a")
	h.AddWorker(pool, node)
	tmpl := h.NewTemplate(as, pool, func(tmpl *ateapi.ActorTemplate) {
		tmpl.Containers[0].Env = []*ateapi.EnvVar{{Name: "LITERAL", Value: "plain"}}
	})
	ref := harness.ActorRef(h.NewActor(as, tmpl))
	callsBefore := len(node.Atelet.Calls())

	if _, err := h.ResumeActor(t.Context(), &ateapi.ResumeActorRequest{Actor: ref}); err != nil {
		t.Fatalf("ResumeActor: %v", err)
	}
	calls := node.Atelet.Calls()[callsBefore:]
	if len(calls) != 1 || calls[0].Op != harness.AteletRestore {
		t.Fatalf("atelet received %v, want exactly one Restore", calls)
	}
	containers := calls[0].Request.(*ateletpb.RestoreRequest).GetSpec().GetContainers()
	if len(containers) != 1 {
		t.Fatalf("Restore spec has %d containers, want 1", len(containers))
	}
	want := []*ateletpb.EnvVar{{Name: "LITERAL", Value: "plain"}}
	if diff := cmp.Diff(want, containers[0].GetEnv(), protocmp.Transform()); diff != "" {
		t.Errorf("Restore container env (-want +got):\n%s", diff)
	}
}

// deletingTag leaves the tag ref marked for deletion: its DeleteTag fails
// while object storage fails to list the tag's snapshot.
func deletingTag(t *testing.T, h *harness.Harness, ref *ateapi.ObjectRef) {
	t.Helper()
	h.ObjectStore.On(harness.ObjectStoreList).FailNext(harness.ErrStorageFailed)
	if _, err := h.Control.DeleteTag(t.Context(), &ateapi.DeleteTagRequest{Tag: ref}); err == nil {
		t.Fatal("DeleteTag succeeded despite failing object storage")
	}
	tag, err := h.Control.GetTag(t.Context(), &ateapi.GetTagRequest{Tag: ref})
	if err != nil {
		t.Fatalf("GetTag after a failed delete: %v", err)
	}
	if tag.GetMetadata().GetDeleteTime() == nil {
		t.Fatalf("tag %s after a failed delete has no delete time", ref.GetName())
	}
}

// Creating an actor from a tag whose delete failed partway, leaving it marked
// for deletion, fails with FAILED_PRECONDITION and creates no actor.
func TestCreateActor_FromDeletingTag(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	h.AddWorker(pool, h.Node("a"))
	tmpl := h.NewTemplate(as, pool)
	createTag(t, h, h.SuspendedActor(as, tmpl), "tag-a")
	tagRef := &ateapi.ObjectRef{Atespace: as, Name: "tag-a"}
	deletingTag(t, h, tagRef)
	before, err := h.Control.ListActors(t.Context(), &ateapi.ListActorsRequest{Atespace: as})
	if err != nil {
		t.Fatalf("ListActors: %v", err)
	}

	_, err = h.Control.CreateActor(t.Context(), &ateapi.CreateActorRequest{Actor: &ateapi.Actor{
		Metadata:      &ateapi.ResourceMetadata{Atespace: as, Name: "clone"},
		ActorTemplate: &ateapi.ObjectRef{Atespace: as, Name: tmpl.GetMetadata().GetName()},
		SourceTag:     tagRef,
	}})
	if status.Code(err) != codes.FailedPrecondition {
		t.Errorf("CreateActor from a deleting tag = %v, want FailedPrecondition", err)
	}
	after, err := h.Control.ListActors(t.Context(), &ateapi.ListActorsRequest{Atespace: as})
	if err != nil {
		t.Fatalf("ListActors: %v", err)
	}
	if diff := cmp.Diff(before, after, protocmp.Transform()); diff != "" {
		t.Errorf("actors after a refused create (-want +got):\n%s", diff)
	}
}

// Creating an actor from a template whose golden tag's delete failed partway,
// leaving it marked for deletion, fails with FAILED_PRECONDITION.
func TestCreateActor_GoldenTagDeleting(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	h.AddWorker(pool, h.Node("a"))
	tmpl := h.NewTemplate(as, pool)
	deletingTag(t, h, tmpl.GetStatus().GetGoldenSnapshotStatus().GetGoldenTag())

	_, err := h.Control.CreateActor(t.Context(), &ateapi.CreateActorRequest{Actor: &ateapi.Actor{
		Metadata:      &ateapi.ResourceMetadata{Atespace: as, Name: "actor-a"},
		ActorTemplate: &ateapi.ObjectRef{Atespace: as, Name: tmpl.GetMetadata().GetName()},
	}})
	if status.Code(err) != codes.FailedPrecondition {
		t.Errorf("CreateActor while the golden tag is being deleted = %v, want FailedPrecondition", err)
	}
}

// Suspends, synchronously, an actor that holds a snapshot of its own, while
// object storage fails to list the snapshot the suspend replaces. The request
// succeeds (OK) with a new snapshot of the actor's own, and object storage
// still holds the replaced one. A later resume restores the new snapshot.
// Deleting the actor then succeeds (OK) and leaves object storage with neither
// snapshot.
func TestSuspendActor_ReplacedSnapshotReleaseFails(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	node := h.Node("a")
	h.AddWorker(pool, node)
	first := h.SuspendedActor(as, h.NewTemplate(as, pool))
	ref := harness.ActorRef(first)
	replacedURI := latestSnapshot(t, first).GetDurableSnapshot().GetObject().GetSnapshotUri()
	objectsBeforeActor := slices.DeleteFunc(h.ObjectStore.Objects(), func(o string) bool {
		return strings.Contains(o, "/actors/"+first.GetMetadata().GetUid()+"/")
	})
	if _, err := h.ResumeActor(t.Context(), &ateapi.ResumeActorRequest{Actor: ref}); err != nil {
		t.Fatalf("ResumeActor: %v", err)
	}
	h.ObjectStore.On(harness.ObjectStoreList).FailNext(harness.ErrStorageFailed)

	resp, err := h.Control.SuspendActor(t.Context(), &ateapi.SuspendActorRequest{
		Actor:   ref,
		Options: &ateapi.SuspendOptions{Mode: ateapi.DurabilityMode_DURABILITY_MODE_SYNCHRONOUS},
	})
	if err != nil {
		t.Fatalf("SuspendActor: %v", err)
	}
	fresh := latestSnapshot(t, resp.GetActor())
	if fresh.GetOwner() != ateapi.SnapshotOwner_SNAPSHOT_OWNER_ACTOR || fresh.GetDurableSnapshot().GetObject().GetSnapshotUri() == replacedURI {
		t.Errorf("snapshot after the suspend = %v, want a new one of the actor's own replacing %q", fresh, replacedURI)
	}
	if names, err := h.ObjectStore.Snapshot(replacedURI); err != nil || len(names) == 0 {
		t.Errorf("replaced snapshot %q in object storage = %v, %v; want it kept", replacedURI, names, err)
	}

	callsBefore := len(node.Atelet.Calls())
	if _, err := h.ResumeActor(t.Context(), &ateapi.ResumeActorRequest{Actor: ref}); err != nil {
		t.Fatalf("ResumeActor after the suspend: %v", err)
	}
	calls := node.Atelet.Calls()[callsBefore:]
	if len(calls) != 1 || calls[0].Op != harness.AteletRestore {
		t.Fatalf("atelet received %v, want exactly one Restore", calls)
	}
	assertRestoreOf(t, calls[0].Request.(*ateletpb.RestoreRequest), fresh)

	if _, err := h.Control.DeleteActor(t.Context(), &ateapi.DeleteActorRequest{Actor: ref}); err != nil {
		t.Fatalf("DeleteActor: %v", err)
	}
	if diff := cmp.Diff(objectsBeforeActor, h.ObjectStore.Objects()); diff != "" {
		t.Errorf("object storage after delete (-want +got):\n%s", diff)
	}
}

// Deletes a suspended actor holding a snapshot of its own while object
// storage reports the snapshot bucket gone. The request succeeds (OK), and a
// later Get fails with NOT_FOUND.
func TestDeleteActor_SnapshotBucketMissing(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	h.AddWorker(pool, h.Node("a"))
	ref := harness.ActorRef(h.SuspendedActor(as, h.NewTemplate(as, pool)))
	h.ObjectStore.On(harness.ObjectStoreList).FailNext(harness.ErrBucketNotExist)

	if _, err := h.Control.DeleteActor(t.Context(), &ateapi.DeleteActorRequest{Actor: ref}); err != nil {
		t.Fatalf("DeleteActor with the snapshot bucket gone: %v", err)
	}
	if _, err := h.Control.GetActor(t.Context(), &ateapi.GetActorRequest{Actor: ref}); status.Code(err) != codes.NotFound {
		t.Errorf("GetActor after delete = %v, want NotFound", err)
	}
}

// Deletes a suspended actor holding a snapshot of its own while object
// storage fails to list it. The request fails with INTERNAL, and the actor
// becomes DELETING. Deleting it again while object storage reports the
// snapshot bucket gone succeeds (OK), and a later Get fails with NOT_FOUND.
func TestDeleteActor_SnapshotBucketMissingOnRetry(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	h.AddWorker(pool, h.Node("a"))
	ref := harness.ActorRef(h.SuspendedActor(as, h.NewTemplate(as, pool)))
	h.ObjectStore.On(harness.ObjectStoreList).FailNext(harness.ErrStorageFailed)

	_, err := h.Control.DeleteActor(t.Context(), &ateapi.DeleteActorRequest{Actor: ref})
	if status.Code(err) != codes.Internal {
		t.Fatalf("DeleteActor with failing object storage = %v, want Internal", err)
	}
	if got := h.GetActor(ref).GetStatus().GetState(); got != ateapi.ActorState_ACTOR_STATE_DELETING {
		t.Fatalf("actor after a failed delete is %v, want DELETING", got)
	}
	h.ObjectStore.On(harness.ObjectStoreList).FailNext(harness.ErrBucketNotExist)
	if _, err := h.Control.DeleteActor(t.Context(), &ateapi.DeleteActorRequest{Actor: ref}); err != nil {
		t.Fatalf("DeleteActor retry with the snapshot bucket gone: %v", err)
	}
	if _, err := h.Control.GetActor(t.Context(), &ateapi.GetActorRequest{Actor: ref}); status.Code(err) != codes.NotFound {
		t.Errorf("GetActor after delete = %v, want NotFound", err)
	}
}

// Reverts an actor suspended synchronously on node a, after its worker is
// removed and node a's atelet is stopped. The request succeeds (OK). The actor
// is SUSPENDED with no worker assignment and no assigned node, and its
// snapshot is no longer local to node a. After a worker is added on node b,
// resuming the actor succeeds (OK) on node b, restoring the same snapshot.
// The resume retries while ate-api catches up.
func TestRevertActor_AteletStopped(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	nodeA := h.Node("a")
	w := h.AddWorker(pool, nodeA)
	actor := h.SuspendedActor(as, h.NewTemplate(as, pool))
	ref := harness.ActorRef(actor)
	snapshot := latestSnapshot(t, actor)
	w.Remove()
	nodeA.StopAtelet()

	resp, err := h.Control.RevertActor(t.Context(), &ateapi.RevertActorRequest{Actor: ref})
	if err != nil {
		t.Fatalf("RevertActor with node a's atelet stopped: %v", err)
	}
	got := resp.GetActor().GetStatus()
	if got.GetState() != ateapi.ActorState_ACTOR_STATE_SUSPENDED || got.GetAssignedNode() != "" || got.GetWorkerAssignment() != nil {
		t.Errorf("reverted actor is %v assigned to %q with assignment %v, want SUSPENDED and unassigned", got.GetState(), got.GetAssignedNode(), got.GetWorkerAssignment())
	}
	if locality := latestSnapshot(t, resp.GetActor()).GetLocality(); locality != "" {
		t.Errorf("reverted actor's snapshot is local to %q, want no node", locality)
	}

	nodeB := h.Node("b")
	h.AddWorker(pool, nodeB)
	resumed, err := h.ResumeActor(t.Context(), &ateapi.ResumeActorRequest{Actor: ref})
	if err != nil {
		t.Fatalf("ResumeActor after revert: %v", err)
	}
	if node := resumed.GetActor().GetStatus().GetAssignedNode(); node != nodeB.Name {
		t.Errorf("actor resumed on node %q, want %q", node, nodeB.Name)
	}
	calls := nodeB.Atelet.Calls()
	if len(calls) != 1 || calls[0].Op != harness.AteletRestore {
		t.Fatalf("node b atelet received %v, want exactly one Restore", calls)
	}
	assertRestoreOf(t, calls[0].Request.(*ateletpb.RestoreRequest), snapshot)
}

// Reverts an actor suspended synchronously on node a, after its worker is
// removed, while node a's atelet is unavailable for the Terminate. The request
// succeeds (OK). The actor is SUSPENDED with no worker assignment and no
// assigned node, and its snapshot is no longer local to node a.
func TestRevertActor_AteletUnavailable(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	nodeA := h.Node("a")
	w := h.AddWorker(pool, nodeA)
	ref := harness.ActorRef(h.SuspendedActor(as, h.NewTemplate(as, pool)))
	w.Remove()
	nodeA.Atelet.On(harness.AteletTerminate).FailNext(harness.ErrUnavailable)

	resp, err := h.Control.RevertActor(t.Context(), &ateapi.RevertActorRequest{Actor: ref})
	if err != nil {
		t.Fatalf("RevertActor with node a's atelet unavailable: %v", err)
	}
	got := resp.GetActor().GetStatus()
	if got.GetState() != ateapi.ActorState_ACTOR_STATE_SUSPENDED || got.GetAssignedNode() != "" || got.GetWorkerAssignment() != nil {
		t.Errorf("reverted actor is %v assigned to %q with assignment %v, want SUSPENDED and unassigned", got.GetState(), got.GetAssignedNode(), got.GetWorkerAssignment())
	}
	if locality := latestSnapshot(t, resp.GetActor()).GetLocality(); locality != "" {
		t.Errorf("reverted actor's snapshot is local to %q, want no node", locality)
	}
}

// Reverts an actor suspended asynchronously on node a, whose latest snapshot
// is local to node a and not uploaded, after its worker is removed, while node
// a's atelet is unavailable for the Terminate. The request fails with
// UNAVAILABLE, and the actor becomes REVERTING, still assigned to node a and
// holding the same snapshots.
func TestRevertActor_PendingSnapshotAteletUnavailable(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	nodeA := h.Node("a")
	w := h.AddWorker(pool, nodeA)
	ref := harness.ActorRef(h.AsyncSuspendedActor(as, h.NewTemplate(as, pool)))
	w.Remove()
	before := h.GetActor(ref)
	nodeA.Atelet.On(harness.AteletTerminate).FailNext(harness.ErrUnavailable)

	_, err := h.Control.RevertActor(t.Context(), &ateapi.RevertActorRequest{Actor: ref})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("RevertActor with node a's atelet unavailable = %v, want Unavailable", err)
	}
	want := proto.Clone(before).(*ateapi.Actor)
	want.Status.State = ateapi.ActorState_ACTOR_STATE_REVERTING
	if diff := cmp.Diff(want, h.GetActor(ref), protocmp.Transform(), ignoreVersion, ignoreTimestamps); diff != "" {
		t.Errorf("actor after an unavailable atelet (-want +got):\n%s", diff)
	}
}

// Resumes an actor suspended asynchronously on node a, whose latest snapshot
// is local to node a and not uploaded, after its worker is removed and node a
// is removed from the cluster, while a worker on node b has room. The request
// fails with FAILED_PRECONDITION, and the actor becomes CRASHED with no worker
// assignment. Its status.crash says the node holding its latest snapshot no
// longer exists, and names the atelet Restore that found no uploaded copy.
// The resume retries while ate-api catches up.
func TestResumeActor_SnapshotNodeGone(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	nodeA := h.Node("a")
	w := h.AddWorker(pool, nodeA)
	ref := harness.ActorRef(h.AsyncSuspendedActor(as, h.NewTemplate(as, pool)))
	w.Remove()
	nodeA.Remove()
	h.AddWorker(pool, h.Node("b"))

	_, err := h.ResumeActor(t.Context(), &ateapi.ResumeActorRequest{Actor: ref})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("ResumeActor with the snapshot's node gone = %v, want FailedPrecondition", err)
	}
	crashed := h.GetActor(ref).GetStatus()
	if crashed.GetState() != ateapi.ActorState_ACTOR_STATE_CRASHED || crashed.GetWorkerAssignment() != nil {
		t.Errorf("actor is %v with assignment %v, want CRASHED and unassigned", crashed.GetState(), crashed.GetWorkerAssignment())
	}
	if msg := crashed.GetCrash().GetMessage(); !strings.Contains(msg, "node holding the actor's latest snapshot no longer exists") || !strings.Contains(msg, "atelet Restore") {
		t.Errorf("crash message %q does not say the snapshot's node is gone and name the atelet Restore", msg)
	}
}

// otherSandboxRuntime is a runtime other than harness.SandboxRuntime, which a
// snapshot taken under harness.SandboxRuntime cannot restore under in full.
func otherSandboxRuntime() *ateapi.ActorSandboxRuntime {
	rt := harness.SandboxRuntime()
	rt.Version.Attributes[1].Value = "hash-b"
	return rt
}

// lastRestore returns the last Restore node's atelet received.
func lastRestore(t *testing.T, node *harness.Node) *ateletpb.RestoreRequest {
	t.Helper()
	calls := node.Atelet.Calls()
	for i := len(calls) - 1; i >= 0; i-- {
		if calls[i].Op == harness.AteletRestore {
			return calls[i].Request.(*ateletpb.RestoreRequest)
		}
	}
	t.Fatalf("atelet on node %s received no Restore", node.Name)
	return nil
}

// Resumes an actor suspended on a worker running the harness runtime, with
// that worker and one running another runtime both idle on its node. The
// request succeeds (OK). The actor runs on the worker with the snapshot's
// runtime, which its assignment records, and the atelet's Restore keeps the
// snapshot's memory. The resume retries while ate-api catches up.
func TestResumeActor_CompatibleRuntime(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	node := h.Node("a")
	wA := h.AddWorker(pool, node)
	ref := harness.ActorRef(h.SuspendedActor(as, h.NewTemplate(as, pool)))
	h.AddWorker(pool, node, harness.WithSandboxRuntime(otherSandboxRuntime()))

	resp, err := h.ResumeActor(t.Context(), &ateapi.ResumeActorRequest{Actor: ref})
	if err != nil {
		t.Fatalf("ResumeActor: %v", err)
	}
	assignment := resp.GetActor().GetStatus().GetWorkerAssignment()
	if assignment.GetWorker().GetName() != wA.Name {
		t.Errorf("actor runs on worker %q, want the compatible worker %q", assignment.GetWorker().GetName(), wA.Name)
	}
	if diff := cmp.Diff(harness.SandboxRuntime(), assignment.GetSandboxRuntime(), protocmp.Transform()); diff != "" {
		t.Errorf("assignment runtime (-want +got):\n%s", diff)
	}
	if got := lastRestore(t, node).GetSnapshot().GetFidelity(); got != ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY {
		t.Errorf("Restore fidelity = %v, want MEMORY", got)
	}
}

// Resumes an actor suspended on a worker running the harness runtime, while
// another actor fills that worker and a worker running another runtime on the
// same node has room. The request succeeds (OK). The actor runs on the other
// worker, its assignment records the other runtime, and the atelet's Restore
// keeps only the snapshot's filesystem. A later synchronous suspend stamps the
// other runtime on the new snapshot. Both resumes retry while ate-api catches
// up.
func TestResumeActor_RuntimeFallback(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	node := h.Node("a")
	wA := h.AddWorker(pool, node)
	tmpl := h.NewTemplate(as, pool)
	ref := harness.ActorRef(h.SuspendedActor(as, tmpl))
	rtB := otherSandboxRuntime()
	wB := h.AddWorker(pool, node, harness.WithSandboxRuntime(rtB))
	if blocker := h.RunningActor(as, tmpl); h.WorkerOf(blocker) != wA {
		t.Fatalf("blocker runs on worker %q, want %q", h.WorkerOf(blocker).Name, wA.Name)
	}

	resp, err := h.ResumeActor(t.Context(), &ateapi.ResumeActorRequest{Actor: ref})
	if err != nil {
		t.Fatalf("ResumeActor: %v", err)
	}
	assignment := resp.GetActor().GetStatus().GetWorkerAssignment()
	if assignment.GetWorker().GetName() != wB.Name {
		t.Errorf("actor runs on worker %q, want the other worker %q", assignment.GetWorker().GetName(), wB.Name)
	}
	if diff := cmp.Diff(rtB, assignment.GetSandboxRuntime(), protocmp.Transform()); diff != "" {
		t.Errorf("assignment runtime (-want +got):\n%s", diff)
	}
	if got := lastRestore(t, node).GetSnapshot().GetFidelity(); got != ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS {
		t.Errorf("Restore fidelity = %v, want ROOTFS", got)
	}
	suspended, err := h.Control.SuspendActor(t.Context(), &ateapi.SuspendActorRequest{
		Actor:   ref,
		Options: &ateapi.SuspendOptions{Mode: ateapi.DurabilityMode_DURABILITY_MODE_SYNCHRONOUS},
	})
	if err != nil {
		t.Fatalf("SuspendActor: %v", err)
	}
	if diff := cmp.Diff(rtB, latestSnapshot(t, suspended.GetActor()).GetSandboxRuntime(), protocmp.Transform()); diff != "" {
		t.Errorf("new snapshot's runtime (-want +got):\n%s", diff)
	}
}

// Resumes a new actor, whose template's golden snapshot was taken under the
// harness runtime, while another actor fills the only worker running that
// runtime and a worker running another runtime on the same node has room. The
// request succeeds (OK). The actor runs on the other worker, and the atelet's
// Restore is of the golden snapshot, shared, keeping only its filesystem. The
// resume retries while ate-api catches up.
func TestResumeActor_GoldenRuntimeFallback(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	node := h.Node("a")
	wA := h.AddWorker(pool, node)
	tmpl := h.NewTemplate(as, pool)
	golden, err := h.Control.GetTag(t.Context(), &ateapi.GetTagRequest{Tag: tmpl.GetStatus().GetGoldenSnapshotStatus().GetGoldenTag()})
	if err != nil {
		t.Fatalf("GetTag: %v", err)
	}
	if blocker := h.RunningActor(as, tmpl); h.WorkerOf(blocker) != wA {
		t.Fatalf("blocker runs on worker %q, want %q", h.WorkerOf(blocker).Name, wA.Name)
	}
	wB := h.AddWorker(pool, node, harness.WithSandboxRuntime(otherSandboxRuntime()))
	ref := harness.ActorRef(h.NewActor(as, tmpl))

	resp, err := h.ResumeActor(t.Context(), &ateapi.ResumeActorRequest{Actor: ref})
	if err != nil {
		t.Fatalf("ResumeActor: %v", err)
	}
	if got := resp.GetActor().GetStatus().GetWorkerAssignment().GetWorker().GetName(); got != wB.Name {
		t.Errorf("actor runs on worker %q, want the other worker %q", got, wB.Name)
	}
	restore := lastRestore(t, node)
	assertRestoreOf(t, restore, golden.GetStatus().GetSnapshot())
	if got := restore.GetSnapshot().GetFidelity(); got != ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS {
		t.Errorf("Restore fidelity = %v, want ROOTFS", got)
	}
	if got := restore.GetSnapshot().GetObject().GetSharing(); got != ateletpb.SnapshotSharing_SNAPSHOT_SHARING_SHARED {
		t.Errorf("Restore sharing = %v, want SHARED", got)
	}
}

// Resumes a new actor, whose template's golden snapshot was taken under the
// harness runtime, while another actor fills the only worker running that
// runtime, and a worker on the same node runs another runtime but restores
// the harness runtime's snapshots. The request succeeds (OK). The actor runs
// on that worker, its assignment records the harness runtime, and the
// atelet's Restore keeps the snapshot's memory. A later asynchronous suspend
// stamps the harness runtime on the new snapshot. The resume retries while
// ate-api catches up.
func TestResumeActor_RestorableRuntime(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	node := h.Node("a")
	wA := h.AddWorker(pool, node)
	tmpl := h.NewTemplate(as, pool)
	if blocker := h.RunningActor(as, tmpl); h.WorkerOf(blocker) != wA {
		t.Fatalf("blocker runs on worker %q, want %q", h.WorkerOf(blocker).Name, wA.Name)
	}
	wB := h.AddWorker(pool, node, harness.WithSandboxRuntime(otherSandboxRuntime(), harness.SandboxRuntime()))
	ref := harness.ActorRef(h.NewActor(as, tmpl))

	resp, err := h.ResumeActor(t.Context(), &ateapi.ResumeActorRequest{Actor: ref})
	if err != nil {
		t.Fatalf("ResumeActor: %v", err)
	}
	assignment := resp.GetActor().GetStatus().GetWorkerAssignment()
	if assignment.GetWorker().GetName() != wB.Name {
		t.Errorf("actor runs on worker %q, want %q", assignment.GetWorker().GetName(), wB.Name)
	}
	if diff := cmp.Diff(harness.SandboxRuntime(), assignment.GetSandboxRuntime(), protocmp.Transform()); diff != "" {
		t.Errorf("assignment runtime (-want +got):\n%s", diff)
	}
	if got := lastRestore(t, node).GetSnapshot().GetFidelity(); got != ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY {
		t.Errorf("Restore fidelity = %v, want MEMORY", got)
	}
	suspended, err := h.Control.SuspendActor(t.Context(), &ateapi.SuspendActorRequest{Actor: ref})
	if err != nil {
		t.Fatalf("SuspendActor: %v", err)
	}
	if diff := cmp.Diff(harness.SandboxRuntime(), latestSnapshot(t, suspended.GetActor()).GetSandboxRuntime(), protocmp.Transform()); diff != "" {
		t.Errorf("new snapshot's runtime (-want +got):\n%s", diff)
	}
}

// uploadedThenUpgraded suspends a new actor asynchronously on w, finishes the
// upload of its local snapshot, and switches w to another runtime.
func uploadedThenUpgraded(t *testing.T, h *harness.Harness, as string, tmpl *ateapi.ActorTemplate, w *harness.Worker) *ateapi.ObjectRef {
	t.Helper()
	actor := h.AsyncSuspendedActor(as, tmpl)
	w.Node.FinishUpload(h.GetActor(harness.ActorRef(actor)))
	w.SetSandboxRuntime(otherSandboxRuntime())
	return harness.ActorRef(actor)
}

// Resumes an actor suspended asynchronously on node a, whose upload finished
// before node a's worker switched to another runtime, while a worker running
// the snapshot's runtime on node b has room. The request succeeds (OK). The
// actor runs on node b's worker under the snapshot's runtime, and node b's
// atelet restores the snapshot keeping its memory. The resume retries while
// ate-api catches up.
func TestResumeActor_UploadedSnapshotOnCompatibleNode(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	wA := h.AddWorker(pool, h.Node("a"))
	ref := uploadedThenUpgraded(t, h, as, h.NewTemplate(as, pool), wA)
	nodeB := h.Node("b")
	wB := h.AddWorker(pool, nodeB)

	resp, err := h.ResumeActor(t.Context(), &ateapi.ResumeActorRequest{Actor: ref})
	if err != nil {
		t.Fatalf("ResumeActor: %v", err)
	}
	assignment := resp.GetActor().GetStatus().GetWorkerAssignment()
	if assignment.GetWorker().GetName() != wB.Name {
		t.Errorf("actor runs on worker %q, want node b's worker %q", assignment.GetWorker().GetName(), wB.Name)
	}
	if diff := cmp.Diff(harness.SandboxRuntime(), assignment.GetSandboxRuntime(), protocmp.Transform()); diff != "" {
		t.Errorf("assignment runtime (-want +got):\n%s", diff)
	}
	if got := lastRestore(t, nodeB).GetSnapshot().GetFidelity(); got != ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY {
		t.Errorf("Restore fidelity = %v, want MEMORY", got)
	}
}

// As TestResumeActor_UploadedSnapshotOnCompatibleNode, while node a's atelet
// is unavailable for the DetachActor. The request succeeds (OK) and the actor
// runs on node b's worker under the snapshot's runtime, restoring the
// uploaded snapshot keeping its memory. The resume retries while ate-api
// catches up.
func TestResumeActor_UploadedSnapshotDetachUnavailable(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	nodeA := h.Node("a")
	wA := h.AddWorker(pool, nodeA)
	ref := uploadedThenUpgraded(t, h, as, h.NewTemplate(as, pool), wA)
	nodeB := h.Node("b")
	wB := h.AddWorker(pool, nodeB)
	nodeA.Atelet.On(harness.AteletDetachActor).FailNext(harness.ErrUnavailable)

	resp, err := h.ResumeActor(t.Context(), &ateapi.ResumeActorRequest{Actor: ref})
	if err != nil {
		t.Fatalf("ResumeActor: %v", err)
	}
	assignment := resp.GetActor().GetStatus().GetWorkerAssignment()
	if assignment.GetWorker().GetName() != wB.Name {
		t.Errorf("actor runs on worker %q, want node b's worker %q", assignment.GetWorker().GetName(), wB.Name)
	}
	if diff := cmp.Diff(harness.SandboxRuntime(), assignment.GetSandboxRuntime(), protocmp.Transform()); diff != "" {
		t.Errorf("assignment runtime (-want +got):\n%s", diff)
	}
	if got := lastRestore(t, nodeB).GetSnapshot().GetFidelity(); got != ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY {
		t.Errorf("Restore fidelity = %v, want MEMORY", got)
	}
}

// Resumes an actor suspended asynchronously on node a, whose upload finished
// before node a's only worker switched to another runtime. The request
// succeeds (OK). The actor runs on that worker under the other runtime, and
// the atelet's Restore keeps only the snapshot's filesystem. The resume
// retries while ate-api catches up.
func TestResumeActor_UploadedSnapshotFallsBack(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	nodeA := h.Node("a")
	wA := h.AddWorker(pool, nodeA)
	ref := uploadedThenUpgraded(t, h, as, h.NewTemplate(as, pool), wA)

	resp, err := h.ResumeActor(t.Context(), &ateapi.ResumeActorRequest{Actor: ref})
	if err != nil {
		t.Fatalf("ResumeActor: %v", err)
	}
	assignment := resp.GetActor().GetStatus().GetWorkerAssignment()
	if assignment.GetWorker().GetName() != wA.Name {
		t.Errorf("actor runs on worker %q, want %q", assignment.GetWorker().GetName(), wA.Name)
	}
	if diff := cmp.Diff(otherSandboxRuntime(), assignment.GetSandboxRuntime(), protocmp.Transform()); diff != "" {
		t.Errorf("assignment runtime (-want +got):\n%s", diff)
	}
	if got := lastRestore(t, nodeA).GetSnapshot().GetFidelity(); got != ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS {
		t.Errorf("Restore fidelity = %v, want ROOTFS", got)
	}
}

// As TestResumeActor_UploadedSnapshotFallsBack, while the atelet fails the
// first Restore with UNAVAILABLE. That resume fails with UNAVAILABLE and
// leaves the actor RESUMING on the worker with no crash. A later resume
// succeeds (OK) and restores the same snapshot as the first. Both resumes
// retry while ate-api catches up.
func TestResumeActor_UploadedSnapshotFallbackRetry(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	nodeA := h.Node("a")
	wA := h.AddWorker(pool, nodeA)
	ref := uploadedThenUpgraded(t, h, as, h.NewTemplate(as, pool), wA)
	nodeA.Atelet.On(harness.AteletRestore).FailNext(harness.ErrUnavailable)

	if _, err := h.ResumeActor(t.Context(), &ateapi.ResumeActorRequest{Actor: ref}); status.Code(err) != codes.Unavailable {
		t.Fatalf("ResumeActor with an unavailable atelet = %v, want Unavailable", err)
	}
	interrupted := h.GetActor(ref).GetStatus()
	if interrupted.GetState() != ateapi.ActorState_ACTOR_STATE_RESUMING || interrupted.GetWorkerAssignment().GetWorker().GetName() != wA.Name || interrupted.GetCrash() != nil {
		t.Errorf("actor after an unavailable atelet is %v on worker %q with crash %v, want RESUMING on %q with none", interrupted.GetState(), interrupted.GetWorkerAssignment().GetWorker().GetName(), interrupted.GetCrash(), wA.Name)
	}
	first := lastRestore(t, nodeA).GetSnapshot()

	if _, err := h.ResumeActor(t.Context(), &ateapi.ResumeActorRequest{Actor: ref}); err != nil {
		t.Fatalf("ResumeActor retry: %v", err)
	}
	if diff := cmp.Diff(first, lastRestore(t, nodeA).GetSnapshot(), protocmp.Transform()); diff != "" {
		t.Errorf("snapshot the retry restored (-first +retry):\n%s", diff)
	}
}

// Resumes an actor holding an older uploaded snapshot and a newer one local to
// node a and not uploaded, after node a's only worker switched to another
// runtime, while node a's atelet is unavailable for the DetachActor. The
// request fails with UNAVAILABLE and the atelet receives no Restore. The
// actor stays SUSPENDED with no worker assignment, holding the same
// snapshots.
func TestResumeActor_PendingSnapshotRefusesRuntimeFallback(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	nodeA := h.Node("a")
	wA := h.AddWorker(pool, nodeA)
	ref := harness.ActorRef(h.SuspendedActor(as, h.NewTemplate(as, pool)))
	if _, err := h.ResumeActor(t.Context(), &ateapi.ResumeActorRequest{Actor: ref}); err != nil {
		t.Fatalf("ResumeActor: %v", err)
	}
	if _, err := h.Control.SuspendActor(t.Context(), &ateapi.SuspendActorRequest{Actor: ref}); err != nil {
		t.Fatalf("SuspendActor: %v", err)
	}
	wA.SetSandboxRuntime(otherSandboxRuntime())
	before := h.GetActor(ref)
	nodeA.Atelet.On(harness.AteletDetachActor).FailNext(harness.ErrUnavailable)
	callsBefore := len(nodeA.Atelet.Calls())

	_, err := h.ResumeActor(t.Context(), &ateapi.ResumeActorRequest{Actor: ref})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("ResumeActor with the pending snapshot's atelet unavailable = %v, want Unavailable", err)
	}
	for _, c := range nodeA.Atelet.Calls()[callsBefore:] {
		if c.Op == harness.AteletRestore {
			t.Errorf("atelet received a Restore %v, want none", c.Request)
		}
	}
	if diff := cmp.Diff(before, h.GetActor(ref), protocmp.Transform(), ignoreVersion, ignoreTimestamps); diff != "" {
		t.Errorf("actor after a refused resume (-want +got):\n%s", diff)
	}
}

// Creating an actor from a template with an external volume, as a client that
// does not opt in to preview features, fails with INVALID_ARGUMENT and the
// error names the preview opt-in header. No actor is created.
func TestCreateActor_ExternalVolumesWithoutPreview(t *testing.T) {
	t.Parallel()
	h := harness.New(t, harness.WithPreviewClient())
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	h.AddWorker(pool, h.Node("a"))
	tmpl := h.NewTemplate(as, pool, harness.WithExternalVolume("data", "/data"))

	_, err := h.ControlWithoutPreview().CreateActor(t.Context(), &ateapi.CreateActorRequest{Actor: &ateapi.Actor{
		Metadata:      &ateapi.ResourceMetadata{Atespace: as, Name: "actor-a"},
		ActorTemplate: &ateapi.ObjectRef{Atespace: as, Name: tmpl.GetMetadata().GetName()},
	}})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("CreateActor without preview opt-in = %v, want InvalidArgument", err)
	}
	if msg := status.Convert(err).Message(); !strings.Contains(msg, preview.Header) {
		t.Errorf("CreateActor error %q does not name the preview header %s", msg, preview.Header)
	}
	listed, err := h.Control.ListActors(t.Context(), &ateapi.ListActorsRequest{Atespace: as})
	if err != nil {
		t.Fatalf("ListActors: %v", err)
	}
	if diff := cmp.Diff(&ateapi.ListActorsResponse{}, listed, protocmp.Transform()); diff != "" {
		t.Errorf("actors after a refused create (-want +got):\n%s", diff)
	}
}

// A client that does not opt in to preview features resumes, suspends and
// deletes an actor with an external volume that another client created. Each
// request succeeds (OK). A later Get fails with NOT_FOUND, and the volume
// plugin holds no volumes.
func TestActorLifecycle_ExternalVolumesWithoutPreview(t *testing.T) {
	t.Parallel()
	h := harness.New(t, harness.WithPreviewClient())
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	h.AddWorker(pool, h.Node("a"), harness.WithActorCapacity(2))
	ref := harness.ActorRef(h.NewActor(as, h.NewTemplate(as, pool, harness.WithExternalVolume("data", "/data"))))
	client := h.ControlWithoutPreview()

	if _, err := client.ResumeActor(t.Context(), &ateapi.ResumeActorRequest{Actor: ref}); err != nil {
		t.Fatalf("ResumeActor without preview opt-in: %v", err)
	}
	if _, err := client.SuspendActor(t.Context(), &ateapi.SuspendActorRequest{Actor: ref}); err != nil {
		t.Fatalf("SuspendActor without preview opt-in: %v", err)
	}
	if _, err := client.DeleteActor(t.Context(), &ateapi.DeleteActorRequest{Actor: ref}); err != nil {
		t.Fatalf("DeleteActor without preview opt-in: %v", err)
	}
	if _, err := h.Control.GetActor(t.Context(), &ateapi.GetActorRequest{Actor: ref}); status.Code(err) != codes.NotFound {
		t.Errorf("GetActor after delete = %v, want NotFound", err)
	}
	if diff := cmp.Diff([]harness.Volume{}, h.Volumes.List()); diff != "" {
		t.Errorf("volumes after delete (-want +got):\n%s", diff)
	}
}

// Resumes a new actor while the atelet holds the reply of its Restore, and the
// worker restarts before the reply reaches ate-api. The request fails with
// UNAVAILABLE, and the actor stays RESUMING on the worker. ate-api eventually
// observes the worker's new epoch, and its reconciler leaves the actor
// RESUMING on the worker. Resuming it again before the new
// worker process reports its sandbox runtimes fails with UNAVAILABLE without
// another Restore. Once the process reports them, a later resume succeeds
// (OK): the actor is RUNNING, its assignment naming the worker's new epoch.
// The first and last resumes retry while ate-api catches up.
func TestResumeActor_WorkerRestartedDuringRestore(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	node := h.Node("a")
	w := h.AddWorker(pool, node)
	ref := harness.ActorRef(h.NewActor(as, h.NewTemplate(as, pool)))

	gate := node.Atelet.On(harness.AteletRestore).HoldReply()
	resumed := make(chan error, 1)
	go func() {
		_, err := h.ResumeActor(t.Context(), &ateapi.ResumeActorRequest{Actor: ref})
		resumed <- err
	}()
	gate.Wait(t)
	w.Restart()
	gate.Release()
	if err := <-resumed; status.Code(err) != codes.Unavailable {
		t.Fatalf("ResumeActor across a worker restart = %v, want Unavailable", err)
	}
	h.Eventually("ate-api to observe the worker's new epoch", func() bool {
		return h.GetWorker(w).GetStatus().GetObservedEpoch() >= w.Epoch
	})
	got := h.GetActor(ref).GetStatus()
	if got.GetState() != ateapi.ActorState_ACTOR_STATE_RESUMING || got.GetWorkerAssignment().GetWorker().GetName() != w.Name {
		t.Fatalf("actor after the restart is %v on worker %q, want RESUMING on %q", got.GetState(), got.GetWorkerAssignment().GetWorker().GetName(), w.Name)
	}

	callsBefore := len(node.Atelet.Calls())
	if _, err := h.Control.ResumeActor(t.Context(), &ateapi.ResumeActorRequest{Actor: ref}); status.Code(err) != codes.Unavailable {
		t.Errorf("ResumeActor before the new process reports its runtimes = %v, want Unavailable", err)
	}
	if calls := node.Atelet.Calls()[callsBefore:]; len(calls) != 0 {
		t.Errorf("atelet received %v before the new process reported its runtimes, want no calls", calls)
	}

	w.SetSandboxRuntime(harness.SandboxRuntime())
	resp, err := h.ResumeActor(t.Context(), &ateapi.ResumeActorRequest{Actor: ref})
	if err != nil {
		t.Fatalf("ResumeActor after the new process reported its runtimes: %v", err)
	}
	assignment := resp.GetActor().GetStatus().GetWorkerAssignment()
	if state := resp.GetActor().GetStatus().GetState(); state != ateapi.ActorState_ACTOR_STATE_RUNNING || assignment.GetWorkerEpoch() != w.Epoch {
		t.Errorf("resumed actor is %v with worker epoch %d, want RUNNING with %d", state, assignment.GetWorkerEpoch(), w.Epoch)
	}
}

// Resumes an actor suspended asynchronously on node a after another actor took
// node a's only worker: with no other worker, the request fails with
// RESOURCE_EXHAUSTED. A worker is added on node b, and node a's atelet then
// uploads the actor's snapshot and drops it from the node on its own. The
// actor is no longer assigned to a node, and resuming it succeeds (OK) on node
// b without a DetachActor to node a. The resume after the detach retries while
// ate-api catches up.
func TestResumeActor_RelocatesAfterDetach(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	nodeA := h.Node("a")
	h.AddWorker(pool, nodeA)
	tmpl := h.NewTemplate(as, pool)
	actor := h.AsyncSuspendedActor(as, tmpl)
	ref := harness.ActorRef(actor)
	h.RunningActor(as, tmpl)

	if _, err := h.Control.ResumeActor(t.Context(), &ateapi.ResumeActorRequest{Actor: ref}); status.Code(err) != codes.ResourceExhausted {
		t.Errorf("ResumeActor with node a full and no other worker = %v, want ResourceExhausted", err)
	}
	nodeB := h.Node("b")
	wB := h.AddWorker(pool, nodeB)
	nodeA.DetachSnapshot(h.GetActor(ref))
	if node := h.GetActor(ref).GetStatus().GetAssignedNode(); node != "" {
		t.Errorf("actor after the detach is assigned to %q, want no node", node)
	}

	callsBefore := len(nodeA.Atelet.Calls())
	resp, err := h.ResumeActor(t.Context(), &ateapi.ResumeActorRequest{Actor: ref})
	if err != nil {
		t.Fatalf("ResumeActor after the detach: %v", err)
	}
	if got := resp.GetActor().GetStatus().GetWorkerAssignment().GetWorker().GetName(); got != wB.Name {
		t.Errorf("actor runs on worker %q, want node b's worker %q", got, wB.Name)
	}
	for _, c := range nodeA.Atelet.Calls()[callsBefore:] {
		if c.Op == harness.AteletDetachActor {
			t.Errorf("node a atelet received a DetachActor %v, want none", c.Request)
		}
	}
}
