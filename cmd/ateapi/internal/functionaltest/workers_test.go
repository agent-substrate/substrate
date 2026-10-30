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
	"fmt"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/ateapi/internal/functionaltest/harness"
	"go.ate.dev/substrate/internal/resources"
)

// workerSpec is a valid Worker named after the pod UID uid.
func workerSpec(uid string) *ateapi.Worker {
	return &ateapi.Worker{
		Metadata:        &ateapi.ResourceMetadata{Name: uid},
		WorkerNamespace: "workers",
		WorkerPool:      "pool1",
		WorkerPod:       "worker-" + uid[:8],
		WorkerPodUid:    uid,
		NodeName:        "node-a",
	}
}

const workerUID = "5f2c1a90-7b34-4e6d-8a11-0c3e9d5b7f42"

// Creates a worker and verifies that the whole resource has the expected
// shape.
func TestCreateWorker(t *testing.T) {
	t.Parallel()
	h := harness.New(t)

	created, err := h.Control.CreateWorker(t.Context(), &ateapi.CreateWorkerRequest{Worker: workerSpec(workerUID)})
	if err != nil {
		t.Fatalf("CreateWorker: %v", err)
	}
	want := &ateapi.Worker{
		Metadata:        &ateapi.ResourceMetadata{Name: workerUID},
		WorkerNamespace: "workers",
		WorkerPool:      "pool1",
		WorkerPod:       "worker-5f2c1a90",
		WorkerPodUid:    workerUID,
		NodeName:        "node-a",
		Status:          &ateapi.WorkerStatus{State: ateapi.WorkerState_WORKER_STATE_UNAVAILABLE},
	}
	if diff := cmp.Diff(want, created, protocmp.Transform(), ignoreServerMetadata); diff != "" {
		t.Errorf("created worker (-want +got):\n%s", diff)
	}
}

// Creating a worker for an existing name fails with ALREADY_EXISTS and
// leaves the existing one unchanged.
func TestCreateWorker_AlreadyExists(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	created, err := h.Control.CreateWorker(t.Context(), &ateapi.CreateWorkerRequest{Worker: workerSpec(workerUID)})
	if err != nil {
		t.Fatalf("CreateWorker: %v", err)
	}

	_, err = h.Control.CreateWorker(t.Context(), &ateapi.CreateWorkerRequest{Worker: workerSpec(workerUID)})
	if status.Code(err) != codes.AlreadyExists {
		t.Errorf("CreateWorker of an existing name = %v, want AlreadyExists", err)
	}
	got, err := h.Control.GetWorker(t.Context(), &ateapi.GetWorkerRequest{Worker: &ateapi.ObjectRef{Name: workerUID}})
	if err != nil {
		t.Fatalf("GetWorker: %v", err)
	}
	if diff := cmp.Diff(created, got, protocmp.Transform()); diff != "" {
		t.Errorf("worker after a refused create (-want +got):\n%s", diff)
	}
}

// Creating a worker with an invalid name fails with INVALID_ARGUMENT and the
// error names the invalid field.
func TestCreateWorker_ValidationError(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	worker := workerSpec(workerUID)
	worker.Metadata.Name = "Not_A_Valid_Name"

	_, err := h.Control.CreateWorker(t.Context(), &ateapi.CreateWorkerRequest{Worker: worker})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("CreateWorker with an invalid name = %v, want InvalidArgument", err)
	}
	if msg := status.Convert(err).Message(); !strings.Contains(msg, "worker.metadata.name") {
		t.Errorf("CreateWorker error %q does not name the invalid field worker.metadata.name", msg)
	}
}

// Getting an existing worker returns exactly the resource CreateWorker
// returned.
func TestGetWorker(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	created, err := h.Control.CreateWorker(t.Context(), &ateapi.CreateWorkerRequest{Worker: workerSpec(workerUID)})
	if err != nil {
		t.Fatalf("CreateWorker: %v", err)
	}

	got, err := h.Control.GetWorker(t.Context(), &ateapi.GetWorkerRequest{Worker: &ateapi.ObjectRef{Name: workerUID}})
	if err != nil {
		t.Fatalf("GetWorker: %v", err)
	}
	if diff := cmp.Diff(created, got, protocmp.Transform()); diff != "" {
		t.Errorf("GetWorker (-created +got):\n%s", diff)
	}
}

// Getting a worker that doesn't exist fails with NOT_FOUND.
func TestGetWorker_NotFound(t *testing.T) {
	t.Parallel()
	h := harness.New(t)

	_, err := h.Control.GetWorker(t.Context(), &ateapi.GetWorkerRequest{Worker: &ateapi.ObjectRef{Name: workerUID}})
	if status.Code(err) != codes.NotFound {
		t.Errorf("GetWorker of a missing worker = %v, want NotFound", err)
	}
}

// Getting a worker with an invalid name fails with INVALID_ARGUMENT and the
// error names the invalid field.
func TestGetWorker_ValidationError(t *testing.T) {
	t.Parallel()
	h := harness.New(t)

	_, err := h.Control.GetWorker(t.Context(), &ateapi.GetWorkerRequest{Worker: &ateapi.ObjectRef{Name: "Not_A_Valid_Name"}})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("GetWorker with an invalid name = %v, want InvalidArgument", err)
	}
	if msg := status.Convert(err).Message(); !strings.Contains(msg, "worker.name") {
		t.Errorf("GetWorker error %q does not name the invalid field worker.name", msg)
	}
}

// Creates N workers and lists them with pages smaller than N. The pages
// together hold exactly the N workers.
func TestListWorkers(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	const n, pageSize = 5, 2
	var created []*ateapi.Worker
	for i := range n {
		worker, err := h.Control.CreateWorker(t.Context(), &ateapi.CreateWorkerRequest{Worker: workerSpec(fmt.Sprintf("5f2c1a90-7b34-4e6d-8a11-0c3e9d5b7f4%d", i))})
		if err != nil {
			t.Fatalf("CreateWorker: %v", err)
		}
		created = append(created, worker)
	}

	var listed []*ateapi.Worker
	req := &ateapi.ListWorkersRequest{Options: &ateapi.ListOptions{PageSize: pageSize}}
	for pages := 1; ; pages++ {
		if pages > n {
			t.Fatalf("listing took more than %d pages", n)
		}
		resp, err := h.Control.ListWorkers(t.Context(), req)
		if err != nil {
			t.Fatalf("ListWorkers page %d: %v", pages, err)
		}
		if got := len(resp.GetWorkers()); got > pageSize {
			t.Errorf("page %d holds %d workers, want at most %d", pages, got, pageSize)
		}
		listed = append(listed, resp.GetWorkers()...)
		if resp.GetNextPageToken() == "" {
			break
		}
		req.Options.PageToken = resp.GetNextPageToken()
	}
	byName := cmpopts.SortSlices(func(a, b *ateapi.Worker) bool { return a.GetMetadata().GetName() < b.GetMetadata().GetName() })
	if diff := cmp.Diff(created, listed, protocmp.Transform(), byName); diff != "" {
		t.Errorf("listed workers (-want +got):\n%s", diff)
	}
}

// Listing workers with a negative page size fails with INVALID_ARGUMENT and
// the error names the invalid field.
func TestListWorkers_ValidationError(t *testing.T) {
	t.Parallel()
	h := harness.New(t)

	_, err := h.Control.ListWorkers(t.Context(), &ateapi.ListWorkersRequest{Options: &ateapi.ListOptions{PageSize: -1}})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("ListWorkers with a negative page size = %v, want InvalidArgument", err)
	}
	if msg := status.Convert(err).Message(); !strings.Contains(msg, "page_size") {
		t.Errorf("ListWorkers error %q does not name the invalid field page_size", msg)
	}
}

// Listing workers with a malformed page token fails with INVALID_ARGUMENT.
func TestListWorkers_InvalidPageToken(t *testing.T) {
	t.Parallel()
	h := harness.New(t)

	_, err := h.Control.ListWorkers(t.Context(), &ateapi.ListWorkersRequest{Options: &ateapi.ListOptions{PageToken: "not-a-real-token"}})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("ListWorkers with a malformed page token = %v, want InvalidArgument", err)
	}
}

// Updates the labels of an existing worker, sending it as read. The result,
// and a later Get, is the resource as read with that field changed and a
// higher version.
func TestUpdateWorker(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	ref := &ateapi.ObjectRef{Name: workerUID}
	before, err := h.Control.CreateWorker(t.Context(), &ateapi.CreateWorkerRequest{Worker: workerSpec(workerUID)})
	if err != nil {
		t.Fatalf("CreateWorker: %v", err)
	}

	toUpdate := proto.Clone(before).(*ateapi.Worker)
	toUpdate.Labels = map[string]string{"tier": "1"}
	updated, err := h.Control.UpdateWorker(t.Context(), &ateapi.UpdateWorkerRequest{Worker: toUpdate})
	if err != nil {
		t.Fatalf("UpdateWorker: %v", err)
	}
	want := proto.Clone(before).(*ateapi.Worker)
	want.Labels = map[string]string{"tier": "1"}
	if diff := cmp.Diff(want, updated, protocmp.Transform(), ignoreVersion, ignoreTimestamps); diff != "" {
		t.Errorf("updated worker (-want +got):\n%s", diff)
	}
	if got, was := updated.GetMetadata().GetVersion(), before.GetMetadata().GetVersion(); got <= was {
		t.Errorf("updated worker has version %d, want higher than %d", got, was)
	}
	got, err := h.Control.GetWorker(t.Context(), &ateapi.GetWorkerRequest{Worker: ref})
	if err != nil {
		t.Fatalf("GetWorker: %v", err)
	}
	if diff := cmp.Diff(updated, got, protocmp.Transform()); diff != "" {
		t.Errorf("GetWorker after update (-updated +got):\n%s", diff)
	}
}

// Updating a worker that doesn't exist fails with NOT_FOUND.
func TestUpdateWorker_NotFound(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	worker := workerSpec(workerUID)
	worker.Metadata.Uid, worker.Metadata.Version = foreignUID, 1

	_, err := h.Control.UpdateWorker(t.Context(), &ateapi.UpdateWorkerRequest{Worker: worker})
	if status.Code(err) != codes.NotFound {
		t.Errorf("UpdateWorker of a missing worker = %v, want NotFound", err)
	}
}

// Updating a worker with the wrong UID fails with ABORTED, and with the
// right UID but the wrong version fails with ABORTED; both leave it
// unchanged. Updating it without a UID or a version fails with
// INVALID_ARGUMENT.
func TestUpdateWorker_WithPreconditions(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	ref := &ateapi.ObjectRef{Name: workerUID}
	before, err := h.Control.CreateWorker(t.Context(), &ateapi.CreateWorkerRequest{Worker: workerSpec(workerUID)})
	if err != nil {
		t.Fatalf("CreateWorker: %v", err)
	}
	update := func(uid string, version int64) error {
		toUpdate := proto.Clone(before).(*ateapi.Worker)
		toUpdate.Metadata.Uid, toUpdate.Metadata.Version = uid, version
		toUpdate.Labels = map[string]string{"tier": "1"}
		_, err := h.Control.UpdateWorker(t.Context(), &ateapi.UpdateWorkerRequest{Worker: toUpdate})
		return err
	}

	if err := update(foreignUID, before.GetMetadata().GetVersion()); status.Code(err) != codes.Aborted {
		t.Errorf("UpdateWorker with the wrong uid = %v, want Aborted", err)
	}
	if err := update(before.GetMetadata().GetUid(), before.GetMetadata().GetVersion()+1); status.Code(err) != codes.Aborted {
		t.Errorf("UpdateWorker with the right uid and the wrong version = %v, want Aborted", err)
	}
	got, err := h.Control.GetWorker(t.Context(), &ateapi.GetWorkerRequest{Worker: ref})
	if err != nil {
		t.Fatalf("GetWorker: %v", err)
	}
	if diff := cmp.Diff(before, got, protocmp.Transform()); diff != "" {
		t.Errorf("worker after refused updates (-want +got):\n%s", diff)
	}
	if err := update("", 0); status.Code(err) != codes.InvalidArgument {
		t.Errorf("UpdateWorker without a uid or version = %v, want InvalidArgument", err)
	}
}

// Updating a worker with an invalid name fails with INVALID_ARGUMENT and the
// error names the invalid field.
func TestUpdateWorker_ValidationError(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	worker := workerSpec(workerUID)
	worker.Metadata.Name = "Not_A_Valid_Name"
	worker.Metadata.Uid, worker.Metadata.Version = foreignUID, 1

	_, err := h.Control.UpdateWorker(t.Context(), &ateapi.UpdateWorkerRequest{Worker: worker})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("UpdateWorker with an invalid name = %v, want InvalidArgument", err)
	}
	if msg := status.Convert(err).Message(); !strings.Contains(msg, "worker.metadata.name") {
		t.Errorf("UpdateWorker error %q does not name the invalid field worker.metadata.name", msg)
	}
}

// Updating a worker's node name, sending it as read with only that field
// changed, fails with INVALID_ARGUMENT and leaves the worker unchanged.
func TestUpdateWorker_ImmutableField(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	w := h.AddWorker(h.CreateWorkerPool("pool1", nil), h.Node("a"))
	before := h.GetWorker(w)

	toUpdate := proto.Clone(before).(*ateapi.Worker)
	toUpdate.NodeName = "another-node"
	_, err := h.Control.UpdateWorker(t.Context(), &ateapi.UpdateWorkerRequest{Worker: toUpdate})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("UpdateWorker changing the node name = %v, want InvalidArgument", err)
	}
	if diff := cmp.Diff(before, h.GetWorker(w), protocmp.Transform()); diff != "" {
		t.Errorf("worker after a refused update (-want +got):\n%s", diff)
	}
}

// Deletes an existing worker. The response is the worker as read before the
// delete, marked DRAINING, and a later Get fails with NOT_FOUND.
func TestDeleteWorker(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	ref := &ateapi.ObjectRef{Name: workerUID}
	created, err := h.Control.CreateWorker(t.Context(), &ateapi.CreateWorkerRequest{Worker: workerSpec(workerUID)})
	if err != nil {
		t.Fatalf("CreateWorker: %v", err)
	}

	deleted, err := h.Control.DeleteWorker(t.Context(), &ateapi.DeleteWorkerRequest{Worker: ref})
	if err != nil {
		t.Fatalf("DeleteWorker: %v", err)
	}
	want := proto.Clone(created).(*ateapi.Worker)
	want.Status.State = ateapi.WorkerState_WORKER_STATE_DRAINING
	if diff := cmp.Diff(want, deleted, protocmp.Transform(), ignoreVersion, ignoreTimestamps, ignoreDeleteTime); diff != "" {
		t.Errorf("DeleteWorker response (-want +got):\n%s", diff)
	}
	if _, err := h.Control.GetWorker(t.Context(), &ateapi.GetWorkerRequest{Worker: ref}); status.Code(err) != codes.NotFound {
		t.Errorf("GetWorker after delete = %v, want NotFound", err)
	}
}

// Deleting a worker that doesn't exist fails with NOT_FOUND.
func TestDeleteWorker_NotFound(t *testing.T) {
	t.Parallel()
	h := harness.New(t)

	_, err := h.Control.DeleteWorker(t.Context(), &ateapi.DeleteWorkerRequest{Worker: &ateapi.ObjectRef{Name: workerUID}})
	if status.Code(err) != codes.NotFound {
		t.Errorf("DeleteWorker of a missing worker = %v, want NotFound", err)
	}
}

// Deleting a worker with the wrong UID fails with ABORTED, and with the
// right UID but the wrong version fails with ABORTED; both leave it
// unchanged. With the right UID and version the delete succeeds.
func TestDeleteWorker_WithPreconditions(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	ref := &ateapi.ObjectRef{Name: workerUID}
	created, err := h.Control.CreateWorker(t.Context(), &ateapi.CreateWorkerRequest{Worker: workerSpec(workerUID)})
	if err != nil {
		t.Fatalf("CreateWorker: %v", err)
	}
	uid, version := created.GetMetadata().GetUid(), created.GetMetadata().GetVersion()

	_, err = h.Control.DeleteWorker(t.Context(), &ateapi.DeleteWorkerRequest{Worker: ref, Options: &ateapi.DeleteOptions{Uid: foreignUID}})
	if status.Code(err) != codes.Aborted {
		t.Errorf("DeleteWorker with the wrong uid = %v, want Aborted", err)
	}
	_, err = h.Control.DeleteWorker(t.Context(), &ateapi.DeleteWorkerRequest{Worker: ref, Options: &ateapi.DeleteOptions{Uid: uid, Version: version + 1}})
	if status.Code(err) != codes.Aborted {
		t.Errorf("DeleteWorker with the right uid and the wrong version = %v, want Aborted", err)
	}
	got, err := h.Control.GetWorker(t.Context(), &ateapi.GetWorkerRequest{Worker: ref})
	if err != nil {
		t.Fatalf("GetWorker after refused deletes: %v", err)
	}
	if diff := cmp.Diff(created, got, protocmp.Transform()); diff != "" {
		t.Errorf("worker after refused deletes (-want +got):\n%s", diff)
	}

	if _, err := h.Control.DeleteWorker(t.Context(), &ateapi.DeleteWorkerRequest{Worker: ref, Options: &ateapi.DeleteOptions{Uid: uid, Version: version}}); err != nil {
		t.Errorf("DeleteWorker with the right uid and version: %v", err)
	}
}

// Deleting a worker with an invalid name fails with INVALID_ARGUMENT and the
// error names the invalid field.
func TestDeleteWorker_ValidationError(t *testing.T) {
	t.Parallel()
	h := harness.New(t)

	_, err := h.Control.DeleteWorker(t.Context(), &ateapi.DeleteWorkerRequest{Worker: &ateapi.ObjectRef{Name: "Not_A_Valid_Name"}})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("DeleteWorker with an invalid name = %v, want InvalidArgument", err)
	}
	if msg := status.Convert(err).Message(); !strings.Contains(msg, "worker.name") {
		t.Errorf("DeleteWorker error %q does not name the invalid field worker.name", msg)
	}
}

// Deleting a worker that hosts a running actor succeeds (OK), and a later
// Get of the worker fails with NOT_FOUND. The actor becomes CRASHED, with no
// worker assignment and a crash message saying the worker pod went away
// while hosting the actor.
func TestDeleteWorker_HostingActor(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	h.AddWorker(pool, h.Node("a"))
	actor := h.RunningActor(as, h.NewTemplate(as, pool))
	ref := harness.ActorRef(actor)
	host := h.WorkerOf(actor)
	before := h.GetActor(ref)

	if _, err := h.Control.DeleteWorker(t.Context(), &ateapi.DeleteWorkerRequest{Worker: host.Ref()}); err != nil {
		t.Fatalf("DeleteWorker: %v", err)
	}
	if _, err := h.Control.GetWorker(t.Context(), &ateapi.GetWorkerRequest{Worker: host.Ref()}); status.Code(err) != codes.NotFound {
		t.Errorf("GetWorker after delete = %v, want NotFound", err)
	}
	want := proto.Clone(before).(*ateapi.Actor)
	want.Status.State = ateapi.ActorState_ACTOR_STATE_CRASHED
	want.Status.WorkerAssignment = nil
	want.Status.Crash = &ateapi.ActorCrash{Message: "worker pod went away while hosting the actor"}
	ignoreCrashTime := protocmp.IgnoreFields(&ateapi.ActorCrash{}, "crash_time")
	if diff := cmp.Diff(want, h.GetActor(ref), protocmp.Transform(), ignoreVersion, ignoreTimestamps, ignoreCrashTime); diff != "" {
		t.Errorf("actor after its worker was deleted (-want +got):\n%s", diff)
	}
}

// Draining an active worker marks it DRAINING. The result, and a later Get,
// is the worker as read with that state and a higher version.
func TestDrainWorker(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	w := h.AddWorker(h.CreateWorkerPool("pool1", nil), h.Node("a"))
	before := h.GetWorker(w)

	resp, err := h.Control.DrainWorker(t.Context(), &ateapi.DrainWorkerRequest{Worker: w.Ref()})
	if err != nil {
		t.Fatalf("DrainWorker: %v", err)
	}
	drained := resp.GetWorker()
	want := proto.Clone(before).(*ateapi.Worker)
	want.Status.State = ateapi.WorkerState_WORKER_STATE_DRAINING
	if diff := cmp.Diff(want, drained, protocmp.Transform(), ignoreVersion, ignoreTimestamps); diff != "" {
		t.Errorf("drained worker (-want +got):\n%s", diff)
	}
	if got, was := drained.GetMetadata().GetVersion(), before.GetMetadata().GetVersion(); got <= was {
		t.Errorf("drained worker has version %d, want higher than %d", got, was)
	}
	if diff := cmp.Diff(drained, h.GetWorker(w), protocmp.Transform()); diff != "" {
		t.Errorf("GetWorker after drain (-drained +got):\n%s", diff)
	}
}

// Draining a worker that doesn't exist fails with NOT_FOUND.
func TestDrainWorker_NotFound(t *testing.T) {
	t.Parallel()
	h := harness.New(t)

	_, err := h.Control.DrainWorker(t.Context(), &ateapi.DrainWorkerRequest{Worker: &ateapi.ObjectRef{Name: workerUID}})
	if status.Code(err) != codes.NotFound {
		t.Errorf("DrainWorker of a missing worker = %v, want NotFound", err)
	}
}

// Draining a worker with an invalid name fails with INVALID_ARGUMENT and the
// error names the invalid field.
func TestDrainWorker_ValidationError(t *testing.T) {
	t.Parallel()
	h := harness.New(t)

	_, err := h.Control.DrainWorker(t.Context(), &ateapi.DrainWorkerRequest{Worker: &ateapi.ObjectRef{Name: "Not_A_Valid_Name"}})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("DrainWorker with an invalid name = %v, want InvalidArgument", err)
	}
	if msg := status.Convert(err).Message(); !strings.Contains(msg, "worker.name") {
		t.Errorf("DrainWorker error %q does not name the invalid field worker.name", msg)
	}
}

// Draining a worker that is already DRAINING succeeds (OK). The response,
// and a later Get, is the worker exactly as it was, its version included.
func TestDrainWorker_AlreadyDraining(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	w := h.AddWorker(h.CreateWorkerPool("pool1", nil), h.Node("a"))
	w.Drain()
	before := h.GetWorker(w)

	resp, err := h.Control.DrainWorker(t.Context(), &ateapi.DrainWorkerRequest{Worker: w.Ref()})
	if status.Code(err) != codes.OK {
		t.Fatalf("DrainWorker of a draining worker = %v, want OK", err)
	}
	if diff := cmp.Diff(before, resp.GetWorker(), protocmp.Transform()); diff != "" {
		t.Errorf("DrainWorker response (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(before, h.GetWorker(w), protocmp.Transform()); diff != "" {
		t.Errorf("worker after draining it again (-want +got):\n%s", diff)
	}
}

// Lists the assignments of a worker hosting N running actors with pages
// smaller than N. The pages together hold exactly one assignment per actor.
func TestListWorkerActorAssignments(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	const n, pageSize = 3, 2
	w := h.AddWorker(pool, h.Node("a"), harness.WithActorCapacity(n))
	tmpl := h.NewTemplate(as, pool)
	var want []*ateapi.ActorAssignment
	for range n {
		actor := h.RunningActor(as, tmpl)
		want = append(want, &ateapi.ActorAssignment{
			Metadata:  &ateapi.ResourceMetadata{Name: actor.GetMetadata().GetUid()},
			Actor:     harness.ActorRef(actor),
			ActorUid:  actor.GetMetadata().GetUid(),
			Resources: resources.CPUMemory(harness.TemplateCPUMillicores, harness.TemplateMemoryBytes),
		})
	}

	var listed []*ateapi.ActorAssignment
	req := &ateapi.ListWorkerActorAssignmentsRequest{Worker: w.Ref(), Options: &ateapi.ListOptions{PageSize: pageSize}}
	for pages := 1; ; pages++ {
		if pages > n {
			t.Fatalf("listing took more than %d pages", n)
		}
		resp, err := h.Control.ListWorkerActorAssignments(t.Context(), req)
		if err != nil {
			t.Fatalf("ListWorkerActorAssignments page %d: %v", pages, err)
		}
		if got := len(resp.GetActorAssignments()); got > pageSize {
			t.Errorf("page %d holds %d assignments, want at most %d", pages, got, pageSize)
		}
		listed = append(listed, resp.GetActorAssignments()...)
		if resp.GetNextPageToken() == "" {
			break
		}
		req.Options.PageToken = resp.GetNextPageToken()
	}
	byActor := cmpopts.SortSlices(func(a, b *ateapi.ActorAssignment) bool { return a.GetActorUid() < b.GetActorUid() })
	if diff := cmp.Diff(want, listed, protocmp.Transform(), ignoreServerMetadata, byActor); diff != "" {
		t.Errorf("listed assignments (-want +got):\n%s", diff)
	}
}

// Listing the assignments of a worker that doesn't exist fails with
// NOT_FOUND.
func TestListWorkerActorAssignments_NotFound(t *testing.T) {
	t.Parallel()
	h := harness.New(t)

	_, err := h.Control.ListWorkerActorAssignments(t.Context(), &ateapi.ListWorkerActorAssignmentsRequest{Worker: &ateapi.ObjectRef{Name: workerUID}})
	if status.Code(err) != codes.NotFound {
		t.Errorf("ListWorkerActorAssignments of a missing worker = %v, want NotFound", err)
	}
}

// Listing the assignments of a worker with a negative page size fails with
// INVALID_ARGUMENT and the error names the invalid field.
func TestListWorkerActorAssignments_ValidationError(t *testing.T) {
	t.Parallel()
	h := harness.New(t)

	_, err := h.Control.ListWorkerActorAssignments(t.Context(), &ateapi.ListWorkerActorAssignmentsRequest{Worker: &ateapi.ObjectRef{Name: workerUID}, Options: &ateapi.ListOptions{PageSize: -1}})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("ListWorkerActorAssignments with a negative page size = %v, want InvalidArgument", err)
	}
	if msg := status.Convert(err).Message(); !strings.Contains(msg, "page_size") {
		t.Errorf("ListWorkerActorAssignments error %q does not name the invalid field page_size", msg)
	}
}

// Listing the assignments of a worker with a malformed page token fails with
// INVALID_ARGUMENT.
func TestListWorkerActorAssignments_InvalidPageToken(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	if _, err := h.Control.CreateWorker(t.Context(), &ateapi.CreateWorkerRequest{Worker: workerSpec(workerUID)}); err != nil {
		t.Fatalf("CreateWorker: %v", err)
	}

	_, err := h.Control.ListWorkerActorAssignments(t.Context(), &ateapi.ListWorkerActorAssignmentsRequest{Worker: &ateapi.ObjectRef{Name: workerUID}, Options: &ateapi.ListOptions{PageToken: "not-a-real-token"}})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("ListWorkerActorAssignments with a malformed page token = %v, want InvalidArgument", err)
	}
}

// Marks an active worker unavailable. The response, and a later Get, is the
// worker as read with state UNAVAILABLE and a higher version.
func TestMarkWorkerUnavailable(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	w := h.AddWorker(h.CreateWorkerPool("pool1", nil), h.Node("a"))
	before := h.GetWorker(w)

	resp, err := h.Control.MarkWorkerUnavailable(t.Context(), &ateapi.MarkWorkerUnavailableRequest{Worker: w.Ref()})
	if err != nil {
		t.Fatalf("MarkWorkerUnavailable: %v", err)
	}
	want := proto.Clone(before).(*ateapi.Worker)
	want.Status.State = ateapi.WorkerState_WORKER_STATE_UNAVAILABLE
	if diff := cmp.Diff(want, resp.GetWorker(), protocmp.Transform(), ignoreVersion, ignoreTimestamps); diff != "" {
		t.Errorf("worker marked unavailable (-want +got):\n%s", diff)
	}
	if got, was := resp.GetWorker().GetMetadata().GetVersion(), before.GetMetadata().GetVersion(); got <= was {
		t.Errorf("worker marked unavailable has version %d, want higher than %d", got, was)
	}
	if diff := cmp.Diff(resp.GetWorker(), h.GetWorker(w), protocmp.Transform()); diff != "" {
		t.Errorf("GetWorker after the mark (-marked +got):\n%s", diff)
	}
}

// Marks an unavailable worker available again. The response, and a later Get,
// is the worker as read with state ACTIVE and a higher version. Resuming an
// actor then succeeds (OK) on that worker, retrying while ate-api catches up.
func TestMarkWorkerAvailable(t *testing.T) {
	t.Parallel()
	h := harness.New(t)
	as := h.CreateAtespace("team-a")
	pool := h.CreateWorkerPool("pool1", nil)
	w := h.AddWorker(pool, h.Node("a"))
	actor := h.NewActor(as, h.NewTemplate(as, pool))
	if _, err := h.Control.MarkWorkerUnavailable(t.Context(), &ateapi.MarkWorkerUnavailableRequest{Worker: w.Ref()}); err != nil {
		t.Fatalf("MarkWorkerUnavailable: %v", err)
	}
	before := h.GetWorker(w)

	resp, err := h.Control.MarkWorkerAvailable(t.Context(), &ateapi.MarkWorkerAvailableRequest{Worker: w.Ref()})
	if err != nil {
		t.Fatalf("MarkWorkerAvailable: %v", err)
	}
	want := proto.Clone(before).(*ateapi.Worker)
	want.Status.State = ateapi.WorkerState_WORKER_STATE_ACTIVE
	if diff := cmp.Diff(want, resp.GetWorker(), protocmp.Transform(), ignoreVersion, ignoreTimestamps); diff != "" {
		t.Errorf("worker marked available (-want +got):\n%s", diff)
	}
	if got, was := resp.GetWorker().GetMetadata().GetVersion(), before.GetMetadata().GetVersion(); got <= was {
		t.Errorf("worker marked available has version %d, want higher than %d", got, was)
	}
	if diff := cmp.Diff(resp.GetWorker(), h.GetWorker(w), protocmp.Transform()); diff != "" {
		t.Errorf("GetWorker after the mark (-marked +got):\n%s", diff)
	}
	placed, err := h.ResumeActor(t.Context(), &ateapi.ResumeActorRequest{Actor: harness.ActorRef(actor)})
	if err != nil {
		t.Fatalf("ResumeActor: %v", err)
	}
	if got := placed.GetActor().GetStatus().GetWorkerAssignment().GetWorker().GetName(); got != w.Name {
		t.Errorf("actor runs on worker %q, want %q", got, w.Name)
	}
}
