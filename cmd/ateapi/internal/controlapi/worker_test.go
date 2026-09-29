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
	"sort"
	"strings"
	"testing"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/storetest"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
)

// Worker names are pod UIDs, which are opaque to everything above the syncer.
const (
	apiWorkerName      = "5f2c1a90-7b34-4e6d-8a11-0c3e9d5b7f42"
	apiOtherWorkerName = "1a7e4c83-6d20-4f95-b3c8-9e0a2f6d4b17"
)

// validWorker returns a Worker in the shape CreateWorker accepts: named, with
// its pod coordinates filled in and no status — status is output-only.
func validWorker(name string, mods ...func(*ateapipb.Worker)) *ateapipb.Worker {
	w := &ateapipb.Worker{
		Metadata:        &ateapipb.ResourceMetadata{Name: name},
		WorkerNamespace: "ate-system",
		WorkerPool:      "pool-1",
		WorkerPod:       "worker-pod-1",
		WorkerPodUid:    name,
		NodeName:        "node-1",
		Ip:              "10.1.2.3",
		SandboxClass:    "gvisor",
	}
	for _, m := range mods {
		m(w)
	}
	return w
}

func newAPIAssignment(actorUID string) *ateapipb.ActorAssignment {
	return &ateapipb.ActorAssignment{
		ActorTemplateRef: &ateapipb.ObjectRef{Atespace: "ate-system", Name: "tmpl"},
		Actor:            &ateapipb.ObjectRef{Atespace: "team-a", Name: "actor-1"},
		ActorUid:         actorUID,
	}
}

// newWorkerAPIService returns a service backed by a real store, which is what
// makes the compare-and-set assertions below meaningful — a fake would decide
// the outcome the test is trying to observe.
// impl is a real ServiceImpl rather than the store itself, as main wires it:
// the service layer is where a read composes the Worker with records kept
// outside it, so a test that hands RPCService the bare store silently skips
// that and reports whatever the store row happens to hold.
func newWorkerAPIService(t *testing.T) (*RPCService, store.Interface) {
	t.Helper()
	persistence, cleanup := storetest.SetupTestStore(t)
	t.Cleanup(cleanup)
	impl := newServiceImpl(persistence, nil)
	return &RPCService{impl: impl, workerWorkflow: NewWorkerWorkflow(persistence)}, persistence
}

// seedAPIWorker registers a worker directly through the store and returns it as
// stored, so tests start from a known uid and version.
func seedAPIWorker(t *testing.T, ctx context.Context, persistence store.Interface, worker *ateapipb.Worker) *ateapipb.Worker {
	t.Helper()
	worker = proto.Clone(worker).(*ateapipb.Worker)
	if worker.GetStatus() == nil {
		worker.Status = &ateapipb.WorkerStatus{State: ateapipb.WorkerState_WORKER_STATE_ACTIVE}
	}
	created, err := persistence.CreateWorker(ctx, worker)
	if err != nil {
		t.Fatalf("seeding worker %s: %v", worker.GetMetadata().GetName(), err)
	}
	return created
}

// assignAPIWorker binds an Actor to a worker the way the resume workflow does:
// in-process, through the store. There is no AssignWorker RPC to go through.
func assignAPIWorker(t *testing.T, ctx context.Context, persistence store.Interface, name, actorUID string) *ateapipb.Worker {
	t.Helper()
	if err := persistence.BindActorToWorker(ctx, name, newAPIAssignment(actorUID), nil); err != nil {
		t.Fatalf("assigning worker %s: %v", name, err)
	}
	assigned, err := persistence.GetWorker(ctx, name)
	if err != nil {
		t.Fatalf("re-reading worker %s after assigning: %v", name, err)
	}
	return assigned
}

func workerRef(name string) *ateapipb.ObjectRef {
	return &ateapipb.ObjectRef{Name: name}
}

// updateFrom builds the body of an UpdateWorker request the way a client does:
// read the worker, change what it means to change, send the whole thing back.
// The metadata comes along as the uid and version guards every update requires,
// and so does everything else — an update replaces the stored worker, so an
// immutable field the request drops reads as a request to clear it.
func updateFrom(observed *ateapipb.Worker, mutate func(*ateapipb.Worker)) *ateapipb.Worker {
	worker := proto.Clone(observed).(*ateapipb.Worker)
	if mutate != nil {
		mutate(worker)
	}
	return worker
}

// The assignments are a subresource, so this RPC is the only way to read them.
// It also pins the identity the store gives each one.
func TestListWorkerAssignments(t *testing.T) {
	ctx := context.Background()
	svc, persistence := newWorkerAPIService(t)
	seedAPIWorker(t, ctx, persistence, validWorker(apiWorkerName))
	assignAPIWorker(t, ctx, persistence, apiWorkerName, "actor-uid-1")
	assignAPIWorker(t, ctx, persistence, apiWorkerName, "actor-uid-2")

	page, err := svc.ListWorkerActorAssignments(ctx, &ateapipb.ListWorkerActorAssignmentsRequest{Worker: workerRef(apiWorkerName)})
	if err != nil {
		t.Fatalf("ListWorkerActorAssignments() failed: %v", err)
	}
	var uids []string
	for _, a := range page.GetActorAssignments() {
		uids = append(uids, a.GetActorUid())
		if got := a.GetMetadata().GetName(); got != a.GetActorUid() {
			t.Errorf("assignment name = %q, want the Actor uid %q", got, a.GetActorUid())
		}
		if got := a.GetMetadata().GetAtespace(); got != "" {
			t.Errorf("assignment atespace = %q, want empty: Workers are global-scoped", got)
		}
		if a.GetMetadata().GetUid() == "" {
			t.Error("assignment uid is unset, want the one the store generated")
		}
	}
	sort.Strings(uids)
	if diff := cmp.Diff([]string{"actor-uid-1", "actor-uid-2"}, uids); diff != "" {
		t.Errorf("assignments mismatch (-want +got):\n%s", diff)
	}
}

// An absent Worker is NOT_FOUND rather than an empty page, which a caller
// cannot tell from a Worker hosting nothing.
func TestListWorkerAssignments_AbsentWorker(t *testing.T) {
	ctx := context.Background()
	svc, _ := newWorkerAPIService(t)

	_, err := svc.ListWorkerActorAssignments(ctx, &ateapipb.ListWorkerActorAssignmentsRequest{
		Worker: workerRef("3b9f1e77-2c4d-4a80-91be-6d5c8f0a7e21"),
	})
	if got := status.Code(err); got != codes.NotFound {
		t.Fatalf("code = %v (err %v), want %v", got, err, codes.NotFound)
	}
}

// Capacity is reported by the Worker, so it lives in status and a client
// cannot move it. An update that tries is ignored rather than refused, as it is
// for anything else a request carries in status.
func TestUpdateWorker_CannotChangeCapacity(t *testing.T) {
	ctx := context.Background()
	svc, persistence := newWorkerAPIService(t)
	seeded := seedAPIWorker(t, ctx, persistence, validWorker(apiWorkerName))
	before := seeded.GetStatus().GetCapacity()

	got, err := svc.UpdateWorker(ctx, &ateapipb.UpdateWorkerRequest{
		Worker: updateFrom(seeded, func(w *ateapipb.Worker) {
			w.Status.Capacity = &ateapipb.WorkerResources{Actors: 4094}
		}),
	})
	if err != nil {
		t.Fatalf("UpdateWorker() failed: %v", err)
	}
	if diff := cmp.Diff(before, got.GetStatus().GetCapacity(), protocmp.Transform()); diff != "" {
		t.Errorf("a client update moved capacity (-want +got):\n%s", diff)
	}
}

func TestGetWorker_Errors(t *testing.T) {
	ctx := context.Background()
	svc, persistence := newWorkerAPIService(t)
	seedAPIWorker(t, ctx, persistence, validWorker(apiWorkerName))

	tests := []struct {
		name string
		req  *ateapipb.GetWorkerRequest
		want codes.Code
	}{
		{"absent", &ateapipb.GetWorkerRequest{Worker: workerRef(apiOtherWorkerName)}, codes.NotFound},
		{"no ref", &ateapipb.GetWorkerRequest{}, codes.InvalidArgument},
		{"no name", &ateapipb.GetWorkerRequest{Worker: &ateapipb.ObjectRef{}}, codes.InvalidArgument},
		// Workers are global-scoped, so naming an atespace is a client bug
		// rather than a lookup that happens to miss.
		{"atespace set", &ateapipb.GetWorkerRequest{Worker: &ateapipb.ObjectRef{Atespace: "team-a", Name: apiWorkerName}}, codes.InvalidArgument},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := svc.GetWorker(ctx, tc.req)
			if got := status.Code(err); got != tc.want {
				t.Errorf("GetWorker() code = %v (err %v), want %v", got, err, tc.want)
			}
		})
	}
}

func TestCreateWorker(t *testing.T) {
	ctx := context.Background()
	svc, persistence := newWorkerAPIService(t)

	got, err := svc.CreateWorker(ctx, &ateapipb.CreateWorkerRequest{Worker: validWorker(apiWorkerName)})
	if err != nil {
		t.Fatalf("CreateWorker() failed: %v", err)
	}
	if got.GetMetadata().GetVersion() != 1 {
		t.Errorf("created worker version = %d, want 1", got.GetMetadata().GetVersion())
	}
	if got.GetMetadata().GetUid() == "" {
		t.Error("created worker has no uid; the store is meant to assign one")
	}
	// A Worker is registered only once its pod is Ready and has an IP, which
	// makes ACTIVE the only state it can be born in.
	if got.GetStatus().GetState() != ateapipb.WorkerState_WORKER_STATE_ACTIVE {
		t.Errorf("created worker state = %v, want %v", got.GetStatus().GetState(), ateapipb.WorkerState_WORKER_STATE_ACTIVE)
	}

	stored, err := persistence.GetWorker(ctx, apiWorkerName)
	if err != nil {
		t.Fatalf("GetWorker() failed: %v", err)
	}
	if diff := cmp.Diff(stored, got, protocmp.Transform()); diff != "" {
		t.Errorf("CreateWorker() returned something other than what it stored (-stored +returned):\n%s", diff)
	}
}

// status is output-only, so a request that carries one has it replaced rather
// than rejected.
func TestCreateWorker_IgnoresRequestStatus(t *testing.T) {
	ctx := context.Background()
	svc, _ := newWorkerAPIService(t)

	in := validWorker(apiWorkerName)
	in.Status = &ateapipb.WorkerStatus{State: ateapipb.WorkerState_WORKER_STATE_DRAINING, Allocated: &ateapipb.WorkerResources{Actors: 9}}

	got, err := svc.CreateWorker(ctx, &ateapipb.CreateWorkerRequest{Worker: in})
	if err != nil {
		t.Fatalf("CreateWorker() failed: %v", err)
	}
	want := &ateapipb.WorkerStatus{State: ateapipb.WorkerState_WORKER_STATE_ACTIVE}
	if diff := cmp.Diff(want, got.GetStatus(), protocmp.Transform()); diff != "" {
		t.Errorf("created worker status mismatch (-want +got):\n%s", diff)
	}
}

func TestCreateWorker_AlreadyExists(t *testing.T) {
	ctx := context.Background()
	svc, persistence := newWorkerAPIService(t)
	seedAPIWorker(t, ctx, persistence, validWorker(apiWorkerName))

	_, err := svc.CreateWorker(ctx, &ateapipb.CreateWorkerRequest{Worker: validWorker(apiWorkerName)})
	if got := status.Code(err); got != codes.AlreadyExists {
		t.Errorf("CreateWorker() code = %v (err %v), want %v", got, err, codes.AlreadyExists)
	}
}

func TestCreateWorker_InvalidArgument(t *testing.T) {
	ctx := context.Background()
	svc, _ := newWorkerAPIService(t)

	tests := []struct {
		name   string
		mutate func(*ateapipb.Worker) // nil sends no worker at all
	}{
		{name: "no worker"},
		{name: "no name", mutate: func(w *ateapipb.Worker) { w.Metadata = &ateapipb.ResourceMetadata{} }},
		{name: "atespace set", mutate: func(w *ateapipb.Worker) { w.Metadata.Atespace = "team-a" }},
		{name: "no ip", mutate: func(w *ateapipb.Worker) { w.Ip = "" }},
		{name: "bad ip", mutate: func(w *ateapipb.Worker) { w.Ip = "not-an-ip" }},
		{name: "no node", mutate: func(w *ateapipb.Worker) { w.NodeName = "" }},
		{name: "no pool", mutate: func(w *ateapipb.Worker) { w.WorkerPool = "" }},
		{name: "no pod", mutate: func(w *ateapipb.Worker) { w.WorkerPod = "" }},
		{name: "pod uid not a uuid", mutate: func(w *ateapipb.Worker) { w.WorkerPodUid = "not-a-uuid" }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := &ateapipb.CreateWorkerRequest{}
			if tc.mutate != nil {
				worker := validWorker(apiWorkerName)
				tc.mutate(worker)
				req.Worker = worker
			}
			_, err := svc.CreateWorker(ctx, req)
			if got := status.Code(err); got != codes.InvalidArgument {
				t.Errorf("CreateWorker() code = %v (err %v), want %v", got, err, codes.InvalidArgument)
			}
		})
	}
}

func TestUpdateWorker(t *testing.T) {
	ctx := context.Background()
	svc, persistence := newWorkerAPIService(t)
	seeded := seedAPIWorker(t, ctx, persistence, validWorker(apiWorkerName))

	got, err := svc.UpdateWorker(ctx, &ateapipb.UpdateWorkerRequest{
		Worker: updateFrom(seeded, func(w *ateapipb.Worker) {
			w.Labels = map[string]string{"tier": "batch"}
		}),
	})
	if err != nil {
		t.Fatalf("UpdateWorker() failed: %v", err)
	}

	want := proto.Clone(seeded).(*ateapipb.Worker)
	want.Labels = map[string]string{"tier": "batch"}
	want.Metadata = got.GetMetadata()
	if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
		t.Errorf("UpdateWorker() mismatch (-want +got):\n%s", diff)
	}
	if got.GetMetadata().GetVersion() != 2 {
		t.Errorf("updated worker version = %d, want 2", got.GetMetadata().GetVersion())
	}
}

// Update replaces rather than patches, so a mutable field the request leaves
// unset is cleared. Immutable fields are the exception: dropping one of those
// is an error rather than a clear, which TestUpdateWorker_Errors covers.
func TestUpdateWorker_OmittedMutableFieldIsCleared(t *testing.T) {
	ctx := context.Background()
	svc, persistence := newWorkerAPIService(t)
	labelled := validWorker(apiWorkerName)
	labelled.Labels = map[string]string{"tier": "batch"}
	seeded := seedAPIWorker(t, ctx, persistence, labelled)

	got, err := svc.UpdateWorker(ctx, &ateapipb.UpdateWorkerRequest{
		Worker: updateFrom(seeded, func(w *ateapipb.Worker) {
			w.Labels = nil
		}),
	})
	if err != nil {
		t.Fatalf("UpdateWorker() failed: %v", err)
	}
	if len(got.GetLabels()) != 0 {
		t.Errorf("labels = %v, want them cleared: the request carried none", got.GetLabels())
	}
}

// status is output-only, so the server keeps its own no matter what the request
// carries. That is what protects the in-process Actor binding, which lives
// under status and is written by the actor workflows rather than over the API.
func TestUpdateWorker_LeavesStatusAlone(t *testing.T) {
	ctx := context.Background()
	svc, persistence := newWorkerAPIService(t)
	seedAPIWorker(t, ctx, persistence, validWorker(apiWorkerName))
	assigned := assignAPIWorker(t, ctx, persistence, apiWorkerName, "actor-uid-1")

	got, err := svc.UpdateWorker(ctx, &ateapipb.UpdateWorkerRequest{
		Worker: updateFrom(assigned, func(w *ateapipb.Worker) {
			w.Labels = map[string]string{"tier": "batch"}
			// A forged status: drained, and with the Actor released out from
			// under the workflow that bound it. Neither may land.
			w.Status = &ateapipb.WorkerStatus{State: ateapipb.WorkerState_WORKER_STATE_DRAINING}
		}),
	})
	if err != nil {
		t.Fatalf("UpdateWorker() failed: %v", err)
	}
	if diff := cmp.Diff(assigned.GetStatus(), got.GetStatus(), protocmp.Transform()); diff != "" {
		t.Errorf("UpdateWorker() disturbed status (-want +got):\n%s", diff)
	}
}

func TestUpdateWorker_Preconditions(t *testing.T) {
	ctx := context.Background()
	svc, persistence := newWorkerAPIService(t)
	seeded := seedAPIWorker(t, ctx, persistence, validWorker(apiWorkerName))

	update := func(bend func(*ateapipb.ResourceMetadata)) error {
		_, err := svc.UpdateWorker(ctx, &ateapipb.UpdateWorkerRequest{
			Worker: updateFrom(seeded, func(w *ateapipb.Worker) {
				w.Labels = map[string]string{"tier": "batch"}
				bend(w.Metadata)
			}),
		})
		return err
	}

	t.Run("stale version", func(t *testing.T) {
		err := update(func(md *ateapipb.ResourceMetadata) { md.Version += 7 })
		if got := status.Code(err); got != codes.Aborted {
			t.Errorf("UpdateWorker() code = %v, want %v", got, codes.Aborted)
		}
	})

	t.Run("foreign uid", func(t *testing.T) {
		err := update(func(md *ateapipb.ResourceMetadata) { md.Uid = apiOtherWorkerName })
		if got := status.Code(err); got != codes.Aborted {
			t.Errorf("UpdateWorker() code = %v, want %v", got, codes.Aborted)
		}
	})

	// Both guards are required: an update that pins neither is a blind write,
	// which is rejected before it reaches the store.
	t.Run("missing uid", func(t *testing.T) {
		err := update(func(md *ateapipb.ResourceMetadata) { md.Uid = "" })
		if got := status.Code(err); got != codes.InvalidArgument {
			t.Errorf("UpdateWorker() code = %v, want %v", got, codes.InvalidArgument)
		}
	})

	t.Run("missing version", func(t *testing.T) {
		err := update(func(md *ateapipb.ResourceMetadata) { md.Version = 0 })
		if got := status.Code(err); got != codes.InvalidArgument {
			t.Errorf("UpdateWorker() code = %v, want %v", got, codes.InvalidArgument)
		}
	})

	t.Run("matching", func(t *testing.T) {
		if err := update(func(*ateapipb.ResourceMetadata) {}); err != nil {
			t.Errorf("UpdateWorker() with matching preconditions failed: %v", err)
		}
	})
}

func TestUpdateWorker_Errors(t *testing.T) {
	ctx := context.Background()
	svc, persistence := newWorkerAPIService(t)
	seeded := seedAPIWorker(t, ctx, persistence, validWorker(apiWorkerName))

	// Every case below carries the guards an update requires and the worker as
	// stored, so the rule it is named for is the one that rejects it.
	tests := []struct {
		name   string
		mutate func(*ateapipb.Worker) // nil sends no worker at all
		want   codes.Code
	}{
		{"no worker", nil, codes.InvalidArgument},
		{"atespace set", func(w *ateapipb.Worker) { w.Metadata.Atespace = "team-a" }, codes.InvalidArgument},
		{"absent", func(w *ateapipb.Worker) {
			w.Metadata.Name = "9d1f7b06-3c58-4a2e-8b40-5f7c1e9a2d63"
		}, codes.NotFound},
		// Immutable fields, changed. A replacement update carries the whole
		// worker, so these are the cases where it carries a different one.
		{"ip changed", func(w *ateapipb.Worker) { w.Ip = "10.9.9.9" }, codes.InvalidArgument},
		{"worker_pod changed", func(w *ateapipb.Worker) { w.WorkerPod = "worker-pod-2" }, codes.InvalidArgument},
		{"node_name changed", func(w *ateapipb.Worker) { w.NodeName = "node-2" }, codes.InvalidArgument},
		{"sandbox_class changed", func(w *ateapipb.Worker) { w.SandboxClass = "microvm" }, codes.InvalidArgument},
		// capacity is deliberately absent here: it may change (the pool's actor
		// ceiling moves, a pod can be resized, a Worker may report its own).
		// TestUpdateWorker_CapacityChanges covers that.
		//
		// And immutable fields dropped, which a replacement update reads as a
		// request to clear them. Rejected rather than silently applied.
		{"ip omitted", func(w *ateapipb.Worker) { w.Ip = "" }, codes.InvalidArgument},
		{"sandbox_class omitted", func(w *ateapipb.Worker) { w.SandboxClass = "" }, codes.InvalidArgument},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := &ateapipb.UpdateWorkerRequest{}
			if tc.mutate != nil {
				req.Worker = updateFrom(seeded, tc.mutate)
			}
			_, err := svc.UpdateWorker(ctx, req)
			if got := status.Code(err); got != tc.want {
				t.Errorf("UpdateWorker() code = %v (err %v), want %v", got, err, tc.want)
			}
		})
	}
}

// A draining worker can still have its labels updated; only its status is
// frozen.
func TestUpdateWorker_DrainingWorkerKeepsLabelsMutable(t *testing.T) {
	ctx := context.Background()
	svc, persistence := newWorkerAPIService(t)
	seedAPIWorker(t, ctx, persistence, validWorker(apiWorkerName))
	drained, err := svc.DrainWorker(ctx, &ateapipb.DrainWorkerRequest{Worker: workerRef(apiWorkerName)})
	if err != nil {
		t.Fatalf("DrainWorker() failed: %v", err)
	}

	got, err := svc.UpdateWorker(ctx, &ateapipb.UpdateWorkerRequest{
		Worker: updateFrom(drained, func(w *ateapipb.Worker) { w.Labels = map[string]string{"tier": "batch"} }),
	})
	if err != nil {
		t.Fatalf("UpdateWorker() failed: %v", err)
	}
	if got.GetLabels()["tier"] != "batch" {
		t.Errorf("labels = %v, want tier=batch", got.GetLabels())
	}
	if got.GetStatus().GetState() != ateapipb.WorkerState_WORKER_STATE_DRAINING {
		t.Errorf("state = %v, want it still %v", got.GetStatus().GetState(), ateapipb.WorkerState_WORKER_STATE_DRAINING)
	}
}

func TestDeleteWorker(t *testing.T) {
	ctx := context.Background()
	svc, persistence := newWorkerAPIService(t)
	seeded := seedAPIWorker(t, ctx, persistence, validWorker(apiWorkerName))

	got, err := svc.DeleteWorker(ctx, &ateapipb.DeleteWorkerRequest{Worker: workerRef(apiWorkerName)})
	if err != nil {
		t.Fatalf("DeleteWorker() failed: %v", err)
	}
	// Delete admits and drains in one write before it sweeps, so it removes one
	// revision past what was seeded.
	if got.GetMetadata().GetDeleteTime() == nil {
		t.Error("DeleteWorker() returned a worker without delete_time")
	}
	want := proto.Clone(seeded).(*ateapipb.Worker)
	want.Metadata.Version = seeded.GetMetadata().GetVersion() + 1
	want.Metadata.UpdateTime = got.GetMetadata().GetUpdateTime()
	want.Metadata.DeleteTime = got.GetMetadata().GetDeleteTime()
	want.Status.State = ateapipb.WorkerState_WORKER_STATE_DRAINING
	if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
		t.Errorf("DeleteWorker() returned something other than the worker it removed (-want +got):\n%s", diff)
	}
	if _, err := persistence.GetWorker(ctx, apiWorkerName); err == nil {
		t.Error("worker still readable after DeleteWorker")
	}
}

// Delete reports absence rather than succeeding silently. Callers that want
// idempotence, like the worker-pod syncer, opt into it by treating NOT_FOUND as
// success.
func TestDeleteWorker_Absent(t *testing.T) {
	ctx := context.Background()
	svc, _ := newWorkerAPIService(t)

	_, err := svc.DeleteWorker(ctx, &ateapipb.DeleteWorkerRequest{Worker: workerRef(apiWorkerName)})
	if got := status.Code(err); got != codes.NotFound {
		t.Errorf("DeleteWorker() code = %v (err %v), want %v", got, err, codes.NotFound)
	}
}

// An assigned worker deletes like any other: the delete does not cascade, and
// an Actor pointing at a Worker that is gone is an expected steady state.
func TestDeleteWorker_AssignedWorkerDeletesAnyway(t *testing.T) {
	ctx := context.Background()
	svc, persistence := newWorkerAPIService(t)
	seedAPIWorker(t, ctx, persistence, validWorker(apiWorkerName))
	assignAPIWorker(t, ctx, persistence, apiWorkerName, "actor-uid-1")

	got, err := svc.DeleteWorker(ctx, &ateapipb.DeleteWorkerRequest{Worker: workerRef(apiWorkerName)})
	if err != nil {
		t.Fatalf("DeleteWorker() failed: %v", err)
	}
	// The record carries the total, not the assignments themselves.
	if n := got.GetStatus().GetAllocated().GetActors(); n != 1 {
		t.Errorf("deleted worker hosted %d actors, want the 1 it was holding", n)
	}
}

func TestDeleteWorker_Preconditions(t *testing.T) {
	ctx := context.Background()
	svc, persistence := newWorkerAPIService(t)
	seeded := seedAPIWorker(t, ctx, persistence, validWorker(apiWorkerName))

	t.Run("stale version", func(t *testing.T) {
		_, err := svc.DeleteWorker(ctx, &ateapipb.DeleteWorkerRequest{
			Worker:  workerRef(apiWorkerName),
			Options: &ateapipb.DeleteOptions{Version: seeded.GetMetadata().GetVersion() + 7},
		})
		if got := status.Code(err); got != codes.Aborted {
			t.Errorf("DeleteWorker() code = %v (err %v), want %v", got, err, codes.Aborted)
		}
	})

	t.Run("foreign uid", func(t *testing.T) {
		_, err := svc.DeleteWorker(ctx, &ateapipb.DeleteWorkerRequest{
			Worker:  workerRef(apiWorkerName),
			Options: &ateapipb.DeleteOptions{Uid: apiOtherWorkerName},
		})
		if got := status.Code(err); got != codes.Aborted {
			t.Errorf("DeleteWorker() code = %v (err %v), want %v", got, err, codes.Aborted)
		}
	})

	// A refused delete must leave the worker where it was.
	if _, err := persistence.GetWorker(ctx, apiWorkerName); err != nil {
		t.Fatalf("worker gone after two refused deletes: %v", err)
	}

	t.Run("matching", func(t *testing.T) {
		if _, err := svc.DeleteWorker(ctx, &ateapipb.DeleteWorkerRequest{
			Worker: workerRef(apiWorkerName),
			Options: &ateapipb.DeleteOptions{
				Uid:     seeded.GetMetadata().GetUid(),
				Version: seeded.GetMetadata().GetVersion(),
			},
		}); err != nil {
			t.Errorf("DeleteWorker() with matching preconditions failed: %v", err)
		}
	})
}

func TestDrainWorker(t *testing.T) {
	ctx := context.Background()
	svc, persistence := newWorkerAPIService(t)
	seedAPIWorker(t, ctx, persistence, validWorker(apiWorkerName))

	got, err := svc.DrainWorker(ctx, &ateapipb.DrainWorkerRequest{Worker: workerRef(apiWorkerName)})
	if err != nil {
		t.Fatalf("DrainWorker() failed: %v", err)
	}
	if got.GetStatus().GetState() != ateapipb.WorkerState_WORKER_STATE_DRAINING {
		t.Errorf("state = %v, want %v", got.GetStatus().GetState(), ateapipb.WorkerState_WORKER_STATE_DRAINING)
	}
	if got.GetMetadata().GetVersion() != 2 {
		t.Errorf("version = %d, want 2", got.GetMetadata().GetVersion())
	}

	// Draining again is a no-op, and specifically must not bump the version:
	// callers re-drive drain on every pod event.
	again, err := svc.DrainWorker(ctx, &ateapipb.DrainWorkerRequest{Worker: workerRef(apiWorkerName)})
	if err != nil {
		t.Fatalf("second DrainWorker() failed: %v", err)
	}
	if diff := cmp.Diff(got, again, protocmp.Transform()); diff != "" {
		t.Errorf("second DrainWorker() changed the worker (-first +second):\n%s", diff)
	}
}

// Drain deliberately leaves the bound Actor alone: it stops the scheduler
// routing new Actors here, it does not evict the one already running.
func TestDrainWorker_KeepsAssignment(t *testing.T) {
	ctx := context.Background()
	svc, persistence := newWorkerAPIService(t)
	seedAPIWorker(t, ctx, persistence, validWorker(apiWorkerName))
	assignAPIWorker(t, ctx, persistence, apiWorkerName, "actor-uid-1")

	got, err := svc.DrainWorker(ctx, &ateapipb.DrainWorkerRequest{Worker: workerRef(apiWorkerName)})
	if err != nil {
		t.Fatalf("DrainWorker() failed: %v", err)
	}
	if n := got.GetStatus().GetAllocated().GetActors(); n != 1 {
		t.Errorf("drained worker hosts %d actors, want the 1 left in place", n)
	}
}

func TestDrainWorker_Errors(t *testing.T) {
	ctx := context.Background()
	svc, _ := newWorkerAPIService(t)

	tests := []struct {
		name string
		req  *ateapipb.DrainWorkerRequest
		want codes.Code
	}{
		{"absent", &ateapipb.DrainWorkerRequest{Worker: workerRef(apiWorkerName)}, codes.NotFound},
		{"no ref", &ateapipb.DrainWorkerRequest{}, codes.InvalidArgument},
		{"no name", &ateapipb.DrainWorkerRequest{Worker: &ateapipb.ObjectRef{}}, codes.InvalidArgument},
		{"atespace set", &ateapipb.DrainWorkerRequest{Worker: &ateapipb.ObjectRef{Atespace: "team-a", Name: apiWorkerName}}, codes.InvalidArgument},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := svc.DrainWorker(ctx, tc.req)
			if got := status.Code(err); got != tc.want {
				t.Errorf("DrainWorker() code = %v (err %v), want %v", got, err, tc.want)
			}
		})
	}
}

// TestServiceImplUpdateWorker_ImmutableFields pins the immutable-field rule at
// the layer that now owns it: declarative validation in ServiceImpl, which
// every write path shares. It moved up from the store contract when the store
// stopped enforcing immutability itself.
func TestServiceImplUpdateWorker_ImmutableFields(t *testing.T) {
	ctx := context.Background()
	persistence, cleanup := storetest.SetupTestStore(t)
	defer cleanup()
	impl := newServiceImpl(persistence, nil)

	// Every case below is rejected, so nothing writes and this stays the
	// current incarnation for all of them.
	created := seedAPIWorker(t, ctx, persistence, validWorker(apiWorkerName))

	for _, tc := range []struct {
		name   string
		field  string
		mutate func(*ateapipb.Worker)
	}{
		{"worker_namespace", "worker_namespace", func(w *ateapipb.Worker) { w.WorkerNamespace = "other-ns" }},
		{"worker_pool", "worker_pool", func(w *ateapipb.Worker) { w.WorkerPool = "other-pool" }},
		{"worker_pod", "worker_pod", func(w *ateapipb.Worker) { w.WorkerPod = "other-pod" }},
		{"worker_pod_uid", "worker_pod_uid", func(w *ateapipb.Worker) { w.WorkerPodUid = apiOtherWorkerName }},
		{"node_name", "node_name", func(w *ateapipb.Worker) { w.NodeName = "other-node" }},
		{"ip", "ip", func(w *ateapipb.Worker) { w.Ip = "10.0.0.9" }},
		// capacity is absent: it is reported into status, which a client
		// cannot write. See TestUpdateWorker_CannotChangeCapacity.
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := impl.UpdateWorker(ctx, apiWorkerName, store.PreconditionFrom(created), func(toUpdate *ateapipb.Worker) error {
				tc.mutate(toUpdate)
				return nil
			})
			if got := status.Code(err); got != codes.InvalidArgument {
				t.Fatalf("changing %s returned %v (err %v), want %v", tc.field, got, err, codes.InvalidArgument)
			}
			if !strings.Contains(err.Error(), tc.field) {
				t.Errorf("error %v does not name the offending field %s", err, tc.field)
			}
			got, err := persistence.GetWorker(ctx, apiWorkerName)
			if err != nil {
				t.Fatalf("GetWorker failed: %v", err)
			}
			if got.GetMetadata().GetVersion() != 1 {
				t.Errorf("rejected mutation bumped the version to %d, want 1", got.GetMetadata().GetVersion())
			}
		})
	}
}

// Server-assigned metadata carried on a create request is scrubbed rather than
// rejected: the fields are documented as ignored on input, so even garbage in
// them must not fail validation.
func TestCreateWorker_IgnoresRequestMetadataServerFields(t *testing.T) {
	ctx := context.Background()
	svc, _ := newWorkerAPIService(t)

	in := validWorker(apiWorkerName)
	in.Metadata.Uid = "not-a-uuid"
	in.Metadata.Version = -5

	got, err := svc.CreateWorker(ctx, &ateapipb.CreateWorkerRequest{Worker: in})
	if err != nil {
		t.Fatalf("CreateWorker() failed: %v", err)
	}
	if got.GetMetadata().GetUid() == "" || got.GetMetadata().GetUid() == "not-a-uuid" {
		t.Errorf("created worker uid = %q, want a server-assigned uid", got.GetMetadata().GetUid())
	}
	if got.GetMetadata().GetVersion() != 1 {
		t.Errorf("created worker version = %d, want 1", got.GetMetadata().GetVersion())
	}
}

// Every stored Worker carries an actor ceiling, so no reader has to know a
// default. A Worker that reports its own keeps it; one that does not is worth
// one Actor, which is what a Worker was before it could report.
func TestCreateWorker_HoldsNoCapacityUntilReported(t *testing.T) {
	ctx := context.Background()
	svc, _ := newWorkerAPIService(t)

	got, err := svc.CreateWorker(ctx, &ateapipb.CreateWorkerRequest{Worker: validWorker(apiWorkerName)})
	if err != nil {
		t.Fatalf("CreateWorker() failed: %v", err)
	}
	if capacity := got.GetStatus().GetCapacity(); capacity != nil {
		t.Errorf("created worker capacity = %v, want none until its ateom reports", capacity)
	}

	// Capacity is status, so a request cannot bring its own: a Worker only
	// gets one by reporting it.
	carried := validWorker("11111111-2222-3333-4444-555555555555")
	carried.Status = &ateapipb.WorkerStatus{Capacity: &ateapipb.WorkerResources{Actors: 4094}}
	got, err = svc.CreateWorker(ctx, &ateapipb.CreateWorkerRequest{Worker: carried})
	if err != nil {
		t.Fatalf("CreateWorker() carrying a capacity failed: %v", err)
	}
	if capacity := got.GetStatus().GetCapacity(); capacity != nil {
		t.Errorf("a request carrying a ceiling set capacity to %v, want none", capacity)
	}
}
