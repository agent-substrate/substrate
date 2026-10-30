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

package harness

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.ate.dev/substrate/internal/proto/ateletpb"
	"go.ate.dev/substrate/internal/resources"
)

func TestAtelet(t *testing.T) {
	const (
		uid          = "0f0e0d0c-0b0a-4908-8706-050403020100"
		worker       = "worker-uid"
		snapshotUUID = "9c2f7b41-6d05-4e83-a1f7-3b8c0d5e2a94"
		epoch        = 3
	)
	uri, err := resources.NewActorSnapshotURI(StorageLocation, "ns", uid, snapshotUUID)
	if err != nil {
		t.Fatalf("NewActorSnapshotURI: %v", err)
	}
	snapshotURI := uri.String()
	snapshot := &ateletpb.Snapshot{
		SnapshotUuid: snapshotUUID,
		Generation:   1,
		Fidelity:     ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
		Object:       &ateletpb.ObjectStorage{SnapshotUri: snapshotURI},
	}
	newAteletWithWorker := func(store *ObjectStore) *Atelet {
		a := newAtelet(store)
		a.registerWorker(worker, epoch)
		return a
	}
	run := &ateletpb.RunRequest{WorkerPodUid: worker, ActorUid: uid, Atespace: "ns", ActorName: "actor-1"}
	checkpoint := func(mode ateletpb.UploadMode) *ateletpb.CheckpointRequest {
		return &ateletpb.CheckpointRequest{
			WorkerPodUid: worker, ActorUid: uid, Atespace: "ns", ActorName: "actor-1",
			Snapshot:     snapshot,
			UploadConfig: &ateletpb.UploadConfig{Mode: mode},
		}
	}
	deferred := checkpoint(ateletpb.UploadMode_UPLOAD_MODE_DEFERRED)
	immediate := checkpoint(ateletpb.UploadMode_UPLOAD_MODE_IMMEDIATE)
	restore := &ateletpb.RestoreRequest{WorkerPodUid: worker, ActorUid: uid, Atespace: "ns", ActorName: "actor-1", Snapshot: snapshot}
	running := []Sandbox{{ActorUID: uid, Atespace: "ns", ActorName: "actor-1", WorkerUID: worker}}
	assertSandboxes := func(t *testing.T, a *Atelet, want []Sandbox) {
		t.Helper()
		if diff := cmp.Diff(want, a.Sandboxes(), cmpopts.EquateEmpty()); diff != "" {
			t.Errorf("sandboxes (-want +got):\n%s", diff)
		}
	}
	assertUploaded := func(t *testing.T, store *ObjectStore, want []string) {
		t.Helper()
		got, err := store.Snapshot(snapshotURI)
		if err != nil {
			t.Fatalf("Snapshot: %v", err)
		}
		if diff := cmp.Diff(want, got, cmpopts.EquateEmpty()); diff != "" {
			t.Errorf("objects of %s (-want +got):\n%s", snapshotURI, diff)
		}
	}

	t.Run("refuses to run on a worker that did not register", func(t *testing.T) {
		a := newAtelet(newObjectStore())
		if _, err := a.Run(t.Context(), run); err == nil {
			t.Fatal("Run succeeded on a worker that is not on the node")
		}
		assertSandboxes(t, a, nil)
	})

	t.Run("run returns the worker's epoch", func(t *testing.T) {
		a := newAteletWithWorker(newObjectStore())
		resp, err := a.Run(t.Context(), run)
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
		if got := resp.GetWorkerEpoch(); got != epoch {
			t.Errorf("Run epoch = %d, want %d", got, epoch)
		}
		assertSandboxes(t, a, running)
	})

	t.Run("refuses to checkpoint an actor that is not running", func(t *testing.T) {
		a := newAteletWithWorker(newObjectStore())
		if _, err := a.Checkpoint(t.Context(), deferred); err == nil {
			t.Fatal("Checkpoint succeeded with no sandbox running")
		}
	})

	t.Run("refuses to checkpoint an actor running on another worker", func(t *testing.T) {
		a := newAteletWithWorker(newObjectStore())
		if _, err := a.Run(t.Context(), run); err != nil {
			t.Fatalf("Run: %v", err)
		}
		elsewhere := checkpoint(ateletpb.UploadMode_UPLOAD_MODE_DEFERRED)
		elsewhere.WorkerPodUid = "other-worker"
		if _, err := a.Checkpoint(t.Context(), elsewhere); err == nil {
			t.Fatal("Checkpoint succeeded on a worker the actor does not run on")
		}
		assertSandboxes(t, a, running)
	})

	t.Run("deferred checkpoint keeps the snapshot local and restores from it", func(t *testing.T) {
		store := newObjectStore()
		a := newAteletWithWorker(store)
		if _, err := a.Run(t.Context(), run); err != nil {
			t.Fatalf("Run: %v", err)
		}
		if _, err := a.Checkpoint(t.Context(), deferred); err != nil {
			t.Fatalf("Checkpoint: %v", err)
		}
		assertSandboxes(t, a, nil)
		assertUploaded(t, store, nil)
		if got := a.LocalSnapshot(uid); got != snapshotUUID {
			t.Errorf("LocalSnapshot after Checkpoint = %q, want %q", got, snapshotUUID)
		}
		resp, err := a.Restore(t.Context(), restore)
		if err != nil {
			t.Fatalf("Restore: %v", err)
		}
		if got := resp.GetWorkerEpoch(); got != epoch {
			t.Errorf("Restore epoch = %d, want %d", got, epoch)
		}
		assertSandboxes(t, a, running)
	})

	t.Run("immediate checkpoint uploads the snapshot", func(t *testing.T) {
		store := newObjectStore()
		a := newAteletWithWorker(store)
		if _, err := a.Run(t.Context(), run); err != nil {
			t.Fatalf("Run: %v", err)
		}
		resp, err := a.Checkpoint(t.Context(), immediate)
		if err != nil {
			t.Fatalf("Checkpoint: %v", err)
		}
		if got := resp.GetState().GetLatestDurable().GetSnapshot().GetSnapshotUuid(); got != snapshotUUID {
			t.Errorf("Checkpoint latest durable = %q, want %q", got, snapshotUUID)
		}
		assertUploaded(t, store, snapshotObjects)
	})

	t.Run("a retried checkpoint is answered from the snapshot it took", func(t *testing.T) {
		store := newObjectStore()
		a := newAteletWithWorker(store)
		if _, err := a.Run(t.Context(), run); err != nil {
			t.Fatalf("Run: %v", err)
		}
		if _, err := a.Checkpoint(t.Context(), deferred); err != nil {
			t.Fatalf("Checkpoint: %v", err)
		}
		resp, err := a.Checkpoint(t.Context(), immediate)
		if err != nil {
			t.Fatalf("retried Checkpoint: %v", err)
		}
		if got := resp.GetState().GetLatestDurable().GetSnapshot().GetSnapshotUuid(); got != snapshotUUID {
			t.Errorf("retried Checkpoint latest durable = %q, want %q", got, snapshotUUID)
		}
		assertUploaded(t, store, snapshotObjects)
		assertSandboxes(t, a, nil)
	})

	t.Run("restores from object storage without a local copy", func(t *testing.T) {
		store := newObjectStore()
		a := newAteletWithWorker(store)
		if _, err := a.Run(t.Context(), run); err != nil {
			t.Fatalf("Run: %v", err)
		}
		if _, err := a.Checkpoint(t.Context(), immediate); err != nil {
			t.Fatalf("Checkpoint: %v", err)
		}
		other := newAteletWithWorker(store)
		if _, err := other.Restore(t.Context(), restore); err != nil {
			t.Fatalf("Restore: %v", err)
		}
		assertSandboxes(t, other, running)
	})

	t.Run("refuses to restore a snapshot neither local nor in object storage", func(t *testing.T) {
		a := newAteletWithWorker(newObjectStore())
		if _, err := a.Restore(t.Context(), restore); status.Code(err) != codes.NotFound {
			t.Fatalf("Restore from a snapshot that is nowhere = %v, want NotFound", err)
		}
		assertSandboxes(t, a, nil)
	})

	t.Run("promote uploads the local snapshot and keeps it", func(t *testing.T) {
		store := newObjectStore()
		a := newAteletWithWorker(store)
		if _, err := a.Run(t.Context(), run); err != nil {
			t.Fatalf("Run: %v", err)
		}
		if _, err := a.Checkpoint(t.Context(), deferred); err != nil {
			t.Fatalf("Checkpoint: %v", err)
		}
		if _, err := a.PromoteSnapshot(t.Context(), &ateletpb.PromoteSnapshotRequest{ActorUid: uid, Atespace: "ns", ActorName: "actor-1", Snapshot: snapshot}); err != nil {
			t.Fatalf("PromoteSnapshot: %v", err)
		}
		assertUploaded(t, store, snapshotObjects)
		if got := a.LocalSnapshot(uid); got != snapshotUUID {
			t.Errorf("LocalSnapshot after PromoteSnapshot = %q, want %q", got, snapshotUUID)
		}
	})

	t.Run("refuses to promote a snapshot it neither holds nor finds durable", func(t *testing.T) {
		store := newObjectStore()
		a := newAteletWithWorker(store)
		_, err := a.PromoteSnapshot(t.Context(), &ateletpb.PromoteSnapshotRequest{ActorUid: uid, Atespace: "ns", ActorName: "actor-1", Snapshot: snapshot})
		if status.Code(err) != codes.NotFound {
			t.Fatalf("PromoteSnapshot = %v, want NotFound", err)
		}
		assertUploaded(t, store, nil)
	})

	t.Run("detach uploads the local snapshot and drops it", func(t *testing.T) {
		store := newObjectStore()
		a := newAteletWithWorker(store)
		if _, err := a.Run(t.Context(), run); err != nil {
			t.Fatalf("Run: %v", err)
		}
		if _, err := a.Checkpoint(t.Context(), deferred); err != nil {
			t.Fatalf("Checkpoint: %v", err)
		}
		resp, err := a.DetachActor(t.Context(), &ateletpb.DetachActorRequest{ActorUid: uid, Atespace: "ns", ActorName: "actor-1"})
		if err != nil {
			t.Fatalf("DetachActor: %v", err)
		}
		if got := resp.GetState().GetLatestDurable().GetSnapshot().GetSnapshotUuid(); got != snapshotUUID {
			t.Errorf("DetachActor latest durable = %q, want %q", got, snapshotUUID)
		}
		assertUploaded(t, store, snapshotObjects)
		if got := a.LocalSnapshot(uid); got != "" {
			t.Errorf("LocalSnapshot after DetachActor = %q, want none", got)
		}
	})

	t.Run("refuses to detach a running actor", func(t *testing.T) {
		a := newAteletWithWorker(newObjectStore())
		if _, err := a.Run(t.Context(), run); err != nil {
			t.Fatalf("Run: %v", err)
		}
		_, err := a.DetachActor(t.Context(), &ateletpb.DetachActorRequest{ActorUid: uid, Atespace: "ns", ActorName: "actor-1"})
		if status.Code(err) != codes.Aborted {
			t.Fatalf("DetachActor = %v, want Aborted", err)
		}
	})

	t.Run("a call failed by a fault leaves no effect", func(t *testing.T) {
		a := newAteletWithWorker(newObjectStore())
		a.On(AteletRun).FailNext(ErrSandboxFailed)
		if _, err := a.Run(t.Context(), run); err != ErrSandboxFailed {
			t.Fatalf("Run = %v, want ErrSandboxFailed", err)
		}
		assertSandboxes(t, a, nil)
	})

	t.Run("terminate drops the sandbox and the local snapshot", func(t *testing.T) {
		a := newAteletWithWorker(newObjectStore())
		if _, err := a.Run(t.Context(), run); err != nil {
			t.Fatalf("Run: %v", err)
		}
		if _, err := a.Checkpoint(t.Context(), deferred); err != nil {
			t.Fatalf("Checkpoint: %v", err)
		}
		if _, err := a.Terminate(t.Context(), &ateletpb.TerminateRequest{WorkerPodUid: worker, ActorUid: uid}); err != nil {
			t.Fatalf("Terminate: %v", err)
		}
		if got := a.LocalSnapshot(uid); got != "" {
			t.Errorf("LocalSnapshot after Terminate = %q, want none", got)
		}
	})

	t.Run("terminating an actor that is not running succeeds", func(t *testing.T) {
		a := newAteletWithWorker(newObjectStore())
		if _, err := a.Terminate(t.Context(), &ateletpb.TerminateRequest{WorkerPodUid: worker, ActorUid: uid}); err != nil {
			t.Errorf("Terminate of an actor that is not running = %v, want no error", err)
		}
	})

	t.Run("a blocked call takes effect only once released", func(t *testing.T) {
		a := newAteletWithWorker(newObjectStore())
		gate := a.On(AteletRun).Block()
		done := make(chan error)
		go func() {
			_, err := a.Run(t.Context(), run)
			done <- err
		}()
		gate.Wait(t)
		assertSandboxes(t, a, nil)
		gate.Release()
		if err := <-done; err != nil {
			t.Fatalf("Run: %v", err)
		}
		assertSandboxes(t, a, running)
	})

	t.Run("a held reply comes after the call took effect", func(t *testing.T) {
		a := newAteletWithWorker(newObjectStore())
		gate := a.On(AteletRun).HoldReply()
		done := make(chan error)
		go func() {
			_, err := a.Run(t.Context(), run)
			done <- err
		}()
		gate.Wait(t)
		assertSandboxes(t, a, running)
		select {
		case err := <-done:
			t.Fatalf("Run replied (%v) before its reply was released", err)
		default:
		}
		gate.Release()
		if err := <-done; err != nil {
			t.Fatalf("Run: %v", err)
		}
	})

	t.Run("records every call, faulted or not", func(t *testing.T) {
		a := newAteletWithWorker(newObjectStore())
		a.On(AteletRun).FailNext(ErrSandboxFailed)
		if _, err := a.Run(t.Context(), run); err == nil {
			t.Fatal("Run succeeded despite the fault")
		}
		if _, err := a.Run(t.Context(), run); err != nil {
			t.Fatalf("Run: %v", err)
		}
		calls := a.Calls()
		if len(calls) != 2 || calls[0].Op != AteletRun || calls[1].Op != AteletRun {
			t.Errorf("calls = %v, want two Run calls", calls)
		}
	})
}
