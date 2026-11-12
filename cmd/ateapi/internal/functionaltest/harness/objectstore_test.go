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

package harness

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/testing/protocmp"

	objectstorev1 "go.ate.dev/substrate/api/objectstore/v1"
	"go.ate.dev/substrate/internal/resources"
)

func TestObjectStore(t *testing.T) {
	snapshotURI := func(t *testing.T, snapshotUID string) string {
		t.Helper()
		uri, err := resources.NewActorSnapshotURI(StorageLocation, "ns", "0f0e0d0c-0b0a-4908-8706-050403020100", snapshotUID)
		if err != nil {
			t.Fatalf("NewActorSnapshotURI: %v", err)
		}
		return uri.String()
	}
	withObjects := func(keys ...string) *ObjectStore {
		s := newObjectStore()
		for _, k := range keys {
			s.objectKeys[k] = true
		}
		return s
	}
	assertObjects := func(t *testing.T, s *ObjectStore, want []string) {
		t.Helper()
		if diff := cmp.Diff(want, s.Objects(), cmpopts.EquateEmpty()); diff != "" {
			t.Errorf("objects (-want +got):\n%s", diff)
		}
	}

	client := func(t *testing.T, s *ObjectStore) objectstorev1.ControlProviderClient {
		t.Helper()
		return objectstorev1.NewControlProviderClient(dialInMemory(t, serveInMemory(t, s.grpcServer())))
	}
	cleanup := func(t *testing.T, s *ObjectStore, uri string) error {
		t.Helper()
		_, err := client(t, s).CleanupSnapshot(t.Context(), &objectstorev1.CleanupSnapshotRequest{SnapshotUri: uri})
		return err
	}
	copySnapshot := func(t *testing.T, s *ObjectStore, src, dst string) error {
		t.Helper()
		_, err := client(t, s).CopySnapshot(t.Context(), &objectstorev1.CopySnapshotRequest{SrcUri: src, DstUri: dst})
		return err
	}

	t.Run("Objects lists every bucket's objects, sorted", func(t *testing.T) {
		s := withObjects("b2/y", "b1/x")
		assertObjects(t, s, []string{"b1/x", "b2/y"})
	})

	t.Run("CleanupSnapshot deletes every object below the URI, and cleaning up an absent one succeeds", func(t *testing.T) {
		s := withObjects("b1/root/a/x", "b1/root/a/sub/y", "b1/root/ab/z", "b2/root/a/x")
		if err := cleanup(t, s, "gs://b1/root/a"); err != nil {
			t.Fatalf("CleanupSnapshot: %v", err)
		}
		if err := cleanup(t, s, "gs://b1/root/a"); err != nil {
			t.Errorf("CleanupSnapshot of an absent snapshot = %v, want no error", err)
		}
		assertObjects(t, s, []string{"b1/root/ab/z", "b2/root/a/x"})
	})

	t.Run("CopySnapshot copies every object below the source and keeps it", func(t *testing.T) {
		s := withObjects("b1/src/x", "b1/src/sub/y", "b1/srcx/z")
		if err := copySnapshot(t, s, "gs://b1/src", "gs://b2/dst"); err != nil {
			t.Fatalf("CopySnapshot: %v", err)
		}
		assertObjects(t, s, []string{"b1/src/sub/y", "b1/src/x", "b1/srcx/z", "b2/dst/sub/y", "b2/dst/x"})
	})

	t.Run("CopySnapshot of an empty source fails with FAILED_PRECONDITION and copies nothing", func(t *testing.T) {
		s := withObjects()
		if err := copySnapshot(t, s, "gs://b1/missing", "gs://b1/dst"); status.Code(err) != codes.FailedPrecondition {
			t.Errorf("CopySnapshot of an empty source = %v, want FailedPrecondition", err)
		}
		assertObjects(t, s, nil)
	})

	t.Run("a URI with no path fails with INVALID_ARGUMENT", func(t *testing.T) {
		s := withObjects("b1/x")
		if err := cleanup(t, s, "gs://b1"); status.Code(err) != codes.InvalidArgument {
			t.Errorf("CleanupSnapshot of a bare bucket = %v, want InvalidArgument", err)
		}
		if err := copySnapshot(t, s, "gs://b1/x", "gs://b2"); status.Code(err) != codes.InvalidArgument {
			t.Errorf("CopySnapshot to a bare bucket = %v, want InvalidArgument", err)
		}
		assertObjects(t, s, []string{"b1/x"})
	})

	t.Run("a faulted CleanupSnapshot fails and keeps the objects", func(t *testing.T) {
		s := withObjects("b1/root/x")
		s.On(ObjectStoreCleanupSnapshot).FailNext(ErrStorageFailed)
		if err := cleanup(t, s, "gs://b1/root"); status.Code(err) != codes.Internal {
			t.Errorf("faulted CleanupSnapshot = %v, want Internal", err)
		}
		assertObjects(t, s, []string{"b1/root/x"})
	})

	t.Run("a faulted CopySnapshot fails and copies nothing", func(t *testing.T) {
		s := withObjects("b1/src/x")
		s.On(ObjectStoreCopySnapshot).FailNext(ErrStorageFailed)
		if err := copySnapshot(t, s, "gs://b1/src", "gs://b2/dst"); status.Code(err) != codes.Internal {
			t.Errorf("faulted CopySnapshot = %v, want Internal", err)
		}
		assertObjects(t, s, []string{"b1/src/x"})
	})

	t.Run("records every call after RecordCalls, faulted or not", func(t *testing.T) {
		s := withObjects("b1/src/x")
		if err := cleanup(t, s, "gs://b1/other"); err != nil {
			t.Fatalf("CleanupSnapshot: %v", err)
		}
		rec := s.RecordCalls()
		s.On(ObjectStoreCopySnapshot).FailNext(ErrStorageFailed)
		if err := copySnapshot(t, s, "gs://b1/src", "gs://b2/dst"); err == nil {
			t.Fatal("faulted CopySnapshot succeeded")
		}
		if err := cleanup(t, s, "gs://b1/src"); err != nil {
			t.Fatalf("CleanupSnapshot: %v", err)
		}
		want := []Call[ObjectStoreMethod]{
			{Method: ObjectStoreCopySnapshot, Request: &objectstorev1.CopySnapshotRequest{SrcUri: "gs://b1/src", DstUri: "gs://b2/dst"}},
			{Method: ObjectStoreCleanupSnapshot, Request: &objectstorev1.CleanupSnapshotRequest{SnapshotUri: "gs://b1/src"}},
		}
		if diff := cmp.Diff(want, rec.Calls(), protocmp.Transform()); diff != "" {
			t.Errorf("recorded calls (-want +got):\n%s", diff)
		}
	})

	t.Run("a written snapshot is listed relative to its URI", func(t *testing.T) {
		s := newObjectStore()
		uri := snapshotURI(t, "9c2f7b41-6d05-4e83-a1f7-3b8c0d5e2a94")
		if err := s.writeSnapshot(uri, "memory.zst", "manifest.json"); err != nil {
			t.Fatalf("writeSnapshot: %v", err)
		}
		got, err := s.Snapshot(uri)
		if err != nil {
			t.Fatalf("Snapshot: %v", err)
		}
		if diff := cmp.Diff([]string{"manifest.json", "memory.zst"}, got); diff != "" {
			t.Errorf("Snapshot (-want +got):\n%s", diff)
		}
	})

	t.Run("a snapshot that was never written is empty", func(t *testing.T) {
		got, err := newObjectStore().Snapshot(snapshotURI(t, "9c2f7b41-6d05-4e83-a1f7-3b8c0d5e2a94"))
		if err != nil || len(got) != 0 {
			t.Errorf("Snapshot of a missing snapshot = %v, %v; want empty", got, err)
		}
	})

	t.Run("DeleteSnapshot removes that snapshot only", func(t *testing.T) {
		s := newObjectStore()
		lost := snapshotURI(t, "9c2f7b41-6d05-4e83-a1f7-3b8c0d5e2a94")
		kept := snapshotURI(t, "1d4e5f60-7a8b-4c9d-8e0f-a1b2c3d4e5f6")
		for _, uri := range []string{lost, kept} {
			if err := s.writeSnapshot(uri, snapshotObjects...); err != nil {
				t.Fatalf("writeSnapshot: %v", err)
			}
		}
		if err := s.DeleteSnapshot(lost); err != nil {
			t.Fatalf("DeleteSnapshot: %v", err)
		}
		if got, _ := s.Snapshot(lost); len(got) != 0 {
			t.Errorf("Snapshot after DeleteSnapshot = %v, want empty", got)
		}
		if got, _ := s.Snapshot(kept); len(got) != len(snapshotObjects) {
			t.Errorf("other snapshot after DeleteSnapshot = %v, want %v", got, snapshotObjects)
		}
	})

	t.Run("a malformed snapshot URI is refused", func(t *testing.T) {
		if _, err := newObjectStore().Snapshot("not-a-snapshot-uri"); err == nil {
			t.Error("Snapshot of a malformed URI succeeded")
		}
	})
}
