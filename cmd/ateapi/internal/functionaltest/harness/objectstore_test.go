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
	"errors"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"

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

	t.Run("List returns the objects below a prefix in one bucket, sorted", func(t *testing.T) {
		s := withObjects("b1/dir/z", "b1/dir/a", "b1/other/x", "b2/dir/y")
		got, err := s.List(t.Context(), "b1", "dir/")
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		if diff := cmp.Diff([]string{"dir/a", "dir/z"}, got); diff != "" {
			t.Errorf("List (-want +got):\n%s", diff)
		}
	})

	t.Run("Objects lists every bucket's objects, sorted", func(t *testing.T) {
		s := withObjects("b2/y", "b1/x")
		assertObjects(t, s, []string{"b1/x", "b2/y"})
	})

	t.Run("Delete removes the object, and deleting a missing one succeeds", func(t *testing.T) {
		s := withObjects("b1/x", "b1/y")
		if err := s.Delete(t.Context(), "b1", "x"); err != nil {
			t.Fatalf("Delete: %v", err)
		}
		if err := s.Delete(t.Context(), "b1", "missing"); err != nil {
			t.Errorf("Delete of a missing object = %v, want no error", err)
		}
		assertObjects(t, s, []string{"b1/y"})
	})

	t.Run("Copy copies the object and keeps the source", func(t *testing.T) {
		s := withObjects("b1/x")
		if err := s.Copy(t.Context(), "b1", "x", "b2", "y"); err != nil {
			t.Fatalf("Copy: %v", err)
		}
		assertObjects(t, s, []string{"b1/x", "b2/y"})
	})

	t.Run("Copy of a missing object fails and copies nothing", func(t *testing.T) {
		s := newObjectStore()
		if err := s.Copy(t.Context(), "b1", "missing", "b1", "dst"); err == nil {
			t.Error("Copy of a missing object succeeded")
		}
		assertObjects(t, s, nil)
	})

	t.Run("a faulted List fails", func(t *testing.T) {
		s := withObjects("b1/x")
		s.On(ObjectStoreList).FailNext(ErrStorageFailed)
		if _, err := s.List(t.Context(), "b1", ""); err == nil {
			t.Error("faulted List succeeded")
		}
	})

	t.Run("a faulted Delete fails and keeps the object", func(t *testing.T) {
		s := withObjects("b1/x")
		s.On(ObjectStoreDelete).FailNext(ErrStorageFailed)
		if err := s.Delete(t.Context(), "b1", "x"); err == nil {
			t.Error("faulted Delete succeeded")
		}
		assertObjects(t, s, []string{"b1/x"})
	})

	t.Run("a faulted Copy fails and copies nothing", func(t *testing.T) {
		s := withObjects("b1/x")
		s.On(ObjectStoreCopy).FailNext(ErrStorageFailed)
		if err := s.Copy(t.Context(), "b1", "x", "b2", "y"); err == nil {
			t.Error("faulted Copy succeeded")
		}
		assertObjects(t, s, []string{"b1/x"})
	})

	t.Run("a missing bucket is reported as the GCS client does", func(t *testing.T) {
		if !errors.Is(ErrBucketNotExist, storage.ErrBucketNotExist) {
			t.Errorf("ErrBucketNotExist = %v, want it to wrap storage.ErrBucketNotExist", ErrBucketNotExist)
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
