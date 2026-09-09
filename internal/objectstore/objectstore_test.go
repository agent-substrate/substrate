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

package objectstore_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/agent-substrate/substrate/internal/objectstore"
	"github.com/agent-substrate/substrate/internal/objectstore/objectstoretest"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

const testLocation = "gs://bucket/root"

func mustActorSnapshotURI(t *testing.T, location, atespace, actorUID, name string) resources.SnapshotURI {
	t.Helper()
	uri, err := resources.NewActorSnapshotURI(location, atespace, actorUID, name)
	if err != nil {
		t.Fatalf("NewActorSnapshotURI(%q, %q, %q, %q) = %v", location, atespace, actorUID, name, err)
	}
	return uri
}

func mustTagSnapshotURI(t *testing.T, location, atespace, name string) resources.SnapshotURI {
	t.Helper()
	uri, err := resources.NewTagSnapshotURI(location, atespace, name)
	if err != nil {
		t.Fatalf("NewTagSnapshotURI(%q, %q, %q) = %v", location, atespace, name, err)
	}
	return uri
}

func mustOwnerPrefix(t *testing.T, owner resources.SnapshotOwner, location string) resources.StoragePrefix {
	t.Helper()
	prefix, err := owner.Prefix(location)
	if err != nil {
		t.Fatalf("%s.Prefix(%q) = %v", owner, location, err)
	}
	return prefix
}

func TestDeletePrefix(t *testing.T) {
	tests := []struct {
		name string
		// seed maps each snapshot to write onto the store to its object names.
		seed          map[resources.SnapshotURI][]string
		target        resources.StoragePrefix
		wantRemaining []string
	}{
		{
			name: "releases every object of one snapshot",
			seed: map[resources.SnapshotURI][]string{
				mustActorSnapshotURI(t, testLocation, "team-a", "actor-1", "snap-1"): {"manifest.json", "memory.zst", "disks/root.zst"},
			},
			target:        mustActorSnapshotURI(t, testLocation, "team-a", "actor-1", "snap-1").Prefix(),
			wantRemaining: nil,
		},
		{
			name: "releases every snapshot an actor owns",
			seed: map[resources.SnapshotURI][]string{
				mustActorSnapshotURI(t, testLocation, "team-a", "actor-1", "snap-1"): {"manifest.json"},
				mustActorSnapshotURI(t, testLocation, "team-a", "actor-1", "snap-2"): {"manifest.json"},
			},
			target:        mustOwnerPrefix(t, resources.ActorSnapshotOwner("team-a", "actor-1"), testLocation),
			wantRemaining: nil,
		},
		{
			name: "leaves a snapshot whose name it is a prefix of",
			seed: map[resources.SnapshotURI][]string{
				mustActorSnapshotURI(t, testLocation, "team-a", "actor-1", "snap-1"):  {"manifest.json"},
				mustActorSnapshotURI(t, testLocation, "team-a", "actor-1", "snap-10"): {"manifest.json"},
			},
			target:        mustActorSnapshotURI(t, testLocation, "team-a", "actor-1", "snap-1").Prefix(),
			wantRemaining: []string{"bucket/root/atespaces/team-a/actors/actor-1/snapshots/snap-10/manifest.json"},
		},
		{
			name: "leaves another actor's snapshots",
			seed: map[resources.SnapshotURI][]string{
				mustActorSnapshotURI(t, testLocation, "team-a", "actor-1", "snap-1"): {"manifest.json"},
				mustActorSnapshotURI(t, testLocation, "team-a", "actor-2", "snap-1"): {"manifest.json"},
			},
			target:        mustOwnerPrefix(t, resources.ActorSnapshotOwner("team-a", "actor-1"), testLocation),
			wantRemaining: []string{"bucket/root/atespaces/team-a/actors/actor-2/snapshots/snap-1/manifest.json"},
		},
		{
			name: "leaves the same actor UID in another atespace",
			seed: map[resources.SnapshotURI][]string{
				mustActorSnapshotURI(t, testLocation, "team-a", "actor-1", "snap-1"): {"manifest.json"},
				mustActorSnapshotURI(t, testLocation, "team-b", "actor-1", "snap-1"): {"manifest.json"},
			},
			target:        mustOwnerPrefix(t, resources.ActorSnapshotOwner("team-a", "actor-1"), testLocation),
			wantRemaining: []string{"bucket/root/atespaces/team-b/actors/actor-1/snapshots/snap-1/manifest.json"},
		},
		{
			// The invariant the layout exists for: an actor's prefix cannot reach
			// a tag's objects, so a clone borrowing one cannot collect it.
			name: "an actor's prefix leaves a tag's snapshot alone",
			seed: map[resources.SnapshotURI][]string{
				mustActorSnapshotURI(t, testLocation, "team-a", "actor-1", "snap-1"): {"manifest.json"},
				mustTagSnapshotURI(t, testLocation, "team-a", "snap-2"):              {"manifest.json"},
			},
			target:        mustOwnerPrefix(t, resources.ActorSnapshotOwner("team-a", "actor-1"), testLocation),
			wantRemaining: []string{"bucket/root/atespaces/team-a/tags/snap-2/manifest.json"},
		},
		{
			name:          "an already collected snapshot succeeds",
			seed:          nil,
			target:        mustActorSnapshotURI(t, testLocation, "team-a", "actor-1", "snap-1").Prefix(),
			wantRemaining: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fake := objectstoretest.New()
			for uri, names := range tt.seed {
				fake.PutSnapshot(t, uri, names...)
			}

			if err := objectstore.DeletePrefix(t.Context(), fake, tt.target); err != nil {
				t.Fatalf("DeletePrefix(%s) = %v, want nil", tt.target, err)
			}
			if diff := cmp.Diff(tt.wantRemaining, fake.Objects(), cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("objects after DeletePrefix(%s) differ (-want +got):\n%s", tt.target, diff)
			}
		})
	}
}

func TestDeletePrefix_Failures(t *testing.T) {
	failure := errors.New("boom")
	tests := []struct {
		name     string
		onList   func(bucket, prefix string) error
		onDelete func(bucket, object string) error
	}{
		{
			name:   "listing fails",
			onList: func(string, string) error { return failure },
		},
		{
			name:     "deleting one object fails",
			onDelete: func(string, string) error { return failure },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fake := objectstoretest.New()
			fake.OnList, fake.OnDelete = tt.onList, tt.onDelete
			uri := mustActorSnapshotURI(t, testLocation, "team-a", "actor-1", "snap-1")
			fake.PutSnapshot(t, uri, "manifest.json", "memory.zst")

			err := objectstore.DeletePrefix(t.Context(), fake, uri.Prefix())
			if !errors.Is(err, failure) {
				t.Fatalf("DeletePrefix(%s) = %v, want it to wrap %v", uri, err, failure)
			}
		})
	}
}

// TestDeletePrefix_Resumes covers the retry after a delete that failed partway:
// the objects already gone stay gone and the rest are collected.
func TestDeletePrefix_Resumes(t *testing.T) {
	fake := objectstoretest.New()
	uri := mustActorSnapshotURI(t, testLocation, "team-a", "actor-1", "snap-1")
	fake.PutSnapshot(t, uri, "manifest.json", "memory.zst", "disks/root.zst")

	failure := errors.New("boom")
	fake.OnDelete = func(_, object string) error {
		if strings.HasSuffix(object, "memory.zst") {
			return failure
		}
		return nil
	}
	if err := objectstore.DeletePrefix(t.Context(), fake, uri.Prefix()); !errors.Is(err, failure) {
		t.Fatalf("DeletePrefix(%s) = %v, want it to wrap %v", uri, err, failure)
	}

	fake.OnDelete = nil
	if err := objectstore.DeletePrefix(t.Context(), fake, uri.Prefix()); err != nil {
		t.Fatalf("DeletePrefix(%s) on retry = %v, want nil", uri, err)
	}
	if remaining := fake.Objects(); len(remaining) != 0 {
		t.Errorf("objects after the retry = %v, want none", remaining)
	}
}

func TestCopyPrefix(t *testing.T) {
	src := mustActorSnapshotURI(t, testLocation, "team-a", "actor-1", "snap-1")
	dst := mustTagSnapshotURI(t, testLocation, "team-a", "snap-2")

	fake := objectstoretest.New()
	fake.PutSnapshot(t, src, "manifest.json", "memory.zst", "disks/root.zst")

	if err := objectstore.CopyPrefix(t.Context(), fake, src.Prefix(), dst.Prefix()); err != nil {
		t.Fatalf("CopyPrefix(%s, %s) = %v, want nil", src, dst, err)
	}

	want := []string{"disks/root.zst", "manifest.json", "memory.zst"}
	if diff := cmp.Diff(want, fake.Snapshot(t, dst)); diff != "" {
		t.Errorf("objects of %s differ (-want +got):\n%s", dst, diff)
	}
	if diff := cmp.Diff(want, fake.Snapshot(t, src)); diff != "" {
		t.Errorf("objects of the source %s differ (-want +got):\n%s", src, diff)
	}
}

// TestCopyPrefix_AcrossBuckets covers a template whose snapshotsConfig.location
// moved: the copy still lands under the destination's own bucket.
func TestCopyPrefix_AcrossBuckets(t *testing.T) {
	src := mustActorSnapshotURI(t, "gs://old-bucket/root", "team-a", "actor-1", "snap-1")
	dst := mustTagSnapshotURI(t, "gs://new-bucket", "team-a", "snap-2")

	fake := objectstoretest.New()
	fake.PutSnapshot(t, src, "manifest.json")

	if err := objectstore.CopyPrefix(t.Context(), fake, src.Prefix(), dst.Prefix()); err != nil {
		t.Fatalf("CopyPrefix(%s, %s) = %v, want nil", src, dst, err)
	}
	want := []string{
		"new-bucket/atespaces/team-a/tags/snap-2/manifest.json",
		"old-bucket/root/atespaces/team-a/actors/actor-1/snapshots/snap-1/manifest.json",
	}
	if diff := cmp.Diff(want, fake.Objects()); diff != "" {
		t.Errorf("objects after CopyPrefix differ (-want +got):\n%s", diff)
	}
}

// TestCopyPrefix_RetryOverwrites covers the retry of a copy that failed partway:
// the destination is deterministic, so nothing is left over from the first run.
func TestCopyPrefix_RetryOverwrites(t *testing.T) {
	src := mustActorSnapshotURI(t, testLocation, "team-a", "actor-1", "snap-1")
	dst := mustTagSnapshotURI(t, testLocation, "team-a", "snap-2")

	fake := objectstoretest.New()
	fake.PutSnapshot(t, src, "manifest.json", "memory.zst")

	failure := errors.New("boom")
	fake.OnCopy = func(_, srcObject, _, _ string) error {
		if strings.HasSuffix(srcObject, "memory.zst") {
			return failure
		}
		return nil
	}
	if err := objectstore.CopyPrefix(t.Context(), fake, src.Prefix(), dst.Prefix()); !errors.Is(err, failure) {
		t.Fatalf("CopyPrefix(%s, %s) = %v, want it to wrap %v", src, dst, err, failure)
	}

	fake.OnCopy = nil
	if err := objectstore.CopyPrefix(t.Context(), fake, src.Prefix(), dst.Prefix()); err != nil {
		t.Fatalf("CopyPrefix(%s, %s) on retry = %v, want nil", src, dst, err)
	}
	want := []string{"manifest.json", "memory.zst"}
	if diff := cmp.Diff(want, fake.Snapshot(t, dst)); diff != "" {
		t.Errorf("objects of %s after the retry differ (-want +got):\n%s", dst, diff)
	}
}

// TestCopyPrefix_EmptySource covers a source that is not there: copying nothing
// would leave the destination naming an external snapshot nothing can restore
// from, so it has to fail instead.
func TestCopyPrefix_EmptySource(t *testing.T) {
	src := mustActorSnapshotURI(t, testLocation, "team-a", "actor-1", "snap-1")
	dst := mustTagSnapshotURI(t, testLocation, "team-a", "snap-2")

	fake := objectstoretest.New()
	if err := objectstore.CopyPrefix(t.Context(), fake, src.Prefix(), dst.Prefix()); err == nil {
		t.Fatalf("CopyPrefix(%s, %s) = nil, want an error", src, dst)
	}
	if objects := fake.Objects(); len(objects) != 0 {
		t.Errorf("objects after the failed copy = %v, want none", objects)
	}
}

func TestBucketPrefix(t *testing.T) {
	tests := []struct {
		name       string
		prefix     resources.StoragePrefix
		wantBucket string
		wantPrefix string
		wantErr    bool
	}{
		{
			name:       "gcs location with a path",
			prefix:     mustActorSnapshotURI(t, "gs://bucket/root", "team-a", "actor-1", "snap-1").Prefix(),
			wantBucket: "bucket",
			wantPrefix: "root/atespaces/team-a/actors/actor-1/snapshots/snap-1/",
		},
		{
			name:       "s3 location at the bucket root",
			prefix:     mustActorSnapshotURI(t, "s3://bucket", "team-a", "actor-1", "snap-1").Prefix(),
			wantBucket: "bucket",
			wantPrefix: "atespaces/team-a/actors/actor-1/snapshots/snap-1/",
		},
		{
			name:       "an actor's owner prefix",
			prefix:     mustOwnerPrefix(t, resources.ActorSnapshotOwner("team-a", "actor-1"), "gs://bucket/root"),
			wantBucket: "bucket",
			wantPrefix: "root/atespaces/team-a/actors/actor-1/",
		},
		{
			name:       "a tag's prefix",
			prefix:     mustTagSnapshotURI(t, "gs://bucket/root", "team-a", "snap-1").Prefix(),
			wantBucket: "bucket",
			wantPrefix: "root/atespaces/team-a/tags/snap-1/",
		},
		{
			name:    "the zero prefix",
			prefix:  resources.StoragePrefix{},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bucket, prefix, err := objectstore.BucketPrefix(tt.prefix)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("BucketPrefix(%s) = (%q, %q, nil), want an error", tt.prefix, bucket, prefix)
				}
				return
			}
			if err != nil {
				t.Fatalf("BucketPrefix(%s) = %v, want nil", tt.prefix, err)
			}
			if bucket != tt.wantBucket || prefix != tt.wantPrefix {
				t.Errorf("BucketPrefix(%s) = (%q, %q), want (%q, %q)", tt.prefix, bucket, prefix, tt.wantBucket, tt.wantPrefix)
			}
		})
	}
}
