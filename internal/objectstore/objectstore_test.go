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

func mustSnapshotURI(t *testing.T, location, atespace, name string) resources.SnapshotURI {
	t.Helper()
	uri, err := resources.NewSnapshotURI(location, atespace, name)
	if err != nil {
		t.Fatalf("NewSnapshotURI(%q, %q, %q) = %v", location, atespace, name, err)
	}
	return uri
}

func TestDeletePrefix(t *testing.T) {
	tests := []struct {
		name string
		// seed maps a snapshot's objects onto the store before the delete.
		seed map[string][]string
		// target is the snapshot to release, as an atespace/name pair under
		// testLocation.
		targetAtespace string
		targetName     string
		wantRemaining  []string
	}{
		{
			name:           "releases every object of the snapshot",
			seed:           map[string][]string{"team-a/snap-1": {"manifest.json", "memory.zst", "disks/root.zst"}},
			targetAtespace: "team-a",
			targetName:     "snap-1",
			wantRemaining:  nil,
		},
		{
			name: "leaves a snapshot whose name it is a prefix of",
			seed: map[string][]string{
				"team-a/snap-1":  {"manifest.json"},
				"team-a/snap-10": {"manifest.json"},
			},
			targetAtespace: "team-a",
			targetName:     "snap-1",
			wantRemaining:  []string{"bucket/root/snapshots/team-a/snap-10/manifest.json"},
		},
		{
			name: "leaves the same name in another atespace",
			seed: map[string][]string{
				"team-a/snap-1": {"manifest.json"},
				"team-b/snap-1": {"manifest.json"},
			},
			targetAtespace: "team-a",
			targetName:     "snap-1",
			wantRemaining:  []string{"bucket/root/snapshots/team-b/snap-1/manifest.json"},
		},
		{
			name:           "an already collected snapshot succeeds",
			seed:           nil,
			targetAtespace: "team-a",
			targetName:     "snap-1",
			wantRemaining:  nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fake := objectstoretest.New()
			for snapshot, names := range tt.seed {
				atespace, name := splitSnapshot(t, snapshot)
				fake.PutSnapshot(t, mustSnapshotURI(t, testLocation, atespace, name), names...)
			}

			target := mustSnapshotURI(t, testLocation, tt.targetAtespace, tt.targetName)
			if err := objectstore.DeletePrefix(t.Context(), fake, target); err != nil {
				t.Fatalf("DeletePrefix(%s) = %v, want nil", target, err)
			}
			if diff := cmp.Diff(tt.wantRemaining, fake.Objects(), cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("objects after DeletePrefix(%s) differ (-want +got):\n%s", target, diff)
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
			uri := mustSnapshotURI(t, testLocation, "team-a", "snap-1")
			fake.PutSnapshot(t, uri, "manifest.json", "memory.zst")

			err := objectstore.DeletePrefix(t.Context(), fake, uri)
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
	uri := mustSnapshotURI(t, testLocation, "team-a", "snap-1")
	fake.PutSnapshot(t, uri, "manifest.json", "memory.zst", "disks/root.zst")

	failure := errors.New("boom")
	fake.OnDelete = func(_, object string) error {
		if strings.HasSuffix(object, "memory.zst") {
			return failure
		}
		return nil
	}
	if err := objectstore.DeletePrefix(t.Context(), fake, uri); !errors.Is(err, failure) {
		t.Fatalf("DeletePrefix(%s) = %v, want it to wrap %v", uri, err, failure)
	}

	fake.OnDelete = nil
	if err := objectstore.DeletePrefix(t.Context(), fake, uri); err != nil {
		t.Fatalf("DeletePrefix(%s) on retry = %v, want nil", uri, err)
	}
	if remaining := fake.Objects(); len(remaining) != 0 {
		t.Errorf("objects after the retry = %v, want none", remaining)
	}
}

func TestCopyPrefix(t *testing.T) {
	src := mustSnapshotURI(t, testLocation, "team-a", "snap-1")
	dst := mustSnapshotURI(t, testLocation, "team-a", "tag-v1")

	fake := objectstoretest.New()
	fake.PutSnapshot(t, src, "manifest.json", "memory.zst", "disks/root.zst")

	if err := objectstore.CopyPrefix(t.Context(), fake, src, dst); err != nil {
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
	src := mustSnapshotURI(t, "gs://old-bucket/root", "team-a", "snap-1")
	dst := mustSnapshotURI(t, "gs://new-bucket", "team-a", "tag-v1")

	fake := objectstoretest.New()
	fake.PutSnapshot(t, src, "manifest.json")

	if err := objectstore.CopyPrefix(t.Context(), fake, src, dst); err != nil {
		t.Fatalf("CopyPrefix(%s, %s) = %v, want nil", src, dst, err)
	}
	want := []string{
		"new-bucket/snapshots/team-a/tag-v1/manifest.json",
		"old-bucket/root/snapshots/team-a/snap-1/manifest.json",
	}
	if diff := cmp.Diff(want, fake.Objects()); diff != "" {
		t.Errorf("objects after CopyPrefix differ (-want +got):\n%s", diff)
	}
}

// TestCopyPrefix_RetryOverwrites covers the retry of a copy that failed partway:
// the destination is deterministic, so nothing is left over from the first run.
func TestCopyPrefix_RetryOverwrites(t *testing.T) {
	src := mustSnapshotURI(t, testLocation, "team-a", "snap-1")
	dst := mustSnapshotURI(t, testLocation, "team-a", "tag-v1")

	fake := objectstoretest.New()
	fake.PutSnapshot(t, src, "manifest.json", "memory.zst")

	failure := errors.New("boom")
	fake.OnCopy = func(_, srcObject, _, _ string) error {
		if strings.HasSuffix(srcObject, "memory.zst") {
			return failure
		}
		return nil
	}
	if err := objectstore.CopyPrefix(t.Context(), fake, src, dst); !errors.Is(err, failure) {
		t.Fatalf("CopyPrefix(%s, %s) = %v, want it to wrap %v", src, dst, err, failure)
	}

	fake.OnCopy = nil
	if err := objectstore.CopyPrefix(t.Context(), fake, src, dst); err != nil {
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
	src := mustSnapshotURI(t, testLocation, "team-a", "snap-1")
	dst := mustSnapshotURI(t, testLocation, "team-a", "tag-v1")

	fake := objectstoretest.New()
	if err := objectstore.CopyPrefix(t.Context(), fake, src, dst); err == nil {
		t.Fatalf("CopyPrefix(%s, %s) = nil, want an error", src, dst)
	}
	if objects := fake.Objects(); len(objects) != 0 {
		t.Errorf("objects after the failed copy = %v, want none", objects)
	}
}

func TestBucketPrefix(t *testing.T) {
	tests := []struct {
		name       string
		uri        resources.SnapshotURI
		wantBucket string
		wantPrefix string
		wantErr    bool
	}{
		{
			name:       "gcs location with a path",
			uri:        mustSnapshotURI(t, "gs://bucket/root", "team-a", "snap-1"),
			wantBucket: "bucket",
			wantPrefix: "root/snapshots/team-a/snap-1/",
		},
		{
			name:       "s3 location at the bucket root",
			uri:        mustSnapshotURI(t, "s3://bucket", "team-a", "snap-1"),
			wantBucket: "bucket",
			wantPrefix: "snapshots/team-a/snap-1/",
		},
		{
			name:    "the zero URI",
			uri:     resources.SnapshotURI{},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bucket, prefix, err := objectstore.BucketPrefix(tt.uri)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("BucketPrefix(%s) = (%q, %q, nil), want an error", tt.uri, bucket, prefix)
				}
				return
			}
			if err != nil {
				t.Fatalf("BucketPrefix(%s) = %v, want nil", tt.uri, err)
			}
			if bucket != tt.wantBucket || prefix != tt.wantPrefix {
				t.Errorf("BucketPrefix(%s) = (%q, %q), want (%q, %q)", tt.uri, bucket, prefix, tt.wantBucket, tt.wantPrefix)
			}
		})
	}
}

// splitSnapshot splits a test's "atespace/name" shorthand.
func splitSnapshot(t *testing.T, snapshot string) (string, string) {
	t.Helper()
	atespace, name, found := strings.Cut(snapshot, "/")
	if !found {
		t.Fatalf("malformed snapshot shorthand %q, want atespace/name", snapshot)
	}
	return atespace, name
}
