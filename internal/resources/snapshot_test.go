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

package resources

import (
	"testing"
)

func TestNewActorSnapshotURI(t *testing.T) {
	tests := []struct {
		name      string
		location  string
		atespace  string
		actorUID  string
		snapshot  string
		want      string
		wantOwner string
		wantErr   bool
	}{
		{
			name:      "no trailing slash",
			location:  "gs://bucket/root",
			atespace:  "team-a",
			actorUID:  "actor-uid",
			snapshot:  "snap-1",
			want:      "gs://bucket/root/atespaces/team-a/actors/actor-uid/snapshots/snap-1",
			wantOwner: "gs://bucket/root/atespaces/team-a/actors/actor-uid",
		},
		{
			name:      "trailing slash",
			location:  "gs://bucket/root/",
			atespace:  "team-a",
			actorUID:  "actor-uid",
			snapshot:  "snap-1",
			want:      "gs://bucket/root/atespaces/team-a/actors/actor-uid/snapshots/snap-1",
			wantOwner: "gs://bucket/root/atespaces/team-a/actors/actor-uid",
		},
		{
			name:      "bucket only",
			location:  "gs://bucket",
			atespace:  "team-a",
			actorUID:  "actor-uid",
			snapshot:  "snap-1",
			want:      "gs://bucket/atespaces/team-a/actors/actor-uid/snapshots/snap-1",
			wantOwner: "gs://bucket/atespaces/team-a/actors/actor-uid",
		},
		{
			name:      "location containing an atespaces segment",
			location:  "gs://my-bucket/atespaces/secret-agent",
			atespace:  "team-a",
			actorUID:  "actor-uid",
			snapshot:  "snap-1",
			want:      "gs://my-bucket/atespaces/secret-agent/atespaces/team-a/actors/actor-uid/snapshots/snap-1",
			wantOwner: "gs://my-bucket/atespaces/secret-agent/atespaces/team-a/actors/actor-uid",
		},
		{
			name:     "empty location",
			location: "",
			atespace: "team-a",
			actorUID: "actor-uid",
			snapshot: "snap-1",
			wantErr:  true,
		},
		{
			name:     "location without a bucket",
			location: "/root",
			atespace: "team-a",
			actorUID: "actor-uid",
			snapshot: "snap-1",
			wantErr:  true,
		},
		{
			name:     "location with a query",
			location: "gs://bucket/root?generation=1",
			atespace: "team-a",
			actorUID: "actor-uid",
			snapshot: "snap-1",
			wantErr:  true,
		},
		{
			name:     "invalid atespace",
			location: "gs://bucket/root",
			atespace: "Team_A",
			actorUID: "actor-uid",
			snapshot: "snap-1",
			wantErr:  true,
		},
		{
			name:     "empty atespace",
			location: "gs://bucket/root",
			atespace: "",
			actorUID: "actor-uid",
			snapshot: "snap-1",
			wantErr:  true,
		},
		{
			name:     "invalid actor UID",
			location: "gs://bucket/root",
			atespace: "team-a",
			actorUID: "Actor_UID",
			snapshot: "snap-1",
			wantErr:  true,
		},
		{
			name:     "empty actor UID",
			location: "gs://bucket/root",
			atespace: "team-a",
			actorUID: "",
			snapshot: "snap-1",
			wantErr:  true,
		},
		{
			name:     "invalid snapshot name",
			location: "gs://bucket/root",
			atespace: "team-a",
			actorUID: "actor-uid",
			snapshot: "2026-08-05T10:04:05Z",
			wantErr:  true,
		},
		{
			name:     "empty snapshot name",
			location: "gs://bucket/root",
			atespace: "team-a",
			actorUID: "actor-uid",
			snapshot: "",
			wantErr:  true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := NewActorSnapshotURI(tc.location, tc.atespace, tc.actorUID, tc.snapshot)
			if (err != nil) != tc.wantErr {
				t.Fatalf("NewActorSnapshotURI(%q, %q, %q, %q) error = %v, wantErr %t", tc.location, tc.atespace, tc.actorUID, tc.snapshot, err, tc.wantErr)
			}
			if got.String() != tc.want {
				t.Errorf("NewActorSnapshotURI(%q, %q, %q, %q) = %q, want %q", tc.location, tc.atespace, tc.actorUID, tc.snapshot, got, tc.want)
			}
			if got.OwnerPrefix().String() != tc.wantOwner {
				t.Errorf("NewActorSnapshotURI(%q, %q, %q, %q).OwnerPrefix() = %q, want %q", tc.location, tc.atespace, tc.actorUID, tc.snapshot, got.OwnerPrefix(), tc.wantOwner)
			}
			if tc.wantErr && !got.IsZero() {
				t.Errorf("NewActorSnapshotURI(%q, %q, %q, %q) returned %q alongside an error, want the zero value", tc.location, tc.atespace, tc.actorUID, tc.snapshot, got)
			}
		})
	}
}

// TestNewTagSnapshotURI covers the shape that distinguishes a tag: it holds
// exactly one snapshot, so its owner prefix and its snapshot's are the same.
func TestNewTagSnapshotURI(t *testing.T) {
	tests := []struct {
		name     string
		location string
		atespace string
		tagUID   string
		want     string
		wantErr  bool
	}{
		{
			name:     "no trailing slash",
			location: "gs://bucket/root",
			atespace: "team-a",
			tagUID:   "tag-uid-1",
			want:     "gs://bucket/root/atespaces/team-a/tags/tag-uid-1",
		},
		{
			name:     "trailing slash",
			location: "gs://bucket/root/",
			atespace: "team-a",
			tagUID:   "tag-uid-1",
			want:     "gs://bucket/root/atespaces/team-a/tags/tag-uid-1",
		},
		{
			name:     "bucket only",
			location: "gs://bucket",
			atespace: "team-a",
			tagUID:   "tag-uid-1",
			want:     "gs://bucket/atespaces/team-a/tags/tag-uid-1",
		},
		{
			name:     "empty location",
			location: "",
			atespace: "team-a",
			tagUID:   "tag-uid-1",
			wantErr:  true,
		},
		{
			name:     "invalid atespace",
			location: "gs://bucket/root",
			atespace: "Team_A",
			tagUID:   "tag-uid-1",
			wantErr:  true,
		},
		{
			name:     "invalid tag UID",
			location: "gs://bucket/root",
			atespace: "team-a",
			tagUID:   "2026-08-05T10:04:05Z",
			wantErr:  true,
		},
		{
			name:     "empty tag UID",
			location: "gs://bucket/root",
			atespace: "team-a",
			tagUID:   "",
			wantErr:  true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := NewTagSnapshotURI(tc.location, tc.atespace, tc.tagUID)
			if (err != nil) != tc.wantErr {
				t.Fatalf("NewTagSnapshotURI(%q, %q, %q) error = %v, wantErr %t", tc.location, tc.atespace, tc.tagUID, err, tc.wantErr)
			}
			if got.String() != tc.want {
				t.Errorf("NewTagSnapshotURI(%q, %q, %q) = %q, want %q", tc.location, tc.atespace, tc.tagUID, got, tc.want)
			}
			if got.OwnerPrefix().String() != got.String() {
				t.Errorf("NewTagSnapshotURI(%q, %q, %q).OwnerPrefix() = %q, want the snapshot's own prefix %q", tc.location, tc.atespace, tc.tagUID, got.OwnerPrefix(), got)
			}
			if tc.wantErr && !got.IsZero() {
				t.Errorf("NewTagSnapshotURI(%q, %q, %q) returned %q alongside an error, want the zero value", tc.location, tc.atespace, tc.tagUID, got)
			}
		})
	}
}

// TestSnapshotOwnedBy covers the check every collector makes before deleting.
// An owner may only reach its own objects, which is what keeps an actor
// borrowing a tag's snapshot from collecting it out from under every other
// clone of that tag.
func TestSnapshotOwnedBy(t *testing.T) {
	actorURI, err := NewActorSnapshotURI("gs://bucket/root", "team-a", "actor-uid", "snap-1")
	if err != nil {
		t.Fatalf("NewActorSnapshotURI: %v", err)
	}
	tagURI, err := NewTagSnapshotURI("gs://bucket/root", "team-a", "snap-2")
	if err != nil {
		t.Fatalf("NewTagSnapshotURI: %v", err)
	}
	tests := []struct {
		name  string
		uri   SnapshotURI
		owner SnapshotOwner
		want  bool
	}{
		{
			name:  "an actor owns the snapshot it took",
			uri:   actorURI,
			owner: ActorSnapshotOwner("team-a", "actor-uid"),
			want:  true,
		},
		{
			name:  "a borrowed tag snapshot is not the actor's",
			uri:   tagURI,
			owner: ActorSnapshotOwner("team-a", "actor-uid"),
			want:  false,
		},
		{
			name:  "a tag owns its own snapshot",
			uri:   tagURI,
			owner: TagSnapshotOwner("team-a", "snap-2"),
			want:  true,
		},
		{
			name:  "another actor's snapshot is not this actor's",
			uri:   actorURI,
			owner: ActorSnapshotOwner("team-a", "other-actor-uid"),
			want:  false,
		},
		{
			name:  "the same UID in another atespace is another owner",
			uri:   actorURI,
			owner: ActorSnapshotOwner("team-b", "actor-uid"),
			want:  false,
		},
		{
			name:  "the zero URI is owned by nobody",
			uri:   SnapshotURI{},
			owner: ActorSnapshotOwner("team-a", "actor-uid"),
			want:  false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.uri.OwnedBy(tc.owner); got != tc.want {
				t.Errorf("%q.OwnedBy(%s) = %t, want %t", tc.uri, tc.owner, got, tc.want)
			}
		})
	}
}

func TestSnapshotOwnerPrefix(t *testing.T) {
	tests := []struct {
		name     string
		owner    SnapshotOwner
		location string
		want     string
		wantErr  bool
	}{
		{
			name:     "actor",
			owner:    ActorSnapshotOwner("team-a", "actor-uid"),
			location: "gs://bucket/root",
			want:     "gs://bucket/root/atespaces/team-a/actors/actor-uid",
		},
		{
			name:     "tag",
			owner:    TagSnapshotOwner("team-a", "snap-1"),
			location: "gs://bucket/root",
			want:     "gs://bucket/root/atespaces/team-a/tags/snap-1",
		},
		{
			name:     "the zero owner has no prefix",
			owner:    SnapshotOwner{},
			location: "gs://bucket/root",
			wantErr:  true,
		},
		{
			name:     "invalid location",
			owner:    ActorSnapshotOwner("team-a", "actor-uid"),
			location: "/root",
			wantErr:  true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.owner.Prefix(tc.location)
			if (err != nil) != tc.wantErr {
				t.Fatalf("%s.Prefix(%q) error = %v, wantErr %t", tc.owner, tc.location, err, tc.wantErr)
			}
			if got.String() != tc.want {
				t.Errorf("%s.Prefix(%q) = %q, want %q", tc.owner, tc.location, got, tc.want)
			}
		})
	}
}

// TestNewSnapshotName covers the one property the layout relies on: a fresh
// name every time. Both an actor's snapshots and a tag's destination are named
// this way, and since a tag's prefix is keyed on the name, a recreated tag
// cannot compute its way onto the objects its predecessor left.
func TestNewSnapshotName(t *testing.T) {
	seen := make(map[string]bool, 100)
	for range 100 {
		name := NewSnapshotName()
		if !IsValidResourceName(name) {
			t.Fatalf("NewSnapshotName() = %q, which is not a valid resource name", name)
		}
		if seen[name] {
			t.Fatalf("NewSnapshotName() returned %q twice", name)
		}
		seen[name] = true
	}
}

func TestParseSnapshotURI(t *testing.T) {
	tests := []struct {
		name         string
		uri          string
		wantLocation string
		wantAtespace string
		wantName     string
		wantOwner    SnapshotOwner
		wantErr      bool
	}{
		{
			name:         "reads an actor snapshot",
			uri:          "gs://bucket/root/atespaces/team-a/actors/actor-uid/snapshots/f47ac10b-58cc-4372-a567-0e02b2c3d479",
			wantLocation: "gs://bucket/root",
			wantAtespace: "team-a",
			wantName:     "f47ac10b-58cc-4372-a567-0e02b2c3d479",
			wantOwner:    ActorSnapshotOwner("team-a", "actor-uid"),
		},
		{
			name:         "reads a tag snapshot",
			uri:          "gs://bucket/root/atespaces/team-a/tags/snap-1",
			wantLocation: "gs://bucket/root",
			wantAtespace: "team-a",
			wantName:     "snap-1",
			wantOwner:    TagSnapshotOwner("team-a", "snap-1"),
		},
		{
			name:         "tolerates a trailing slash",
			uri:          "gs://bucket/root/atespaces/team-a/actors/actor-uid/snapshots/snap-1/",
			wantLocation: "gs://bucket/root",
			wantAtespace: "team-a",
			wantName:     "snap-1",
			wantOwner:    ActorSnapshotOwner("team-a", "actor-uid"),
		},
		{
			name:         "bucket-only location",
			uri:          "gs://bucket/atespaces/team-a/actors/actor-uid/snapshots/snap-1",
			wantLocation: "gs://bucket",
			wantAtespace: "team-a",
			wantName:     "snap-1",
			wantOwner:    ActorSnapshotOwner("team-a", "actor-uid"),
		},
		{
			name:         "location containing an atespaces segment",
			uri:          "gs://my-bucket/atespaces/secret-agent/atespaces/team-a/actors/actor-uid/snapshots/snap-1",
			wantLocation: "gs://my-bucket/atespaces/secret-agent",
			wantAtespace: "team-a",
			wantName:     "snap-1",
			wantOwner:    ActorSnapshotOwner("team-a", "actor-uid"),
		},
		{
			name:    "rejects an actor prefix with no snapshot",
			uri:     "gs://bucket/root/atespaces/team-a/actors/actor-uid",
			wantErr: true,
		},
		{
			name:    "rejects an object within an actor snapshot",
			uri:     "gs://bucket/root/atespaces/team-a/actors/actor-uid/snapshots/snap-1/manifest.json",
			wantErr: true,
		},
		{
			name:    "rejects an object within a tag snapshot",
			uri:     "gs://bucket/root/atespaces/team-a/tags/snap-1/manifest.json",
			wantErr: true,
		},
		{
			name:    "rejects the old flat layout",
			uri:     "gs://bucket/root/snapshots/team-a/snap-1",
			wantErr: true,
		},
		{
			name:    "rejects an unknown owner kind",
			uri:     "gs://bucket/root/atespaces/team-a/workers/worker-uid",
			wantErr: true,
		},
		{
			name:    "rejects an actor snapshot with no atespaces segment",
			uri:     "gs://bucket/root/team-a/actors/actor-uid/snapshots/snap-1",
			wantErr: true,
		},
		{name: "rejects a bare name", uri: "snap-1", wantErr: true},
		{name: "rejects an empty URI", uri: "", wantErr: true},
		{
			name:    "rejects a missing snapshot name",
			uri:     "gs://bucket/root/atespaces/team-a/actors/actor-uid/snapshots/",
			wantErr: true,
		},
		{
			name:    "rejects a missing bucket",
			uri:     "/root/atespaces/team-a/actors/actor-uid/snapshots/snap-1",
			wantErr: true,
		},
		{
			name:    "rejects a name that is not a resource name",
			uri:     "gs://bucket/root/atespaces/team-a/actors/actor-uid/snapshots/2026-08-05T10:04:05Z-ABCDEFG",
			wantErr: true,
		},
		{
			name:    "rejects an atespace that is not a resource name",
			uri:     "gs://bucket/root/atespaces/Team_A/actors/actor-uid/snapshots/snap-1",
			wantErr: true,
		},
		{
			name:    "rejects an actor UID that is not a resource name",
			uri:     "gs://bucket/root/atespaces/team-a/actors/Actor_UID/snapshots/snap-1",
			wantErr: true,
		},
		{
			name:    "rejects a URI with a trailing query",
			uri:     "gs://bucket/root/atespaces/team-a/actors/actor-uid/snapshots/snap-1?generation=1",
			wantErr: true,
		},
		{
			name:    "rejects a URI with a trailing fragment",
			uri:     "gs://bucket/root/atespaces/team-a/actors/actor-uid/snapshots/snap-1#frag",
			wantErr: true,
		},
		{
			name:    "rejects malformed URI syntax",
			uri:     "gs://bucket/root/atespaces/team-a/actors/actor-uid/snapshots/snap-1%zz",
			wantErr: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseSnapshotURI(tc.uri)
			if (err != nil) != tc.wantErr {
				t.Fatalf("ParseSnapshotURI(%q) error = %v, wantErr %t", tc.uri, err, tc.wantErr)
			}
			if got.Location() != tc.wantLocation {
				t.Errorf("ParseSnapshotURI(%q).Location() = %q, want %q", tc.uri, got.Location(), tc.wantLocation)
			}
			if got.Atespace() != tc.wantAtespace {
				t.Errorf("ParseSnapshotURI(%q).Atespace() = %q, want %q", tc.uri, got.Atespace(), tc.wantAtespace)
			}
			if got.Name() != tc.wantName {
				t.Errorf("ParseSnapshotURI(%q).Name() = %q, want %q", tc.uri, got.Name(), tc.wantName)
			}
			if got.Owner() != tc.wantOwner {
				t.Errorf("ParseSnapshotURI(%q).Owner() = %s, want %s", tc.uri, got.Owner(), tc.wantOwner)
			}
			if tc.wantErr {
				return
			}
			// A URI is recorded and read back, so parsing has to reproduce
			// exactly what was written, prefixes included.
			if got.String() != tc.uri && got.String()+"/" != tc.uri {
				t.Errorf("ParseSnapshotURI(%q) = %q, want the URI it was given", tc.uri, got)
			}
		})
	}
}

func TestSnapshotURIObject(t *testing.T) {
	actorURI, err := NewActorSnapshotURI("gs://bucket/root", "team-a", "actor-uid", "snap-1")
	if err != nil {
		t.Fatalf("NewActorSnapshotURI: %v", err)
	}
	tagURI, err := NewTagSnapshotURI("gs://bucket/root", "team-a", "snap-2")
	if err != nil {
		t.Fatalf("NewTagSnapshotURI: %v", err)
	}
	tests := []struct {
		name       string
		uri        SnapshotURI
		objectName string
		want       string
		wantErr    bool
	}{
		{
			name:       "manifest",
			uri:        actorURI,
			objectName: "manifest.json",
			want:       "gs://bucket/root/atespaces/team-a/actors/actor-uid/snapshots/snap-1/manifest.json",
		},
		{
			name:       "image file",
			uri:        actorURI,
			objectName: "memory.img.zstd",
			want:       "gs://bucket/root/atespaces/team-a/actors/actor-uid/snapshots/snap-1/memory.img.zstd",
		},
		{
			name:       "tag snapshot object",
			uri:        tagURI,
			objectName: "manifest.json",
			want:       "gs://bucket/root/atespaces/team-a/tags/snap-2/manifest.json",
		},
		{
			name:       "malformed escape",
			uri:        actorURI,
			objectName: "100%done.img",
			wantErr:    true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.uri.ObjectURI(tc.objectName)
			if (err != nil) != tc.wantErr {
				t.Fatalf("ObjectURI(%q) error = %v, wantErr %t", tc.objectName, err, tc.wantErr)
			}
			if got != tc.want {
				t.Errorf("ObjectURI(%q) = %q, want %q", tc.objectName, got, tc.want)
			}
		})
	}
}

func TestSnapshotURIZeroValue(t *testing.T) {
	var zero SnapshotURI
	if !zero.IsZero() {
		t.Error("the zero SnapshotURI does not report IsZero")
	}
	if got := zero.String(); got != "" {
		t.Errorf("zero SnapshotURI renders as %q, want the empty string", got)
	}
	if !zero.Prefix().IsZero() {
		t.Errorf("zero SnapshotURI has prefix %q, want the zero prefix", zero.Prefix())
	}
	if !zero.OwnerPrefix().IsZero() {
		t.Errorf("zero SnapshotURI has owner prefix %q, want the zero prefix", zero.OwnerPrefix())
	}
	uri, err := NewActorSnapshotURI("gs://bucket/root", "team-a", "actor-uid", "snap-1")
	if err != nil {
		t.Fatalf("NewActorSnapshotURI: %v", err)
	}
	if uri.IsZero() {
		t.Errorf("%q reports IsZero", uri)
	}
}
