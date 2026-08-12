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

func TestNewSnapshotURI(t *testing.T) {
	tests := []struct {
		name     string
		location string
		atespace string
		snapshot string
		want     string
		wantErr  bool
	}{
		{
			name: "no trailing slash", location: "gs://bucket/root", atespace: "team-a", snapshot: "snap-1",
			want: "gs://bucket/root/snapshots/team-a/snap-1",
		},
		{
			name: "trailing slash", location: "gs://bucket/root/", atespace: "team-a", snapshot: "snap-1",
			want: "gs://bucket/root/snapshots/team-a/snap-1",
		},
		{
			name: "bucket only", location: "gs://bucket", atespace: "team-a", snapshot: "snap-1",
			want: "gs://bucket/snapshots/team-a/snap-1",
		},
		{
			name: "location containing a snapshots segment", location: "gs://my-bucket/snapshots/secret-agent", atespace: "team-a", snapshot: "snap-1",
			want: "gs://my-bucket/snapshots/secret-agent/snapshots/team-a/snap-1",
		},
		{name: "empty location", location: "", atespace: "team-a", snapshot: "snap-1", wantErr: true},
		{name: "location without a bucket", location: "/root", atespace: "team-a", snapshot: "snap-1", wantErr: true},
		{
			name: "location with a query", location: "gs://bucket/root?generation=1", atespace: "team-a", snapshot: "snap-1", wantErr: true,
		},
		{name: "invalid atespace", location: "gs://bucket/root", atespace: "Team_A", snapshot: "snap-1", wantErr: true},
		{name: "empty atespace", location: "gs://bucket/root", atespace: "", snapshot: "snap-1", wantErr: true},
		{name: "invalid snapshot name", location: "gs://bucket/root", atespace: "team-a", snapshot: "2026-08-05T10:04:05Z", wantErr: true},
		{name: "empty snapshot name", location: "gs://bucket/root", atespace: "team-a", snapshot: "", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := NewSnapshotURI(tc.location, tc.atespace, tc.snapshot)
			if (err != nil) != tc.wantErr {
				t.Fatalf("NewSnapshotURI(%q, %q, %q) error = %v, wantErr %t", tc.location, tc.atespace, tc.snapshot, err, tc.wantErr)
			}
			if got.String() != tc.want {
				t.Errorf("NewSnapshotURI(%q, %q, %q) = %q, want %q", tc.location, tc.atespace, tc.snapshot, got, tc.want)
			}
			if tc.wantErr && !got.IsZero() {
				t.Errorf("NewSnapshotURI(%q, %q, %q) returned %q alongside an error, want the zero value", tc.location, tc.atespace, tc.snapshot, got)
			}
		})
	}
}

func TestParseSnapshotURI(t *testing.T) {
	tests := []struct {
		name         string
		uri          string
		wantLocation string
		wantAtespace string
		wantName     string
		wantErr      bool
	}{
		{
			name:         "reads all three parts",
			uri:          "gs://bucket/root/snapshots/team-a/f47ac10b-58cc-4372-a567-0e02b2c3d479",
			wantLocation: "gs://bucket/root",
			wantAtespace: "team-a",
			wantName:     "f47ac10b-58cc-4372-a567-0e02b2c3d479",
		},
		{
			name:         "tolerates a trailing slash",
			uri:          "gs://bucket/root/snapshots/team-a/snap-1/",
			wantLocation: "gs://bucket/root",
			wantAtespace: "team-a",
			wantName:     "snap-1",
		},
		{
			name:         "bucket-only location",
			uri:          "gs://bucket/snapshots/team-a/snap-1",
			wantLocation: "gs://bucket",
			wantAtespace: "team-a",
			wantName:     "snap-1",
		},
		{
			name:         "location containing a snapshots segment",
			uri:          "gs://my-bucket/snapshots/secret-agent/snapshots/team-a/snap-1",
			wantLocation: "gs://my-bucket/snapshots/secret-agent",
			wantAtespace: "team-a",
			wantName:     "snap-1",
		},
		{
			name:    "rejects a URI with no atespace segment",
			uri:     "gs://bucket/root/snapshots/snap-1",
			wantErr: true,
		},
		{
			name:         "atespace named snapshots",
			uri:          "gs://bucket/root/snapshots/snapshots/snap-1",
			wantLocation: "gs://bucket/root",
			wantAtespace: "snapshots",
			wantName:     "snap-1",
		},
		{
			name:    "rejects an object within a snapshot",
			uri:     "gs://bucket/root/snapshots/team-a/snap-1/manifest.json",
			wantErr: true,
		},
		{name: "rejects a URI with no snapshots segment", uri: "gs://bucket/root/team-a/snap-1", wantErr: true},
		{name: "rejects a bare name", uri: "snap-1", wantErr: true},
		{name: "rejects an empty URI", uri: "", wantErr: true},
		{name: "rejects a missing name", uri: "gs://bucket/root/snapshots/team-a/", wantErr: true},
		{name: "rejects a missing bucket", uri: "/root/snapshots/team-a/snap-1", wantErr: true},
		{
			name:    "rejects a name that is not a resource name",
			uri:     "gs://bucket/root/snapshots/team-a/2026-08-05T10:04:05Z-ABCDEFG",
			wantErr: true,
		},
		{name: "rejects an atespace that is not a resource name", uri: "gs://bucket/root/snapshots/Team_A/snap-1", wantErr: true},
		{
			name:    "rejects a URI with a trailing query",
			uri:     "gs://bucket/root/snapshots/team-a/snap-1?generation=1",
			wantErr: true,
		},
		{name: "rejects a URI with a trailing fragment", uri: "gs://bucket/root/snapshots/team-a/snap-1#frag", wantErr: true},
		{name: "rejects malformed URI syntax", uri: "gs://bucket/root/snapshots/team-a/snap-1%zz", wantErr: true},
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
		})
	}
}

func TestSnapshotURIObject(t *testing.T) {
	uri, err := NewSnapshotURI("gs://bucket/root", "team-a", "snap-1")
	if err != nil {
		t.Fatalf("NewSnapshotURI: %v", err)
	}
	tests := []struct {
		name       string
		objectName string
		want       string
		wantErr    bool
	}{
		{
			name: "manifest", objectName: "manifest.json",
			want: "gs://bucket/root/snapshots/team-a/snap-1/manifest.json",
		},
		{
			name: "image file", objectName: "memory.img.zstd",
			want: "gs://bucket/root/snapshots/team-a/snap-1/memory.img.zstd",
		},
		{
			name: "malformed escape", objectName: "100%done.img", wantErr: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := uri.ObjectURI(tc.objectName)
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
	uri, err := NewSnapshotURI("gs://bucket/root", "team-a", "snap-1")
	if err != nil {
		t.Fatalf("NewSnapshotURI: %v", err)
	}
	if uri.IsZero() {
		t.Errorf("%q reports IsZero", uri)
	}
}
