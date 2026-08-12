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
	"fmt"
	"net/url"
	"strings"

	"github.com/google/uuid"
)

const snapshotsPathSegment = "snapshots"

// NewSnapshotName returns a unique name for a new snapshot, durable or
// node-local.
func NewSnapshotName() string {
	return uuid.NewString()
}

// SnapshotURI is where one ActorSnapshot's objects live in object storage:
// an ActorTemplate's snapshotsConfig.location, plus /snapshots/<atespace>/<name>.
//
//	gs://bucket/root                              location
//	gs://bucket/root/snapshots/team-a/<name>      this URI
//	gs://bucket/root/snapshots/team-a/<name>/...  an object in the snapshot
type SnapshotURI struct {
	uri      string
	location string
	atespace string
	name     string
}

// NewSnapshotURI returns the URI of a snapshot of an actor in a given
// atespace, stored and under an ActorTemplate's snapshotsConfig.location.
func NewSnapshotURI(location, atespace, name string) (SnapshotURI, error) {
	if err := ValidateSnapshotLocation(location); err != nil {
		return SnapshotURI{}, err
	}
	if !IsValidResourceName(atespace) {
		return SnapshotURI{}, fmt.Errorf("invalid snapshot URI: atespace %q is not a valid resource name", atespace)
	}
	if !IsValidResourceName(name) {
		return SnapshotURI{}, fmt.Errorf("invalid snapshot URI: snapshot name %q is not a valid resource name", name)
	}
	uri, err := url.JoinPath(location, snapshotsPathSegment, atespace, name)
	if err != nil {
		return SnapshotURI{}, fmt.Errorf("invalid snapshot URI: %w", err)
	}
	return SnapshotURI{uri: uri, location: location, atespace: atespace, name: name}, nil
}

// ParseSnapshotURI parses a given snapshot URI.
func ParseSnapshotURI(uri string) (SnapshotURI, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return SnapshotURI{}, fmt.Errorf("invalid snapshot URI %q: %v", uri, err)
	}

	segments := strings.Split(strings.TrimSuffix(u.Path, "/"), "/")
	if len(segments) < 3 || segments[len(segments)-3] != snapshotsPathSegment {
		return SnapshotURI{}, fmt.Errorf("invalid snapshot URI %q", uri)
	}
	atespace, name := segments[len(segments)-2], segments[len(segments)-1]

	u.Path = strings.Join(segments[:len(segments)-3], "/")
	return NewSnapshotURI(u.String(), atespace, name)
}

// Location returns the ActorTemplate snapshotsConfig.location this snapshot is stored under.
func (u SnapshotURI) Location() string { return u.location }

// Atespace returns the atespace of the actor the snapshot was taken from.
func (u SnapshotURI) Atespace() string { return u.atespace }

// Name returns the snapshot's resource name.
func (u SnapshotURI) Name() string { return u.name }

// IsZero reports whether u is the zero SnapshotURI.
func (u SnapshotURI) IsZero() bool { return u == SnapshotURI{} }

func (u SnapshotURI) String() string { return u.uri }

// ObjectURI returns the address of a single object stored within the
// snapshot.
func (u SnapshotURI) ObjectURI(name string) (string, error) {
	return url.JoinPath(u.String(), name)
}
