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

const (
	atespacesPathSegment = "atespaces"
	snapshotsPathSegment = "snapshots"

	// actorsOwnerKind and tagsOwnerKind are the path segments naming the kind
	// of resource that owns a snapshot. They are what makes ownership readable
	// from an object's name, so a collector can tell whose objects it is
	// looking at without consulting the store.
	actorsOwnerKind = "actors"
	tagsOwnerKind   = "tags"
)

// NewSnapshotName returns a unique name for a new snapshot, durable or
// node-local.
func NewSnapshotName() string {
	return uuid.NewString()
}

// StoragePrefix is a validated object-storage prefix: a scheme, a bucket and a
// path, with no query, fragment or userinfo. Every object below this prefix has a name
// starting with its path, so deleting the prefix collects all of them.
type StoragePrefix struct {
	uri string
}

// IsZero reports whether p is empty
func (p StoragePrefix) IsZero() bool { return p == StoragePrefix{} }

func (p StoragePrefix) String() string { return p.uri }

// SnapshotOwner is the resource whose objects a snapshot's prefix holds: the
// Actor that took it, or the Tag that copied it. Deletion is expressed as
// "collect everything under my own prefix", so an owner cannot name another's
// objects, and a borrowed snapshot is recognized by the URI alone rather than
// by a flag the two have to keep in sync.
type SnapshotOwner struct {
	kind     string
	atespace string
	// id is the owning Actor's or Tag's UID, which no later resource reuses.
	id string
}

// ActorSnapshotOwner returns the owner of the snapshots an Actor takes. It is
// keyed on the UID rather than the name so that an Actor recreated under the
// same name never inherits its predecessor's objects.
func ActorSnapshotOwner(atespace, actorUID string) SnapshotOwner {
	return SnapshotOwner{kind: actorsOwnerKind, atespace: atespace, id: actorUID}
}

// TagSnapshotOwner returns the owner of the one snapshot a Tag holds. It is
// keyed on the UID so a recreated tag never inherits its predecessor's objects.
func TagSnapshotOwner(atespace, tagUID string) SnapshotOwner {
	return SnapshotOwner{kind: tagsOwnerKind, atespace: atespace, id: tagUID}
}

// IsZero reports whether o is the zero SnapshotOwner.
func (o SnapshotOwner) IsZero() bool { return o == SnapshotOwner{} }

// Atespace returns the atespace the owner belongs to.
func (o SnapshotOwner) Atespace() string { return o.atespace }

func (o SnapshotOwner) String() string {
	return fmt.Sprintf("%s/%s/%s", o.atespace, o.kind, o.id)
}

func (o SnapshotOwner) validate() error {
	switch o.kind {
	case actorsOwnerKind, tagsOwnerKind:
	default:
		return fmt.Errorf("invalid snapshot owner: unknown kind %q", o.kind)
	}
	if !IsValidResourceName(o.atespace) {
		return fmt.Errorf("invalid snapshot owner: atespace %q is not a valid resource name", o.atespace)
	}
	if !IsValidResourceName(o.id) {
		return fmt.Errorf("invalid snapshot owner: %q is not a valid resource name", o.id)
	}
	return nil
}

// Prefix returns the prefix holding every object this owner's snapshots are
// made of, under an ActorTemplate's snapshotsConfig.location.
func (o SnapshotOwner) Prefix(location string) (StoragePrefix, error) {
	if err := o.validate(); err != nil {
		return StoragePrefix{}, err
	}
	if err := ValidateSnapshotLocation(location); err != nil {
		return StoragePrefix{}, err
	}
	uri, err := url.JoinPath(location, atespacesPathSegment, o.atespace, o.kind, o.id)
	if err != nil {
		return StoragePrefix{}, fmt.Errorf("invalid storage prefix: %w", err)
	}
	return StoragePrefix{uri: uri}, nil
}

// SnapshotURI is where one external snapshot's objects live in object storage:
// an ActorTemplate's snapshotsConfig.location, plus the prefix of the resource
// that owns the snapshot.
//
//	gs://bucket/root                                                    location
//	gs://bucket/root/atespaces/team-a/actors/<uid>                      an Actor's prefix
//	gs://bucket/root/atespaces/team-a/actors/<uid>/snapshots/<name>     one of its snapshots
//	gs://bucket/root/atespaces/team-a/tags/<uid>                        a tag's prefix, and
//	                                                                    its only snapshot
type SnapshotURI struct {
	uri         string
	location    string
	owner       SnapshotOwner
	ownerPrefix string
	name        string
}

// NewActorSnapshotURI returns the URI of a snapshot an Actor took, stored
// under an ActorTemplate's snapshotsConfig.location.
func NewActorSnapshotURI(location, atespace, actorUID, name string) (SnapshotURI, error) {
	owner := ActorSnapshotOwner(atespace, actorUID)
	prefix, err := owner.Prefix(location)
	if err != nil {
		return SnapshotURI{}, err
	}
	if !IsValidResourceName(name) {
		return SnapshotURI{}, fmt.Errorf("invalid snapshot URI: snapshot name %q is not a valid resource name", name)
	}
	uri, err := url.JoinPath(prefix.String(), snapshotsPathSegment, name)
	if err != nil {
		return SnapshotURI{}, fmt.Errorf("invalid snapshot URI: %w", err)
	}
	return SnapshotURI{uri: uri, location: location, owner: owner, ownerPrefix: prefix.String(), name: name}, nil
}

// NewTagSnapshotURI returns the URI of the snapshot a Tag owns, stored under
// an ActorTemplate's snapshotsConfig.location. The tag's prefix and its
// snapshot's are the same: a tag holds exactly one snapshot.
func NewTagSnapshotURI(location, atespace, tagUID string) (SnapshotURI, error) {
	owner := TagSnapshotOwner(atespace, tagUID)
	prefix, err := owner.Prefix(location)
	if err != nil {
		return SnapshotURI{}, err
	}
	return SnapshotURI{uri: prefix.String(), location: location, owner: owner, ownerPrefix: prefix.String(), name: tagUID}, nil
}

// ParseSnapshotURI parses a given snapshot URI.
func ParseSnapshotURI(uri string) (SnapshotURI, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return SnapshotURI{}, fmt.Errorf("invalid snapshot URI %q: %v", uri, err)
	}

	segments := strings.Split(strings.TrimSuffix(u.Path, "/"), "/")
	n := len(segments)
	switch {
	// /root/atespaces/team-a/actors/<uid>/snapshots/<name>
	//       n-6       n-5    n-4    n-3   n-2       n-1
	case n >= 6 && segments[n-6] == atespacesPathSegment && segments[n-4] == actorsOwnerKind && segments[n-2] == snapshotsPathSegment:
		u.Path = strings.Join(segments[:n-6], "/")
		return NewActorSnapshotURI(u.String(), segments[n-5], segments[n-3], segments[n-1])
	// /root/atespaces/team-a/tags/<uid>
	//       n-4       n-3    n-2  n-1
	case n >= 4 && segments[n-4] == atespacesPathSegment && segments[n-2] == tagsOwnerKind:
		u.Path = strings.Join(segments[:n-4], "/")
		return NewTagSnapshotURI(u.String(), segments[n-3], segments[n-1])
	}
	return SnapshotURI{}, fmt.Errorf("invalid snapshot URI %q", uri)
}

// Location returns the ActorTemplate snapshotsConfig.location this snapshot is stored under.
func (u SnapshotURI) Location() string { return u.location }

// Atespace returns the atespace of the resource that owns the snapshot.
func (u SnapshotURI) Atespace() string { return u.owner.Atespace() }

// Name returns the snapshot's resource name.
func (u SnapshotURI) Name() string { return u.name }

// Owner returns the resource whose prefix the snapshot lives under.
func (u SnapshotURI) Owner() SnapshotOwner { return u.owner }

// OwnedBy reports whether the snapshot is owned by o. A collector asks this
// before deleting: an Actor whose snapshot came from a tag does not own it,
// and releasing it would break every other Actor cloned from that tag.
func (u SnapshotURI) OwnedBy(o SnapshotOwner) bool { return !u.IsZero() && u.owner == o }

// OwnerPrefix returns the prefix of the resource that owns the snapshot,
// holding this snapshot and every other that owner wrote. Collecting an owner
// takes only a URI it recorded, with no need to resolve its ActorTemplate for
// the storage location.
func (u SnapshotURI) OwnerPrefix() StoragePrefix {
	return StoragePrefix{uri: u.ownerPrefix}
}

// Prefix returns the prefix holding this one snapshot's objects.
func (u SnapshotURI) Prefix() StoragePrefix {
	if u.IsZero() {
		return StoragePrefix{}
	}
	return StoragePrefix{uri: u.uri}
}

// IsZero reports whether u is the zero SnapshotURI.
func (u SnapshotURI) IsZero() bool { return u == SnapshotURI{} }

func (u SnapshotURI) String() string { return u.uri }

// ObjectURI returns the address of a single object stored within the
// snapshot.
func (u SnapshotURI) ObjectURI(name string) (string, error) {
	return url.JoinPath(u.String(), name)
}
