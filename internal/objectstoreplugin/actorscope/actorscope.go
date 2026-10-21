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

// Package actorscope derives the object-storage prefixes an actor's transfers
// may touch from the actor record, with the access each needs. It knows no
// storage provider: a provider's credential exchange turns the grants into
// whatever scoped credential its storage supports.
package actorscope

import (
	"fmt"
	"sort"

	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
)

// Access is what a grant permits under its prefix.
type Access int

const (
	// ReadOnly covers the tag or golden snapshot the actor was created from.
	ReadOnly Access = iota + 1
	// ReadWrite covers the actor's own snapshots.
	ReadWrite
)

func (a Access) String() string {
	switch a {
	case ReadOnly:
		return "ro"
	case ReadWrite:
		return "rw"
	}
	return fmt.Sprintf("Access(%d)", int(a))
}

// Grant is one prefix and the access allowed under it. Prefix is the actor's
// own prefix or a tag's prefix, as the control plane built it, so a provider
// derives its bucket and path with the same code the rest of Substrate uses.
type Grant struct {
	Prefix resources.StoragePrefix
	Access Access
}

// FromActor derives the grants from status.snapshots. Every snapshot the
// control plane writes for the actor carries a URI the control plane built, so
// the storage location and the tag prefixes come from those URIs, parsed with
// the same code that built them, and the actor's own prefix is built with the
// helper the control plane places snapshots with.
//
//   - Every durable URI names the storage location it sits under. For each
//     location seen, the actor gets ReadWrite on its own prefix there,
//     <location>/atespaces/<atespace>/actors/<uid>. So the grant exists from
//     the first generation on, before the actor has uploaded anything, and
//     a repointed actor keeps both locations.
//   - Each TAG-owned generation with a COMPLETED durable URI grants ReadOnly
//     on the tag's prefix. That is the golden or tag the actor was created
//     from, listed until the actor's own first snapshot completes.
//
// The status is taken as ate-api-server wrote it. An owner this code does not
// know is an error, so a new kind is never silently dropped. Local-only
// generations have no URI and contribute nothing.
func FromActor(actor *ateapipb.Actor) ([]Grant, error) {
	meta := actor.GetMetadata()
	self := resources.ActorSnapshotOwner(meta.GetAtespace(), meta.GetUid())
	seen := map[Grant]bool{}
	var grants []Grant
	add := func(prefix resources.StoragePrefix, access Access) {
		g := Grant{Prefix: prefix, Access: access}
		if !seen[g] {
			seen[g] = true
			grants = append(grants, g)
		}
	}
	for _, gen := range actor.GetStatus().GetSnapshots() {
		durable := gen.GetDurableSnapshot()
		uri := durable.GetObject().GetSnapshotUri()
		if uri == "" {
			continue
		}
		parsed, err := resources.ParseSnapshotURI(uri)
		if err != nil {
			return nil, fmt.Errorf("snapshot generation %d of actor %s/%s: %w", gen.GetGeneration(), meta.GetAtespace(), meta.GetName(), err)
		}
		own, err := self.Prefix(parsed.Location())
		if err != nil {
			return nil, fmt.Errorf("snapshot generation %d of actor %s/%s: %w", gen.GetGeneration(), meta.GetAtespace(), meta.GetName(), err)
		}
		add(own, ReadWrite)
		switch gen.GetOwner() {
		case ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR:
			// Its own prefix is already granted above.
		case ateapipb.SnapshotOwner_SNAPSHOT_OWNER_TAG:
			if durable.GetStatus() != ateapipb.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED {
				continue
			}
			add(parsed.OwnerPrefix(), ReadOnly)
		default:
			return nil, fmt.Errorf("snapshot generation %d of actor %s/%s has unknown owner %v", gen.GetGeneration(), meta.GetAtespace(), meta.GetName(), gen.GetOwner())
		}
	}
	// ReadWrite first, then by prefix, so equal inputs give equal output.
	sort.SliceStable(grants, func(i, j int) bool {
		if grants[i].Access != grants[j].Access {
			return grants[i].Access > grants[j].Access
		}
		return grants[i].Prefix.String() < grants[j].Prefix.String()
	})
	return grants, nil
}
