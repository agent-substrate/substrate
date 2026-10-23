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

package actorscope

import (
	"testing"

	"github.com/google/go-cmp/cmp"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/internal/resources"
)

const (
	loc      = "gs://snaps/ate-demo"
	atespace = "team-a"
	uid      = "9f1e0b2a-9e4f-4c3d-8a6b-5f2e1d0c9b8a"
	tagUID   = "7a3c0b2a-9e4f-4c3d-8a6b-5f2e1d0c9b8a"
)

func actorURI(location, name string) string {
	return location + "/atespaces/" + atespace + "/actors/" + uid + "/snapshots/" + name
}

func gen(n int32, owner ateapi.SnapshotOwner, status ateapi.SnapshotStorageStatus, uri string) *ateapi.Snapshot {
	s := &ateapi.Snapshot{Generation: n, Owner: owner}
	if uri != "" {
		s.DurableSnapshot = &ateapi.SnapshotStorage{Status: status, Object: &ateapi.ObjectSnapshot{SnapshotUri: uri}}
	} else {
		s.Locality = "node-1"
	}
	return s
}

func actorWith(snaps ...*ateapi.Snapshot) *ateapi.Actor {
	return &ateapi.Actor{
		Metadata: &ateapi.ResourceMetadata{Atespace: atespace, Name: "bot", Uid: uid},
		Status:   &ateapi.ActorStatus{Snapshots: snaps},
	}
}

// prefix builds the StoragePrefix a grant carries, from the string form the
// control plane records.
func prefix(uri string) resources.StoragePrefix {
	p, err := resources.ParseStoragePrefix(uri)
	if err != nil {
		panic(err)
	}
	return p
}

var (
	tagURI     = loc + "/atespaces/ate-golden/tags/" + tagUID
	tagGrant   = Grant{Prefix: prefix(tagURI), Access: ReadOnly}
	actorGrant = Grant{Prefix: prefix(loc + "/atespaces/" + atespace + "/actors/" + uid), Access: ReadWrite}
	completed  = ateapi.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED
	pending    = ateapi.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_PENDING
	ownerActor = ateapi.SnapshotOwner_SNAPSHOT_OWNER_ACTOR
	ownerTag   = ateapi.SnapshotOwner_SNAPSHOT_OWNER_TAG
)

func TestFromActor(t *testing.T) {
	for _, tc := range []struct {
		name  string
		actor *ateapi.Actor
		want  []Grant
	}{
		{
			name:  "no generations",
			actor: actorWith(),
			want:  nil,
		},
		{
			name:  "tag only, before the first suspend",
			actor: actorWith(gen(1, ownerTag, completed, tagURI)),
			want:  []Grant{actorGrant, tagGrant},
		},
		{
			name: "tag plus pending actor generation, during the first suspend",
			actor: actorWith(
				gen(1, ownerTag, completed, tagURI),
				gen(2, ownerActor, pending, actorURI(loc, "c0de")),
			),
			want: []Grant{actorGrant, tagGrant},
		},
		{
			name:  "actor only, after the first suspend",
			actor: actorWith(gen(2, ownerActor, completed, actorURI(loc, "c0de"))),
			want:  []Grant{actorGrant},
		},
		{
			name: "local-only generation adds nothing",
			actor: actorWith(
				gen(2, ownerActor, completed, actorURI(loc, "c0de")),
				gen(3, ownerActor, completed, ""),
			),
			want: []Grant{actorGrant},
		},
		{
			name: "two actor generations under one prefix give one grant",
			actor: actorWith(
				gen(2, ownerActor, completed, actorURI(loc, "c0de")),
				gen(3, ownerActor, pending, actorURI(loc, "d00d")),
			),
			want: []Grant{actorGrant},
		},
		{
			name: "a repointed actor keeps both locations",
			actor: actorWith(
				gen(2, ownerActor, completed, actorURI(loc, "c0de")),
				gen(3, ownerActor, pending, actorURI("gs://other-bucket/root", "d00d")),
			),
			want: []Grant{
				{Prefix: prefix("gs://other-bucket/root/atespaces/" + atespace + "/actors/" + uid), Access: ReadWrite},
				actorGrant,
			},
		},
		{
			name:  "a pending tag copy is not yet readable, but names the location",
			actor: actorWith(gen(1, ownerTag, pending, tagURI)),
			want:  []Grant{actorGrant},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := FromActor(tc.actor)
			if err != nil {
				t.Fatalf("FromActor: %v", err)
			}
			if diff := cmp.Diff(tc.want, got, cmp.AllowUnexported(resources.StoragePrefix{})); diff != "" {
				t.Errorf("grants (-want +got):\n%s", diff)
			}
		})
	}
}

func TestFromActor_RefusesUnknownOwner(t *testing.T) {
	if _, err := FromActor(actorWith(gen(1, ateapi.SnapshotOwner_SNAPSHOT_OWNER_UNSPECIFIED, completed, tagURI))); err == nil {
		t.Fatal("a generation with an unspecified owner was accepted")
	}
}

func TestFromActor_RefusesMalformedURI(t *testing.T) {
	if _, err := FromActor(actorWith(gen(2, ownerActor, completed, "gs://snaps/not/a/snapshot"))); err == nil {
		t.Fatal("a URI outside the snapshot layout was accepted")
	}
}
