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

	"github.com/google/go-cmp/cmp"
)

func TestActorRefFromSPIFFEIDRejects(t *testing.T) {
	for _, id := range []string{
		"",
		"spiffe://substrate-actor.local",
		"spiffe://substrate-actor.local/atespace/team/actor",
		"spiffe://substrate-actor.local/atespace/team/actor/agent/extra",
		"spiffe://substrate-actor.local/namespace/team/actor/agent",
		"spiffe://substrate-actor.local/atespace/team/pod/agent",
		"spiffe://substrate-actor.local/atespace//actor/agent",
		"spiffe://substrate-actor.local/atespace/Team/actor/agent",
		"spiffe://substrate-actor.local/atespace/team/actor/agent?x=1",
		"spiffe://substrate-actor.local/atespace/team/actor/agent#f",
		"spiffe://cluster.local/atespace/team/actor/agent",
		"spiffe://user@substrate-actor.local/atespace/team/actor/agent",
		"https://substrate-actor.local/atespace/team/actor/agent",
		"substrate-actor.local/atespace/team/actor/agent",
		"spiffe://substrate-actor.local/atespace/team/actor/agent/",
		"spiffe://substrate-actor.local/atespace/te%2Fam/actor/agent",
	} {
		if ref, err := ActorRefFromAteomForActorSPIFFEID(id); err == nil {
			t.Errorf("ActorRefFromSPIFFEID(%q) = %v, want error", id, ref)
		}
	}
}

func TestActorRoundtrip(t *testing.T) {
	testCases := []struct {
		uri     string
		wantRef ActorRef
	}{
		{
			uri: "spiffe://substrate-actor.local/actor/foo/bar",
			wantRef: ActorRef{
				Atespace: "foo",
				Name:     "bar",
			},
		},
		{
			uri: "spiffe://substrate-actor.local/actor/fd6cab8c-17c8-4c9e-8893-28e28aff724b/045841a7-5dcb-47eb-a76f-6d8460bfe009",
			wantRef: ActorRef{
				Atespace: "fd6cab8c-17c8-4c9e-8893-28e28aff724b",
				Name:     "045841a7-5dcb-47eb-a76f-6d8460bfe009",
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.uri, func(t *testing.T) {
			ref, err := ActorRefFromActorSPIFFEID(tc.uri)
			if err != nil {
				t.Fatalf("Unexpected error parsing uri: %v", err)
			}

			if diff := cmp.Diff(ref, tc.wantRef); diff != "" {
				t.Fatalf("Bad ref; diff (-got +want)\n%s", diff)
			}

			gotURI := ActorSPIFFEID(ref)
			if diff := cmp.Diff(gotURI.String(), tc.uri); diff != "" {
				t.Fatalf("URI didn't round-trip; diff (-got +want)\n%s", diff)
			}
		})
	}
}

func TestAteomForActorRoundtrip(t *testing.T) {
	testCases := []struct {
		uri     string
		wantRef ActorRef
	}{
		{
			uri: "spiffe://substrate-actor.local/ateom-for-actor/foo/bar",
			wantRef: ActorRef{
				Atespace: "foo",
				Name:     "bar",
			},
		},
		{
			uri: "spiffe://substrate-actor.local/ateom-for-actor/fd6cab8c-17c8-4c9e-8893-28e28aff724b/045841a7-5dcb-47eb-a76f-6d8460bfe009",
			wantRef: ActorRef{
				Atespace: "fd6cab8c-17c8-4c9e-8893-28e28aff724b",
				Name:     "045841a7-5dcb-47eb-a76f-6d8460bfe009",
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.uri, func(t *testing.T) {
			ref, err := ActorRefFromAteomForActorSPIFFEID(tc.uri)
			if err != nil {
				t.Fatalf("Unexpected error parsing uri: %v", err)
			}

			if diff := cmp.Diff(ref, tc.wantRef); diff != "" {
				t.Fatalf("Bad ref; diff (-got +want)\n%s", diff)
			}

			gotURI := AteomForActorSPIFFEID(ref)
			if diff := cmp.Diff(gotURI.String(), tc.uri); diff != "" {
				t.Fatalf("URI didn't round-trip; diff (-got +want)\n%s", diff)
			}
		})
	}
}
