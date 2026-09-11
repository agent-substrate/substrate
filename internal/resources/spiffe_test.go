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

import "testing"

func TestActorSPIFFEIDRoundTrip(t *testing.T) {
	ref := ActorRef{Atespace: "team-a", Name: "agent-7"}
	id := ActorSPIFFEID(ref)
	if want := "spiffe://substrate-actor.local/atespace/team-a/actor/agent-7"; id.String() != want {
		t.Fatalf("ActorSPIFFEID(%v) = %q, want %q", ref, id, want)
	}
	got, err := ActorRefFromSPIFFEID(id.String())
	if err != nil {
		t.Fatalf("ActorRefFromSPIFFEID(%q): %v", id, err)
	}
	if got != ref {
		t.Errorf("ActorRefFromSPIFFEID(%q) = %v, want %v", id, got, ref)
	}
}

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
		if ref, err := ActorRefFromSPIFFEID(id); err == nil {
			t.Errorf("ActorRefFromSPIFFEID(%q) = %v, want error", id, ref)
		}
	}
}
