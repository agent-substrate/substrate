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

package inner

import "testing"

func TestParseActorSplitsTheSpiffeID(t *testing.T) {
	for _, tc := range []struct {
		name              string
		uri               string
		atespace, actorNm string
	}{
		{"a real one", "spiffe://substrate-actor.local/atespace/demo/actor/egress-demo", "demo", "egress-demo"},
		{"names with hyphens", "spiffe://substrate-actor.local/atespace/acme-prod/actor/invoice-agent", "acme-prod", "invoice-agent"},

		// Everything below must yield "", "".
		{"unasserted", "", "", ""},
		{"wrong trust domain", "spiffe://substrate-pod.local/atespace/demo/actor/egress-demo", "", ""},
		{"not spiffe", "https://substrate-actor.local/atespace/demo/actor/egress-demo", "", ""},
		{"no actor segment", "spiffe://substrate-actor.local/atespace/demo", "", ""},
		{"empty atespace", "spiffe://substrate-actor.local/atespace//actor/egress-demo", "", ""},
		{"empty actor name", "spiffe://substrate-actor.local/atespace/demo/actor/", "", ""},
		{"trailing path segment", "spiffe://substrate-actor.local/atespace/demo/actor/egress-demo/extra", "", ""},
		{"prefix only", "spiffe://substrate-actor.local/atespace/", "", ""},
		// A name that merely contains an actor URI is not one. This is the
		// case a prefix or substring match on Actor would get wrong.
		{"embedded, not prefixed", "spiffe://evil.example/x/spiffe://substrate-actor.local/atespace/demo/actor/root", "", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			atespace, actorNm := parseActor(tc.uri)
			if atespace != tc.atespace || actorNm != tc.actorNm {
				t.Errorf("parseActor(%q) = %q, %q; want %q, %q", tc.uri, atespace, actorNm, tc.atespace, tc.actorNm)
			}
		})
	}
}
