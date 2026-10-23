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

package gcsboundary

import (
	"fmt"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"

	"go.ate.dev/substrate/internal/objectstoreplugin/actorscope"
	"go.ate.dev/substrate/internal/resources"
)

func prefix(t *testing.T, uri string) resources.StoragePrefix {
	t.Helper()
	p, err := resources.ParseStoragePrefix(uri)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestFromGrants(t *testing.T) {
	grants := []actorscope.Grant{
		{Prefix: prefix(t, "gs://snaps/ate-demo/atespaces/team-a/actors/9f1e"), Access: actorscope.ReadWrite},
		{Prefix: prefix(t, "gs://snaps/ate-demo/atespaces/ate-golden/tags/7a3c"), Access: actorscope.ReadOnly},
	}
	b, err := FromGrants(grants)
	if err != nil {
		t.Fatalf("FromGrants: %v", err)
	}
	want := []Rule{
		{
			Resource:    "//storage.googleapis.com/projects/_/buckets/snaps",
			Permissions: []string{"inRole:roles/storage.objectAdmin"},
			Condition:   "resource.name.startsWith('projects/_/buckets/snaps/objects/ate-demo/atespaces/team-a/actors/9f1e/')",
		},
		{
			Resource:    "//storage.googleapis.com/projects/_/buckets/snaps",
			Permissions: []string{"inRole:roles/storage.objectViewer"},
			Condition:   "resource.name.startsWith('projects/_/buckets/snaps/objects/ate-demo/atespaces/ate-golden/tags/7a3c/')",
		},
	}
	if diff := cmp.Diff(want, b); diff != "" {
		t.Errorf("rules (-want +got):\n%s", diff)
	}
}

func TestFromGrants_Refuses(t *testing.T) {
	for _, tc := range []struct {
		name   string
		grants []actorscope.Grant
	}{
		{"not gs", []actorscope.Grant{{Prefix: prefix(t, "s3://b/p"), Access: actorscope.ReadOnly}}},
		{"zero prefix", []actorscope.Grant{{Prefix: resources.StoragePrefix{}, Access: actorscope.ReadOnly}}},
		{"unknown access", []actorscope.Grant{{Prefix: prefix(t, "gs://b/p"), Access: 0}}},
		{"no grants", nil},
		{"more than ten grants", manyGrants(t, 11)},
		{"a condition over 512 bytes", []actorscope.Grant{longGrant(t, 513)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := FromGrants(tc.grants); err == nil {
				t.Fatal("accepted, want error")
			}
		})
	}
}

func TestFromGrants_EscapesTheCELLiteral(t *testing.T) {
	// Substrate accepts a quote in a storage location, so the condition must
	// escape it rather than let it end the string literal.
	rules, err := FromGrants([]actorscope.Grant{{Prefix: prefix(t, `gs://snaps/tenant's/atespaces/team-a/actors/9f1e`), Access: actorscope.ReadWrite}})
	if err != nil {
		t.Fatalf("FromGrants: %v", err)
	}
	want := `resource.name.startsWith('projects/_/buckets/snaps/objects/tenant\'s/atespaces/team-a/actors/9f1e/')`
	if got := rules[0].Condition; got != want {
		t.Errorf("expression = %s, want %s", got, want)
	}
	if got := celEscape(`a\b'c`); got != `a\\b\'c` {
		t.Errorf("celEscape = %s", got)
	}
}

func manyGrants(t *testing.T, n int) []actorscope.Grant {
	var gs []actorscope.Grant
	for i := range n {
		gs = append(gs, actorscope.Grant{Prefix: prefix(t, fmt.Sprintf("gs://b/p%d", i)), Access: actorscope.ReadOnly})
	}
	return gs
}

// longGrant is a grant whose condition literal is n bytes:
// "projects/_/buckets/b/objects/" and the path with its trailing slash.
func longGrant(t *testing.T, n int) actorscope.Grant {
	path := strings.Repeat("p", n-len("projects/_/buckets/b/objects/")-1)
	return actorscope.Grant{Prefix: prefix(t, "gs://b/"+path), Access: actorscope.ReadWrite}
}

func TestFromGrants_AcceptsA512ByteCondition(t *testing.T) {
	rules, err := FromGrants([]actorscope.Grant{longGrant(t, 512)})
	if err != nil {
		t.Fatalf("FromGrants: %v", err)
	}
	if got := len(rules[0].Condition) - len("resource.name.startsWith('')"); got != 512 {
		t.Fatalf("literal is %d bytes, want 512", got)
	}
}
