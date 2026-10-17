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
	"testing"

	"github.com/google/go-cmp/cmp"
	"golang.org/x/oauth2/google/downscope"

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
	want := []downscope.AccessBoundaryRule{
		{
			AvailableResource:    "//storage.googleapis.com/projects/_/buckets/snaps",
			AvailablePermissions: []string{"inRole:roles/storage.objectAdmin"},
			Condition:            &downscope.AvailabilityCondition{Expression: "resource.name.startsWith('projects/_/buckets/snaps/objects/ate-demo/atespaces/team-a/actors/9f1e/')"},
		},
		{
			AvailableResource:    "//storage.googleapis.com/projects/_/buckets/snaps",
			AvailablePermissions: []string{"inRole:roles/storage.objectViewer"},
			Condition:            &downscope.AvailabilityCondition{Expression: "resource.name.startsWith('projects/_/buckets/snaps/objects/ate-demo/atespaces/ate-golden/tags/7a3c/')"},
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
	if got := rules[0].Condition.Expression; got != want {
		t.Errorf("expression = %s, want %s", got, want)
	}
	if got := celEscape(`a\b'c`); got != `a\\b\'c` {
		t.Errorf("celEscape = %s", got)
	}
}
