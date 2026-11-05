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

package actorauthz

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.ate.dev/substrate/internal/objectstoreplugin/actorscope"
	"go.ate.dev/substrate/internal/resources"
)

// TestToGrants checks every actorscope.Access maps to its own Access, and an
// unknown one is refused instead of reaching a provider.
func TestToGrants(t *testing.T) {
	prefix, err := resources.ParseStoragePrefix("gs://snaps/root/atespaces/a/actors/u")
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name     string
		access   actorscope.Access
		want     []Grant
		wantCode codes.Code
	}{
		{
			name:   "read-only",
			access: actorscope.ReadOnly,
			want:   []Grant{{Prefix: "gs://snaps/root/atespaces/a/actors/u/", Access: ReadOnly}},
		},
		{
			name:   "read-write",
			access: actorscope.ReadWrite,
			want:   []Grant{{Prefix: "gs://snaps/root/atespaces/a/actors/u/", Access: ReadWrite}},
		},
		{name: "unknown", access: actorscope.ReadWrite + 1, wantCode: codes.Internal},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := toGrants([]actorscope.Grant{{Prefix: prefix, Access: tc.access}})
			if code := status.Code(err); code != tc.wantCode {
				t.Fatalf("toGrants() code = %v, want %v (err %v)", code, tc.wantCode, err)
			}
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("toGrants() (-want +got):\n%s", diff)
			}
		})
	}
}
