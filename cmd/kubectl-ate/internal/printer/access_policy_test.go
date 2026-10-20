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

package printer

import (
	"bytes"
	"strings"
	"testing"

	"go.ate.dev/substrate/api/ateapi/v1"
)

func TestPrintAccessPolicyTo(t *testing.T) {
	policy := &ateapi.AccessPolicy{
		Metadata: &ateapi.ResourceMetadata{Name: "default", Uid: "u-1", Version: 2},
		Bindings: []*ateapi.Binding{
			{Role: "owner", Members: []string{"user:alice", "user:bob"}},
			{Role: "viewer", Members: []string{"user:carol"}},
		},
	}
	tests := []struct {
		name     string
		atespace string
		policy   *ateapi.AccessPolicy
		format   string
		want     []string
		wantErr  bool
	}{
		{
			name: "table", atespace: "team-a", policy: policy, format: "table",
			want: []string{"SCOPE", "team-a   owner    user:alice,user:bob", "team-a   viewer   user:carol"},
		},
		{
			name: "global table", policy: policy, format: "table",
			want: []string{"<global>   owner"},
		},
		{
			name: "no bindings", atespace: "team-a", policy: &ateapi.AccessPolicy{}, format: "table",
			want: []string{"team-a   <none>   <none>"},
		},
		{
			name: "yaml carries preconditions", atespace: "team-a", policy: policy, format: "yaml",
			want: []string{"uid: u-1", `version: "2"`, "role: owner"},
		},
		{name: "unknown format", policy: policy, format: "xml", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var out bytes.Buffer
			err := PrintAccessPolicyTo(&out, tc.atespace, tc.policy, tc.format)
			if tc.wantErr {
				if err == nil {
					t.Fatal("PrintAccessPolicyTo() succeeded, want an error")
				}
				return
			}
			if err != nil {
				t.Fatalf("PrintAccessPolicyTo() error = %v", err)
			}
			for _, w := range tc.want {
				if !strings.Contains(out.String(), w) {
					t.Errorf("output = %q, want it to contain %q", out.String(), w)
				}
			}
		})
	}
}
