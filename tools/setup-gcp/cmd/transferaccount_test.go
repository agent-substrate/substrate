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

package cmd

import (
	"slices"
	"testing"

	iam "google.golang.org/api/iam/v1"
)

func TestAddServiceAccountBinding(t *testing.T) {
	const member = "principal://iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/proj.svc.id.goog/subject/ns/ate-system/sa/ate-api-server"
	for _, tc := range []struct {
		name        string
		bindings    []*iam.Binding
		wantChanged bool
		wantMembers []string
	}{
		{name: "no binding yet", wantChanged: true, wantMembers: []string{member}},
		{name: "joins the role's binding", bindings: []*iam.Binding{{Role: tokenCreatorRole, Members: []string{"user:a@example.com"}}},
			wantChanged: true, wantMembers: []string{"user:a@example.com", member}},
		{name: "already a member", bindings: []*iam.Binding{{Role: tokenCreatorRole, Members: []string{member}}},
			wantMembers: []string{member}},
		{name: "a conditional binding does not count", bindings: []*iam.Binding{{Role: tokenCreatorRole, Members: []string{member}, Condition: &iam.Expr{Expression: "false"}}},
			wantChanged: true, wantMembers: []string{member}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			policy := &iam.Policy{Bindings: tc.bindings}
			if got := addServiceAccountBinding(policy, tokenCreatorRole, member); got != tc.wantChanged {
				t.Errorf("changed = %v, want %v", got, tc.wantChanged)
			}
			var got []string
			for _, b := range policy.Bindings {
				if b.Role == tokenCreatorRole && b.Condition == nil {
					got = append(got, b.Members...)
				}
			}
			if !slices.Equal(got, tc.wantMembers) {
				t.Errorf("unconditional token creator members = %v, want %v", got, tc.wantMembers)
			}
		})
	}
}

func TestTransferServiceAccountMember(t *testing.T) {
	cfg := &Config{ProjectID: "proj", TransferGSAName: "xfer"}
	if got, want := transferServiceAccountMember(cfg), "serviceAccount:xfer@proj.iam.gserviceaccount.com"; got != want {
		t.Errorf("member = %q, want %q", got, want)
	}
}
