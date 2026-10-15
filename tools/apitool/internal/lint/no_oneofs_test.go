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

package lint_test

import (
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/agent-substrate/substrate/tools/apitool/internal/lint"
	"github.com/agent-substrate/substrate/tools/apitool/internal/model"
)

func TestNoOneofs(t *testing.T) {
	tests := []struct {
		name string
		api  *model.API
		want []lint.Finding
	}{
		{
			name: "plain field",
			api:  messagesAPI(msg("Widget", model.Field{Name: "gadget"})),
		},
		{
			name: "proto3 optional scalar - not a real oneof",
			api:  messagesAPI(msg("Widget", model.Field{Name: "gadget", Proto3Optional: true})),
		},
		{
			name: "member of a real oneof",
			api:  messagesAPI(msg("Widget", model.Field{Name: "gadget", OneofName: "reference"})),
			want: []lint.Finding{
				{Subject: "test.Widget.gadget", Message: `belongs to oneof "reference" - oneofs are not used in this API`},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			findings, err := lint.NoOneofs.Check(tt.api)
			if err != nil {
				t.Fatalf("Check() error = %v", err)
			}
			if diff := cmp.Diff(tt.want, findings); diff != "" {
				t.Errorf("Check() findings mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
