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

package lint

import (
	"errors"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/agent-substrate/substrate/tools/apitool/internal/model"
)

func TestAllOf(t *testing.T) {
	finds := func(subject string) func(*model.API) ([]Finding, error) {
		return func(*model.API) ([]Finding, error) {
			return []Finding{{Subject: subject, Message: "m"}}, nil
		}
	}
	fails := func(*model.API) ([]Finding, error) {
		return nil, errors.New("boom")
	}

	tests := []struct {
		name    string
		checks  []func(*model.API) ([]Finding, error)
		want    []Finding
		wantErr bool
	}{
		{
			name:   "joins findings in order",
			checks: []func(*model.API) ([]Finding, error){finds("a"), finds("b")},
			want:   []Finding{{Subject: "a", Message: "m"}, {Subject: "b", Message: "m"}},
		},
		{
			name:    "an error drops every finding",
			checks:  []func(*model.API) ([]Finding, error){finds("a"), fails},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := allOf(tt.checks...)(&model.API{})
			if (err != nil) != tt.wantErr {
				t.Fatalf("allOf() error = %v, want an error: %v", err, tt.wantErr)
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("allOf() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
