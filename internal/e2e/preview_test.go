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

package e2e

import (
	"os"
	"slices"
	"testing"

	"k8s.io/utils/ptr"
)

func TestPreviewGates(t *testing.T) {
	for _, tc := range []struct {
		name    string
		ci      string
		value   *string
		want    []string
		wantErr bool
	}{
		{name: "unset locally"},
		{name: "unset in CI", ci: "true", wantErr: true},
		{name: "empty in CI", ci: "true", value: ptr.To("")},
		{name: "star", ci: "true", value: ptr.To("*"), want: []string{"*"}},
		{name: "list", value: ptr.To("A, B C"), want: []string{"A", "B", "C"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("CI", tc.ci)
			t.Setenv(PreviewEnv, "")
			if tc.value != nil {
				t.Setenv(PreviewEnv, *tc.value)
			} else {
				os.Unsetenv(PreviewEnv)
			}

			got, err := previewGates()
			if gotErr := err != nil; gotErr != tc.wantErr {
				t.Fatalf("previewGates() error = %v, wantErr %v", err, tc.wantErr)
			}
			if !tc.wantErr && !slices.Equal(got, tc.want) {
				t.Errorf("previewGates() = %q, want %q", got, tc.want)
			}
		})
	}
}
