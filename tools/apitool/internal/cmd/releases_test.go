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
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestReleaseTags(t *testing.T) {
	for _, tc := range []struct {
		name  string
		tags  []string
		since []int
		want  []string
	}{
		{
			name:  "newest first, by number",
			tags:  []string{"v0.9.0", "v0.10.0", "v0.10.1"},
			since: []int{0, 0, 0},
			want:  []string{"v0.10.1", "v0.10.0", "v0.9.0"},
		},
		{
			name:  "since is included, older dropped",
			tags:  []string{"v0.5.0", "v0.6.0", "v0.7.0"},
			since: []int{0, 6, 0},
			want:  []string{"v0.7.0", "v0.6.0"},
		},
		{
			name:  "pre-releases and other tags skipped",
			tags:  []string{"v0.6.0", "v0.7.0-rc.1", "v1", "nightly"},
			since: []int{0, 0, 0},
			want:  []string{"v0.6.0"},
		},
		{
			name:  "none at or after since",
			tags:  []string{"v0.6.0"},
			since: []int{1, 0, 0},
			want:  []string{},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if diff := cmp.Diff(tc.want, releaseTags(tc.tags, tc.since)); diff != "" {
				t.Errorf("releaseTags() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
