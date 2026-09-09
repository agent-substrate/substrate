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

package render

import "testing"

func TestExpand(t *testing.T) {
	for _, tc := range []struct {
		name   string
		body   string
		values map[string]string
		drop   []string
		want   string
	}{
		{
			name:   "substitutes every occurrence",
			body:   "bucket: ${BUCKET_NAME}\nother: ${BUCKET_NAME}\n",
			values: map[string]string{"BUCKET_NAME": "ate-snapshots"},
			want:   "bucket: ate-snapshots\nother: ate-snapshots\n",
		},
		{
			name: "drops lines naming a dropped placeholder",
			body: "spec:\n${EXTERNAL_VOLUMES}\n  replicas: 1\n",
			drop: []string{"EXTERNAL_VOLUMES"},
			want: "spec:\n  replicas: 1\n",
		},
		{
			name: "drop wins over an indented placeholder line",
			body: "args:\n    ${VALIDATE_EXISTING_FILE_PATH_ARG}\n",
			drop: []string{"VALIDATE_EXISTING_FILE_PATH_ARG"},
			want: "args:\n",
		},
		{
			name:   "multiline substitution keeps YAML structure",
			body:   "volumes:\n${EXTERNAL_VOLUMES}\n",
			values: map[string]string{"EXTERNAL_VOLUMES": "  - name: external-data\n    capacity: 1Gi"},
			want:   "volumes:\n  - name: external-data\n    capacity: 1Gi\n",
		},
		{
			name:   "leaves unknown placeholders alone",
			body:   "image: ${WORKLOAD_IMAGE}\n",
			values: map[string]string{"BUCKET_NAME": "b"},
			want:   "image: ${WORKLOAD_IMAGE}\n",
		},
		{
			name:   "preserves absence of a trailing newline",
			body:   "a: ${V}",
			values: map[string]string{"V": "1"},
			want:   "a: 1",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := string(Expand(tc.body, tc.values, tc.drop))
			if got != tc.want {
				t.Errorf("Expand() =\n%q\nwant\n%q", got, tc.want)
			}
		})
	}
}
