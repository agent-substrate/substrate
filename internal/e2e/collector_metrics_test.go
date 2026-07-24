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
	"sort"
	"testing"
)

// A trimmed sample of what the Collector's prometheus exporter emits: HELP/TYPE
// comments plus suffixed series lines, so the matcher is exercised against the
// real exposition shape.
const sampleScrape = `# HELP ate_actor_lifecycle_operation_duration_seconds Duration of an actor lifecycle operation.
# TYPE ate_actor_lifecycle_operation_duration_seconds histogram
ate_actor_lifecycle_operation_duration_seconds_bucket{ate_actor_operation_name="resume",le="0.1"} 2
ate_actor_lifecycle_operation_duration_seconds_count{ate_actor_operation_name="resume"} 2
# TYPE ate_workerpool_workers gauge
ate_workerpool_workers{ate_workerpool_name="pool-a",ate_worker_state="idle"} 3
# TYPE atenet_router_route_duration_seconds histogram
atenet_router_route_duration_seconds_count 1
# TYPE atelet_snapshot_size_bytes histogram
atelet_snapshot_size_bytes_count 1
# TYPE ate_actor_restored_total counter
ate_actor_restored_total 5
`

func TestMissingPlatformMetrics(t *testing.T) {
	tests := []struct {
		name     string
		scrape   string
		prefixes []string
		want     []string
	}{
		{
			name:   "all present via suffix, exact, and comment forms",
			scrape: sampleScrape,
			prefixes: []string{
				"ate_actor_lifecycle_operation_duration",
				"ate_workerpool_workers",
				"atenet_router_route_duration",
				"atelet_snapshot_size",
			},
			want: nil,
		},
		{
			name:     "absent prefix is reported",
			scrape:   sampleScrape,
			prefixes: []string{"ate_scheduler_assignment_duration"},
			want:     []string{"ate_scheduler_assignment_duration"},
		},
		{
			name:     "underscore boundary avoids restore matching restored",
			scrape:   sampleScrape,
			prefixes: []string{"ate_actor_restore_duration"},
			want:     []string{"ate_actor_restore_duration"},
		},
		{
			name:     "empty scrape misses everything",
			scrape:   "",
			prefixes: []string{"ate_workerpool_workers", "atelet_snapshot_size"},
			want:     []string{"ate_workerpool_workers", "atelet_snapshot_size"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := MissingPlatformMetrics(tt.scrape, tt.prefixes)
			sort.Strings(got)
			sort.Strings(tt.want)
			if len(got) != len(tt.want) {
				t.Fatalf("missing = %v, want %v", got, tt.want)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Fatalf("missing = %v, want %v", got, tt.want)
				}
			}
		})
	}
}

func TestMetricNameFromLine(t *testing.T) {
	tests := []struct {
		line string
		want string
	}{
		{"ate_workerpool_workers{pool=\"a\"} 3", "ate_workerpool_workers"},
		{"ate_scheduler_assignment_duration_seconds_count 7", "ate_scheduler_assignment_duration_seconds_count"},
		{"# TYPE ate_workerpool_workers gauge", "ate_workerpool_workers"},
		{"# HELP ate_workerpool_workers Number of workers.", "ate_workerpool_workers"},
		{"   ", ""},
		{"# some other comment", ""},
	}
	for _, tt := range tests {
		if got := metricNameFromLine(tt.line); got != tt.want {
			t.Errorf("metricNameFromLine(%q) = %q, want %q", tt.line, got, tt.want)
		}
	}
}
