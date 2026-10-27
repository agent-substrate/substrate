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

package report

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func sample() *Report {
	return &Report{
		Scenario: "EgressIdleConns", Description: "idle", Unit: "held connection", Step: 10 * time.Second,
		Rungs: []Rung{
			{
				Load:    Load{Offered: 1000, Achieved: 1000, Completed: 1000, Latency: Percentiles{P50: 2.1, P99: 6.5, Max: 150}},
				Tunnel:  Resources{Cores: 0.12, MicrosPerUnit: 1238, HeapMaxBytes: 156 << 20, HeapLiveBytes: 155 << 20, GoroutinesLive: 5100, CgroupMemEndBytes: 223 << 20, CgroupMemMaxBytes: 223 << 20},
				Verdict: VerdictOK,
			},
			{Load: Load{Offered: 8000}, Verdict: VerdictDown, Reason: "atunnel stopped answering"},
		},
		SinkCounters: map[string]any{"gateway_tunnels": 1000},
	}
}

func TestString(t *testing.T) {
	out := sample().String()
	for _, want := range []string{
		"=== EgressIdleConns: idle",
		"cg-cores", // shown because a rung has cgroup figures
		"2.10ms", "150ms",
		"holding 1000: live heap 155.0MiB after GC, 158.7 KiB and 5.1 goroutines per connection",
		"DOWN (atunnel stopped answering)",
		`sink counters: {"gateway_tunnels":1000}`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("report lacks %q:\n%s", want, out)
		}
	}
}

func TestWrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "results.json")
	if err := Write(path, []*Report{sample()}); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var got []*Report
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || len(got[0].Rungs) != 2 || got[0].Rungs[1].Verdict != VerdictDown {
		t.Errorf("round trip = %+v", got)
	}
}
