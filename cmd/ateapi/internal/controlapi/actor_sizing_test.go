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

package controlapi

import (
	"testing"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// TestActorResourceLimits covers the actor-side extraction: the CPU/memory limits
// an ActorTemplate declares become the sandbox size and the scheduling floor.
func TestActorResourceLimits(t *testing.T) {
	tests := []struct {
		name       string
		resources  *ateapipb.Resources
		wantCPU    int64
		wantMemory int64
	}{
		{
			name:       "nil resources yields zero",
			resources:  nil,
			wantCPU:    0,
			wantMemory: 0,
		},
		{
			name: "cpu and memory limits are read",
			resources: &ateapipb.Resources{Limits: []*ateapipb.Limits{
				{Name: "cpu", Quantity: "2"},
				{Name: "memory", Quantity: "4Gi"},
			}},
			wantCPU:    2000,
			wantMemory: 4 << 30,
		},
		{
			name: "millicpu is preserved",
			resources: &ateapipb.Resources{Limits: []*ateapipb.Limits{
				{Name: "cpu", Quantity: "1500m"},
			}},
			wantCPU:    1500,
			wantMemory: 0,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tmpl := &ateapipb.ActorTemplate{Resources: tc.resources}
			cpu, mem, err := actorResourceLimits(tmpl)
			if err != nil {
				t.Fatalf("actorResourceLimits() error: %v", err)
			}
			if cpu != tc.wantCPU || mem != tc.wantMemory {
				t.Fatalf("actorResourceLimits() = (%d, %d), want (%d, %d)", cpu, mem, tc.wantCPU, tc.wantMemory)
			}
		})
	}
}
