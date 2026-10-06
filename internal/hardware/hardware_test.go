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

package hardware

import (
	"runtime"
	"testing"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

func TestProbeHost(t *testing.T) {
	hw := ProbeHost()
	if got := hw.GetAttributes()[AttrArchitecture]; got != runtime.GOARCH {
		t.Errorf("ProbeHost() architecture = %q, want %q", got, runtime.GOARCH)
	}
}

func TestMatches(t *testing.T) {
	amd64Worker := &ateapipb.Hardware{Attributes: map[string]string{AttrArchitecture: "amd64"}}
	arm64Worker := &ateapipb.Hardware{Attributes: map[string]string{AttrArchitecture: "arm64"}}

	tests := []struct {
		name   string
		worker *ateapipb.Hardware
		snap   *ateapipb.Hardware
		want   bool
	}{
		{"nil snapshot imposes no constraint", amd64Worker, nil, true},
		{"empty snapshot imposes no constraint", amd64Worker, &ateapipb.Hardware{}, true},
		{"same architecture matches", amd64Worker, amd64Worker, true},
		{"different architecture fails", arm64Worker, amd64Worker, false},
		{"nil worker fails when snapshot is stamped", nil, amd64Worker, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := Matches(tc.worker, tc.snap); got != tc.want {
				t.Errorf("Matches() = %v, want %v", got, tc.want)
			}
		})
	}
}
