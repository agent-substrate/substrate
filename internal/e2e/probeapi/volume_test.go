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

package probeapi

import "testing"

func TestValidateRetainedState(t *testing.T) {
	valid := VolumeStateResponse{Challenge: "c", Cached: "v", ByPath: VolumeObservation{Data: "v"}, Held: VolumeObservation{Data: "v"}, Mapped: VolumeObservation{Data: "v"}, Unlinked: VolumeObservation{Data: "v"}, Negative: VolumeObservation{NotExist: true, Error: "not found"}, Directory: []string{"entry"}}
	for _, tc := range []struct {
		name       string
		generation int
		mutate     func(*VolumeStateResponse)
		bad        bool
	}{
		{"first generation", 1, func(*VolumeStateResponse) {}, false},
		{"second generation", 2, func(*VolumeStateResponse) {}, false},
		{"known EIO", 2, func(v *VolumeStateResponse) {
			v.Unlinked = VolumeObservation{Error: "read: input/output error", ErrorCode: "EIO"}
		}, false},
		{"premature EIO", 1, func(v *VolumeStateResponse) {
			v.Unlinked = VolumeObservation{Error: "read: input/output error", ErrorCode: "EIO"}
		}, true},
		{"unrelated error", 2, func(v *VolumeStateResponse) { v.Unlinked = VolumeObservation{Error: "permission denied"} }, true},
		{"empty directory", 2, func(v *VolumeStateResponse) { v.Directory = nil }, true},
		{"wrong directory", 2, func(v *VolumeStateResponse) { v.Directory = []string{"other"} }, true},
		{"negative exists", 2, func(v *VolumeStateResponse) { v.Negative = VolumeObservation{Data: "leaked"} }, true},
		{"mapped mismatch", 2, func(v *VolumeStateResponse) { v.Mapped.Data = "other" }, true},
		{"held error", 2, func(v *VolumeStateResponse) { v.Held.Error = "failed" }, true},
		{"cached mismatch", 2, func(v *VolumeStateResponse) { v.Cached = "other" }, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := valid
			tc.mutate(&v)
			if err := ValidateRetainedState(v, "c", "v", tc.generation, true); (err != nil) != tc.bad {
				t.Fatalf("got %v, want bad=%v", err, tc.bad)
			}
		})
	}
}

func TestValidateRetainedStateRejectsUnapprovedEIO(t *testing.T) {
	obs := VolumeStateResponse{Challenge: "c", Cached: "v", ByPath: VolumeObservation{Data: "v"}, Held: VolumeObservation{Data: "v"}, Mapped: VolumeObservation{Data: "v"}, Unlinked: VolumeObservation{Data: "v"}, Negative: VolumeObservation{NotExist: true, Error: "not found"}, Directory: []string{"entry"}}
	if err := ValidateRetainedState(obs, "c", "v", 2, false); err != nil {
		t.Fatal(err)
	}
	obs.Unlinked = VolumeObservation{Error: "read: input/output error", ErrorCode: "EIO"}
	if err := ValidateRetainedState(obs, "c", "v", 2, false); err == nil {
		t.Fatal("unapproved deleted-open EIO accepted")
	}
}
