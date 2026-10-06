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

// Package hardware probes host hardware identity and evaluates snapshot
// hardware compatibility between workers and snapshots.
package hardware

import (
	"runtime"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

const (
	// AttrArchitecture is the CPU architecture ("amd64", "arm64", etc.).
	AttrArchitecture = "architecture"

	// TODO: Add AttrCPUVendor ("cpu_vendor"), AttrCPUModel ("cpu_model"), and
	// other hardware attributes as snapshot compatibility expands.
)

// ProbeHost inspects the current host and returns its Hardware attributes.
//
// TODO: Probe and populate cpu_vendor, cpu_model, and other host hardware
// attributes (e.g. via CPUID on amd64 and MIDR_EL1 on arm64).
func ProbeHost() *ateapipb.Hardware {
	return &ateapipb.Hardware{
		Attributes: map[string]string{
			AttrArchitecture: runtime.GOARCH,
		},
	}
}

// Matches reports whether worker satisfies the hardware attributes recorded on
// snap. A nil or empty snapshot Hardware imposes no constraint.
//
// TODO: Distinguish memory-restore matching (full CPU vendor/model) from
// cold-boot fallback matching (architecture only) once additional hardware
// attributes are populated.
func Matches(worker, snap *ateapipb.Hardware) bool {
	for k, v := range snap.GetAttributes() {
		if worker.GetAttributes()[k] != v {
			return false
		}
	}
	return true
}
