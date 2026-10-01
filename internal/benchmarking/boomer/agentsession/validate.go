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

package agentsession

import (
	"fmt"
)

// Budget is what a script declares it will hold in the sandbox: resident
// RAM (the largest fill per array) and files in the data dir (the largest
// object per key). The data dir is tmpfs, so both come out of the actor's
// memory limit. It does not model the guest's real peak: kernel, kata-agent,
// and Go allocator transients sit on top.
type Budget struct {
	RAM  int64
	Disk int64
}

// Budgets sums the bytes a step sequence declares.
func Budgets(steps []Step) Budget {
	ramMax := map[string]int64{}
	diskMax := map[string]int64{}
	for _, s := range steps {
		for _, o := range s.Ops {
			switch o.kind {
			case opFillRAM:
				ramMax[o.key] = max(ramMax[o.key], o.bytes)
			case opIngest, opWriteDisk:
				diskMax[o.key] = max(diskMax[o.key], o.bytes)
			}
		}
	}
	var b Budget
	for _, v := range ramMax {
		b.RAM += v
	}
	for _, v := range diskMax {
		b.Disk += v
	}
	return b
}

// Validate pins a script's invariants: a usable name, a memory floor that
// covers what the script declares, unique non-empty steps with positive
// think times, and no op that consumes a sandbox object before an earlier
// step created it. A broken ordering would fail at run time with NotFound
// from glutton; this catches it when the script is loaded.
func Validate(s *Script) error {
	if !scriptNameRE.MatchString(s.Name) {
		return fmt.Errorf("script name %q must match %s", s.Name, scriptNameRE)
	}
	if len(s.Steps) == 0 {
		return fmt.Errorf("script %q has no steps", s.Name)
	}
	seen := map[string]bool{}
	ramFilled := map[string]bool{}
	diskWritten := map[string]bool{}
	for i, st := range s.Steps {
		where := fmt.Sprintf("step %d (%q)", i+1, st.Name)
		if st.Name == "" || st.Agent == "" {
			return fmt.Errorf("%s: name and agent must be set", where)
		}
		if seen[st.Name] {
			return fmt.Errorf("%s: duplicate step name", where)
		}
		seen[st.Name] = true
		if st.Think <= 0 {
			return fmt.Errorf("%s: think time must be positive", where)
		}
		if len(st.Ops) == 0 {
			return fmt.Errorf("%s: has no ops", where)
		}
		for j, o := range st.Ops {
			switch o.kind {
			case opFillRAM:
				ramFilled[o.key] = true
			case opChurnRAM, opWalkRAM:
				if !ramFilled[o.key] {
					return fmt.Errorf("%s op %d: %s of RAM array %q before any fill_ram", where, j+1, opNames[o.kind], o.key)
				}
			case opIngest, opWriteDisk:
				diskWritten[o.key] = true
			case opReadDiskDigest, opReadDiskData:
				if !diskWritten[o.key] {
					return fmt.Errorf("%s op %d: %s of %q before any ingest or write_disk", where, j+1, opNames[o.kind], o.key)
				}
			}
		}
	}
	b := Budgets(s.Steps)
	if s.MinActorMemory <= 0 {
		return fmt.Errorf("script %q: min_actor_memory is required (declared RAM %s + disk %s)", s.Name, formatSize(b.RAM), formatSize(b.Disk))
	}
	if declared := b.RAM + b.Disk; s.MinActorMemory < declared {
		return fmt.Errorf("script %q: min_actor_memory %s is below the declared RAM %s + disk %s", s.Name, formatSize(s.MinActorMemory), formatSize(b.RAM), formatSize(b.Disk))
	}
	return nil
}
