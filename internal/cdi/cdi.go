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

// Package cdi reads a Container Device Interface spec and resolves what a named
// set of devices asks a container runtime to do.
//
// An ateom cannot leave this to the container runtime. containerd applies CDI to
// the pod's containers, and an actor's containers are created by the ateom
// underneath one of them, so whatever hands devices to actors reads the spec
// itself. That holds for any sandbox, and under DRA too, which names allocated
// devices as CDI device IDs.
//
// Applying the edits is a separate step and is not generic: it depends on how the
// sandbox gets an OCI spec and whether it shares the host's device nodes. See
// cmd/ateom-gvisor/internal/cdiinject for the gVisor one.
package cdi

import (
	"encoding/json"
	"fmt"
	"slices"
)

// Spec is the part of a CDI spec that is consumed here: the devices and the
// edits applied to every container.
type Spec struct {
	Devices []Device `json:"devices"`
	// ContainerEdits apply to any container the spec touches, whichever devices
	// were named (driver libraries, control device nodes, env).
	ContainerEdits Edits `json:"containerEdits"`
}

// Device is one addressable device in a spec. Generators name the same
// underlying hardware several ways -- nvidia-ctk emits per-index ("0"), per-UUID
// and an "all" device that repeats every node -- so applying every device in a
// spec injects each node several times over.
type Device struct {
	Name           string `json:"name"`
	ContainerEdits Edits  `json:"containerEdits"`
}

// Edits are the changes a device makes to a container.
type Edits struct {
	Env         []string `json:"env,omitempty"`
	DeviceNodes []Dev    `json:"deviceNodes,omitempty"`
	Mounts      []Mount  `json:"mounts,omitempty"`
	Hooks       []Hook   `json:"hooks,omitempty"`
}

// Dev is a device node to create in the container. Major and Minor are often
// absent: CDI leaves resolving them to the OCI runtime, which stats the host.
type Dev struct {
	Path  string `json:"path"`
	Type  string `json:"type,omitempty"`
	Major int64  `json:"major,omitempty"`
	Minor int64  `json:"minor,omitempty"`
}

// Mount is a host path to mount into the container.
type Mount struct {
	HostPath      string   `json:"hostPath"`
	ContainerPath string   `json:"containerPath"`
	Type          string   `json:"type,omitempty"`
	Options       []string `json:"options,omitempty"`
}

// Hook is a lifecycle hook the spec asks the runtime to run.
type Hook struct {
	HookName string   `json:"hookName"`
	Path     string   `json:"path"`
	Args     []string `json:"args,omitempty"`
	Env      []string `json:"env,omitempty"`
}

// Parse reads a CDI spec in JSON form.
func Parse(data []byte) (*Spec, error) {
	var spec Spec
	if err := json.Unmarshal(data, &spec); err != nil {
		return nil, fmt.Errorf("parsing CDI spec: %w", err)
	}
	return &spec, nil
}

// EditsFor collects the spec-level edits plus those of the named devices. A name
// the spec does not carry is an error rather than a silent no-op: it means the
// caller and the generator disagree about what the host has.
func (s *Spec) EditsFor(devices []string) (Edits, error) {
	// Cloned, not copied: a struct copy shares the spec's backing arrays, so
	// appending here could write into them.
	edits := Edits{
		Env:         slices.Clone(s.ContainerEdits.Env),
		DeviceNodes: slices.Clone(s.ContainerEdits.DeviceNodes),
		Mounts:      slices.Clone(s.ContainerEdits.Mounts),
		Hooks:       slices.Clone(s.ContainerEdits.Hooks),
	}
	for _, want := range devices {
		i := -1
		for j := range s.Devices {
			if s.Devices[j].Name == want {
				i = j
				break
			}
		}
		if i < 0 {
			return Edits{}, fmt.Errorf("CDI spec has no %q device", want)
		}
		d := s.Devices[i].ContainerEdits
		edits.Env = append(edits.Env, d.Env...)
		edits.DeviceNodes = append(edits.DeviceNodes, d.DeviceNodes...)
		edits.Mounts = append(edits.Mounts, d.Mounts...)
		edits.Hooks = append(edits.Hooks, d.Hooks...)
	}
	return edits, nil
}
