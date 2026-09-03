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

package ocispec

import (
	"slices"

	"github.com/agent-substrate/substrate/internal/sizing"
	"github.com/opencontainers/runtime-spec/specs-go"
)

// PauseContainer is the name of the sandbox root container.
const PauseContainer = "pause"

// resolvConf is the host resolver config bound into the sandbox.
const resolvConf = "/etc/resolv.conf"

// GVisorOptions describes the gVisor-specific context of one actor container.
type GVisorOptions struct {
	ActorUID      string
	ContainerName string
	// DurableVolumes are declared on the sandbox (pause) spec only.
	DurableVolumes []string
	// Size sizes the container's cgroup leaf. Only gVisor applies it; a micro-VM
	// container's limits come from its own declared resources (see sizing).
	Size sizing.SandboxSize
}

// ShapeGVisor adds runsc CRI annotations, durable-dir mount hints, host
// resolv.conf, and per-container cgroups to the spec. It is idempotent.
func ShapeGVisor(spec *specs.Spec, o GVisorOptions) {
	if spec.Annotations == nil {
		spec.Annotations = make(map[string]string)
	}
	spec.Annotations["io.kubernetes.cri.container-name"] = o.ContainerName
	if o.ContainerName == PauseContainer {
		spec.Annotations["io.kubernetes.cri.container-type"] = "sandbox"
	} else {
		spec.Annotations["io.kubernetes.cri.container-type"] = "container"
		spec.Annotations["io.kubernetes.cri.sandbox-id"] = PauseContainer
	}

	// Insert resolv.conf before any volume bind mounts.
	if !slices.ContainsFunc(spec.Mounts, func(m specs.Mount) bool { return m.Destination == resolvConf }) {
		i := slices.IndexFunc(spec.Mounts, func(m specs.Mount) bool { return m.Type == "bind" })
		if i < 0 {
			i = len(spec.Mounts)
		}
		spec.Mounts = slices.Insert(spec.Mounts, i, specs.Mount{
			Destination: resolvConf,
			Type:        "bind",
			Source:      resolvConf,
			Options:     []string{"ro"},
		})
	}

	if spec.Linux == nil {
		spec.Linux = &specs.Linux{}
	}
	// Set a colon-free default cgroupsPath relative to the pod scope.
	if spec.Linux.CgroupsPath == "" {
		spec.Linux.CgroupsPath = "/" + o.ContainerName
	}
	o.Size.ApplyToOCISpec(spec)
}
