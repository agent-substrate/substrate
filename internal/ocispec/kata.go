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
	"strconv"

	"github.com/opencontainers/runtime-spec/specs-go"

	"go.ate.dev/substrate/internal/sizing"
)

// KataOptions describes the Kata shim context of one actor container.
type KataOptions struct {
	// ActorUID names the actor. The container that boots the VM takes it as
	// its ID, so the shim's sandbox and its socket are unique per actor.
	ActorUID string
	// ContainerName is the container's name.
	ContainerName string
	// BootsVM is true for the first container, which the shim creates the VM
	// for. The others join that VM.
	BootsVM bool
	// Size is the actor's declared size. It sizes the VM of the container that
	// boots it.
	Size sizing.SandboxSize
}

// KataContainerID returns the shim ID of a container. The container that boots
// the VM has the actor UID, and a later one the guest container ID of its name
// (see GuestContainerID for why a name cannot be used as is).
func KataContainerID(actorUID, name string, bootsVM bool) string {
	if bootsVM {
		return actorUID
	}
	return GuestContainerID(name)
}

// KataCgroupsPath returns the linux.cgroupsPath of a container. The shim puts
// itself and the VM in the path of the container that boots the VM, which is
// the actor's cgroup leaf. Another container needs a path of its own: the guest
// agent signals every process under one.
func KataCgroupsPath(actorUID, name string, bootsVM bool) string {
	if bootsVM {
		return "/" + actorUID
	}
	return "/" + actorUID + "-" + name
}

// ShapeKata gives the spec the CRI annotations the Kata shim uses to put the
// containers of an actor in one VM, replaces the host system mounts with the
// guest's, and fills in the cgroup settings the shim needs. It must run on an
// unshaped spec. Volume bind mounts keep their host sources: the shim shares
// them into the guest.
func ShapeKata(spec *specs.Spec, o KataOptions) {
	if spec.Annotations == nil {
		spec.Annotations = make(map[string]string)
	}
	spec.Annotations["io.kubernetes.cri.container-name"] = o.ContainerName
	if o.BootsVM {
		spec.Annotations["io.kubernetes.cri.container-type"] = "sandbox"
	} else {
		spec.Annotations["io.kubernetes.cri.container-type"] = "container"
	}
	spec.Annotations["io.kubernetes.cri.sandbox-id"] = o.ActorUID
	if o.BootsVM {
		o.Size.ApplyToOCISpec(spec)
		kataSandboxSize(spec.Annotations, o.Size)
	}

	mounts := guestSystemMounts()
	for _, m := range spec.Mounts {
		if m.Type == "bind" {
			mounts = append(mounts, m)
		}
	}
	spec.Mounts = mounts

	if spec.Linux == nil {
		spec.Linux = &specs.Linux{}
	}
	spec.Linux.Resources = mergeKataResources(spec.Linux.Resources)
	spec.Linux.CgroupsPath = KataCgroupsPath(o.ActorUID, o.ContainerName, o.BootsVM)
}

// kataSandboxSize records the size as the CRI annotations a Kata shim sizes
// its VM from at boot, when its configuration sets static_sandbox_resource_mgmt.
// Otherwise it starts the VM at the size of its configuration and tries to
// resize it to the resources of the containers.
func kataSandboxSize(annotations map[string]string, size sizing.SandboxSize) {
	if size.MilliCPU > 0 {
		const period = 100000
		annotations["io.kubernetes.cri.sandbox-cpu-period"] = strconv.Itoa(period)
		annotations["io.kubernetes.cri.sandbox-cpu-quota"] = strconv.FormatInt(size.MilliCPU*period/1000, 10)
	}
	if size.MemoryBytes > 0 {
		annotations["io.kubernetes.cri.sandbox-memory"] = strconv.FormatInt(size.MemoryBytes, 10)
	}
}
