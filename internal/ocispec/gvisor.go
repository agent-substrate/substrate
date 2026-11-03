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
	"fmt"
	"path/filepath"
	"slices"
	"strings"

	"github.com/opencontainers/runtime-spec/specs-go"
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
	"go.ate.dev/substrate/internal/sizing"
)

// SandboxID is the runsc ID of the sandbox. runsc creates it with
// -no-root-container, so it has no process and no rootfs. The underscore
// keeps it outside the k8s-short-name an ActorTemplate container
// name is drawn from, so no actor container can collide with it.
const SandboxID = "_sandbox"

// GVisorOptions describes the gVisor-specific context of one actor container.
type GVisorOptions struct {
	ActorUID      string
	ActorDirs     *ateworkerpb.ActorDirs
	ContainerName string
	// DurableVolumes are declared on the sandbox spec only.
	DurableVolumes []string
	// Size sizes the container's cgroup leaf and caps its /tmp. Only gVisor
	// applies it; a microVM container's limits come from its own declared
	// resources (see sizing).
	Size sizing.SandboxSize
}

// GVisorCgroupLeaf names an actor container's cgroup relative to the pod scope.
func GVisorCgroupLeaf(actorUID, containerName string) string {
	return actorUID + "-" + containerName
}

// ShapeGVisor adds runsc CRI annotations, durable-dir mount hints, and
// per-container cgroups to the spec, caps /tmp at half the memory limit, and
// drops mounts and mount options runsc supplies or ignores. It is idempotent.
func ShapeGVisor(spec *specs.Spec, o GVisorOptions) {
	if spec.Annotations == nil {
		spec.Annotations = make(map[string]string)
	}
	spec.Annotations["io.kubernetes.cri.container-name"] = o.ContainerName
	if o.ContainerName == SandboxID {
		spec.Annotations["io.kubernetes.cri.container-type"] = "sandbox"
		for _, v := range o.DurableVolumes {
			spec.Annotations["dev.gvisor.spec.mount."+v+".type"] = "tmpfs"
			spec.Annotations["dev.gvisor.spec.mount."+v+".share"] = "container"
			spec.Annotations["dev.gvisor.spec.mount."+v+".source"] = filepath.Join(o.ActorDirs.GetDurableDirVolumeMountsDir(), v)
		}
	} else {
		spec.Annotations["io.kubernetes.cri.container-type"] = "container"
		spec.Annotations["io.kubernetes.cri.sandbox-id"] = SandboxID
	}

	// runsc replaces /dev and /dev/pts with its own mounts and ignores mqueue
	// mounts and the nodev and strictatime options, logging a warning for each.
	spec.Mounts = slices.DeleteFunc(spec.Mounts, func(m specs.Mount) bool {
		return m.Destination == "/dev" || m.Destination == "/dev/pts" || m.Type == "mqueue"
	})
	for i := range spec.Mounts {
		spec.Mounts[i].Options = slices.DeleteFunc(slices.Clone(spec.Mounts[i].Options), func(opt string) bool {
			return opt == "nodev" || opt == "strictatime"
		})
	}
	// runsc's tmpfs defaults to half of the node's memory. Cap /tmp at half the
	// actor's memory limit, matching the guest kernel's default on microVM.
	// runsc requires unchanged mount options across MEMORY restores.
	if mem := o.Size.MemoryBytes; mem > 0 {
		if i := slices.IndexFunc(spec.Mounts, func(m specs.Mount) bool { return m.Destination == "/tmp" && m.Type == "tmpfs" }); i >= 0 {
			opts := slices.DeleteFunc(spec.Mounts[i].Options, func(opt string) bool { return strings.HasPrefix(opt, "size=") })
			spec.Mounts[i].Options = append(opts, fmt.Sprintf("size=%d", max(mem/2, 1)))
		}
	}

	if spec.Linux == nil {
		spec.Linux = &specs.Linux{}
	}
	// Set a colon-free default cgroupsPath relative to the pod scope.
	if spec.Linux.CgroupsPath == "" {
		spec.Linux.CgroupsPath = "/" + GVisorCgroupLeaf(o.ActorUID, o.ContainerName)
	}
	o.Size.ApplyToOCISpec(spec)
}
