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

// Package ocispec builds the runtime-neutral OCI spec for actor bundles, which
// each ateom shapes for its runtime.
package ocispec

import (
	"encoding/json"
	"fmt"
	"os"
	"path"

	"github.com/agent-substrate/substrate/internal/ateompath"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/opencontainers/runtime-spec/specs-go"
)

// specFile is the OCI spec file name within a bundle.
const specFile = "config.json"

// hostname is the UTS hostname for actor containers.
const hostname = "actor"

// Options describes one actor container. Args, Env and Capabilities arrive
// already resolved.
type Options struct {
	ActorUID      string
	ContainerName string
	Args          []string
	Env           []string
	// NetNSPath is the network namespace the ateom runs the actor in.
	NetNSPath    string
	Volumes      []*ateletpb.Volume
	VolumeMounts []*ateletpb.VolumeMount
	Capabilities []string
	// Resources are the container's own declared limits, or nil for none.
	Resources *ateletpb.ResourceLimits
}

const (
	// cpuQuotaPeriodUS is the CFS period the CPU quota is expressed against: a
	// container limited to N milli-cores may run for N/1000 of every period.
	cpuQuotaPeriodUS = 100000
	// cpuQuotaMinUS is the smallest quota the kernel accepts. tg_set_cfs_bandwidth
	// rejects a quota below 1ms, so a limit under
	// cpuQuotaMinUS*1000/cpuQuotaPeriodUS milli-cores (10m) is raised to this
	// floor rather than producing a spec the guest refuses.
	cpuQuotaMinUS = 1000
)

// ociResources maps the resolved limits onto the OCI spec's linux.resources.
// Returns nil when nothing is set, so the spec carries no linux.resources at
// all for a container that declares no limits.
func ociResources(r *ateletpb.ResourceLimits) *specs.LinuxResources {
	if r == nil || (r.GetMemoryBytes() <= 0 && r.GetCpuMillis() <= 0) {
		return nil
	}
	out := &specs.LinuxResources{}
	if b := r.GetMemoryBytes(); b > 0 {
		out.Memory = &specs.LinuxMemory{Limit: &b}
	}
	if m := r.GetCpuMillis(); m > 0 {
		quota := max(m*cpuQuotaPeriodUS/1000, cpuQuotaMinUS)
		period := uint64(cpuQuotaPeriodUS)
		out.CPU = &specs.LinuxCPU{Quota: &quota, Period: &period}
	}
	return out
}

// Build returns a runtime-neutral OCI spec for an actor container.
func Build(o Options) *specs.Spec {
	spec := &specs.Spec{
		Process: &specs.Process{
			User: specs.User{
				UID: 0,
				GID: 0,
			},
			Args: o.Args,
			Env:  o.Env,
			Cwd:  "/",
			Capabilities: &specs.LinuxCapabilities{
				Bounding:  o.Capabilities,
				Effective: o.Capabilities,
				Permitted: o.Capabilities,
				// Inheritable and Ambient stay empty; capabilities inherit via
				// Bounding.
				//
				// TODO(gvisor.dev/issue/3166): support ambient capabilities
			},
			Rlimits: []specs.POSIXRlimit{
				{
					Type: "RLIMIT_NOFILE",
					Hard: 1024,
					Soft: 1024,
				},
			},
		},
		Root: &specs.Root{
			Path:     "rootfs",
			Readonly: false,
		},
		Hostname: hostname,
		Mounts: []specs.Mount{
			{
				Destination: "/proc",
				Type:        "proc",
				Source:      "proc",
			},
			{
				Destination: "/dev",
				Type:        "tmpfs",
				Source:      "tmpfs",
			},
			{
				Destination: "/sys",
				Type:        "sysfs",
				Source:      "sysfs",
				Options: []string{
					"nosuid",
					"noexec",
					"nodev",
					"ro",
				},
			},
		},
		Linux: &specs.Linux{
			Namespaces: []specs.LinuxNamespace{
				{
					Type: "pid",
				},
				{
					Type: "network",
					Path: o.NetNSPath, // Will be created by ateom
				},
				{
					Type: "ipc",
				},
				{
					Type: "uts",
				},
				{
					Type: "mount",
				},
			},
			Resources: ociResources(o.Resources),
		},
	}

	volumesByName := make(map[string]*ateletpb.Volume, len(o.Volumes))
	for _, vol := range o.Volumes {
		volumesByName[vol.GetName()] = vol
	}
	for _, vm := range o.VolumeMounts {
		var srcPath string
		options := []string{"bind", "rw"}
		switch volumesByName[vm.GetName()].GetSource().(type) {
		case *ateletpb.Volume_DurableDir:
			srcPath = ateompath.DurableDirVolumeMountPoint(o.ActorUID, vm.GetName())
		case *ateletpb.Volume_External:
			srcPath = ateompath.VolumeHostPath(o.ActorUID, vm.GetName())
		case *ateletpb.Volume_SystemInfo:
			srcPath = ateompath.SystemInfoVolumeRoot(o.ActorUID, vm.GetName())
			options = []string{"bind", "ro"}
		case *ateletpb.Volume_Image:
			srcPath = ateompath.ImageVolumeMountPath(o.ActorUID, o.ContainerName, vm.GetName())
			options = []string{"bind", "ro"}
		default:
			continue
		}
		spec.Mounts = append(spec.Mounts, specs.Mount{
			Destination: vm.GetMountPath(),
			Type:        "bind",
			Source:      srcPath,
			Options:     options,
		})
	}

	return spec
}

// Load reads the OCI spec of the bundle at bundlePath.
func Load(bundlePath string) (*specs.Spec, error) {
	specPath := path.Join(bundlePath, specFile)
	b, err := os.ReadFile(specPath)
	if err != nil {
		return nil, fmt.Errorf("reading %q: %w", specPath, err)
	}
	var spec specs.Spec
	if err := json.Unmarshal(b, &spec); err != nil {
		return nil, fmt.Errorf("parsing %q: %w", specPath, err)
	}
	return &spec, nil
}

// Save writes spec as the OCI spec of the bundle at bundlePath.
func Save(bundlePath string, spec *specs.Spec) error {
	specPath := path.Join(bundlePath, specFile)
	b, err := json.MarshalIndent(spec, "", "  ")
	if err != nil {
		return fmt.Errorf("marshaling %q: %w", specPath, err)
	}
	if err := os.WriteFile(specPath, b, 0o600); err != nil {
		return fmt.Errorf("writing %q: %w", specPath, err)
	}
	return nil
}
