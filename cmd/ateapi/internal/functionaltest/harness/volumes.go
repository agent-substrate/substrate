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

package harness

import (
	"cmp"
	"context"
	"slices"
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.ate.dev/substrate/internal/volume"
)

const (
	// VolumeDriver is the CSI driver name the fake volume plugin serves.
	VolumeDriver = "volumes.harness.substrate.io"
	// StorageClass names the cluster's StorageClass provisioned by the fake
	// volume plugin.
	StorageClass = "harness-volumes"
)

// VolumeOp names a volume plugin operation a fault can be injected into.
type VolumeOp string

const (
	VolumeCreate VolumeOp = "CreateVolume"
	VolumeDelete VolumeOp = "DeleteVolume"
	VolumeAttach VolumeOp = "AttachVolume"
	VolumeDetach VolumeOp = "DetachVolume"
)

// Volume is a volume the fake plugin holds.
type Volume struct {
	ID   string
	Name string
	// AttachedTo lists the nodes the volume is attached to, sorted.
	AttachedTo []string
}

// Volumes is a stateful fake of the control-plane half of a CSI volume plugin.
type Volumes struct {
	mu          sync.Mutex
	volumesByID map[string]*Volume
	faults      map[VolumeOp]*Fault
}

var _ volume.VolumePluginControlPlane = (*Volumes)(nil)

func newVolumes() *Volumes {
	return &Volumes{
		volumesByID: map[string]*Volume{},
		faults:      map[VolumeOp]*Fault{VolumeCreate: {}, VolumeDelete: {}, VolumeAttach: {}, VolumeDetach: {}},
	}
}

// On returns the Fault deciding how calls to op behave.
func (v *Volumes) On(op VolumeOp) *Fault {
	return v.faults[op]
}

// List returns every volume that exists, sorted by ID.
func (v *Volumes) List() []Volume {
	v.mu.Lock()
	defer v.mu.Unlock()
	out := make([]Volume, 0, len(v.volumesByID))
	for _, vol := range v.volumesByID {
		out = append(out, Volume{ID: vol.ID, Name: vol.Name, AttachedTo: slices.Clone(vol.AttachedTo)})
	}
	slices.SortFunc(out, func(a, b Volume) int { return cmp.Compare(a.ID, b.ID) })
	return out
}

// DriverName implements volume.VolumePluginControlPlane.
func (v *Volumes) DriverName(context.Context) (string, error) {
	return VolumeDriver, nil
}

// CreateVolume implements volume.VolumePluginControlPlane. Creating a volume
// that exists returns it again, as CSI CreateVolume is idempotent by name.
func (v *Volumes) CreateVolume(ctx context.Context, req volume.CreateVolumeRequest) (volume.CreateVolumeResponse, error) {
	out := v.faults[VolumeCreate].decide(ctx)
	id := "vol-" + req.Name
	if out.apply {
		v.mu.Lock()
		if _, ok := v.volumesByID[id]; !ok {
			v.volumesByID[id] = &Volume{ID: id, Name: req.Name}
		}
		v.mu.Unlock()
	}
	if err := out.result(ctx); err != nil {
		return volume.CreateVolumeResponse{}, err
	}
	return volume.CreateVolumeResponse{VolumeID: id, VolumeContext: req.Parameters}, nil
}

// DeleteVolume implements volume.VolumePluginControlPlane. Deleting a missing
// volume succeeds; deleting an attached one fails, as a real driver refuses.
func (v *Volumes) DeleteVolume(ctx context.Context, volumeID string) error {
	out := v.faults[VolumeDelete].decide(ctx)
	if out.apply {
		v.mu.Lock()
		defer v.mu.Unlock()
		if vol, ok := v.volumesByID[volumeID]; ok {
			if len(vol.AttachedTo) > 0 {
				return status.Errorf(codes.FailedPrecondition, "volume %s is still attached to %v", volumeID, vol.AttachedTo)
			}
			delete(v.volumesByID, volumeID)
		}
	}
	return out.result(ctx)
}

// AttachVolume implements volume.VolumePluginControlPlane. Attaching is
// idempotent.
func (v *Volumes) AttachVolume(ctx context.Context, req volume.AttachVolumeRequest) (volume.AttachVolumeResponse, error) {
	out := v.faults[VolumeAttach].decide(ctx)
	if out.apply {
		v.mu.Lock()
		defer v.mu.Unlock()
		vol, ok := v.volumesByID[req.VolumeID]
		if !ok {
			return volume.AttachVolumeResponse{}, status.Errorf(codes.NotFound, "volume %s not found", req.VolumeID)
		}
		if !slices.Contains(vol.AttachedTo, req.Node) {
			vol.AttachedTo = append(vol.AttachedTo, req.Node)
			slices.Sort(vol.AttachedTo)
		}
	}
	return volume.AttachVolumeResponse{}, out.result(ctx)
}

// DetachVolume implements volume.VolumePluginControlPlane. Detaching a volume
// that is not attached, or does not exist, succeeds.
func (v *Volumes) DetachVolume(ctx context.Context, volumeID, node string) error {
	out := v.faults[VolumeDetach].decide(ctx)
	if out.apply {
		v.mu.Lock()
		if vol, ok := v.volumesByID[volumeID]; ok {
			vol.AttachedTo = slices.DeleteFunc(vol.AttachedTo, func(n string) bool { return n == node })
		}
		v.mu.Unlock()
	}
	return out.result(ctx)
}
