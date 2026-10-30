// Copyright 2026 The Agent Substrate Authors
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
	"maps"
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

// VolumeDeleteRequest holds the arguments of a DeleteVolume, as its RPC
// records them.
type VolumeDeleteRequest struct {
	VolumeID string
}

// VolumeDetachRequest holds the arguments of a DetachVolume, as its RPC
// records them.
type VolumeDetachRequest struct {
	VolumeID, Node string
}

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
	rpcs        rpcLog[VolumeOp]
}

// volumePlugin serves Volumes to ate-api as a volume plugin.
type volumePlugin struct {
	volumes *Volumes
}

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

// plugin returns the volume plugin v serves to ate-api. Tests hold only the
// Volumes, so they reach the plugin only through ate-api.
func (v *Volumes) plugin() volume.VolumePluginControlPlane {
	return volumePlugin{volumes: v}
}

// RecordRPCs starts recording the calls ate-api makes to v from now on.
func (v *Volumes) RecordRPCs() *RPCRecording[VolumeOp] {
	return v.rpcs.record()
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
func (volumePlugin) DriverName(context.Context) (string, error) {
	return VolumeDriver, nil
}

// CreateVolume implements volume.VolumePluginControlPlane. Creating a volume
// that exists returns it again, as CSI CreateVolume is idempotent by name.
func (p volumePlugin) CreateVolume(ctx context.Context, req volume.CreateVolumeRequest) (volume.CreateVolumeResponse, error) {
	v := p.volumes
	logged := req
	logged.Parameters = maps.Clone(req.Parameters)
	v.rpcs.add(VolumeCreate, logged)
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
func (p volumePlugin) DeleteVolume(ctx context.Context, volumeID string) error {
	v := p.volumes
	v.rpcs.add(VolumeDelete, VolumeDeleteRequest{VolumeID: volumeID})
	out := v.faults[VolumeDelete].decide(ctx)
	if out.apply {
		if err := v.deleteVolume(volumeID); err != nil {
			return err
		}
	}
	return out.result(ctx)
}

func (v *Volumes) deleteVolume(volumeID string) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	if vol, ok := v.volumesByID[volumeID]; ok {
		if len(vol.AttachedTo) > 0 {
			return status.Errorf(codes.FailedPrecondition, "volume %s is still attached to %v", volumeID, vol.AttachedTo)
		}
		delete(v.volumesByID, volumeID)
	}
	return nil
}

// AttachVolume implements volume.VolumePluginControlPlane. Its publish context
// names the node, as a driver's device path is specific to it. Attaching is
// idempotent.
func (p volumePlugin) AttachVolume(ctx context.Context, req volume.AttachVolumeRequest) (volume.AttachVolumeResponse, error) {
	v := p.volumes
	v.rpcs.add(VolumeAttach, req)
	out := v.faults[VolumeAttach].decide(ctx)
	if out.apply {
		if err := v.attachVolume(req); err != nil {
			return volume.AttachVolumeResponse{}, err
		}
	}
	if err := out.result(ctx); err != nil {
		return volume.AttachVolumeResponse{}, err
	}
	return volume.AttachVolumeResponse{PublishContext: map[string]string{"node": req.Node}}, nil
}

func (v *Volumes) attachVolume(req volume.AttachVolumeRequest) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	vol, ok := v.volumesByID[req.VolumeID]
	if !ok {
		return status.Errorf(codes.NotFound, "volume %s not found", req.VolumeID)
	}
	if !slices.Contains(vol.AttachedTo, req.Node) {
		vol.AttachedTo = append(vol.AttachedTo, req.Node)
		slices.Sort(vol.AttachedTo)
	}
	return nil
}

// DetachVolume implements volume.VolumePluginControlPlane. Detaching a volume
// that is not attached, or does not exist, succeeds.
func (p volumePlugin) DetachVolume(ctx context.Context, volumeID, node string) error {
	v := p.volumes
	v.rpcs.add(VolumeDetach, VolumeDetachRequest{VolumeID: volumeID, Node: node})
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
