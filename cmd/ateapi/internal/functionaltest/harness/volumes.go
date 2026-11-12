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
	"slices"
	"sync"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// VolumeMethod is the gRPC full name of a CSI Controller RPC ate-api calls on
// the volume plugin, which a fault can be injected into.
type VolumeMethod string

const (
	VolumeCreate    = VolumeMethod(csi.Controller_CreateVolume_FullMethodName)
	VolumeDelete    = VolumeMethod(csi.Controller_DeleteVolume_FullMethodName)
	VolumePublish   = VolumeMethod(csi.Controller_ControllerPublishVolume_FullMethodName)
	VolumeUnpublish = VolumeMethod(csi.Controller_ControllerUnpublishVolume_FullMethodName)
)

// Volume is a volume the fake plugin holds.
type Volume struct {
	ID   string
	Name string
	// AttachedTo lists the nodes the volume is published to, sorted.
	AttachedTo []string
}

// Volumes is a stateful fake of a CSI driver's Identity and Controller
// services, which ate-api finds through a CSIDriverConfig.
type Volumes struct {
	driver       string
	storageClass string

	fake *fakeServer[VolumeMethod]

	mu          sync.Mutex
	volumesByID map[string]*Volume
}

// volumeServer serves Volumes to ate-api as a CSI driver.
type volumeServer struct {
	csi.UnimplementedIdentityServer
	csi.UnimplementedControllerServer
	volumes *Volumes
}

func newVolumes(driver, storageClass string) *Volumes {
	return &Volumes{
		driver:       driver,
		storageClass: storageClass,
		fake:         newFakeServer(VolumeCreate, VolumeDelete, VolumePublish, VolumeUnpublish),
		volumesByID:  map[string]*Volume{},
	}
}

// Driver returns the CSI driver name the fake serves.
func (v *Volumes) Driver() string { return v.driver }

// StorageClass returns the name of the StorageClass the fake provisions.
func (v *Volumes) StorageClass() string { return v.storageClass }

// On returns the Fault deciding how calls to method behave.
func (v *Volumes) On(method VolumeMethod) *Fault {
	return v.fake.on(method)
}

// grpcServer returns a gRPC server of the CSI driver v serves to ate-api,
// which applies v's faults and records its calls. Tests hold only the
// Volumes, so they reach the driver only through ate-api.
func (v *Volumes) grpcServer() *grpc.Server {
	srv := v.fake.newServer()
	csi.RegisterIdentityServer(srv, &volumeServer{volumes: v})
	csi.RegisterControllerServer(srv, &volumeServer{volumes: v})
	return srv
}

// RecordCalls starts recording the Controller calls ate-api makes to v from
// now on. The Identity and capability calls ate-api makes when it connects
// are neither faulted nor recorded.
func (v *Volumes) RecordCalls() *Recording[VolumeMethod] {
	return v.fake.record()
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

// GetPluginInfo implements csi.IdentityServer.
func (s *volumeServer) GetPluginInfo(context.Context, *csi.GetPluginInfoRequest) (*csi.GetPluginInfoResponse, error) {
	return &csi.GetPluginInfoResponse{Name: s.volumes.driver, VendorVersion: "1"}, nil
}

// ControllerGetCapabilities implements csi.ControllerServer. The fake creates,
// deletes, publishes and unpublishes volumes.
func (s *volumeServer) ControllerGetCapabilities(context.Context, *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error) {
	var caps []*csi.ControllerServiceCapability
	for _, rpc := range []csi.ControllerServiceCapability_RPC_Type{
		csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
		csi.ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME,
	} {
		caps = append(caps, &csi.ControllerServiceCapability{
			Type: &csi.ControllerServiceCapability_Rpc{Rpc: &csi.ControllerServiceCapability_RPC{Type: rpc}},
		})
	}
	return &csi.ControllerGetCapabilitiesResponse{Capabilities: caps}, nil
}

// CreateVolume implements csi.ControllerServer. Creating a volume that exists
// returns it again, as CreateVolume is idempotent by name.
func (s *volumeServer) CreateVolume(_ context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	v := s.volumes
	id := "vol-" + req.GetName()
	v.mu.Lock()
	if _, ok := v.volumesByID[id]; !ok {
		v.volumesByID[id] = &Volume{ID: id, Name: req.GetName()}
	}
	v.mu.Unlock()
	return &csi.CreateVolumeResponse{Volume: &csi.Volume{
		VolumeId:      id,
		CapacityBytes: req.GetCapacityRange().GetRequiredBytes(),
		VolumeContext: req.GetParameters(),
	}}, nil
}

// DeleteVolume implements csi.ControllerServer. Deleting a missing volume
// succeeds; deleting a published one fails, as a real driver refuses.
func (s *volumeServer) DeleteVolume(_ context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	v := s.volumes
	v.mu.Lock()
	defer v.mu.Unlock()
	if vol, ok := v.volumesByID[req.GetVolumeId()]; ok {
		if len(vol.AttachedTo) > 0 {
			return nil, status.Errorf(codes.FailedPrecondition, "volume %s is still published to %v", req.GetVolumeId(), vol.AttachedTo)
		}
		delete(v.volumesByID, req.GetVolumeId())
	}
	return &csi.DeleteVolumeResponse{}, nil
}

// ControllerPublishVolume implements csi.ControllerServer. Its publish context
// names the node, as a driver's device path is specific to it. Publishing is
// idempotent.
func (s *volumeServer) ControllerPublishVolume(_ context.Context, req *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
	v := s.volumes
	v.mu.Lock()
	defer v.mu.Unlock()
	vol, ok := v.volumesByID[req.GetVolumeId()]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "volume %s not found", req.GetVolumeId())
	}
	if !slices.Contains(vol.AttachedTo, req.GetNodeId()) {
		vol.AttachedTo = append(vol.AttachedTo, req.GetNodeId())
		slices.Sort(vol.AttachedTo)
	}
	return &csi.ControllerPublishVolumeResponse{PublishContext: map[string]string{"node": req.GetNodeId()}}, nil
}

// ControllerUnpublishVolume implements csi.ControllerServer. Unpublishing a
// volume that is not published, or does not exist, succeeds.
func (s *volumeServer) ControllerUnpublishVolume(_ context.Context, req *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {
	v := s.volumes
	v.mu.Lock()
	defer v.mu.Unlock()
	if vol, ok := v.volumesByID[req.GetVolumeId()]; ok {
		vol.AttachedTo = slices.DeleteFunc(vol.AttachedTo, func(n string) bool { return n == req.GetNodeId() })
	}
	return &csi.ControllerUnpublishVolumeResponse{}, nil
}
