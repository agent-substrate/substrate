// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package volume

import (
	"context"
)

// CreateVolumeRequest describes a volume to provision.
type CreateVolumeRequest struct {
	// Name is the name to provision the volume under.
	Name string
	// Capacity is the requested size as a Kubernetes resource.Quantity string.
	Capacity string
	// Parameters are the driver-specific parameters from the StorageClass.
	Parameters map[string]string
}

// CreateVolumeResponse describes a provisioned volume.
type CreateVolumeResponse struct {
	// VolumeID is the globally unique ID assigned by the storage system.
	VolumeID string
	// VolumeContext is driver-defined metadata that the node plugin needs to
	// mount the volume.
	VolumeContext map[string]string
}

// AttachVolumeRequest describes a volume to attach to a node.
type AttachVolumeRequest struct {
	// VolumeID is the ID assigned by the storage system at creation.
	VolumeID string
	// Node is the node to attach the volume to.
	Node string
}

// AttachVolumeResponse describes a completed attachment.
type AttachVolumeResponse struct {
	// PublishContext is driver-defined attachment metadata that must be echoed
	// back on the node's mount calls (e.g. the device path for AWS EBS). It is
	// empty for drivers that do not implement attachment, and is only
	// meaningful for the node named in the request.
	PublishContext map[string]string
}

// MountVolumeRequest describes a volume to mount on the local node.
type MountVolumeRequest struct {
	// VolumeID is the ID assigned by the storage system at creation.
	VolumeID string
	// TargetPath is the host path to mount the volume at.
	TargetPath string
	// VolumeContext is the metadata returned when the volume was created.
	VolumeContext map[string]string
	// PublishContext is the metadata returned when the volume was attached to
	// this node. It is empty for drivers that do not implement attachment.
	PublishContext map[string]string
}

// VolumePluginControlPlane abstracts storage operations performed on the control plane.
type VolumePluginControlPlane interface {
	DriverName(ctx context.Context) (string, error)
	CreateVolume(ctx context.Context, req CreateVolumeRequest) (CreateVolumeResponse, error)
	DeleteVolume(ctx context.Context, volumeID string) error
	AttachVolume(ctx context.Context, req AttachVolumeRequest) (AttachVolumeResponse, error)
	DetachVolume(ctx context.Context, volumeID string, node string) error
}

// VolumePluginWorkerPlane abstracts storage operations performed on worker nodes.
type VolumePluginWorkerPlane interface {
	MountVolume(ctx context.Context, req MountVolumeRequest) error
	UnmountVolume(ctx context.Context, volumeID string, targetPath string) error
}
