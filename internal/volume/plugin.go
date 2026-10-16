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
	// DriverName selects the provisioner.
	DriverName string
	// SourceSnapshotID seeds the new volume from an existing snapshot. Empty
	// provisions an empty volume. The snapshot must belong to the same driver:
	// a handle means nothing to any other one.
	SourceSnapshotID string
}

// CreateVolumeResponse describes a provisioned volume.
type CreateVolumeResponse struct {
	// VolumeID is the globally unique ID assigned by the storage system.
	VolumeID string
	// VolumeContext is driver-defined metadata that the node plugin needs to
	// mount the volume.
	VolumeContext map[string]string
	// ContentSourceSnapshotID is the snapshot the driver reports it restored
	// from, empty for an empty volume. Callers that requested a source must
	// check this: a driver that ignores the request returns an empty volume and
	// reports success, which for a restore is silent data loss.
	ContentSourceSnapshotID string
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

// CreateSnapshotRequest describes a snapshot to take.
type CreateSnapshotRequest struct {
	// Name is the caller's name for the snapshot. Like volume provisioning,
	// this is idempotent on (Name, SourceVolumeID), so a retry within one call
	// returns the snapshot the previous attempt created.
	Name string
	// SourceVolumeID is the volume to capture.
	SourceVolumeID string
	// Parameters are opaque driver parameters, passed through as the CSI
	// CreateSnapshot parameters. No caller sets them yet, so drivers use their
	// default snapshot settings. They can be expanded in the future to carry
	// the parameters of a VolumeSnapshotClass-like object selected per volume.
	Parameters map[string]string
}

// Snapshot is a point-in-time copy of a volume held by the storage system.
type Snapshot struct {
	// SnapshotID is the storage system's handle for the snapshot.
	SnapshotID string
	// SourceVolumeID is the volume it was captured from.
	SourceVolumeID string
	// ReadyToUse is whether the storage system has finished the copy. Drivers
	// may return a handle before it is usable and finish in the background, so
	// this is a point-in-time observation rather than a durable property.
	ReadyToUse bool
	// SizeBytes is the snapshot's size, or 0 if the driver did not report one.
	SizeBytes int64
}

// Capabilities are the optional operations a driver's controller supports.
// These are a property of the driver, not of any one volume, so a plugin
// queries the driver once and reports them from its cached view.
type Capabilities struct {
	// CreateDeleteSnapshot is whether the driver can snapshot volumes. Without
	// it, a volume it provisions can never back a tag that captures volumes.
	CreateDeleteSnapshot bool
	// ListSnapshots is whether the driver can report a snapshot's state after
	// creating it. Without it, readiness can only be observed in the
	// CreateSnapshot response.
	ListSnapshots bool
}

// VolumePluginControlPlane abstracts storage operations performed on the control plane.
type VolumePluginControlPlane interface {
	DriverName(ctx context.Context) (string, error)
	CreateVolume(ctx context.Context, req CreateVolumeRequest) (CreateVolumeResponse, error)
	DeleteVolume(ctx context.Context, volumeID string) error
	AttachVolume(ctx context.Context, req AttachVolumeRequest) (AttachVolumeResponse, error)
	DetachVolume(ctx context.Context, volumeID string, node string) error
	// CreateSnapshot captures a volume. It may return before the copy is
	// complete, with ReadyToUse false.
	CreateSnapshot(ctx context.Context, req CreateSnapshotRequest) (Snapshot, error)
	// GetSnapshot looks a snapshot up by handle, reporting whether the storage
	// system still has it. A snapshot deleted out from under us is a missing
	// snapshot, not an error.
	GetSnapshot(ctx context.Context, snapshotID string) (snapshot Snapshot, found bool, err error)
	// DeleteSnapshot releases a snapshot. Deleting one that is already gone
	// succeeds, so that cleanup can be retried.
	DeleteSnapshot(ctx context.Context, snapshotID string) error
	// ControllerCapabilities reports which optional operations the driver
	// supports.
	ControllerCapabilities(ctx context.Context) (Capabilities, error)
}

// VolumePluginWorkerPlane abstracts storage operations performed on worker nodes.
type VolumePluginWorkerPlane interface {
	MountVolume(ctx context.Context, req MountVolumeRequest) error
	UnmountVolume(ctx context.Context, volumeID string, targetPath string) error
}
