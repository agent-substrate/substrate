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

package csi

import (
	"context"

	"github.com/container-storage-interface/spec/lib/go/csi"
)

// CreateVolume provisions a new volume.
func (c *Client) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	return c.controller.CreateVolume(ctx, req)
}

// DeleteVolume deprovisions a volume.
func (c *Client) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	return c.controller.DeleteVolume(ctx, req)
}

// ControllerPublishVolume attaches the volume to a node.
func (c *Client) ControllerPublishVolume(ctx context.Context, req *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
	return c.controller.ControllerPublishVolume(ctx, req)
}

// ControllerUnpublishVolume detaches the volume from a node.
func (c *Client) ControllerUnpublishVolume(ctx context.Context, req *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {
	return c.controller.ControllerUnpublishVolume(ctx, req)
}
