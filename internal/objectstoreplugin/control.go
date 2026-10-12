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

package objectstoreplugin

import (
	"context"

	"github.com/agent-substrate/substrate/internal/objectstore"
	"github.com/agent-substrate/substrate/internal/resources"
	objectstorev1 "github.com/agent-substrate/substrate/pkg/proto/objectstorepb/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ControlPlugin serves ControlProvider on an object store.
type ControlPlugin struct {
	objectstorev1.UnimplementedControlProviderServer

	store objectstore.Store
}

// NewControlPlugin returns a ControlPlugin backed by store.
func NewControlPlugin(store objectstore.Store) *ControlPlugin {
	return &ControlPlugin{store: store}
}

// CleanupSnapshot deletes every object under the given prefix.
func (p *ControlPlugin) CleanupSnapshot(ctx context.Context, req *objectstorev1.CleanupSnapshotRequest) (*objectstorev1.CleanupSnapshotResponse, error) {
	prefix, err := resources.ParseStoragePrefix(req.GetSnapshotUri())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if err := objectstore.DeletePrefix(ctx, p.store, prefix); err != nil {
		return nil, toStatus(err)
	}
	return &objectstorev1.CleanupSnapshotResponse{}, nil
}

// CopySnapshot copies every object of src_uri to dst_uri.
func (p *ControlPlugin) CopySnapshot(ctx context.Context, req *objectstorev1.CopySnapshotRequest) (*objectstorev1.CopySnapshotResponse, error) {
	src, err := resources.ParseStoragePrefix(req.GetSrcUri())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	dst, err := resources.ParseStoragePrefix(req.GetDstUri())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if err := objectstore.CopyPrefix(ctx, p.store, src, dst); err != nil {
		return nil, toStatus(err)
	}
	return &objectstorev1.CopySnapshotResponse{}, nil
}
