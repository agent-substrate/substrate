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

// Package objectstoreplugintest provides snapshot plugin clients that call the
// object-store plugins directly, without a socket, for tests that drive atelet
// or ate-api-server through the real plugin.
package objectstoreplugintest

import (
	"context"

	"github.com/agent-substrate/substrate/internal/objectstore"
	"github.com/agent-substrate/substrate/internal/objectstoreplugin"
	objectstorev1 "github.com/agent-substrate/substrate/pkg/proto/objectstorepb/v1"
	"google.golang.org/grpc"
)

// NodeClient returns a client that calls p directly.
func NodeClient(p *objectstoreplugin.NodePlugin) objectstorev1.NodeProviderClient {
	return nodeClient{p}
}

type nodeClient struct {
	p *objectstoreplugin.NodePlugin
}

func (c nodeClient) FetchSnapshot(ctx context.Context, in *objectstorev1.FetchSnapshotRequest, _ ...grpc.CallOption) (*objectstorev1.FetchSnapshotResponse, error) {
	return c.p.FetchSnapshot(ctx, in)
}

func (c nodeClient) UploadSnapshot(ctx context.Context, in *objectstorev1.UploadSnapshotRequest, _ ...grpc.CallOption) (*objectstorev1.UploadSnapshotResponse, error) {
	return c.p.UploadSnapshot(ctx, in)
}

// ControlClient returns a client that calls a ControlPlugin on store directly.
func ControlClient(store objectstore.Store) objectstorev1.ControlProviderClient {
	return controlClient{objectstoreplugin.NewControlPlugin(store)}
}

type controlClient struct {
	p *objectstoreplugin.ControlPlugin
}

func (c controlClient) CleanupSnapshot(ctx context.Context, in *objectstorev1.CleanupSnapshotRequest, _ ...grpc.CallOption) (*objectstorev1.CleanupSnapshotResponse, error) {
	return c.p.CleanupSnapshot(ctx, in)
}

func (c controlClient) CopySnapshot(ctx context.Context, in *objectstorev1.CopySnapshotRequest, _ ...grpc.CallOption) (*objectstorev1.CopySnapshotResponse, error) {
	return c.p.CopySnapshot(ctx, in)
}
