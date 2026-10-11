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

package controlapi

import (
	"context"
	"testing"

	"github.com/agent-substrate/substrate/internal/ateinterceptors"
	"github.com/agent-substrate/substrate/internal/resources"
	objectstorev1 "github.com/agent-substrate/substrate/pkg/proto/objectstorepb/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// failingControlPlugin fails every call with err.
type failingControlPlugin struct {
	err error
}

func (p failingControlPlugin) CleanupSnapshot(context.Context, *objectstorev1.CleanupSnapshotRequest, ...grpc.CallOption) (*objectstorev1.CleanupSnapshotResponse, error) {
	return nil, p.err
}

func (p failingControlPlugin) CopySnapshot(context.Context, *objectstorev1.CopySnapshotRequest, ...grpc.CallOption) (*objectstorev1.CopySnapshotResponse, error) {
	return nil, p.err
}

// A control snapshot plugin that cannot be reached fails the request with
// Unavailable, which a client retries; other plugin errors stay Internal.
func TestSnapshotPluginErrorCodeToClient(t *testing.T) {
	prefix, err := resources.ParseStoragePrefix("gs://bucket/root/atespaces/team-a/actors/uid1/snapshots/snap1")
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		pluginCode codes.Code
		want       codes.Code
	}{
		{codes.Unavailable, codes.Unavailable},
		{codes.Internal, codes.Internal},
		{codes.FailedPrecondition, codes.Internal},
	} {
		t.Run(tc.pluginCode.String(), func(t *testing.T) {
			w := &ActorWorkflow{snapshotPlugin: failingControlPlugin{err: status.Error(tc.pluginCode, "plugin failed")}}
			for name, call := range map[string]func(context.Context) error{
				"cleanup": func(ctx context.Context) error { return w.cleanupSnapshot(ctx, prefix) },
				"copy":    func(ctx context.Context) error { return w.copySnapshot(ctx, prefix, prefix) },
			} {
				// The public server interceptor decides the code the client sees.
				_, err := ateinterceptors.ServerUnaryInterceptor(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/test"},
					func(ctx context.Context, _ any) (any, error) { return nil, call(ctx) })
				if got := status.Code(err); got != tc.want {
					t.Errorf("%s: code = %s (%v), want %s", name, got, err, tc.want)
				}
			}
		})
	}
}
