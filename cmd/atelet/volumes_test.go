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

package main

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/internal/volume"
)

type fakeWorkerPlugin struct {
	mountErr   error
	unmountErr error
	unmounted  []string
}

func (f *fakeWorkerPlugin) MountVolume(ctx context.Context, volumeID string, targetPath string) error {
	return f.mountErr
}

func (f *fakeWorkerPlugin) UnmountVolume(ctx context.Context, volumeID string, targetPath string) error {
	f.unmounted = append(f.unmounted, volumeID)
	return f.unmountErr
}

var _ volume.VolumePluginWorkerPlane = (*fakeWorkerPlugin)(nil)

func TestUnmountExternalVolumes(t *testing.T) {
	origPlugin := globalVolumePlugin
	defer func() { globalVolumePlugin = origPlugin }()

	ctx := context.Background()
	actorUID := "test-actor-123"

	extVol1 := &ateletpb.Volume{
		Name: "vol-1",
		Type: ateletpb.VolumeType_VOLUME_TYPE_EXTERNAL,
		Source: &ateletpb.Volume_External{
			External: &ateletpb.ExternalVolumeSource{
				StorageVolumeId: "mock-vol-1",
			},
		},
	}
	extVol2 := &ateletpb.Volume{
		Name: "vol-2",
		Type: ateletpb.VolumeType_VOLUME_TYPE_EXTERNAL,
		Source: &ateletpb.Volume_External{
			External: &ateletpb.ExternalVolumeSource{
				StorageVolumeId: "mock-vol-2",
			},
		},
	}
	durableVol := &ateletpb.Volume{
		Name: "durable-1",
		Type: ateletpb.VolumeType_VOLUME_TYPE_DURABLE_DIR,
	}

	t.Run("success", func(t *testing.T) {
		fake := &fakeWorkerPlugin{}
		globalVolumePlugin = fake

		s := &AteomHerder{}
		err := s.unmountExternalVolumes(ctx, actorUID, []*ateletpb.Volume{extVol1, durableVol, extVol2})
		if err != nil {
			t.Fatalf("unmountExternalVolumes failed unexpectedly: %v", err)
		}
		if len(fake.unmounted) != 2 || fake.unmounted[0] != "mock-vol-1" || fake.unmounted[1] != "mock-vol-2" {
			t.Errorf("unmounted volumes = %v, want [mock-vol-1, mock-vol-2]", fake.unmounted)
		}
	})

	t.Run("unmount failure is blocking", func(t *testing.T) {
		fake := &fakeWorkerPlugin{
			unmountErr: errors.New("device or resource busy"),
		}
		globalVolumePlugin = fake

		s := &AteomHerder{}
		err := s.unmountExternalVolumes(ctx, actorUID, []*ateletpb.Volume{extVol1})
		if err == nil {
			t.Fatal("unmountExternalVolumes returned nil, want blocking error")
		}
		if !errors.Is(err, fake.unmountErr) {
			t.Errorf("error = %v, want to contain unmount error", err)
		}
	})

	t.Run("multiple unmount failures return joined error", func(t *testing.T) {
		fake := &fakeWorkerPlugin{
			unmountErr: fmt.Errorf("unmount failed"),
		}
		globalVolumePlugin = fake

		s := &AteomHerder{}
		err := s.unmountExternalVolumes(ctx, actorUID, []*ateletpb.Volume{extVol1, extVol2})
		if err == nil {
			t.Fatal("unmountExternalVolumes returned nil, want blocking error")
		}
		// Both volumes should have attempt made
		if len(fake.unmounted) != 2 {
			t.Errorf("attempted unmount count = %d, want 2", len(fake.unmounted))
		}
	})
}
