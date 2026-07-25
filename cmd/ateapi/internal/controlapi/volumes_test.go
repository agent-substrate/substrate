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

package controlapi

import (
	"context"
	"testing"

	"github.com/agent-substrate/substrate/internal/volume"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// ctxCapturingPlugin records the context each DeleteVolume call is made with,
// so a test can assert cleanup does not inherit a canceled context.
type ctxCapturingPlugin struct {
	*volume.MockVolumePlugin

	deleteCtxErr error
	deleted      []string
}

func (p *ctxCapturingPlugin) DeleteVolume(ctx context.Context, volumeID string) error {
	p.deleteCtxErr = ctx.Err()
	p.deleted = append(p.deleted, volumeID)
	return p.MockVolumePlugin.DeleteVolume(ctx, volumeID)
}

// TestCleanupActorVolumesDetachesFromCanceledContext covers the case where the
// caller's context is itself why actor creation failed: the volumes must still
// be deleted rather than leaked.
func TestCleanupActorVolumesDetachesFromCanceledContext(t *testing.T) {
	plugin := &ctxCapturingPlugin{MockVolumePlugin: volume.NewMockVolumePlugin()}
	prev := globalVolumePlugin
	globalVolumePlugin = plugin
	t.Cleanup(func() { globalVolumePlugin = prev })

	volumeID, err := plugin.CreateVolume(context.Background(), "atespace-actor-data", "1Gi", "standard")
	if err != nil {
		t.Fatalf("CreateVolume() failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	s := &Service{}
	s.cleanupActorVolumes(ctx, &ateapipb.ObjectRef{Atespace: "atespace", Name: "actor"},
		[]*ateapipb.ExternalVolume{{StorageVolumeId: volumeID}})

	if got, want := len(plugin.deleted), 1; got != want {
		t.Fatalf("DeleteVolume called %d times, want %d", got, want)
	}
	if plugin.deleteCtxErr != nil {
		t.Errorf("DeleteVolume ran with a done context: %v", plugin.deleteCtxErr)
	}
}
