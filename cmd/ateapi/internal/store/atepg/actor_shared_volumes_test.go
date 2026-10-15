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

package atepg

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

func TestCreateActorSharedVolumeTransaction(t *testing.T) {
	p := setupPostgresPersistence(t)
	ctx := context.Background()
	createTestAtespace(t, p, "team")
	v, err := p.CreateSharedVolume(ctx, &ateapipb.SharedVolume{Metadata: &ateapipb.ResourceMetadata{Atespace: "team", Name: "data"}, VolumeId: "disk", VolumeType: "mock", Status: &ateapipb.SharedVolumeStatus{State: ateapipb.SharedVolumeState_SHARED_VOLUME_STATE_READY}})
	if err != nil {
		t.Fatal(err)
	}
	actor := func(name string) *ateapipb.Actor {
		return &ateapipb.Actor{Metadata: &ateapipb.ResourceMetadata{Atespace: "team", Name: name}, Status: &ateapipb.ActorStatus{ExternalVolumes: []*ateapipb.ExternalVolumeRef{{Name: "data", SharedVolumeName: "data", StorageVolumeId: "disk", VolumeType: "mock"}}}}
	}
	const count = 12
	errs := make(chan error, count)
	var wg sync.WaitGroup
	for i := range count {
		wg.Go(func() { _, err := p.CreateActor(ctx, actor(fmt.Sprintf("actor-%d", i))); errs <- err })
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	ref := resources.SharedVolumeRef{Atespace: "team", Name: "data"}
	v, err = p.GetSharedVolume(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	if len(v.GetStatus().GetRefs()) != count {
		t.Fatalf("got %d references, want %d", len(v.GetStatus().GetRefs()), count)
	}
	if _, err = p.CreateActor(ctx, actor("actor-0")); !errors.Is(err, store.ErrAlreadyExists) {
		t.Fatalf("duplicate: %v", err)
	}
	v, err = p.GetSharedVolume(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	if len(v.GetStatus().GetRefs()) != count {
		t.Fatal("failed create leaked a reference")
	}
	// A later volume failure must roll back earlier references as well as the actor.
	bad := actor("partial")
	bad.Status.ExternalVolumes = append(bad.Status.ExternalVolumes, &ateapipb.ExternalVolumeRef{Name: "missing", SharedVolumeName: "missing", StorageVolumeId: "none"})
	if _, err = p.CreateActor(ctx, bad); !errors.Is(err, store.ErrFailedPrecondition) {
		t.Fatalf("partial create: %v", err)
	}
	if _, err = p.GetActor(ctx, resources.ActorRef{Atespace: "team", Name: "partial"}); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("partial actor remains: %v", err)
	}
	v, err = p.GetSharedVolume(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	if len(v.GetStatus().GetRefs()) != count {
		t.Fatal("partial create leaked a reference")
	}
	_, err = p.UpdateSharedVolume(ctx, ref, store.PreconditionFrom(v), func(v *ateapipb.SharedVolume) error {
		v.Status.State = ateapipb.SharedVolumeState_SHARED_VOLUME_STATE_DELETING
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = p.CreateActor(ctx, actor("late")); !errors.Is(err, store.ErrFailedPrecondition) {
		t.Fatalf("deleting volume accepted a new actor: %v", err)
	}
}
