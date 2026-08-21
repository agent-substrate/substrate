//go:build linux

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

package main

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/agent-substrate/substrate/cmd/ateom-microvm/internal/kata"
	"github.com/agent-substrate/substrate/internal/ateompath"
	"github.com/agent-substrate/substrate/internal/checkpointmarker"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	"github.com/agent-substrate/substrate/internal/resources"
)

// useTempActorsDir points the shared actor-state root at a temp directory and
// creates the actor's checkpoint dir.
func useTempActorsDir(t *testing.T, actorUID string) {
	t.Helper()
	orig := ateompath.ActorsDir
	t.Cleanup(func() { ateompath.ActorsDir = orig })
	ateompath.ActorsDir = t.TempDir()

	if err := os.MkdirAll(ateompath.CheckpointStateDir(actorUID), 0o700); err != nil {
		t.Fatalf("creating checkpoint dir: %v", err)
	}
}

// A marker outlives the attempt that wrote it until resetActorDirs clears it,
// so a late retry can arrive after the ateom has been handed to another actor.
// Parts of the post-checkpoint teardown are the ateom's rather than the
// actor's — the interior network, the stats attribution — so running it then
// would cut the network out from under the actor now holding the ateom.
//
// This is the one replay path drivable from `go test`: it returns before
// reaching netlink or cloud-hypervisor, which is exactly the property under
// test.
func TestCheckpointWorkloadReplaySkipsTeardownForAReassignedAteom(t *testing.T) {
	const actorUID = "actor-1"
	useTempActorsDir(t, actorUID)

	want := []string{"snapshot.mem", "snapshot.state"}
	if err := checkpointmarker.Write(actorUID, ateompb.SnapshotScope_SNAPSHOT_SCOPE_FULL.String(), want); err != nil {
		t.Fatalf("checkpointmarker.Write: %v", err)
	}

	// The ateom has moved on: it now runs a different actor.
	successor := &resources.ActorAttribution{
		Ref: resources.ActorRef{Atespace: "ate-demo", Name: "counter-2"},
		UID: "actor-2",
	}
	s := &AteomService{}
	s.activeActor.Store(successor)
	s.guestStats.Store(&guestStatsTarget{actorUID: successor.UID})

	resp, err := s.CheckpointWorkload(context.Background(), &ateompb.CheckpointWorkloadRequest{
		Atespace:  "ate-demo",
		ActorName: "counter-1",
		ActorUid:  actorUID,
		Scope:     ateompb.SnapshotScope_SNAPSHOT_SCOPE_FULL,
	})
	if err != nil {
		t.Fatalf("CheckpointWorkload: %v", err)
	}
	// The recorded result is still owed to the caller: the checkpoint did
	// happen, and only its teardown is someone else's business now.
	if !slices.Equal(resp.GetSnapshotFiles(), want) {
		t.Errorf("SnapshotFiles = %v, want %v (the recorded result, replayed verbatim)", resp.GetSnapshotFiles(), want)
	}

	if got := s.activeActor.Load(); got != successor {
		t.Errorf("activeActor = %v, want the successor %v left untouched", got, successor)
	}
	if got := s.guestStats.Load(); got == nil || got.actorUID != successor.UID {
		t.Errorf("guestStats = %v, want the successor's target left untouched", got)
	}
}

// RunWorkload and RestoreWorkload launch their VMMs on different api-socket
// paths, so with no record of which one this actor came up through, ateom
// cannot assume the boot path. Guessing wrong aims the teardown at a socket
// nothing is listening on, and has CheckpointWorkload read that absence as "no
// guest remains" and crash an actor whose VMM is alive on the other socket.
func TestCHSocketCandidates(t *testing.T) {
	const actorUID = "actor-1"

	t.Run("no record covers both conventions", func(t *testing.T) {
		got := chSocketCandidates(actorUID, nil)
		want := []string{kata.CLHSocketPath(actorUID), kata.RestoredCLHSocketPath(actorUID)}
		if !slices.Equal(got, want) {
			t.Errorf("chSocketCandidates = %v, want %v", got, want)
		}
	})

	t.Run("a recorded socket settles it", func(t *testing.T) {
		got := chSocketCandidates(actorUID, &runningActor{apiSocket: "/run/recorded.sock"})
		if !slices.Equal(got, []string{"/run/recorded.sock"}) {
			t.Errorf("chSocketCandidates = %v, want only the recorded socket", got)
		}
	})
}

func TestFirstExistingPath(t *testing.T) {
	dir := t.TempDir()
	missing := filepath.Join(dir, "clh-api.sock")
	present := filepath.Join(dir, "clh-api-restore.sock")
	if err := os.WriteFile(present, nil, 0o600); err != nil {
		t.Fatalf("creating socket file: %v", err)
	}

	if got := firstExistingPath([]string{missing, present}); got != present {
		t.Errorf("firstExistingPath = %q, want the one that exists (%q)", got, present)
	}
	// None of them present is an answer too: the caller reads it as the guest
	// being gone, so the likeliest path is what the error should name.
	if got := firstExistingPath([]string{missing, missing + ".2"}); got != missing {
		t.Errorf("firstExistingPath = %q, want the likeliest path %q", got, missing)
	}
}
