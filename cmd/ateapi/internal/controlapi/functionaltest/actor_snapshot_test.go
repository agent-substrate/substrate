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

package functionaltest

import (
	"context"
	"fmt"
	"testing"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

// TestUpdateActorSnapshotTag_Preconditions verifies the optional version and uid
// guards carried in the tag's metadata.
func TestUpdateActorSnapshotTag_Preconditions(t *testing.T) {
	ns := namespaceForTest("ns-update-tag-preconditions")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	createTemplate(t, tc, ns)

	ctx := context.Background()
	const snapshotName, tagName = "snapshot-1", "before-upgrade"
	snapshotRef := createActorSnapshot(t, tc, snapshotName)

	// Each call to update() flips the scope, so every accepted update is an
	// observable write that bumps the version.
	update := func(meta *ateapipb.ResourceMetadata, scope ateapipb.ActorSnapshotTagScope) (*ateapipb.ActorSnapshotTag, error) {
		return updateActorSnapshotTagScope(tc, tagName, meta, scope)
	}

	// Delete and recreate the same atespace/name tag, so the first lifecycle's
	// uid becomes stale.
	staleUID := tagActorSnapshot(t, tc, snapshotRef, tagName).GetMetadata().GetUid()
	if _, err := tc.client.DeleteActorSnapshotTag(ctx, &ateapipb.DeleteActorSnapshotTagRequest{
		Tag: &ateapipb.ObjectRef{Atespace: testAtespace, Name: tagName},
	}); err != nil {
		t.Fatalf("DeleteActorSnapshotTag failed: %v", err)
	}

	tagged := tagActorSnapshot(t, tc, snapshotRef, tagName)
	staleVersion := tagged.GetMetadata().GetVersion()
	uid := tagged.GetMetadata().GetUid()
	if uid == staleUID {
		t.Fatalf("recreated tag reused uid %s, want a fresh one", uid)
	}
	// The uid from the deleted lifecycle must be rejected, even though the
	// atespace/name it was observed under still resolves.
	_, err := update(&ateapipb.ResourceMetadata{Uid: staleUID}, ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_PUBLISHED)
	assertGrpcError(t, err, codes.Aborted, fmt.Sprintf("ActorSnapshot tag %s/%s not found with uid %s", testAtespace, tagName, staleUID))

	// An unguarded update is last-writer-wins, and moves the tag past the
	// version observed above.
	unguarded, err := update(&ateapipb.ResourceMetadata{}, ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_PUBLISHED)
	if err != nil {
		t.Fatalf("UpdateActorSnapshotTag(no guards) failed: %v", err)
	}
	currentVersion := unguarded.GetMetadata().GetVersion()
	if currentVersion <= staleVersion {
		t.Fatalf("version = %d, want greater than %d after an update", currentVersion, staleVersion)
	}
	if got, want := unguarded.GetScope(), ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_PUBLISHED; got != want {
		t.Errorf("scope = %v, want %v", got, want)
	}

	// The version observed before that write is now stale: rejected rather than
	// silently overwriting the concurrent change.
	_, err = update(&ateapipb.ResourceMetadata{Version: staleVersion}, ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_ATESPACE)
	assertGrpcError(t, err, codes.Aborted, "concurrent update conflict, please retry")

	// Both uid and version matching the observed state: the update goes through.
	updated, err := update(&ateapipb.ResourceMetadata{Uid: uid, Version: currentVersion}, ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_ATESPACE)
	if err != nil {
		t.Fatalf("UpdateActorSnapshotTag(matching guards) failed: %v", err)
	}
	if got, want := updated.GetScope(), ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_ATESPACE; got != want {
		t.Errorf("scope = %v, want %v", got, want)
	}
	if updated.GetMetadata().GetVersion() <= currentVersion {
		t.Errorf("version = %d, want greater than %d", updated.GetMetadata().GetVersion(), currentVersion)
	}

	// The guard the client just satisfied is now stale in turn.
	_, err = update(&ateapipb.ResourceMetadata{Version: currentVersion}, ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_PUBLISHED)
	assertGrpcError(t, err, codes.Aborted, "concurrent update conflict, please retry")
}

func TestUpdateActorSnapshotTag_NotFound(t *testing.T) {
	ns := namespaceForTest("ns-update-tag-notfound")
	tc := setupTest(t, ns)
	defer tc.cleanup()

	_, err := tc.client.UpdateActorSnapshotTag(context.Background(), &ateapipb.UpdateActorSnapshotTagRequest{
		Tag: &ateapipb.ActorSnapshotTag{
			Metadata: &ateapipb.ResourceMetadata{Atespace: testAtespace, Name: "does-not-exist"},
			Scope:    ateapipb.ActorSnapshotTagScope_ACTOR_SNAPSHOT_TAG_SCOPE_PUBLISHED,
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"scope"}},
	})
	assertGrpcError(t, err, codes.NotFound, "ActorSnapshot tag test-atespace/does-not-exist not found")
}
