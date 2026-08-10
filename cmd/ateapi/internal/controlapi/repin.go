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
	"errors"
	"fmt"
	"slices"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// validateVersionRepin checks that an actor may be re-pinned to the
// ActorTemplateVersion named target. Shared by ResumeActor (upgrade on
// resume) and UpdateActor (pin revert). The actor's guest memory never
// survives a re-pin — resume restores only durable data — so the actor must
// be off-worker with no node-local state.
func validateVersionRepin(ctx context.Context, persistence store.Interface, actor *ateapipb.Actor, target string) error {
	if actor.GetActorTemplate() == "" {
		return status.Error(codes.FailedPrecondition, "only actors created from a control-plane ActorTemplate can be re-pinned")
	}
	switch actor.GetStatus() {
	case ateapipb.Actor_STATUS_SUSPENDED, ateapipb.Actor_STATUS_CRASHED:
	default:
		return status.Errorf(codes.FailedPrecondition, "actor %s is %s; re-pinning requires %s or %s",
			actor.GetMetadata().GetName(), actor.GetStatus(), ateapipb.Actor_STATUS_SUSPENDED, ateapipb.Actor_STATUS_CRASHED)
	}
	// A pause checkpoint is bound to its node and the images it ran; it cannot
	// restore under a different version.
	if actor.GetLocalSnapshotInfo() != nil {
		return status.Error(codes.FailedPrecondition, "actor holds a node-local snapshot; re-pinning requires a durable suspend")
	}

	atv, err := persistence.GetActorTemplateVersion(ctx, target)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return status.Errorf(codes.FailedPrecondition, "ActorTemplateVersion %q not found", target)
		}
		return fmt.Errorf("while getting ActorTemplateVersion: %w", err)
	}
	if atv.GetActorTemplate().GetName() != actor.GetActorTemplate() {
		return status.Errorf(codes.FailedPrecondition,
			"ActorTemplateVersion %q belongs to ActorTemplate %q, not %q",
			target, atv.GetActorTemplate().GetName(), actor.GetActorTemplate())
	}
	if state := atv.GetStatus().GetState(); state != ateapipb.ActorTemplateVersionStatus_STATE_READY {
		return status.Errorf(codes.FailedPrecondition, "ActorTemplateVersion %q is not READY (state %s)", target, state)
	}

	current, err := persistence.GetActorTemplateVersion(ctx, actor.GetActorTemplateVersion())
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return status.Errorf(codes.FailedPrecondition, "current ActorTemplateVersion %q not found", actor.GetActorTemplateVersion())
		}
		return fmt.Errorf("while getting current ActorTemplateVersion: %w", err)
	}
	if cur, next := current.GetStatus().GetResolvedSandbox().GetSandboxClass(), atv.GetStatus().GetResolvedSandbox().GetSandboxClass(); cur != next {
		return status.Errorf(codes.FailedPrecondition,
			"ActorTemplateVersion %q has sandbox class %s; the actor runs %s and cannot change class", target, next, cur)
	}
	if err := checkVolumesUnchanged(current, atv); err != nil {
		return err
	}
	return nil
}

// checkVolumesUnchanged rejects a re-pin that would add, remove, or alter
// volumes or their mount paths: the actor's durable data must re-materialize
// at exactly the paths the previous version wrote it.
func checkVolumesUnchanged(current, next *ateapipb.ActorTemplateVersion) error {
	curVols := volumesByName(current)
	nextVols := volumesByName(next)
	for name, vol := range curVols {
		other, ok := nextVols[name]
		if !ok {
			return status.Errorf(codes.FailedPrecondition, "ActorTemplateVersion %q removes volume %q; volumes are immutable across versions", next.GetMetadata().GetName(), name)
		}
		if !proto.Equal(vol, other) {
			return status.Errorf(codes.FailedPrecondition, "ActorTemplateVersion %q changes volume %q; volumes are immutable across versions", next.GetMetadata().GetName(), name)
		}
	}
	for name := range nextVols {
		if _, ok := curVols[name]; !ok {
			return status.Errorf(codes.FailedPrecondition, "ActorTemplateVersion %q adds volume %q; volumes are immutable across versions", next.GetMetadata().GetName(), name)
		}
	}
	if cur, next2 := volumeMounts(current), volumeMounts(next); !slices.Equal(cur, next2) {
		return status.Errorf(codes.FailedPrecondition, "ActorTemplateVersion %q changes volume mount paths; volumes are immutable across versions", next.GetMetadata().GetName())
	}
	return nil
}

func volumesByName(atv *ateapipb.ActorTemplateVersion) map[string]*ateapipb.Volume {
	out := make(map[string]*ateapipb.Volume, len(atv.GetSpec().GetVolumes()))
	for _, vol := range atv.GetSpec().GetVolumes() {
		out[vol.GetName()] = vol
	}
	return out
}

// volumeMounts flattens a version's (volume, mount path) pairs into a sorted
// list, deliberately ignoring which container mounts them: containers may be
// renamed or replaced between versions, but the paths where volume data
// appears must not move.
func volumeMounts(atv *ateapipb.ActorTemplateVersion) []string {
	var out []string
	for _, ctr := range atv.GetSpec().GetContainers() {
		for _, mount := range ctr.GetVolumeMounts() {
			out = append(out, mount.GetName()+":"+mount.GetMountPath())
		}
	}
	slices.Sort(out)
	return slices.Compact(out)
}
