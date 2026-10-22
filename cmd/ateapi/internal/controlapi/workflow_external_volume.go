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
	"log/slog"

	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
)

// ExternalVolumeWorkflow creates and deletes ExternalVolume records. A delete
// spans a CSI call and a store write.
type ExternalVolumeWorkflow struct {
	store          externalVolumeWorkflowStore
	pluginRegistry VolumePluginRegistry
}

// NewExternalVolumeWorkflow creates a new ExternalVolumeWorkflow.
func NewExternalVolumeWorkflow(store externalVolumeWorkflowStore, pluginRegistry VolumePluginRegistry) *ExternalVolumeWorkflow {
	return &ExternalVolumeWorkflow{store: store, pluginRegistry: pluginRegistry}
}

type externalVolumeWorkflowStore interface {
	CreateExternalVolume(ctx context.Context, volume *ateapipb.ExternalVolume) (*ateapipb.ExternalVolume, error)
	GetExternalVolume(ctx context.Context, volumeRef resources.ExternalVolumeRef) (*ateapipb.ExternalVolume, error)
	DeleteExternalVolume(ctx context.Context, volumeRef resources.ExternalVolumeRef, precondition store.DeletePreconditions) (*ateapipb.ExternalVolume, error)
}

// CreateExternalVolume registers a volume that already exists in the storage
// system.
func (w *ExternalVolumeWorkflow) CreateExternalVolume(ctx context.Context, volume *ateapipb.ExternalVolume) (*ateapipb.ExternalVolume, error) {
	volumeRef := resources.ExternalVolumeRefFromExternalVolume(volume)
	stored, err := w.store.CreateExternalVolume(ctx, volume)
	switch {
	case err == nil:
		return stored, nil
	case errors.Is(err, store.ErrFailedPrecondition):
		return nil, apierror.FailedPrecondition("Atespace %s not found", volumeRef.Atespace)
	case errors.Is(err, store.ErrAlreadyExists):
		return nil, apierror.AlreadyExists("ExternalVolume %s already exists", volumeRef)
	}
	return nil, fmt.Errorf("while creating external volume %s: %w", volumeRef, err)
}

// DeleteExternalVolume deletes the storage and then removes the record, so a
// failure part way is retried by deleting again. Rejected while any actor
// references the volume; the record's version guards the delete, so a
// reference taken after the check fails it.
func (w *ExternalVolumeWorkflow) DeleteExternalVolume(ctx context.Context, volumeRef resources.ExternalVolumeRef, precondition store.DeletePreconditions) (*ateapipb.ExternalVolume, error) {
	stored, err := w.store.GetExternalVolume(ctx, volumeRef)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, apierror.NotFound("ExternalVolume %s not found", volumeRef)
		}
		return nil, fmt.Errorf("while getting external volume: %w", err)
	}
	if err := precondition.Check(stored.GetMetadata()); err != nil {
		return nil, externalVolumeDeleteConflict(err, volumeRef, precondition)
	}
	if refs := stored.GetRefs(); len(refs) > 0 {
		return nil, apierror.FailedPrecondition("ExternalVolume %s is still referenced by %d actor(s), starting with %q", volumeRef, len(refs), refs[0].GetActorName())
	}
	if err := w.reclaimExternalVolume(ctx, stored); err != nil {
		return nil, err
	}

	guard := store.DeletePreconditions{UID: stored.GetMetadata().GetUid(), Version: stored.GetMetadata().GetVersion()}
	volume, err := w.store.DeleteExternalVolume(ctx, volumeRef, guard)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, apierror.NotFound("ExternalVolume %s not found", volumeRef)
		}
		if errors.Is(err, store.ErrUIDConflict) || errors.Is(err, store.ErrVersionConflict) {
			return nil, externalVolumeDeleteConflict(err, volumeRef, guard)
		}
		return nil, fmt.Errorf("while deleting external volume: %w", err)
	}
	return volume, nil
}

func externalVolumeDeleteConflict(err error, volumeRef resources.ExternalVolumeRef, precondition store.DeletePreconditions) error {
	if errors.Is(err, store.ErrUIDConflict) {
		return apierror.Aborted("ExternalVolume %s does not have uid %s", volumeRef, precondition.UID)
	}
	return apierror.Aborted("concurrent update conflict, please retry")
}

// TODO: add a reclaim policy that leaves the storage in place.
func (w *ExternalVolumeWorkflow) reclaimExternalVolume(ctx context.Context, volume *ateapipb.ExternalVolume) (err error) {
	ctx, done := stepSpan(ctx, "ReclaimExternalVolume")
	defer func() { err = done(err) }()

	volumeRef := resources.ExternalVolumeRefFromExternalVolume(volume)
	plugin, err := w.pluginRegistry.GetPlugin(ctx, volume.GetVolumeType())
	if err != nil {
		return fmt.Errorf("while getting the volume plugin for %q of external volume %s: %w", volume.GetVolumeType(), volumeRef, err)
	}
	if err := plugin.DeleteVolume(ctx, volume.GetVolumeId()); err != nil {
		if apierror.Code(err) == codes.NotFound {
			slog.WarnContext(ctx, "Volume not found during delete, assuming already deleted", slog.String("volume_id", volume.GetVolumeId()))
			return nil
		}
		return fmt.Errorf("while reclaiming the storage %q of external volume %s: %w", volume.GetVolumeId(), volumeRef, err)
	}
	return nil
}
