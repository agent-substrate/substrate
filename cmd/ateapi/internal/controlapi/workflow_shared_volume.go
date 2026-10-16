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
	"go.ate.dev/substrate/internal/volume"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	storagev1 "k8s.io/api/storage/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	storagev1listers "k8s.io/client-go/listers/storage/v1"
)

// SharedVolumeWorkflow handles the multi-step create and delete of an
// SharedVolume, each of which spans a store write and a CSI call.
type SharedVolumeWorkflow struct {
	store              sharedVolumeWorkflowStore
	storageClassLister storagev1listers.StorageClassLister
	pluginRegistry     VolumePluginRegistry
}

// NewSharedVolumeWorkflow creates a new SharedVolumeWorkflow.
func NewSharedVolumeWorkflow(
	store sharedVolumeWorkflowStore,
	storageClassLister storagev1listers.StorageClassLister,
	pluginRegistry VolumePluginRegistry,
) *SharedVolumeWorkflow {
	return &SharedVolumeWorkflow{
		store:              store,
		storageClassLister: storageClassLister,
		pluginRegistry:     pluginRegistry,
	}
}

type sharedVolumeWorkflowStore interface {
	CreateSharedVolume(ctx context.Context, volume *ateapipb.SharedVolume) (*ateapipb.SharedVolume, error)
	GetSharedVolume(ctx context.Context, volumeRef resources.SharedVolumeRef) (*ateapipb.SharedVolume, error)
	UpdateSharedVolume(ctx context.Context, volumeRef resources.SharedVolumeRef, precondition store.Precondition, mutate func(toUpdate *ateapipb.SharedVolume) error) (*ateapipb.SharedVolume, error)
	DeleteSharedVolume(ctx context.Context, volumeRef resources.SharedVolumeRef, precondition store.DeletePreconditions) (*ateapipb.SharedVolume, error)
	AcquireLease(ctx context.Context, key string) (*store.Lease, error)
}

// CreateSharedVolume reserves the name, provisions the volume under an ID
// derived from the record's UID, and stamps the handle the driver returned.
// A create that dies leaves a PENDING record; delete it to retry.
func (w *SharedVolumeWorkflow) CreateSharedVolume(ctx context.Context, volume *ateapipb.SharedVolume) (*ateapipb.SharedVolume, error) {
	volumeRef := resources.SharedVolumeRefFromSharedVolume(volume)

	ctx, lease, err := acquireSharedVolumeLease(ctx, w.store, volumeRef)
	if err != nil {
		return nil, err
	}
	defer lease.Close()

	toCreate, provisioning, err := w.resolveExternalVolumeSource(ctx, volume)
	if err != nil {
		return nil, err
	}
	reserved, err := w.ensureSharedVolumeReserved(ctx, volumeRef, toCreate)
	if err != nil {
		return nil, err
	}
	if provisioning == nil {
		return reserved, nil
	}
	volumeID, volumeContext, err := w.ensureSharedVolumeProvisioned(ctx, reserved, provisioning)
	if err != nil {
		return nil, err
	}
	return w.ensureSharedVolumeFinalized(ctx, reserved, volumeID, volumeContext)
}

type sharedVolumeProvisioning struct {
	provisioner string
	parameters  map[string]string
}

// resolveExternalVolumeSource returns the record to reserve and, unless the
// request registers an existing volume, what provisioning it needs.
func (w *SharedVolumeWorkflow) resolveExternalVolumeSource(ctx context.Context, volume *ateapipb.SharedVolume) (_ *ateapipb.SharedVolume, _ *sharedVolumeProvisioning, err error) {
	_, done := stepSpan(ctx, "ResolveExternalVolumeSource")
	defer func() { err = done(err) }()

	toCreate := &ateapipb.SharedVolume{
		Metadata: &ateapipb.ResourceMetadata{
			Atespace: volume.GetMetadata().GetAtespace(),
			Name:     volume.GetMetadata().GetName(),
		},
		DeleteTrigger:    volume.GetDeleteTrigger(),
		AccessMode:       volume.GetAccessMode(),
		StorageClassName: volume.GetStorageClassName(),
		VolumeId:         volume.GetVolumeId(),
		VolumeType:       volume.GetVolumeType(),
		VolumeContext:    volume.GetVolumeContext(),
		Capacity:         volume.GetCapacity(),
		Status:           &ateapipb.SharedVolumeStatus{State: ateapipb.SharedVolumeState_SHARED_VOLUME_STATE_PENDING},
	}

	var sc *storagev1.StorageClass
	if scName := volume.GetStorageClassName(); scName != "" {
		var err error
		if sc, err = w.storageClassLister.Get(scName); err != nil {
			if k8serrors.IsNotFound(err) {
				return nil, nil, apierror.FailedPrecondition("StorageClass %q not found", scName)
			}
			return nil, nil, apierror.Internal("failed to get StorageClass %q: %v", scName, err)
		}
	}

	if toCreate.VolumeId != "" {
		if toCreate.DeleteTrigger == ateapipb.DeleteTrigger_DELETE_TRIGGER_UNSPECIFIED {
			toCreate.DeleteTrigger = ateapipb.DeleteTrigger_DELETE_TRIGGER_MANUAL
		}
		if toCreate.VolumeType == "" && sc != nil {
			toCreate.VolumeType = sc.Provisioner
		}
		toCreate.Status.State = ateapipb.SharedVolumeState_SHARED_VOLUME_STATE_READY
		return toCreate, nil, nil
	}
	if sc == nil {
		return nil, nil, apierror.InvalidArgument("one of storage_class_name or volume_id is required")
	}
	if toCreate.DeleteTrigger == ateapipb.DeleteTrigger_DELETE_TRIGGER_UNSPECIFIED {
		toCreate.DeleteTrigger = ateapipb.DeleteTrigger_DELETE_TRIGGER_LAST_ACTOR
	}
	toCreate.VolumeType = sc.Provisioner
	return toCreate, &sharedVolumeProvisioning{provisioner: sc.Provisioner, parameters: sc.Parameters}, nil
}

func (w *SharedVolumeWorkflow) ensureSharedVolumeReserved(ctx context.Context, volumeRef resources.SharedVolumeRef, toCreate *ateapipb.SharedVolume) (_ *ateapipb.SharedVolume, err error) {
	ctx, done := stepSpan(ctx, "ReserveSharedVolume")
	defer func() { err = done(err) }()

	stored, err := w.store.CreateSharedVolume(ctx, toCreate)
	switch {
	case err == nil:
		return stored, nil
	case errors.Is(err, store.ErrFailedPrecondition):
		return nil, apierror.FailedPrecondition("Atespace %s not found", volumeRef.Atespace)
	case errors.Is(err, store.ErrAlreadyExists):
		return nil, apierror.AlreadyExists("SharedVolume %s already exists; delete it and create it again to retry", volumeRef)
	}
	return nil, fmt.Errorf("while reserving shared volume %s: %w", volumeRef, err)
}

// TODO: pass access_mode to CreateVolume as the volume capability once
// READ_WRITE_MANY is accepted.
func (w *SharedVolumeWorkflow) ensureSharedVolumeProvisioned(ctx context.Context, volume *ateapipb.SharedVolume, provisioning *sharedVolumeProvisioning) (_ string, _ map[string]string, err error) {
	ctx, done := stepSpan(ctx, "ProvisionSharedVolume")
	defer func() { err = done(err) }()

	volumeRef := resources.SharedVolumeRefFromSharedVolume(volume)
	plugin, err := w.pluginRegistry.GetPlugin(ctx, provisioning.provisioner)
	if err != nil {
		return "", nil, apierror.FailedPrecondition("failed to get volume plugin for driver %q (StorageClass %q): %v", provisioning.provisioner, volume.GetStorageClassName(), err)
	}
	resp, err := plugin.CreateVolume(ctx, createVolumeRequest(sharedVolumeID(volume.GetMetadata().GetUid()), volume.GetCapacity(), provisioning.parameters))
	if err != nil {
		return "", nil, apierror.Internal("failed to create shared volume %s: %v", volumeRef, err)
	}
	return resp.VolumeID, resp.VolumeContext, nil
}

// createVolumeRequest keeps the request construction out of callers whose
// parameters shadow the volume package.
func createVolumeRequest(name, capacity string, parameters map[string]string) volume.CreateVolumeRequest {
	return volume.CreateVolumeRequest{Name: name, Capacity: capacity, Parameters: parameters}
}

func (w *SharedVolumeWorkflow) ensureSharedVolumeFinalized(ctx context.Context, volume *ateapipb.SharedVolume, volumeID string, volumeContext map[string]string) (_ *ateapipb.SharedVolume, err error) {
	ctx, done := stepSpan(ctx, "FinalizeSharedVolume")
	defer func() { err = done(err) }()

	volumeRef := resources.SharedVolumeRefFromSharedVolume(volume)
	stored, err := w.store.UpdateSharedVolume(ctx, volumeRef, store.PreconditionFrom(volume), func(toUpdate *ateapipb.SharedVolume) error {
		toUpdate.VolumeId = volumeID
		toUpdate.VolumeContext = volumeContext
		toUpdate.Status.State = ateapipb.SharedVolumeState_SHARED_VOLUME_STATE_READY
		return nil
	})
	if err != nil {
		if errors.Is(err, store.ErrVersionConflict) {
			return nil, apierror.Aborted("concurrent update conflict, please retry")
		}
		if errors.Is(err, store.ErrNotFound) || errors.Is(err, store.ErrUIDConflict) {
			return nil, apierror.Aborted("SharedVolume %s was deleted while it was being created, please retry", volumeRef)
		}
		return nil, fmt.Errorf("while finalizing shared volume %s: %w", volumeRef, err)
	}
	return stored, nil
}

// DeleteSharedVolume deletes the storage and then removes the record, so a
// failure part way is retried by deleting again. Rejected while any actor
// references the volume.
func (w *SharedVolumeWorkflow) DeleteSharedVolume(ctx context.Context, volumeRef resources.SharedVolumeRef, precondition store.DeletePreconditions) (*ateapipb.SharedVolume, error) {
	ctx, lease, err := acquireSharedVolumeLease(ctx, w.store, volumeRef)
	if err != nil {
		return nil, err
	}
	defer lease.Close()

	stored, err := w.store.GetSharedVolume(ctx, volumeRef)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, apierror.NotFound("SharedVolume %s not found", volumeRef)
		}
		return nil, fmt.Errorf("while getting shared volume: %w", err)
	}
	if err := precondition.Check(stored.GetMetadata()); err != nil {
		return nil, sharedVolumeDeleteConflict(err, volumeRef, precondition)
	}
	if refs := stored.GetStatus().GetRefs(); len(refs) > 0 {
		return nil, apierror.FailedPrecondition("SharedVolume %s is still referenced by %d actor(s), starting with %q", volumeRef, len(refs), refs[0].GetActorName())
	}
	if err := w.reclaimSharedVolume(ctx, stored); err != nil {
		return nil, err
	}

	volume, err := w.store.DeleteSharedVolume(ctx, volumeRef, precondition)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, apierror.NotFound("SharedVolume %s not found", volumeRef)
		}
		if errors.Is(err, store.ErrUIDConflict) || errors.Is(err, store.ErrVersionConflict) {
			return nil, sharedVolumeDeleteConflict(err, volumeRef, precondition)
		}
		return nil, fmt.Errorf("while deleting shared volume: %w", err)
	}
	return volume, nil
}

func sharedVolumeDeleteConflict(err error, volumeRef resources.SharedVolumeRef, precondition store.DeletePreconditions) error {
	if errors.Is(err, store.ErrUIDConflict) {
		return apierror.Aborted("SharedVolume %s does not have uid %s", volumeRef, precondition.UID)
	}
	return apierror.Aborted("concurrent update conflict, please retry")
}

// TODO: add a reclaim policy that leaves the storage in place.
func (w *SharedVolumeWorkflow) reclaimSharedVolume(ctx context.Context, volume *ateapipb.SharedVolume) (err error) {
	ctx, done := stepSpan(ctx, "ReclaimSharedVolume")
	defer func() { err = done(err) }()

	volumeRef := resources.SharedVolumeRefFromSharedVolume(volume)
	if volume.GetVolumeType() == "" {
		markSkipped(ctx, "volume names no driver")
		return nil
	}
	volumeID := volume.GetVolumeId()
	if volumeID == "" {
		volumeID = sharedVolumeID(volume.GetMetadata().GetUid())
	}
	plugin, err := w.pluginRegistry.GetPlugin(ctx, volume.GetVolumeType())
	if err != nil {
		return fmt.Errorf("while getting the volume plugin for %q of shared volume %s: %w", volume.GetVolumeType(), volumeRef, err)
	}
	if err := plugin.DeleteVolume(ctx, volumeID); err != nil {
		if apierror.Code(err) == codes.NotFound {
			slog.WarnContext(ctx, "Volume not found during delete, assuming already deleted", slog.String("volume_id", volumeID))
			return nil
		}
		return fmt.Errorf("while reclaiming the storage %q of shared volume %s: %w", volumeID, volumeRef, err)
	}
	return nil
}

func sharedVolumeID(volumeUID string) string {
	return "substrate-" + volumeUID
}
