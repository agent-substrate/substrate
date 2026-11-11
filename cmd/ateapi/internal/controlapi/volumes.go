// Copyright 2026 The Agent Substrate Authors
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
	"crypto/sha256"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/ateapi/internal/store"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/internal/volume"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	storagev1listers "k8s.io/client-go/listers/storage/v1"
)

// externalVolumesStatus wraps vols for ActorStatus, leaving the field unset
// when the actor has no external volumes.
func externalVolumesStatus(vols []*ateapi.ExternalVolumeStatus) *ateapi.ExternalVolumes {
	if len(vols) == 0 {
		return nil
	}
	return &ateapi.ExternalVolumes{Volumes: vols}
}

// initialExternalVolumes constructs the actor's volume objects before volume
// creation. A volume has no storage volume ID until it is provisioned.
func initialExternalVolumes(ctx context.Context, scLister storagev1listers.StorageClassLister, template *ateapi.ActorTemplate) ([]*ateapi.ExternalVolumeStatus, error) {
	if template == nil {
		return nil, apierror.InvalidArgument("template is required")
	}
	var volumes []*ateapi.ExternalVolumeStatus
	for _, vol := range template.GetVolumes() {
		if vol.GetExternal().GetTemplate() != nil {
			scName := vol.GetExternal().GetTemplate().GetStorageClassName()
			sc, err := scLister.Get(scName)
			if err != nil {
				if k8serrors.IsNotFound(err) {
					return nil, apierror.FailedPrecondition("StorageClass %q not found", scName)
				}
				return nil, apierror.Internal("failed to get StorageClass %q: %v", scName, err)
			}

			volumes = append(volumes, &ateapi.ExternalVolumeStatus{
				Name:       vol.GetName(),
				VolumeType: sc.Provisioner,
			})
		}
	}
	return volumes, nil
}

// createActorVolumes provisions the external volumes in volumesToCreate that have no storage volume ID, using the provided volume plugin.
// It returns the list of external volumes (with storage IDs), or an error if any creation fails.
// Any volumes processed before or during a failure are returned alongside the error so they can be persisted on the actor.
func createActorVolumes(ctx context.Context, registry VolumePluginRegistry, scLister storagev1listers.StorageClassLister, actorUID string, template *ateapi.ActorTemplate, volumesToCreate []*ateapi.ExternalVolumeStatus) (resultVolumes []*ateapi.ExternalVolumeStatus, err error) {
	resultVolumes = make([]*ateapi.ExternalVolumeStatus, 0, len(volumesToCreate))

	var currentIdx int
	defer func() {
		if err != nil {
			// If we encounter an error, append the rest of the volumes to the result with the last
			// known state. This allows the caller to persist the state of the volumes.
			resultVolumes = append(resultVolumes, volumesToCreate[currentIdx:]...)
		}
	}()

	for idx, vol := range volumesToCreate {
		currentIdx = idx

		volName := vol.GetName()
		specVol := findTemplateVolume(template, volName)
		if specVol == nil || specVol.GetExternal().GetTemplate() == nil {
			return resultVolumes, apierror.NotFound("volume %q not found in template", volName)
		}

		if vol.GetStorageVolumeId() != "" {
			resultVolumes = append(resultVolumes, vol)
			continue
		}

		actVolID := actorVolumeID(actorUID, volName)

		scName := specVol.GetExternal().GetTemplate().GetStorageClassName()
		sc, err := scLister.Get(scName)
		if err != nil {
			if k8serrors.IsNotFound(err) {
				return resultVolumes, apierror.FailedPrecondition("StorageClass %q not found", scName)
			}
			return resultVolumes, apierror.Internal("failed to get StorageClass %q: %v", scName, err)
		}

		if sc.Provisioner != vol.GetVolumeType() {
			return resultVolumes, apierror.FailedPrecondition("volume %q has mismatched type %q (expected %q from StorageClass %q)", volName, vol.GetVolumeType(), sc.Provisioner, scName)
		}

		plugin, err := registry.GetPlugin(ctx, vol.GetVolumeType())
		if err != nil {
			return resultVolumes, apierror.FailedPrecondition("failed to get volume plugin for driver %q (StorageClass %q): %v", sc.Provisioner, scName, err)
		}

		resp, volErr := plugin.CreateVolume(ctx, volume.CreateVolumeRequest{
			Name:             actVolID,
			CapacityBytes:    specVol.GetExternal().GetTemplate().GetCapacityBytes(),
			Parameters:       sc.Parameters,
			SourceSnapshotID: vol.GetSourceSnapshotId(),
		})
		if volErr != nil {
			return resultVolumes, createVolumeError(volName, vol.GetSourceSnapshotId(), volErr)
		}

		if resp.VolumeID == "" {
			// An empty ID would read as not yet provisioned and be created again.
			return resultVolumes, apierror.Internal("volume plugin returned no ID for volume %q", specVol.GetName())
		}

		resultVolumes = append(resultVolumes, &ateapi.ExternalVolumeStatus{
			Name:             volName,
			StorageVolumeId:  resp.VolumeID,
			VolumeType:       sc.Provisioner,
			VolumeContext:    resp.VolumeContext,
			SourceSnapshotId: vol.GetSourceSnapshotId(),
		})
	}
	return resultVolumes, nil
}

// createVolumeError maps a failed CreateVolume to the error resume fails with,
// keeping the driver's meaning. sourceSnapshotID is the snapshot the volume was
// being restored from, or empty.
func createVolumeError(volName, sourceSnapshotID string, err error) error {
	code := status.Code(err)
	if sourceSnapshotID != "" {
		switch code {
		case codes.NotFound:
			// The snapshot does not exist, as when the tag was deleted
			// before this Actor first resumed, or the template names a
			// wrong snapshot. Retrying cannot help.
			return apierror.FailedPrecondition("snapshot %q to restore volume %q from does not exist in the storage system: %v", sourceSnapshotID, volName, err)
		case codes.FailedPrecondition:
			// For example, a snapshot that is not ready to use yet, from a
			// driver that cannot report readiness.
			return apierror.FailedPrecondition("cannot restore volume %q from snapshot %q: %v", volName, sourceSnapshotID, err)
		case codes.Aborted:
			return apierror.Aborted("failed to create volume %q: %v", volName, err)
		}
	}
	switch code {
	case codes.OutOfRange, codes.InvalidArgument:
		// For example, a capacity smaller than the snapshot.
		return apierror.FailedPrecondition("failed to create volume %q: %v", volName, err)
	case codes.Unavailable:
		return apierror.Unavailable("failed to create volume %q: %v", volName, err)
	case codes.DeadlineExceeded:
		return apierror.DeadlineExceeded("failed to create volume %q: %v", volName, err)
	case codes.ResourceExhausted:
		return apierror.ResourceExhausted("failed to create volume %q: %v", volName, err)
	}
	return apierror.Internal("failed to create volume %q: %v", volName, err)
}

// setTagVolumeSources sets source_snapshot_id on each of vols, the external
// volumes of an Actor created from tag, that tag captured, so its first resume
// restores them. A volume the tag did not capture keeps no source here, whether
// the tag left it out or captured no volumes. setContentSources may then give
// it its template's content_source.
//
// CreateTag does not wait for snapshots to become ready. A snapshot that is
// not yet ready to use is looked up in the storage system, and the call fails
// with FailedPrecondition until it is ready. One now ready is recorded as
// ready to use, so later Actors skip the lookup. A driver that cannot report
// readiness never has its snapshots marked ready, so they are used as they
// are, and provisioning fails if the snapshot cannot be restored.
//
// TODO: Keep ExternalVolumeRef volumes out of this once
// ExternalVolumeSource.ref lands. Tags never capture them.
func (s *ServiceImpl) setTagVolumeSources(ctx context.Context, tag *ateapi.Tag, vols []*ateapi.ExternalVolumeStatus) error {
	if len(vols) == 0 {
		return nil
	}
	tagRef := resources.TagRefFromTag(tag)
	tagUID := tag.GetMetadata().GetUid()
	snapshots, err := s.store.ListTagExternalVolumeSnapshots(ctx, tagRef, tagUID)
	if err != nil {
		return fmt.Errorf("while listing the external volume snapshots of tag %s: %w", tagRef, err)
	}
	// DeleteTag marks the tag before it deletes any of its snapshots, so a
	// tag that is still unmarked now had all of them when they were listed.
	// Check even when the list is empty: DeleteTag may have deleted them all.
	current, err := s.store.GetTag(ctx, tagRef)
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		return fmt.Errorf("while getting tag: %w", err)
	}
	if err != nil || current.GetMetadata().GetUid() != tagUID || current.GetMetadata().GetDeleteTime() != nil {
		return apierror.FailedPrecondition("source Tag %s is being deleted", tagRef)
	}

	byVolume := make(map[string]*ateapi.ExternalVolumeSnapshot, len(snapshots))
	for _, snap := range snapshots {
		byVolume[snap.GetOrigin().GetTagCapture().GetSourceVolumeName()] = snap
	}
	sources := make([]*ateapi.ExternalVolumeSnapshot, len(vols))
	var creating []*ateapi.ExternalVolumeSnapshot
	for i, vol := range vols {
		volName := vol.GetName()
		snap := byVolume[volName]
		if snap == nil {
			continue
		}
		switch st := snap.GetStatus(); {
		case st.GetSnapshotId() == "":
			// CreateTag completes a tag only once every snapshot has a
			// handle. One without means the stored status is wrong, so fail
			// instead of provisioning the volume empty.
			return apierror.Internal("Tag %s has no snapshot handle for volume %q", tagRef, volName)
		case !st.GetReadyToUse():
			creating = append(creating, snap)
		}
		// A handle means nothing to another driver. The StorageClass may have
		// been recreated with a different provisioner since the tag was taken.
		if snap.GetDriverName() != vol.GetVolumeType() {
			return apierror.FailedPrecondition("Tag %s snapshot of volume %q was taken by driver %q, but the volume is provisioned by %q", tagRef, volName, snap.GetDriverName(), vol.GetVolumeType())
		}
		sources[i] = snap
	}

	lookups := s.lookupVolumeSnapshots(ctx, creating)
	var nowReady []volumeSnapshotLookup
	var errs []error
	for _, l := range lookups {
		snapshotID := l.snapshot.GetStatus().GetSnapshotId()
		volName := l.snapshot.GetOrigin().GetTagCapture().GetSourceVolumeName()
		switch {
		case errors.Is(l.err, errors.ErrUnsupported):
			slog.WarnContext(ctx, "driver cannot report snapshot readiness; restoring anyway",
				slog.String("snapshot_id", snapshotID), slog.String("volume_name", volName))
		case l.err != nil:
			errs = append(errs, l.err)
		case !l.found:
			errs = append(errs, apierror.FailedPrecondition("Tag %s snapshot %q of volume %q no longer exists in the storage system", tagRef, snapshotID, volName))
		case !l.observed.ReadyToUse:
			errs = append(errs, apierror.FailedPrecondition("Tag %s snapshot %q of volume %q is not ready to use yet; retry later", tagRef, snapshotID, volName))
		default:
			nowReady = append(nowReady, l)
		}
	}
	// Recorded even when another snapshot is not ready, so a retry skips the
	// lookups that already succeeded.
	s.recordVolumeSnapshotsReady(ctx, nowReady)
	if len(errs) > 0 {
		return errs[0]
	}

	for i, vol := range vols {
		if sources[i] != nil {
			vol.SourceSnapshotId = sources[i].GetStatus().GetSnapshotId()
		}
	}
	return nil
}

// findTemplateVolume returns the template's volume with the given name, or nil
// if there is none.
func findTemplateVolume(template *ateapi.ActorTemplate, volName string) *ateapi.Volume {
	for _, vol := range template.GetVolumes() {
		if vol.GetName() == volName {
			return vol
		}
	}
	return nil
}

// setContentSources sets source_snapshot_id on each of vols that has none to
// its template's content_source, so its first resume restores the volume from
// that snapshot. Call it after setTagVolumeSources: a tag's snapshot of a
// volume takes precedence. It fails with FailedPrecondition if the volume's
// driver cannot restore snapshots.
func (s *ServiceImpl) setContentSources(ctx context.Context, template *ateapi.ActorTemplate, vols []*ateapi.ExternalVolumeStatus) error {
	for _, vol := range vols {
		volName := vol.GetName()
		snapshotID := findTemplateVolume(template, volName).GetExternal().GetTemplate().GetContentSource().GetSnapshot().GetStorageSnapshotId()
		if vol.GetSourceSnapshotId() != "" || snapshotID == "" {
			continue
		}
		plugin, err := s.pluginRegistry.GetPlugin(ctx, vol.GetVolumeType())
		if err != nil {
			return apierror.FailedPrecondition("failed to get volume plugin for driver %q: %v", vol.GetVolumeType(), err)
		}
		caps, err := plugin.ControllerCapabilities(ctx)
		if err != nil {
			return apierror.FailedPrecondition("failed to read capabilities of driver %q for volume %q: %v", vol.GetVolumeType(), volName, err)
		}
		if !caps.CreateDeleteSnapshot {
			return apierror.FailedPrecondition("volume %q restores from a snapshot, but its driver %q does not support snapshots", volName, vol.GetVolumeType())
		}
		vol.SourceSnapshotId = snapshotID
	}
	return nil
}

// refreshTagVolumeSnapshots asks the storage system whether the tag's
// ExternalVolumeSnapshots that have a handle but are not ready to use are
// ready now, and records those that are. CreateTag does not wait for
// snapshots, so GetTag uses this to show when a tag can seed an Actor. Only a
// tag that tagRefreshable accepts is refreshed. A failed lookup or write is
// logged and leaves the snapshot not ready.
func (s *ServiceImpl) refreshTagVolumeSnapshots(ctx context.Context, tag *ateapi.Tag) {
	if !tagRefreshable(tag) {
		return
	}
	tagRef := resources.TagRefFromTag(tag)
	snapshots, err := s.store.ListTagExternalVolumeSnapshots(ctx, tagRef, tag.GetMetadata().GetUid())
	if err != nil {
		slog.WarnContext(ctx, "failed to list the external volume snapshots of a tag",
			slog.String("tag", tagRef.String()), slog.Any("error", err))
		return
	}
	var creating []*ateapi.ExternalVolumeSnapshot
	for _, snap := range snapshots {
		if st := snap.GetStatus(); st.GetSnapshotId() != "" && !st.GetReadyToUse() {
			creating = append(creating, snap)
		}
	}
	s.recordVolumeSnapshotsReady(ctx, readyVolumeSnapshots(ctx, s.lookupVolumeSnapshots(ctx, creating)))
}

// refreshVolumeSnapshot asks the storage system whether a snapshot that has a
// handle but is not ready to use is ready now, as refreshTagVolumeSnapshots
// does, and returns the snapshot as recorded. A snapshot whose tag
// tagRefreshable refuses is returned unchanged.
func (s *ServiceImpl) refreshVolumeSnapshot(ctx context.Context, snap *ateapi.ExternalVolumeSnapshot) *ateapi.ExternalVolumeSnapshot {
	if st := snap.GetStatus(); st.GetSnapshotId() == "" || st.GetReadyToUse() {
		return snap
	}
	origin := snap.GetOrigin().GetTagCapture()
	tag, err := s.store.GetTag(ctx, resources.TagRefFromObjectRef(origin.GetTag()))
	if err != nil {
		if !errors.Is(err, store.ErrNotFound) {
			slog.WarnContext(ctx, "failed to get the tag of an external volume snapshot",
				slog.String("tag", resources.TagRefFromObjectRef(origin.GetTag()).String()), slog.Any("error", err))
		}
		return snap
	}
	if tag.GetMetadata().GetUid() != origin.GetTagUid() || !tagRefreshable(tag) {
		return snap
	}
	ready := readyVolumeSnapshots(ctx, []volumeSnapshotLookup{s.lookupVolumeSnapshot(ctx, snap)})
	if len(ready) == 0 {
		return snap
	}
	return s.recordVolumeSnapshotsReady(ctx, ready)[0]
}

// tagRefreshable reports whether the tag's snapshots may be refreshed: the tag
// is finished and is not being deleted.
func tagRefreshable(tag *ateapi.Tag) bool {
	completed := tagDurableSnapshotURI(tag, ateapi.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_COMPLETED) != ""
	return completed && tag.GetMetadata().GetDeleteTime() == nil
}

// readyVolumeSnapshots returns the lookups that found their snapshot ready. A
// failed lookup is logged.
func readyVolumeSnapshots(ctx context.Context, lookups []volumeSnapshotLookup) []volumeSnapshotLookup {
	var ready []volumeSnapshotLookup
	for _, l := range lookups {
		switch {
		case errors.Is(l.err, errors.ErrUnsupported):
		case l.err != nil:
			slog.WarnContext(ctx, "failed to read an external volume snapshot",
				slog.String("external_volume_snapshot", resources.ExternalVolumeSnapshotRefFromExternalVolumeSnapshot(l.snapshot).String()),
				slog.Any("error", l.err))
		case l.found && l.observed.ReadyToUse:
			ready = append(ready, l)
		}
	}
	return ready
}

// volumeSnapshotLookup is what the storage system reported about snapshot.
type volumeSnapshotLookup struct {
	snapshot *ateapi.ExternalVolumeSnapshot
	observed volume.Snapshot
	found    bool
	// err wraps errors.ErrUnsupported if the driver cannot look snapshots up.
	// Otherwise it is an apierror.
	err error
}

// lookupVolumeSnapshots asks the driver of each snapshot about it. Driver
// calls can take seconds, so they run concurrently.
func (s *ServiceImpl) lookupVolumeSnapshots(ctx context.Context, snapshots []*ateapi.ExternalVolumeSnapshot) []volumeSnapshotLookup {
	lookups := make([]volumeSnapshotLookup, len(snapshots))
	var wg sync.WaitGroup
	for i, snap := range snapshots {
		wg.Go(func() {
			lookups[i] = s.lookupVolumeSnapshot(ctx, snap)
		})
	}
	wg.Wait()
	return lookups
}

// lookupVolumeSnapshot asks the driver that took snap about it.
func (s *ServiceImpl) lookupVolumeSnapshot(ctx context.Context, snap *ateapi.ExternalVolumeSnapshot) volumeSnapshotLookup {
	l := volumeSnapshotLookup{snapshot: snap}
	driver := snap.GetDriverName()
	plugin, err := s.pluginRegistry.GetPlugin(ctx, driver)
	if err != nil {
		l.err = apierror.FailedPrecondition("failed to get volume plugin for driver %q: %v", driver, err)
		return l
	}
	snapshotID := snap.GetStatus().GetSnapshotId()
	l.observed, l.found, err = plugin.GetSnapshot(ctx, snapshotID)
	switch {
	case errors.Is(err, errors.ErrUnsupported):
		l.err = err
	case err != nil:
		l.err = getSnapshotError(snapshotID, snap.GetOrigin().GetTagCapture().GetSourceVolumeName(), err)
	}
	return l
}

// getSnapshotError maps a failed snapshot lookup to an apierror. Transient and
// quota codes pass through, as in createVolumeError, so callers can retry.
func getSnapshotError(snapshotID, volName string, err error) error {
	const format = "failed to read snapshot %q of volume %q: %v"
	switch status.Code(err) {
	case codes.Unavailable:
		return apierror.Unavailable(format, snapshotID, volName, err)
	case codes.DeadlineExceeded:
		return apierror.DeadlineExceeded(format, snapshotID, volName, err)
	case codes.ResourceExhausted:
		return apierror.ResourceExhausted(format, snapshotID, volName, err)
	}
	return apierror.Internal(format, snapshotID, volName, err)
}

// recordVolumeSnapshotsReady records each looked-up snapshot as ready to use,
// with the size the driver reported if none is recorded, and returns the
// stored snapshots in the same order. Recording readiness only saves later
// callers a lookup, so a failed write is logged, and the snapshot is returned
// with ready_to_use set but not stored. A snapshot deleted meanwhile, as
// DeleteTag does, is returned as it was looked up.
func (s *ServiceImpl) recordVolumeSnapshotsReady(ctx context.Context, lookups []volumeSnapshotLookup) []*ateapi.ExternalVolumeSnapshot {
	recorded := make([]*ateapi.ExternalVolumeSnapshot, len(lookups))
	for i, l := range lookups {
		recorded[i] = s.recordVolumeSnapshotReady(ctx, l.snapshot, l.observed)
	}
	return recorded
}

func (s *ServiceImpl) recordVolumeSnapshotReady(ctx context.Context, snap *ateapi.ExternalVolumeSnapshot, observed volume.Snapshot) *ateapi.ExternalVolumeSnapshot {
	ref := resources.ExternalVolumeSnapshotRefFromExternalVolumeSnapshot(snap)
	setReady := func(toUpdate *ateapi.ExternalVolumeSnapshot) error {
		toUpdate.Status.ReadyToUse = true
		if toUpdate.GetStatus().GetRestoreSizeBytes() == 0 && observed.SizeBytes > 0 {
			toUpdate.Status.RestoreSizeBytes = observed.SizeBytes
		}
		return nil
	}
	updated, err := s.UpdateExternalVolumeSnapshot(ctx, ref, store.PreconditionFrom(snap), setReady)
	if errors.Is(err, store.ErrVersionConflict) {
		// Another reader may have recorded it first. Retry once against the
		// stored snapshot.
		var stored *ateapi.ExternalVolumeSnapshot
		if stored, err = s.store.GetExternalVolumeSnapshot(ctx, ref); err == nil {
			updated, err = s.UpdateExternalVolumeSnapshot(ctx, ref, store.PreconditionFrom(stored), setReady)
		}
	}
	switch {
	case err == nil:
		return updated
	case errors.Is(err, store.ErrNotFound) || errors.Is(err, store.ErrUIDConflict):
		return snap
	}
	slog.WarnContext(ctx, "failed to record an external volume snapshot as ready",
		slog.String("external_volume_snapshot", ref.String()), slog.Any("error", err))
	stamped := proto.CloneOf(snap)
	_ = setReady(stamped)
	return stamped
}

// deleteActorVolumes deletes all external volumes in the list.
func deleteActorVolumes(ctx context.Context, registry VolumePluginRegistry, actorUID string, volumes []*ateapi.ExternalVolumeStatus) error {
	if actorUID == "" {
		return errors.New("actorUID is required")
	}
	var errs []error
	for _, vol := range volumes {
		volID := vol.GetStorageVolumeId()
		if volID == "" {
			// If the volume hasn't been successfully created yet, it's possible
			// that it doesn't have a storage volume ID. In that case, fallback
			// to the original requested volID.
			volID = actorVolumeID(actorUID, vol.GetName())
		}
		// TODO: Standardize volume plugin lookup and error handling across control plane
		// and worker plane (e.g. via a shared helper).
		plugin, err := registry.GetPlugin(ctx, vol.GetVolumeType())
		if err != nil {
			errs = append(errs, fmt.Errorf("failed to get volume plugin for %q: %w", vol.GetVolumeType(), err))
			continue
		}
		if err := plugin.DeleteVolume(ctx, volID); err != nil {
			if status.Code(err) == codes.NotFound {
				slog.WarnContext(ctx, "Volume not found during delete, assuming already deleted", slog.String("volume_id", volID))
				continue
			}
			errs = append(errs, fmt.Errorf("failed to delete volume %q: %w", volID, err))
		}
	}
	return errors.Join(errs...)
}

// getMountedActorVolumes filters the actor's volumes and returns only those that are declared and mounted in the ActorTemplate.
func getMountedActorVolumes(ctx context.Context, ref *ateapi.ObjectRef, volumes []*ateapi.ExternalVolumeStatus, template *ateapi.ActorTemplate) []*ateapi.ExternalVolumeStatus {
	var mounted []*ateapi.ExternalVolumeStatus
	for _, vol := range volumes {
		// Find the corresponding volume in the ActorTemplate to check if it's mounted
		var matchedTemplateVol *ateapi.Volume
		for _, tVol := range template.GetVolumes() {
			if vol.GetName() == tVol.GetName() {
				matchedTemplateVol = tVol
				break
			}
		}

		if matchedTemplateVol == nil {
			slog.WarnContext(ctx, "Volume not found in template, skipping", slog.String("volume_id", vol.GetStorageVolumeId()))
			continue
		}

		if !isVolumeMounted(matchedTemplateVol.GetName(), template) {
			slog.InfoContext(ctx, "Volume not mounted in template, skipping", slog.String("volume_id", vol.GetStorageVolumeId()))
			continue
		}
		mounted = append(mounted, vol)
	}
	return mounted
}

func actorVolumeID(actorUID string, volumeName string) string {
	return fmt.Sprintf("substrate-%s-%s", actorUID, volumeName)
}

// tagVolumeSnapshotID names the snapshot a tag takes of one volume, both in the
// storage system and as an ExternalVolumeSnapshot. It uses the tag's UID so
// that a second create under a reused tag name gets new snapshot names. The
// volume name is hashed so the result stays at 62 characters for any volume
// name. Some drivers, like the Compute Engine PD driver, use it as a resource
// name limited to 63 characters.
func tagVolumeSnapshotID(tagUID string, volumeName string) string {
	sum := sha256.Sum256([]byte(volumeName))
	return fmt.Sprintf("substrate-snap-%s-%x", tagUID, sum[:5])
}

// detachActorVolumes detaches all mounted external volumes for an actor from its assigned node.
func detachActorVolumes(ctx context.Context, registry VolumePluginRegistry, actor *ateapi.Actor, template *ateapi.ActorTemplate, action string) error {
	node := actor.GetStatus().GetAssignedNode()
	if node == "" {
		slog.WarnContext(ctx, fmt.Sprintf("Actor has no assigned node during %s, skipping detach volumes", action), slog.String("actor_id", actor.GetMetadata().GetName()))
		return nil
	}

	ref := &ateapi.ObjectRef{Atespace: actor.GetMetadata().GetAtespace(), Name: actor.GetMetadata().GetName()}
	// If the template is available, only detach volumes that are actively mounted
	// in the template's containers. If the template is missing/deleted, fall back to
	// attempting detachment for all external volumes recorded on the actor so we do
	// not orphan attached disks on the worker node.
	volumesToDetach := actor.GetStatus().GetExternalVolumes().GetVolumes()
	if template != nil {
		volumesToDetach = getMountedActorVolumes(ctx, ref, actor.GetStatus().GetExternalVolumes().GetVolumes(), template)
	}
	// Collect errors for all volumes to detach, but continue processing so we attempt to detach all volumes.
	var errs []error
	for _, vol := range volumesToDetach {
		// StorageVolumeId is only populated once the volume is provisioned.
		// Skip volumes that were never created (e.g. creation failed).
		if vol.GetStorageVolumeId() == "" {
			slog.WarnContext(ctx, "Volume has no storage volume ID, skipping detach", slog.String("volume_name", vol.GetName()), slog.String("actor_id", actor.GetMetadata().GetName()))
			continue
		}
		slog.InfoContext(ctx, "Detaching volume from node", slog.String("volume_id", vol.GetStorageVolumeId()), slog.String("node", node))
		plugin, err := registry.GetPlugin(ctx, vol.GetVolumeType())
		if err != nil {
			errs = append(errs, fmt.Errorf("failed to get volume plugin for %q: %w", vol.GetVolumeType(), err))
			continue
		}
		if err := plugin.DetachVolume(ctx, vol.GetStorageVolumeId(), node); err != nil {
			if status.Code(err) == codes.NotFound {
				slog.WarnContext(ctx, "Volume not found during detach, assuming already detached", slog.String("volume_id", vol.GetStorageVolumeId()), slog.String("node", node))
				continue
			}
			errs = append(errs, fmt.Errorf("failed to detach volume %q from node %q: %w", vol.GetStorageVolumeId(), node, err))
		}
	}
	return errors.Join(errs...)
}
