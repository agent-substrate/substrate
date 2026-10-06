//  Copyright 2026 Google LLC
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

package main

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"github.com/agent-substrate/substrate/cmd/atelet/internal/ateletpath"
	"github.com/agent-substrate/substrate/cmd/atelet/internal/trustbundle"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/internal/volumepath"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
)

// systemInfoVolumesFor lists spec's system-info volumes with their host roots.
func systemInfoVolumesFor(actorUID string, spec *ateletpb.WorkloadSpec) []*systemInfoVolume {
	var volumes []*systemInfoVolume
	for _, vol := range spec.GetVolumes() {
		if si := vol.GetSystemInfo(); si != nil {
			volumes = append(volumes, &systemInfoVolume{
				Name: vol.GetName(),
				Root: ateletpath.SystemInfoVolumeRoot(actorUID, vol.GetName()),
				Spec: si,
			})
		}
	}
	return volumes
}

type registeredActor struct {
	uid string
	ref resources.ActorRef

	// mu covers the volumes' file writes and stale.
	mu      sync.Mutex
	stale   bool
	volumes []*systemInfoVolume
}

// systemInfoVolumeRefresher writes system-info volumes when an actor starts
// and refreshes them as needed.
type systemInfoVolumeRefresher struct {
	bundles   *trustbundle.Source
	hasSynced cache.InformerSynced

	// queue carries bundle names from informer events to the run loop.
	queue workqueue.TypedRateLimitingInterface[string]

	// mu covers actors
	mu sync.Mutex
	// TODO(#1372): in memory only; an atelet restart drops the registry until
	// each actor's next Run/Restore.
	actors map[string]*registeredActor
}

// newSystemInfoVolumeRefresher subscribes to ClusterTrustBundle events.
func newSystemInfoVolumeRefresher(bundles *trustbundle.Source, informer cache.SharedIndexInformer) *systemInfoVolumeRefresher {
	r := &systemInfoVolumeRefresher{
		bundles: bundles,
		queue:   workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[string]()),
		actors:  map[string]*registeredActor{},
	}
	if informer != nil {
		informer.AddEventHandler(r.eventHandler())
		r.hasSynced = informer.HasSynced
	}
	return r
}

// Register records actorUID's system-info volumes and writes their contents
// from current cluster state. If actorUID is already registered (for example
// after a worker pod crash left a stale entry without Terminate), the previous
// registration is superseded, even if this registration's initial write fails.
// The failed registration is removed without restoring the previous one.
func (r *systemInfoVolumeRefresher) Register(actorUID string, ref resources.ActorRef, volumes []*systemInfoVolume) (*registeredActor, error) {
	actor := &registeredActor{uid: actorUID, ref: ref, volumes: volumes}
	// Held until the initial write finishes so a refresh cannot interleave.
	actor.mu.Lock()
	defer actor.mu.Unlock()

	r.mu.Lock()
	prev := r.actors[actorUID]
	r.actors[actorUID] = actor
	r.mu.Unlock()
	if prev != nil {
		prev.mu.Lock()
		prev.stale = true
		prev.mu.Unlock()
		slog.Info("Superseded a stale system-info volume registration",
			slog.String("actor_uid", actorUID),
			slog.Any("actor", ref))
	}

	for _, v := range volumes {
		if err := r.write(ref, actorUID, v); err != nil {
			r.mu.Lock()
			if r.actors[actorUID] == actor {
				delete(r.actors, actorUID)
				actor.stale = true
			}
			r.mu.Unlock()
			return nil, fmt.Errorf("while populating system-info volume %q: %w", v.Name, err)
		}
	}
	return actor, nil
}

// Deregister drops actorUID's registration. After Deregister returns, no more
// system-info volumes will be written for the actor.
func (r *systemInfoVolumeRefresher) Deregister(actorUID string) {
	r.mu.Lock()
	actor := r.actors[actorUID]
	delete(r.actors, actorUID)
	r.mu.Unlock()
	if actor != nil {
		// Setting stale stops a refresh that snapshotted the entry before
		// the delete.
		actor.mu.Lock()
		actor.stale = true
		actor.mu.Unlock()
	}
}

// DeregisterOwned removes owner only when its UID still points at it. This
// protects a newer registration from cleanup belonging to an older operation.
func (r *systemInfoVolumeRefresher) DeregisterOwned(owner *registeredActor) {
	if owner == nil {
		return
	}
	r.mu.Lock()
	if r.actors[owner.uid] != owner {
		r.mu.Unlock()
		return
	}
	delete(r.actors, owner.uid)
	r.mu.Unlock()

	owner.mu.Lock()
	owner.stale = true
	owner.mu.Unlock()
}

// collectData builds the volume's contents keyed by volume-relative path.
func (r *systemInfoVolumeRefresher) collectData(ref resources.ActorRef, actorUID string, si *ateletpb.SystemInfoVolume) (payload map[string][]byte, err error) {
	payload = map[string][]byte{}
	for _, dataSourceAny := range si.GetDataSources() {
		switch dataSource := dataSourceAny.GetDataSource().(type) {
		case *ateletpb.SystemInfoDataSource_TrustBundle:
			tb := dataSource.TrustBundle
			pemBundle, err := r.bundles.Combined(tb.GetNames())
			if err != nil {
				return nil, fmt.Errorf("system-info projection %q: %w", tb.GetPath(), err)
			}
			payload[tb.GetPath()] = pemBundle
		case *ateletpb.SystemInfoDataSource_ActorMetadata:
			for _, item := range dataSource.ActorMetadata.GetItems() {
				var value string
				switch item.GetField() {
				case ateletpb.ActorMetadataField_ACTOR_METADATA_FIELD_NAME:
					value = ref.Name
				case ateletpb.ActorMetadataField_ACTOR_METADATA_FIELD_ATESPACE:
					value = ref.Atespace
				case ateletpb.ActorMetadataField_ACTOR_METADATA_FIELD_UID:
					value = actorUID
				default:
					// Unknown fields come only from a newer ateapi; skip the
					// item rather than write an empty file under its path.
					continue
				}
				payload[item.GetPath()] = []byte(value)
			}
		}
	}
	return payload, nil
}

// write brings the volume's files up to date.
func (r *systemInfoVolumeRefresher) write(ref resources.ActorRef, actorUID string, v *systemInfoVolume) error {
	payload, err := r.collectData(ref, actorUID, v.Spec)
	if err != nil {
		return fmt.Errorf("while collecting volume contents: %w", err)
	}
	_, err = v.apply(payload)
	return err
}

// systemInfoVolume is one system-info volume of an actor and its host root.
type systemInfoVolume struct {
	Name string
	Root string
	Spec *ateletpb.SystemInfoVolume
}

// apply writes payload into the volume and reports whether any file changed.
func (v *systemInfoVolume) apply(payload map[string][]byte) (changed bool, err error) {
	if err := os.MkdirAll(v.Root, 0o755); err != nil {
		return false, fmt.Errorf("while creating %q: %w", v.Root, err)
	}
	root, err := os.OpenRoot(v.Root)
	if err != nil {
		return false, fmt.Errorf("while opening %q: %w", v.Root, err)
	}
	defer root.Close()
	for _, relPath := range slices.Sorted(maps.Keys(payload)) {
		written, err := writeSystemInfoFile(root, relPath, payload[relPath])
		if err != nil {
			return changed, err
		}
		changed = changed || written
	}
	return changed, nil
}

// writeSystemInfoFile writes one projected file inside root, skipping it if
// the contents already match, and reports whether it wrote. Also re-validates
// relPath.
func writeSystemInfoFile(root *os.Root, relPath string, data []byte) (bool, error) {
	if err := volumepath.ValidateProjected(relPath); err != nil {
		return false, fmt.Errorf("invalid system-info path %q: %w", relPath, err)
	}
	dst := filepath.FromSlash(relPath)
	if existing, err := root.ReadFile(dst); err == nil && bytes.Equal(existing, data) {
		return false, nil
	}
	if dir := filepath.Dir(dst); dir != "." {
		if err := root.MkdirAll(dir, 0o755); err != nil {
			return false, fmt.Errorf("while creating parent of %q under %q: %w", relPath, root.Name(), err)
		}
	}
	if err := writeFileAtomicRoot(root, dst, data, 0o644); err != nil {
		return false, fmt.Errorf("while writing system-info file %q under %q: %w", relPath, root.Name(), err)
	}
	return true, nil
}

// writeFileAtomicRoot is writeFileAtomic confined to root. The fixed temp
// name is safe because writers of an actor's volumes serialize on its lock.
func writeFileAtomicRoot(root *os.Root, relPath string, data []byte, perm os.FileMode) error {
	dir, base := filepath.Dir(relPath), filepath.Base(relPath)
	tmp := filepath.Join(dir, "."+base+".tmp")
	f, err := root.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, perm)
	if err != nil {
		return err
	}
	defer func() { _ = root.Remove(tmp) }()

	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Chmod(perm); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return root.Rename(tmp, relPath)
}

// eventHandler enqueues the bundle names an event touches.
func (r *systemInfoVolumeRefresher) eventHandler() cache.ResourceEventHandler {
	enqueue := func(obj any) {
		if d, ok := obj.(cache.DeletedFinalStateUnknown); ok {
			obj = d.Obj
		}
		ctb, err := meta.Accessor(obj)
		if err != nil {
			return
		}

		// Map clustertrustbundles to the particular Substrate trust bundle names they back.
		switch ctb.GetName() {
		case trustbundle.EgressCTB:
			r.queue.Add(trustbundle.EgressName)
		}
	}
	return cache.ResourceEventHandlerFuncs{
		AddFunc:    enqueue,
		UpdateFunc: func(_, obj any) { enqueue(obj) },
		DeleteFunc: enqueue,
	}
}

// run drains the queue until ctx ends.
func (r *systemInfoVolumeRefresher) run(ctx context.Context) {
	defer r.queue.ShutDown()
	if r.hasSynced != nil && !cache.WaitForCacheSync(ctx.Done(), r.hasSynced) {
		return
	}
	go wait.UntilWithContext(ctx, r.runWorker, time.Second)
	<-ctx.Done()
}

func (r *systemInfoVolumeRefresher) runWorker(ctx context.Context) {
	for r.processNextWorkItem(ctx) {
	}
}

func (r *systemInfoVolumeRefresher) processNextWorkItem(ctx context.Context) bool {
	bundleName, shutdown := r.queue.Get()
	if shutdown {
		return false
	}
	defer r.queue.Done(bundleName)

	if err := r.refreshBundle(ctx, bundleName); err != nil {
		r.queue.AddRateLimited(bundleName)
		return true
	}
	r.queue.Forget(bundleName)
	return true
}

// refreshBundle rewrites every registered volume that projects bundleName and
// is behind its current contents.
func (r *systemInfoVolumeRefresher) refreshBundle(ctx context.Context, bundleName string) error {
	targets := r.projecting(bundleName)
	if len(targets) == 0 {
		return nil
	}
	var writeErr error
	refreshed := 0
	for _, actor := range targets {
		actor.mu.Lock()
		if actor.stale {
			actor.mu.Unlock()
			continue
		}
		for _, v := range actor.volumes {
			if !projectsBundle(v.Spec, bundleName) {
				continue
			}
			payload, err := r.collectData(actor.ref, actor.uid, v.Spec)
			if err != nil {
				// Not requeued: the informer delivers the next change to
				// the bundle, which is what could make it resolvable.
				slog.WarnContext(ctx, "Trust bundle unresolvable; projected files keep their last contents", slog.String("actor_uid", actor.uid), slog.String("volume", v.Name), slog.String("bundle", bundleName), slog.Any("err", err))
				continue
			}
			changed, err := v.apply(payload)
			if err != nil {
				slog.ErrorContext(ctx, "Failed to refresh system-info volume", slog.String("actor_uid", actor.uid), slog.String("volume", v.Name), slog.String("bundle", bundleName), slog.Any("err", err))
				writeErr = err
				continue
			}
			if changed {
				refreshed++
			}
		}
		actor.mu.Unlock()
	}
	if refreshed > 0 {
		slog.InfoContext(ctx, "Refreshed projected trust bundle under running actors", slog.String("bundle", bundleName), slog.Int("volumes", refreshed))
	}
	return writeErr
}

// projectsBundle reports whether the volume spec projects the named bundle.
func projectsBundle(si *ateletpb.SystemInfoVolume, bundleName string) bool {
	for _, ds := range si.GetDataSources() {
		if tb := ds.GetTrustBundle(); tb != nil && slices.Contains(tb.GetNames(), bundleName) {
			return true
		}
	}
	return false
}

// projecting snapshots the actors with a volume projecting bundleName.
func (r *systemInfoVolumeRefresher) projecting(bundleName string) []*registeredActor {
	r.mu.Lock()
	defer r.mu.Unlock()
	var targets []*registeredActor
	for _, actor := range r.actors {
		for _, v := range actor.volumes {
			if projectsBundle(v.Spec, bundleName) {
				targets = append(targets, actor)
				break
			}
		}
	}
	return targets
}
