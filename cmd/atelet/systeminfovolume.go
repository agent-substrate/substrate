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

	"github.com/agent-substrate/substrate/internal/ateompath"
	"github.com/agent-substrate/substrate/internal/pemutil"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/internal/volumepath"
	certsv1beta1 "k8s.io/api/certificates/v1beta1"
	"k8s.io/apimachinery/pkg/util/wait"
	certlisters "k8s.io/client-go/listers/certificates/v1beta1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
)

// systemInfoVolume is one system-info volume of an actor and its host root.
type systemInfoVolume struct {
	Name string
	Root string
	Spec *ateletpb.SystemInfoVolume

	// appliedHashes maps each projected bundle name to the trustBundleHash
	// last written into this volume.
	appliedHashes map[string]string
}

// systemInfoVolumesFor lists spec's system-info volumes with their host roots.
func systemInfoVolumesFor(actorUID string, spec *ateletpb.WorkloadSpec) []*systemInfoVolume {
	var volumes []*systemInfoVolume
	for _, vol := range spec.GetVolumes() {
		if si := vol.GetSystemInfo(); si != nil {
			volumes = append(volumes, &systemInfoVolume{
				Name: vol.GetName(),
				Root: ateompath.SystemInfoVolumeRoot(actorUID, vol.GetName()),
				Spec: si,
			})
		}
	}
	return volumes
}

type registeredActor struct {
	uid string
	ref resources.ActorRef

	// mu covers the volumes' file writes, appliedHashes, and stale.
	mu      sync.Mutex
	stale   bool
	volumes []*systemInfoVolume
}

// systemInfoVolumeRefresher writes system-info volumes when an actor starts
// and refreshes them as needed.
type systemInfoVolumeRefresher struct {
	lister    certlisters.ClusterTrustBundleLister
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
func newSystemInfoVolumeRefresher(lister certlisters.ClusterTrustBundleLister, informer cache.SharedIndexInformer) *systemInfoVolumeRefresher {
	r := &systemInfoVolumeRefresher{
		lister: lister,
		queue:  workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[string]()),
		actors: map[string]*registeredActor{},
	}
	if informer != nil {
		informer.AddEventHandler(r.eventHandler())
		r.hasSynced = informer.HasSynced
	}
	return r
}

// Register records actorUID's system-info volumes and writes their contents
// from current cluster state.
// Double registering an actor causes a panic.
func (r *systemInfoVolumeRefresher) Register(actorUID string, ref resources.ActorRef, volumes []*systemInfoVolume) error {
	actor := &registeredActor{uid: actorUID, ref: ref, volumes: volumes}
	// Held until the initial write finishes so a refresh cannot interleave.
	actor.mu.Lock()
	defer actor.mu.Unlock()

	r.mu.Lock()
	_, dup := r.actors[actorUID]
	if !dup {
		r.actors[actorUID] = actor
	}
	r.mu.Unlock()
	if dup {
		panic(fmt.Sprintf("system-info volumes: actor %s registered twice", actorUID))
	}

	for _, v := range volumes {
		if err := r.write(ref, actorUID, v); err != nil {
			return fmt.Errorf("while populating system-info volume %q: %w", v.Name, err)
		}
	}
	return nil
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

// collectData builds the volume's contents keyed by volume-relative path,
// plus each projected bundle's trustBundleHash.
func (r *systemInfoVolumeRefresher) collectData(ref resources.ActorRef, actorUID string, si *ateletpb.SystemInfoVolume) (payload map[string][]byte, bundleHashes map[string]string, err error) {
	payload = map[string][]byte{}
	bundleHashes = map[string]string{}
	for _, dataSourceAny := range si.GetDataSources() {
		switch dataSource := dataSourceAny.GetDataSource().(type) {
		case *ateletpb.SystemInfoDataSource_TrustBundle:
			tb := dataSource.TrustBundle
			objectName, raw, err := rawTrustBundle(r.lister, tb.GetName())
			if err != nil {
				return nil, nil, fmt.Errorf("system-info projection %q: %w", tb.GetPath(), err)
			}
			pemBundle, err := pemutil.SanitizeCertificateBundle([]byte(raw))
			if err != nil {
				return nil, nil, fmt.Errorf("system-info projection %q: unusable ClusterTrustBundle %q: %w", tb.GetPath(), objectName, err)
			}
			payload[tb.GetPath()] = pemBundle
			bundleHashes[tb.GetName()] = trustBundleHash(raw)
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
	return payload, bundleHashes, nil
}

// write brings the volume's files up to date.
func (r *systemInfoVolumeRefresher) write(ref resources.ActorRef, actorUID string, v *systemInfoVolume) error {
	payload, bundleHashes, err := r.collectData(ref, actorUID, v.Spec)
	if err != nil {
		return fmt.Errorf("while collecting volume contents: %w", err)
	}
	if err := os.MkdirAll(v.Root, 0o755); err != nil {
		return fmt.Errorf("while creating %q: %w", v.Root, err)
	}
	root, err := os.OpenRoot(v.Root)
	if err != nil {
		return fmt.Errorf("while opening %q: %w", v.Root, err)
	}
	defer root.Close()
	for _, relPath := range slices.Sorted(maps.Keys(payload)) {
		if err := writeSystemInfoFile(root, relPath, payload[relPath]); err != nil {
			return err
		}
	}
	v.appliedHashes = bundleHashes
	return nil
}

// writeSystemInfoFile writes one projected file inside root, skipping it if
// the contents already match. Also re-validates relPath.
func writeSystemInfoFile(root *os.Root, relPath string, data []byte) error {
	if err := volumepath.ValidateProjected(relPath); err != nil {
		return fmt.Errorf("invalid system-info path %q: %w", relPath, err)
	}
	dst := filepath.FromSlash(relPath)
	if existing, err := root.ReadFile(dst); err == nil && bytes.Equal(existing, data) {
		return nil
	}
	if dir := filepath.Dir(dst); dir != "." {
		if err := root.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("while creating parent of %q under %q: %w", relPath, root.Name(), err)
		}
	}
	if err := writeFileAtomicRoot(root, dst, data, 0o644); err != nil {
		return fmt.Errorf("while writing system-info file %q under %q: %w", relPath, root.Name(), err)
	}
	return nil
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
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := root.Rename(tmp, relPath); err != nil {
		return err
	}

	d, err := root.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// eventHandler enqueues the bundle names an event touches.
func (r *systemInfoVolumeRefresher) eventHandler() cache.ResourceEventHandler {
	enqueue := func(obj any) {
		if d, ok := obj.(cache.DeletedFinalStateUnknown); ok {
			obj = d.Obj
		}
		ctb, ok := obj.(*certsv1beta1.ClusterTrustBundle)
		if !ok {
			return
		}
		for _, name := range bundleNamesFor(ctb.Name) {
			r.queue.Add(name)
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
	_, raw, err := rawTrustBundle(r.lister, bundleName)
	if err != nil {
		slog.WarnContext(ctx, "Trust bundle unreadable; projected files keep their last contents", slog.String("bundle", bundleName), slog.Any("err", err))
		return nil
	}
	h := trustBundleHash(raw)
	var writeErr error
	refreshed := 0
	for _, actor := range targets {
		actor.mu.Lock()
		if actor.stale {
			actor.mu.Unlock()
			continue
		}
		for _, v := range actor.volumes {
			if !projectsBundle(v.Spec, bundleName) || v.appliedHashes[bundleName] == h {
				continue
			}
			if err := r.write(actor.ref, actor.uid, v); err != nil {
				slog.ErrorContext(ctx, "Failed to refresh system-info volume", slog.String("actor_uid", actor.uid), slog.String("volume", v.Name), slog.String("bundle", bundleName), slog.Any("err", err))
				writeErr = err
				continue
			}
			refreshed++
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
		if tb := ds.GetTrustBundle(); tb != nil && tb.GetName() == bundleName {
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
