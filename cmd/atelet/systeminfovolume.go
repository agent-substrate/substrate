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
	"strings"
	"sync"
	"time"

	"github.com/agent-substrate/substrate/internal/pemutil"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/internal/resources"
	certsv1beta1 "k8s.io/api/certificates/v1beta1"
	certlisters "k8s.io/client-go/listers/certificates/v1beta1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
)

// trustBundleResyncPeriod is the ClusterTrustBundle informer's resync: a
// guard against missed watch events, not a retry mechanism — failed writes
// redrive through the refresher's workqueue.
const trustBundleResyncPeriod = 24 * time.Hour

// systemInfoVolume is one system-info volume of a registered actor: its spec
// plus where its files land on the host.
type systemInfoVolume struct {
	Name string
	Root string
	Spec *ateletpb.SystemInfoVolume

	// appliedHashes maps each projected bundle name to the trustBundleHash
	// last written into this volume. In memory only: after an atelet restart
	// the next Run/Restore rewrites the volume from current cluster state.
	appliedHashes map[string]string
}

type registeredActor struct {
	ref     resources.ActorRef
	volumes []systemInfoVolume
}

// systemInfoVolumeRefresher owns the contents of system-info volumes,
// modeled on kubelet's projected volumes: collectData is the one place that
// builds a volume's complete contents, write applies them, and every
// lifecycle point goes through the pair — Run/Restore via Register
// (fail-closed), and ClusterTrustBundle events via the workqueue while the
// actor runs (keep last-good, retry).
//
// TODO(#802): identity JWTs and certificates will ride the same refresh;
// adopt kubelet's whole-volume AtomicWriter swap if a source ever needs
// multi-file atomicity.
type systemInfoVolumeRefresher struct {
	lister certlisters.ClusterTrustBundleLister

	// queue carries bundle names from informer events to the run loop.
	queue workqueue.TypedRateLimitingInterface[string]

	// mu serializes registry mutations AND projection writes: Deregister
	// returning only after an in-flight refresh finishes is what lets
	// Checkpoint/Terminate wipe the actor's directories safely afterwards.
	mu     sync.Mutex
	actors map[string]*registeredActor
}

func newSystemInfoVolumeRefresher(lister certlisters.ClusterTrustBundleLister) *systemInfoVolumeRefresher {
	return &systemInfoVolumeRefresher{
		lister: lister,
		queue:  workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[string]()),
		actors: map[string]*registeredActor{},
	}
}

// Register records actorUID's system-info volumes and writes their complete
// contents from current cluster state, replacing any previous registration.
// Every error fails the actor start: an actor that declared a projection
// must not start without it. The caller deregisters if the start fails
// afterwards.
func (r *systemInfoVolumeRefresher) Register(actorUID string, ref resources.ActorRef, volumes []systemInfoVolume) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(volumes) == 0 {
		delete(r.actors, actorUID)
		return nil
	}
	actor := &registeredActor{ref: ref, volumes: volumes}
	for i := range actor.volumes {
		v := &actor.volumes[i]
		if err := r.write(ref, actorUID, v); err != nil {
			return fmt.Errorf("while populating system-info volume %q: %w", v.Name, err)
		}
	}
	r.actors[actorUID] = actor
	return nil
}

// Deregister drops actorUID's registration once the sandbox is down (or
// never came up). Per the mu contract, it returns only after any in-flight
// refresh finishes.
func (r *systemInfoVolumeRefresher) Deregister(actorUID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.actors, actorUID)
}

// collectData builds the complete contents of one system-info volume, keyed
// by volume-relative path — the single source of projected payloads, after
// kubelet's projected.go collectData. It also returns the raw-contents hash
// of each trust bundle it resolved: refreshes compare raw, never projected,
// bytes, because sanitization shuffles the anchors.
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

// write makes the volume's on-disk contents match collectData. Every file is
// a plain file at a stable real path, replaced by temp-and-rename: a reader
// sees old bytes or new, never partial, and restore re-binds suspend-time
// guest state by recorded path (virtiofsd find-paths, gVisor's gofer).
// Files whose contents already match are left alone — replacing an inode
// costs a suspended guest an EIO re-bind on resume.
func (r *systemInfoVolumeRefresher) write(ref resources.ActorRef, actorUID string, v *systemInfoVolume) error {
	payload, bundleHashes, err := r.collectData(ref, actorUID, v.Spec)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(v.Root, 0o755); err != nil {
		return fmt.Errorf("while creating %q: %w", v.Root, err)
	}
	for _, relPath := range slices.Sorted(maps.Keys(payload)) {
		if err := writeSystemInfoFile(v.Root, relPath, payload[relPath]); err != nil {
			return err
		}
	}
	v.appliedHashes = bundleHashes
	return nil
}

// writeSystemInfoFile writes one projected file at relPath under rootPath
// via write-to-temp-and-rename, creating parent directories as needed, and
// leaves a file whose contents already match untouched. relPath is validated
// defensively even though ActorTemplate validation already rejects non-clean
// paths: atelet is the last line before the value hits the host filesystem.
func writeSystemInfoFile(rootPath, relPath string, data []byte) error {
	if relPath == "" || strings.HasPrefix(relPath, "/") {
		return fmt.Errorf("invalid system-info path %q: must be a non-empty relative path", relPath)
	}
	for _, seg := range strings.Split(relPath, "/") {
		if seg == ".." || seg == "." || seg == "" {
			return fmt.Errorf("invalid system-info path %q: must not contain empty, '.', or '..' segments", relPath)
		}
	}
	dst := filepath.Join(rootPath, filepath.FromSlash(relPath))
	if existing, err := os.ReadFile(dst); err == nil && bytes.Equal(existing, data) {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return fmt.Errorf("while creating parent of %q: %w", dst, err)
	}
	if err := writeFileAtomic(dst, data, 0o644); err != nil {
		return fmt.Errorf("while writing system-info file %q: %w", dst, err)
	}
	return nil
}

// eventHandler adapts the refresher to the ClusterTrustBundle informer:
// events only enqueue, the run loop writes. Deletion takes the same path —
// refreshBundle keeps last-good contents when resolution fails, so running
// actors' files never go missing underneath them. Adding the handler after
// cache sync is fine: client-go replays the existing object as an Add.
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

// run drains the refresh queue until ctx ends, requeueing failed refreshes
// with backoff.
func (r *systemInfoVolumeRefresher) run(ctx context.Context) {
	go func() {
		<-ctx.Done()
		r.queue.ShutDown()
	}()
	for {
		bundleName, shutdown := r.queue.Get()
		if shutdown {
			return
		}
		if err := r.refreshBundle(ctx, bundleName); err != nil {
			r.queue.AddRateLimited(bundleName)
		} else {
			r.queue.Forget(bundleName)
		}
		r.queue.Done(bundleName)
	}
}

// refreshBundle rewrites every registered volume that projects bundleName
// and has not applied its current contents. All failures keep the affected
// volume's last-good contents: resolution failures return nil and wait for
// the bundle's next event, write failures return the error for the queue to
// redrive without stopping the remaining volumes.
func (r *systemInfoVolumeRefresher) refreshBundle(ctx context.Context, bundleName string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, raw, err := rawTrustBundle(r.lister, bundleName)
	if err != nil {
		if r.projectsLocked(bundleName) {
			slog.WarnContext(ctx, "Trust bundle unreadable; projected files keep their last contents", slog.String("bundle", bundleName), slog.Any("err", err))
		}
		return nil
	}
	h := trustBundleHash(raw)
	var writeErr error
	refreshed := 0
	for actorUID, actor := range r.actors {
		for i := range actor.volumes {
			v := &actor.volumes[i]
			if !projectsBundle(v.Spec, bundleName) || v.appliedHashes[bundleName] == h {
				continue
			}
			if err := r.write(actor.ref, actorUID, v); err != nil {
				slog.ErrorContext(ctx, "Failed to refresh system-info volume", slog.String("actor_uid", actorUID), slog.String("volume", v.Name), slog.String("bundle", bundleName), slog.Any("err", err))
				writeErr = err
				continue
			}
			refreshed++
		}
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

func (r *systemInfoVolumeRefresher) projectsLocked(bundleName string) bool {
	for _, actor := range r.actors {
		for _, v := range actor.volumes {
			if projectsBundle(v.Spec, bundleName) {
				return true
			}
		}
	}
	return false
}
