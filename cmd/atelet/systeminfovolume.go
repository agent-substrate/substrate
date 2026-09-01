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
	"k8s.io/apimachinery/pkg/util/wait"
	certlisters "k8s.io/client-go/listers/certificates/v1beta1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
)

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
	volumes []*systemInfoVolume
}

// systemInfoVolumeRefresher owns the contents of system-info volumes, after
// kubelet's projected volumes: collectData builds a volume's complete
// contents, write applies them, and every lifecycle point uses the pair.
//
// TODO(#802): adopt kubelet's whole-volume AtomicWriter swap if a source
// ever needs multi-file atomicity.
type systemInfoVolumeRefresher struct {
	lister    certlisters.ClusterTrustBundleLister
	hasSynced cache.InformerSynced

	// queue carries bundle names from informer events to the run loop.
	queue workqueue.TypedRateLimitingInterface[string]

	// mu covers actors.
	mu     sync.Mutex
	actors map[string]*registeredActor
}

// newSystemInfoVolumeRefresher builds the refresher and, when informer is
// non-nil (unit tests pass nil), subscribes to ClusterTrustBundle events.
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

// Register records actorUID's system-info volumes — possibly none, every
// actor is tracked — and writes their complete contents from current cluster
// state, replacing any prior registration. Fail-closed: an actor that
// declared a projection must not start without it.
func (r *systemInfoVolumeRefresher) Register(actorUID string, ref resources.ActorRef, volumes []*systemInfoVolume) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, v := range volumes {
		if err := r.write(ref, actorUID, v); err != nil {
			return fmt.Errorf("while populating system-info volume %q: %w", v.Name, err)
		}
	}
	r.actors[actorUID] = &registeredActor{ref: ref, volumes: volumes}
	return nil
}

// Deregister drops actorUID's registration once the sandbox is down (or
// never came up). It returns only after any in-flight refresh finishes, so
// Checkpoint/Terminate can wipe the actor's directories safely afterwards.
func (r *systemInfoVolumeRefresher) Deregister(actorUID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.actors, actorUID)
}

// collectData builds the complete contents of one system-info volume, keyed
// by volume-relative path, after kubelet's projected.go. The bundle hashes
// fingerprint raw contents: sanitization shuffles the projected bytes.
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

// write makes the volume's on-disk contents match collectData: files are
// replaced via temp-and-rename and unchanged ones left alone, because
// restores re-bind suspend-time guest state by real path and inode.
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

// writeSystemInfoFile writes one projected file via temp-and-rename inside
// root, skipping it if contents already match. relPath is re-validated here
// and confined by root: atelet is the last line before the host filesystem.
func writeSystemInfoFile(root *os.Root, relPath string, data []byte) error {
	if relPath == "" || strings.HasPrefix(relPath, "/") {
		return fmt.Errorf("invalid system-info path %q: must be a non-empty relative path", relPath)
	}
	for _, seg := range strings.Split(relPath, "/") {
		if seg == ".." || seg == "." || seg == "" {
			return fmt.Errorf("invalid system-info path %q: must not contain empty, '.', or '..' segments", relPath)
		}
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
// name is safe under the single-writer mu contract.
func writeFileAtomicRoot(root *os.Root, relPath string, data []byte, perm os.FileMode) error {
	dir, base := filepath.Dir(relPath), filepath.Base(relPath)
	tmp := filepath.Join(dir, "."+base+".tmp")
	f, err := root.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, perm)
	if err != nil {
		return err
	}
	defer func() { _ = root.Remove(tmp) }() // no-op once the rename succeeds

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

// eventHandler adapts the ClusterTrustBundle informer: events only enqueue,
// the run loop writes. Deletion enqueues too — refreshBundle keeps last-good
// contents, so a running actor's files never go missing underneath it.
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

// run drains the refresh queue until ctx ends, in the repo's controller
// form (signercontroller). One worker: writes serialize under mu anyway.
// wait.UntilWithContext gives a worker panic the standard logged-stack crash.
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

// refreshBundle rewrites every registered volume that projects bundleName
// and has not applied its current contents, keeping last-good files on any
// failure: resolution errors wait for the next event, write errors requeue.
// A bundle no registered actor projects is not even resolved.
func (r *systemInfoVolumeRefresher) refreshBundle(ctx context.Context, bundleName string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.projectsLocked(bundleName) {
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
	for actorUID, actor := range r.actors {
		for _, v := range actor.volumes {
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
