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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"

	"github.com/agent-substrate/substrate/internal/ateompath"
	certsv1beta1 "k8s.io/api/certificates/v1beta1"
	certlisters "k8s.io/client-go/listers/certificates/v1beta1"
	"k8s.io/client-go/tools/cache"
)

// trustBundleProjectionsFileName is the per-actor record recoverFromDisk
// rebuilds the registry from after an atelet restart. It sits beside (not
// inside) the volume roots under SystemInfoVolumeRootsDir, sharing their
// lifecycle: wiped by resetActorDirs, never mounted, never snapshotted. The
// dot cannot collide with a volume root — volume names are DNS labels.
const trustBundleProjectionsFileName = "trust-bundle-projections.json"

// trustBundleProjection is one projected trustBundle file of one actor.
type trustBundleProjection struct {
	// Bundle is the template-facing bundle name (allowlisted, see
	// supportedTrustBundles).
	Bundle string `json:"bundle"`
	// Root is the host path of the system-info volume root holding the file.
	Root string `json:"root"`
	// Path is the file's path relative to Root.
	Path string `json:"path"`
	// AppliedHash is the trustBundleHash of the raw backing contents last
	// projected to the file. Persisted so an atelet restart rewrites the file
	// only if the bundle actually changed in between: a no-op rewrite still
	// replaces an inode the guest may reference across a suspend.
	AppliedHash string `json:"appliedHash"`
}

// trustBundleRefresher keeps running actors' projected trust-bundle files
// current, matching kubelet's live-update semantics for clusterTrustBundle
// projections but writing via writeSystemInfoFile's temp-and-rename so
// visible paths never move. Run/Restore registers an actor's projections,
// Checkpoint/Terminate deregister, and in between the ClusterTrustBundle
// informer's events rewrite stale files in place.
//
// Refresh failures keep each file's last good contents; fail-closed applies
// at actor start only. The informer's relist-on-reconnect is the
// reconciliation loop, and the AppliedHash compare makes replays idempotent.
type trustBundleRefresher struct {
	lister certlisters.ClusterTrustBundleLister

	// actorsDir and stateFile default to the ateompath layout; tests inject
	// temp-dir equivalents.
	actorsDir string
	stateFile func(actorUID string) string

	mu     sync.Mutex
	actors map[string][]trustBundleProjection
}

func newTrustBundleRefresher(lister certlisters.ClusterTrustBundleLister) *trustBundleRefresher {
	return &trustBundleRefresher{
		lister:    lister,
		actorsDir: ateompath.ActorsDir,
		stateFile: func(actorUID string) string {
			return filepath.Join(ateompath.SystemInfoVolumeRootsDir(actorUID), trustBundleProjectionsFileName)
		},
		actors: map[string][]trustBundleProjection{},
	}
}

// Register records the projections writeSystemInfoVolume just wrote for
// actorUID, replacing any previous registration. It re-checks each bundle
// against the lister first: an update whose event fired before the entry
// existed reached no handler, but is already in the lister cache. Resolution
// failures keep the just-written contents, per the refresh contract; write
// failures (node filesystem trouble) fail the actor start.
func (r *trustBundleRefresher) Register(ctx context.Context, actorUID string, projections []trustBundleProjection) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(projections) == 0 {
		r.deregisterLocked(ctx, actorUID)
		return nil
	}
	for i := range projections {
		p := &projections[i]
		objectName, raw, err := rawTrustBundle(r.lister, p.Bundle)
		if err != nil {
			slog.WarnContext(ctx, "Trust bundle unreadable at registration; keeping the projection as written", slog.String("actor_uid", actorUID), slog.String("bundle", p.Bundle), slog.Any("err", err))
			continue
		}
		h := trustBundleHash(raw)
		if p.AppliedHash == h {
			continue
		}
		pemBundle, err := sanitizeTrustBundle(p.Bundle, objectName, raw)
		if err != nil {
			slog.WarnContext(ctx, "Trust bundle unusable at registration; keeping the projection as written", slog.String("actor_uid", actorUID), slog.String("bundle", p.Bundle), slog.Any("err", err))
			continue
		}
		if err := writeSystemInfoFile(p.Root, p.Path, pemBundle); err != nil {
			return fmt.Errorf("while re-projecting trust bundle %q: %w", p.Bundle, err)
		}
		p.AppliedHash = h
	}
	r.actors[actorUID] = projections
	if err := r.persistLocked(actorUID); err != nil {
		delete(r.actors, actorUID)
		return fmt.Errorf("while recording trust-bundle projections: %w", err)
	}
	return nil
}

// Deregister drops actorUID's registration and its on-disk record, once the
// sandbox is down (or never came up).
func (r *trustBundleRefresher) Deregister(ctx context.Context, actorUID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.deregisterLocked(ctx, actorUID)
}

func (r *trustBundleRefresher) deregisterLocked(ctx context.Context, actorUID string) {
	delete(r.actors, actorUID)
	if err := os.Remove(r.stateFile(actorUID)); err != nil && !errors.Is(err, os.ErrNotExist) {
		slog.WarnContext(ctx, "Failed to remove trust-bundle projections record", slog.String("actor_uid", actorUID), slog.Any("err", err))
	}
}

// eventHandler adapts the refresher to the ClusterTrustBundle informer.
// Adding it after cache sync is fine: client-go replays the existing object
// as an Add, and refreshBundle is idempotent.
func (r *trustBundleRefresher) eventHandler(ctx context.Context) cache.ResourceEventHandler {
	return cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj any) { r.onBundleObject(ctx, obj) },
		UpdateFunc: func(_, obj any) { r.onBundleObject(ctx, obj) },
		DeleteFunc: func(obj any) { r.onBundleDeleted(ctx, obj) },
	}
}

func (r *trustBundleRefresher) onBundleObject(ctx context.Context, obj any) {
	ctb, ok := obj.(*certsv1beta1.ClusterTrustBundle)
	if !ok {
		return
	}
	for _, name := range bundleNamesFor(ctb.Name) {
		r.refreshBundle(ctx, name)
	}
}

// onBundleDeleted only logs: a deleted backing object means every projection
// keeps its last good contents until the object returns (whereupon the
// relisted Add refreshes them) — the running actors' files must not go
// missing or empty underneath them.
func (r *trustBundleRefresher) onBundleDeleted(ctx context.Context, obj any) {
	if d, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		obj = d.Obj
	}
	ctb, ok := obj.(*certsv1beta1.ClusterTrustBundle)
	if !ok {
		return
	}
	for _, name := range bundleNamesFor(ctb.Name) {
		slog.WarnContext(ctx, "Backing ClusterTrustBundle deleted; projected files keep their last contents", slog.String("bundle", name), slog.String("object", ctb.Name))
	}
}

// bundleNamesFor returns the allowlisted bundle names backed by the
// ClusterTrustBundle objectName.
func bundleNamesFor(objectName string) []string {
	var names []string
	for name, object := range supportedTrustBundles {
		if object == objectName {
			names = append(names, name)
		}
	}
	return names
}

// refreshBundle re-projects bundleName for every registered projection whose
// AppliedHash no longer matches the backing contents. Errors are logged and
// leave last-good contents in place; per-file write errors don't stop the
// remaining projections (the next event or relist retries them all).
func (r *trustBundleRefresher) refreshBundle(ctx context.Context, bundleName string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	objectName, raw, err := rawTrustBundle(r.lister, bundleName)
	if err != nil {
		if r.projectsLocked(bundleName) {
			slog.WarnContext(ctx, "Trust bundle unreadable; projected files keep their last contents", slog.String("bundle", bundleName), slog.Any("err", err))
		}
		return
	}
	h := trustBundleHash(raw)
	var pemBundle []byte
	refreshed := 0
	for actorUID, projections := range r.actors {
		changed := false
		for i := range projections {
			p := &projections[i]
			if p.Bundle != bundleName || p.AppliedHash == h {
				continue
			}
			if pemBundle == nil {
				if pemBundle, err = sanitizeTrustBundle(bundleName, objectName, raw); err != nil {
					slog.WarnContext(ctx, "Trust bundle unusable; projected files keep their last contents", slog.String("bundle", bundleName), slog.Any("err", err))
					return
				}
			}
			if err := writeSystemInfoFile(p.Root, p.Path, pemBundle); err != nil {
				slog.ErrorContext(ctx, "Failed to refresh projected trust bundle", slog.String("actor_uid", actorUID), slog.String("bundle", bundleName), slog.Any("err", err))
				continue
			}
			p.AppliedHash = h
			changed = true
			refreshed++
		}
		if changed {
			if err := r.persistLocked(actorUID); err != nil {
				// Only the restart-recovery baseline is stale; the files are
				// current, and recovery re-syncing them again is harmless.
				slog.WarnContext(ctx, "Failed to record refreshed trust-bundle projections", slog.String("actor_uid", actorUID), slog.Any("err", err))
			}
		}
	}
	if refreshed > 0 {
		slog.InfoContext(ctx, "Refreshed projected trust bundle under running actors", slog.String("bundle", bundleName), slog.Int("files", refreshed))
	}
}

func (r *trustBundleRefresher) projectsLocked(bundleName string) bool {
	for _, projections := range r.actors {
		for _, p := range projections {
			if p.Bundle == bundleName {
				return true
			}
		}
	}
	return false
}

func (r *trustBundleRefresher) persistLocked(actorUID string) error {
	b, err := json.Marshal(r.actors[actorUID])
	if err != nil {
		return err
	}
	return writeFileAtomic(r.stateFile(actorUID), b, 0o600)
}

// recoverFromDisk rebuilds the registry after an atelet restart: running
// sandboxes outlive atelet, and losing their registrations would silently
// freeze their bundles until the next Run/Restore. Recovered bundles re-sync
// immediately, applying any rotation missed while atelet was down. Records
// of actors that stopped meanwhile only cause writes into directories the
// next Run/Restore or Terminate wipes.
func (r *trustBundleRefresher) recoverFromDisk(ctx context.Context) {
	entries, err := os.ReadDir(r.actorsDir)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			slog.ErrorContext(ctx, "Failed to scan for trust-bundle projection records", slog.Any("err", err))
		}
		return
	}
	bundles := map[string]bool{}
	r.mu.Lock()
	recovered := 0
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		actorUID := e.Name()
		b, err := os.ReadFile(r.stateFile(actorUID))
		if errors.Is(err, os.ErrNotExist) {
			continue
		} else if err != nil {
			slog.WarnContext(ctx, "Failed to read trust-bundle projections record", slog.String("actor_uid", actorUID), slog.Any("err", err))
			continue
		}
		var projections []trustBundleProjection
		if err := json.Unmarshal(b, &projections); err != nil {
			slog.WarnContext(ctx, "Discarding unparsable trust-bundle projections record", slog.String("actor_uid", actorUID), slog.Any("err", err))
			continue
		}
		if len(projections) == 0 {
			continue
		}
		r.actors[actorUID] = projections
		recovered++
		for _, p := range projections {
			bundles[p.Bundle] = true
		}
	}
	r.mu.Unlock()
	if recovered == 0 {
		return
	}
	slog.InfoContext(ctx, "Recovered trust-bundle projection registrations", slog.Int("actors", recovered))
	for name := range bundles {
		r.refreshBundle(ctx, name)
	}
}
