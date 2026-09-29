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

package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"github.com/agent-substrate/substrate/internal/imagecache"
	"github.com/agent-substrate/substrate/internal/imagestreaming"
)

// scanActiveStreamedLeases scans the actors directory for active bundle overlay specs
// and returns all streamed image leases currently mounted by resident actors.
func scanActiveStreamedLeases(actorsDir string) ([]*imagestreaming.ActiveLease, error) {
	if actorsDir == "" {
		return nil, nil
	}
	actorEntries, err := os.ReadDir(actorsDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("listing actors dir %q: %w", actorsDir, err)
	}

	activeMap := make(map[string]*imagestreaming.ActiveLease)
	recordLease := func(imageRef, imageDigest string, streamed bool, layers []string) {
		if len(layers) == 0 || (!streamed && !isStreamedLayerSet(layers)) {
			return
		}
		key := imageRef
		if key == "" {
			key = imageDigest
		}
		if key == "" {
			return
		}
		lease, ok := activeMap[key]
		if !ok {
			lease = &imagestreaming.ActiveLease{
				ImageRef:    imageRef,
				ImageDigest: imageDigest,
				LayerDirs:   append([]string(nil), layers...),
				RefCount:    0,
			}
			activeMap[key] = lease
		}
		lease.RefCount++
	}

	for _, actor := range actorEntries {
		if !actor.IsDir() {
			continue
		}
		bundlesDir := filepath.Join(actorsDir, actor.Name(), "bundles")
		bundles, err := os.ReadDir(bundlesDir)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			slog.Warn("Failed to list bundles for actor during streaming reconciliation",
				slog.String("actor", actor.Name()), slog.Any("err", err))
			continue
		}
		for _, bundle := range bundles {
			if !bundle.IsDir() {
				continue
			}
			spec, err := imagecache.ReadSpec(filepath.Join(bundlesDir, bundle.Name()))
			if err != nil || spec == nil {
				continue
			}
			recordLease(spec.ImageRef, spec.ImageDigest, spec.Streamed, spec.Layers)
			for _, vol := range spec.ImageVolumes {
				recordLease(vol.ImageRef, vol.ImageDigest, vol.Streamed, vol.Layers)
			}
		}
	}

	result := make([]*imagestreaming.ActiveLease, 0, len(activeMap))
	for _, l := range activeMap {
		result = append(result, l)
	}
	return result, nil
}

func isStreamedLayerSet(layers []string) bool {
	for _, l := range layers {
		if strings.Contains(l, "gcfs") ||
			strings.Contains(l, "soci") ||
			strings.Contains(l, "stream") ||
			strings.HasPrefix(l, "/run/") {
			return true
		}
	}
	return false
}

// reconcileStreamingLeases discovers all active streamed images mounted by
// running or sleeping actors on this node, restores their leases in the
// ImageStreamer, and sweeps orphaned streaming workdirs.
func reconcileStreamingLeases(ctx context.Context, streamer imagestreaming.ImageStreamer, actorsDir string) error {
	if streamer == nil {
		return nil
	}
	leases, err := scanActiveStreamedLeases(actorsDir)
	if err != nil {
		return fmt.Errorf("scanning active streamed leases: %w", err)
	}
	if err := streamer.ReconcileLeases(ctx, leases); err != nil {
		return fmt.Errorf("reconciling leases with %s: %w", streamer.Name(), err)
	}
	if len(leases) == 0 {
		slog.DebugContext(ctx, "No active streamed image leases to reconcile on startup", slog.String("provider", streamer.Name()))
		return nil
	}
	slog.InfoContext(ctx, "Successfully reconciled streamed image leases on startup",
		slog.String("provider", streamer.Name()),
		slog.Int("reconciledImages", len(leases)))
	return nil
}
