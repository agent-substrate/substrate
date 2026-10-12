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

package ateom

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"

	"google.golang.org/protobuf/proto"

	"github.com/agent-substrate/substrate/internal/hardware"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
)

// RuntimeProber runs one sandbox version's assets on this worker and returns
// the class-specific compatibility attributes of the runtime they make, such
// as the CPU features it exposes to guests.
type RuntimeProber interface {
	Probe(ctx context.Context, v *ateletpb.SandboxVersion) ([]*ateletpb.AttributeEntry, error)
}

// runtimeCache keeps the prober's findings per sandbox_assets_hash, so a
// refresh reruns only the versions whose assets changed.
type runtimeCache struct {
	probed map[string][]*ateletpb.AttributeEntry
}

func newRuntimeCache() *runtimeCache {
	return &runtimeCache{probed: map[string][]*ateletpb.AttributeEntry{}}
}

// probe returns p's attributes for v, from the cache when v's assets were
// probed before. Failures are not cached.
func (c *runtimeCache) probe(ctx context.Context, p RuntimeProber, hash string, v *ateletpb.SandboxVersion) ([]*ateletpb.AttributeEntry, error) {
	if attrs, ok := c.probed[hash]; ok {
		return attrs, nil
	}
	attrs, err := p.Probe(ctx, v)
	if err != nil {
		return nil, err
	}
	c.probed[hash] = attrs
	return attrs, nil
}

// retain drops every entry whose hash is not in keep.
func (c *runtimeCache) retain(keep map[string]bool) {
	maps.DeleteFunc(c.probed, func(hash string, _ []*ateletpb.AttributeEntry) bool { return !keep[hash] })
}

// sandboxRuntimes runs every version in vers and returns the runtime actors
// start under (the default version's) and the other runtimes the worker can
// restore snapshots from.
//
// The default version must probe: without it the worker cannot start actors.
// Any other version that fails is left out of restorable, so its snapshots
// restore elsewhere or cold boot rather than keeping the worker from
// registering at all.
func sandboxRuntimes(ctx context.Context, class string, p RuntimeProber, cache *runtimeCache, vers []*ateletpb.SandboxVersion) (*ateletpb.SandboxRuntime, []*ateletpb.SandboxRuntime, error) {
	current := make(map[string]bool, len(vers))
	for _, v := range vers {
		current[sandboxAssetsHash(class, v)] = true
	}
	// Even on failure: versions that are gone are never asked for again.
	defer cache.retain(current)

	i := slices.IndexFunc(vers, (*ateletpb.SandboxVersion).GetIsDefault)
	if i < 0 {
		return nil, nil, errors.New("sandbox versions name no default")
	}
	def, err := buildRuntime(ctx, class, p, cache, vers[i])
	if err != nil {
		return nil, nil, fmt.Errorf("default sandbox version %q: %w", vers[i].GetName(), err)
	}
	var restorable []*ateletpb.SandboxRuntime
	for _, v := range vers {
		if v.GetIsDefault() {
			continue
		}
		rt, err := buildRuntime(ctx, class, p, cache, v)
		if err != nil {
			slog.WarnContext(ctx, "Leaving sandbox version out of restorable runtimes",
				slog.String("version", v.GetName()), slog.Any("err", err))
			continue
		}
		// restorable excludes the default, and two versions with the same
		// assets are one runtime.
		same := func(o *ateletpb.SandboxRuntime) bool { return proto.Equal(o.GetVersion(), rt.GetVersion()) }
		if same(def) || slices.ContainsFunc(restorable, same) {
			continue
		}
		restorable = append(restorable, rt)
	}
	return def, restorable, nil
}

// buildRuntime is the runtime v makes on this worker: the host's attributes,
// the prober's, and the identity of v's assets. A nil prober adds none.
func buildRuntime(ctx context.Context, class string, p RuntimeProber, cache *runtimeCache, v *ateletpb.SandboxVersion) (*ateletpb.SandboxRuntime, error) {
	compat := hardware.ProbeHost()
	hash := sandboxAssetsHash(class, v)
	if p != nil {
		attrs, err := cache.probe(ctx, p, hash, v)
		if err != nil {
			return nil, err
		}
		compat.Attributes = append(compat.Attributes, attrs...)
	}
	compat.Attributes = append(compat.Attributes, &ateletpb.AttributeEntry{
		Key:   hardware.AttrSandboxAssetsHash,
		Value: hash,
	})
	return &ateletpb.SandboxRuntime{SandboxClass: class, Version: compat}, nil
}

// sandboxAssetsHash is the hex SHA-256 of the class, pause image, and asset
// digests of v, sorted by asset name. Local paths are left out: they are
// where this node keeps the assets, not what the assets are.
func sandboxAssetsHash(class string, v *ateletpb.SandboxVersion) string {
	h := sha256.New()
	fmt.Fprintf(h, "class=%s\npause=%s\n", class, v.GetPauseImage())
	digests := v.GetAssetDigests()
	for _, name := range slices.Sorted(maps.Keys(digests)) {
		fmt.Fprintf(h, "asset=%s:%s\n", name, digests[name])
	}
	return hex.EncodeToString(h.Sum(nil))
}
