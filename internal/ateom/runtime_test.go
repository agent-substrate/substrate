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
	"errors"
	"runtime"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/agent-substrate/substrate/internal/hardware"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
)

// fakeProber reports a cpu_features attribute derived from each version's
// assets, as running the same assets on one host finds the same features, or
// fails the versions in errs.
type fakeProber struct {
	errs map[string]error
	got  []string
}

func (p *fakeProber) Probe(_ context.Context, v *ateletpb.SandboxVersion) ([]*ateletpb.AttributeEntry, error) {
	p.got = append(p.got, v.GetName())
	if err := p.errs[v.GetName()]; err != nil {
		return nil, err
	}
	return []*ateletpb.AttributeEntry{{Key: hardware.AttrCPUFeatures, Value: "features-of-" + v.GetAssetDigests()["gvisor"]}}, nil
}

func testVersion(name string, isDefault bool) *ateletpb.SandboxVersion {
	return &ateletpb.SandboxVersion{
		Name:         name,
		IsDefault:    isDefault,
		PauseImage:   "pause-" + name,
		AssetPaths:   map[string]string{"gvisor": "/static/sha-" + name + "/runsc"},
		AssetDigests: map[string]string{"gvisor": "sha-" + name},
	}
}

// wantRuntime is the runtime testVersion(name) makes under fakeProber, or
// under no prober when cpuFeatures is false.
func wantRuntime(name string, cpuFeatures bool) *ateletpb.SandboxRuntime {
	attrs := []*ateletpb.AttributeEntry{{Key: hardware.AttrArchitecture, Value: runtime.GOARCH}}
	if cpuFeatures {
		attrs = append(attrs, &ateletpb.AttributeEntry{Key: hardware.AttrCPUFeatures, Value: "features-of-sha-" + name})
	}
	attrs = append(attrs, &ateletpb.AttributeEntry{Key: hardware.AttrSandboxAssetsHash, Value: sandboxAssetsHash("gvisor", testVersion(name, false))})
	return &ateletpb.SandboxRuntime{
		SandboxClass: "gvisor",
		Version:      &ateletpb.VersionedSandboxCompat{SchemaVersion: hardware.SchemaVersionV1, Attributes: attrs},
	}
}

func TestSandboxAssetsHash(t *testing.T) {
	base := testVersion("v1", true)
	want := sandboxAssetsHash("gvisor", base)

	// The name, default-ness, and where this node keeps the assets are not
	// what the assets are.
	same := testVersion("renamed", false)
	same.PauseImage, same.AssetDigests = base.PauseImage, base.AssetDigests
	same.AssetPaths = map[string]string{"gvisor": "/elsewhere/runsc"}
	if got := sandboxAssetsHash("gvisor", same); got != want {
		t.Errorf("hash changed with only name, default, and paths: %s, want %s", got, want)
	}

	multi := func(order ...string) *ateletpb.SandboxVersion {
		v := &ateletpb.SandboxVersion{AssetDigests: map[string]string{}}
		for _, name := range order {
			v.AssetDigests[name] = "sha-" + name
		}
		return v
	}
	if a, b := sandboxAssetsHash("microvm", multi("kernel", "shim", "rootfs")), sandboxAssetsHash("microvm", multi("rootfs", "kernel", "shim")); a != b {
		t.Errorf("hash depends on asset order: %s vs %s", a, b)
	}

	for name, v := range map[string]*ateletpb.SandboxVersion{
		"pause image":  {PauseImage: "other", AssetDigests: base.AssetDigests},
		"asset digest": {PauseImage: base.PauseImage, AssetDigests: map[string]string{"gvisor": "sha-other"}},
		"asset name":   {PauseImage: base.PauseImage, AssetDigests: map[string]string{"runsc": "sha-v1"}},
	} {
		if got := sandboxAssetsHash("gvisor", v); got == want {
			t.Errorf("hash unchanged with a different %s", name)
		}
	}
	if got := sandboxAssetsHash("microvm", base); got == want {
		t.Error("hash unchanged with a different sandbox class")
	}
}

func TestSandboxRuntimes(t *testing.T) {
	ctx := context.Background()
	// v1dup has v1's assets: one runtime, reported once.
	v1dup := testVersion("v1", false)
	v1dup.Name = "v1-again"

	tests := []struct {
		name           string
		prober         RuntimeProber
		vers           []*ateletpb.SandboxVersion
		wantDefault    *ateletpb.SandboxRuntime
		wantRestorable []*ateletpb.SandboxRuntime
		wantErr        bool
	}{{
		name:           "default and the other versions, each probed",
		prober:         &fakeProber{},
		vers:           []*ateletpb.SandboxVersion{testVersion("v2", true), testVersion("v1", false)},
		wantDefault:    wantRuntime("v2", true),
		wantRestorable: []*ateletpb.SandboxRuntime{wantRuntime("v1", true)},
	}, {
		name:        "default need not come first",
		prober:      &fakeProber{},
		vers:        []*ateletpb.SandboxVersion{testVersion("v1", false), testVersion("v2", true)},
		wantDefault: wantRuntime("v2", true),
		wantRestorable: []*ateletpb.SandboxRuntime{
			wantRuntime("v1", true),
		},
	}, {
		name:   "versions with the same assets are one runtime",
		prober: &fakeProber{},
		vers: []*ateletpb.SandboxVersion{
			testVersion("v2", true), testVersion("v1", false), v1dup,
		},
		wantDefault:    wantRuntime("v2", true),
		wantRestorable: []*ateletpb.SandboxRuntime{wantRuntime("v1", true)},
	}, {
		name:           "no prober reports host attributes and the assets only",
		vers:           []*ateletpb.SandboxVersion{testVersion("v2", true), testVersion("v1", false)},
		wantDefault:    wantRuntime("v2", false),
		wantRestorable: []*ateletpb.SandboxRuntime{wantRuntime("v1", false)},
	}, {
		// Its snapshots restore elsewhere or cold boot; the worker still
		// registers.
		name:        "a version that fails to probe is left out",
		prober:      &fakeProber{errs: map[string]error{"v1": errors.New("runsc crashed")}},
		vers:        []*ateletpb.SandboxVersion{testVersion("v2", true), testVersion("v1", false)},
		wantDefault: wantRuntime("v2", true),
	}, {
		name:    "the default failing to probe fails",
		prober:  &fakeProber{errs: map[string]error{"v2": errors.New("runsc crashed")}},
		vers:    []*ateletpb.SandboxVersion{testVersion("v2", true), testVersion("v1", false)},
		wantErr: true,
	}, {
		name:    "no default version fails",
		prober:  &fakeProber{},
		vers:    []*ateletpb.SandboxVersion{testVersion("v1", false)},
		wantErr: true,
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			def, restorable, err := sandboxRuntimes(ctx, "gvisor", tt.prober, newRuntimeCache(), tt.vers)
			if (err != nil) != tt.wantErr {
				t.Fatalf("sandboxRuntimes() error = %v, want error %v", err, tt.wantErr)
			}
			if diff := cmp.Diff(tt.wantDefault, def, protocmp.Transform()); diff != "" {
				t.Errorf("default runtime mismatch (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(tt.wantRestorable, restorable, protocmp.Transform()); diff != "" {
				t.Errorf("restorable runtimes mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestRuntimeCache(t *testing.T) {
	ctx := context.Background()
	hash := func(name string) string { return sandboxAssetsHash("gvisor", testVersion(name, false)) }

	tests := []struct {
		name string
		// rounds are successive version sets built over one cache.
		rounds [][]*ateletpb.SandboxVersion
		errs   map[string]error
		// wantProbed is every version name the prober ran, in order.
		wantProbed []string
		wantCached []string
	}{{
		name: "a second build with the same assets runs no probe",
		rounds: [][]*ateletpb.SandboxVersion{
			{testVersion("v2", true), testVersion("v1", false)},
			{testVersion("v2", true), testVersion("v1", false)},
		},
		wantProbed: []string{"v2", "v1"},
		wantCached: []string{hash("v2"), hash("v1")},
	}, {
		name: "only new assets are probed",
		rounds: [][]*ateletpb.SandboxVersion{
			{testVersion("v2", true), testVersion("v1", false)},
			{testVersion("v3", true), testVersion("v2", false)},
		},
		wantProbed: []string{"v2", "v1", "v3"},
		wantCached: []string{hash("v3"), hash("v2")},
	}, {
		name: "a failed probe is run again",
		rounds: [][]*ateletpb.SandboxVersion{
			{testVersion("v2", true), testVersion("v1", false)},
			{testVersion("v2", true), testVersion("v1", false)},
		},
		errs:       map[string]error{"v1": errors.New("runsc crashed")},
		wantProbed: []string{"v2", "v1", "v1"},
		wantCached: []string{hash("v2")},
	}, {
		// The entry of a version that went away is dropped even when the
		// build fails.
		name: "entries of versions that are gone are dropped",
		rounds: [][]*ateletpb.SandboxVersion{
			{testVersion("v2", true), testVersion("v1", false)},
			{testVersion("v3", true)},
		},
		errs:       map[string]error{"v3": errors.New("runsc crashed")},
		wantProbed: []string{"v2", "v1", "v3"},
		wantCached: nil,
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &fakeProber{errs: tt.errs}
			cache := newRuntimeCache()
			for _, vers := range tt.rounds {
				sandboxRuntimes(ctx, "gvisor", p, cache, vers) //nolint:errcheck // failures are part of the table
			}
			if diff := cmp.Diff(tt.wantProbed, p.got); diff != "" {
				t.Errorf("probed versions mismatch (-want +got):\n%s", diff)
			}
			var cached []string
			for h := range cache.probed {
				cached = append(cached, h)
			}
			if diff := cmp.Diff(tt.wantCached, cached, cmpopts.SortSlices(func(a, b string) bool { return a < b })); diff != "" {
				t.Errorf("cached hashes mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
