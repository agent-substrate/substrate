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
	"slices"
	"testing"

	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/pkg/api/v1alpha1"
	"google.golang.org/grpc/codes"
)

// hashedTestVersion is a version of config whose runtime is identified by
// its one asset's digest.
func hashedTestVersion(t *testing.T, name string, isDefault bool, sha string) hashedVersion {
	t.Helper()
	rec := &sandboxAssetsRecord{
		SandboxClass: "gvisor",
		PauseImage:   testPauseImage,
		Assets:       map[string]assetEntry{gvisorAssetName: {URL: "gs://bucket/" + name, SHA256: sha}},
	}
	h, err := versionBundleHash(rec)
	if err != nil {
		t.Fatal(err)
	}
	return hashedVersion{versionPlan: versionPlan{name: name, isDefault: isDefault, rec: rec}, hash: h}
}

func TestRuntimeIndex(t *testing.T) {
	const (
		shaA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		shaB = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	)
	x := newRuntimeIndex()
	v1, v2 := hashedTestVersion(t, "v1", true, shaA), hashedTestVersion(t, "v2", false, shaB)

	gvisor := v1alpha1.SandboxClassGvisor
	x.register("pod-1", advertisedClass{class: gvisor, config: "config-x", hashed: []hashedVersion{v1, v2}})
	for _, v := range []hashedVersion{v1, v2} {
		rt, ok := x.lookup(v.hash)
		if !ok || rt.rec != v.rec || rt.config != "config-x" || rt.version != v.name {
			t.Errorf("lookup(%s) = %+v, %v; want %s's record from config-x", v.name, rt, ok, v.name)
		}
	}
	wr, ok := x.worker("pod-1")
	if !ok {
		t.Fatal("worker(pod-1) not indexed")
	}
	cr, ok := wr.class(gvisor)
	if !ok || cr.config != "config-x" || cr.defaultHash != v1.hash || !slices.Equal(cr.hashes, []string{v1.hash, v2.hash}) {
		t.Errorf("worker(pod-1).class(gvisor) = %+v, %v; want config-x with %s default and both hashes", cr, ok, v1.hash)
	}
	if _, ok := wr.class(v1alpha1.SandboxClassMicroVM); ok {
		t.Error("worker(pod-1) advertises a class its pool does not run")
	}

	// Another config with the same binaries under another name shares the
	// runtime; the entry's provenance follows the latest registration.
	same := hashedTestVersion(t, "release-1", true, shaA)
	x.register("pod-2", advertisedClass{class: gvisor, config: "config-y", hashed: []hashedVersion{same}})
	if same.hash != v1.hash {
		t.Fatal("identical binaries hashed differently")
	}
	if rt, _ := x.lookup(v1.hash); rt.config != "config-y" || rt.version != "release-1" {
		t.Errorf("shared runtime provenance = %s/%s, want config-y/release-1", rt.config, rt.version)
	}

	// A pod's re-plan replaces what it advertises; the old runtime stays
	// resolvable, since a request may still carry its hash.
	x.register("pod-1", advertisedClass{class: gvisor, config: "config-x", hashed: []hashedVersion{hashedTestVersion(t, "v2", true, shaB)}})
	if wr, _ := x.worker("pod-1"); wr.classes[gvisor].defaultHash != v2.hash || len(wr.classes[gvisor].hashes) != 1 {
		t.Errorf("worker(pod-1) after re-plan = %+v, want v2 as the only runtime", wr.classes[gvisor])
	}
	if _, ok := x.lookup(v1.hash); !ok {
		t.Error("runtime v1 forgotten after pod-1 stopped advertising it")
	}

	// A gone pod drops out; its runtimes do not.
	x.forget("pod-2")
	if _, ok := x.worker("pod-2"); ok {
		t.Error("pod-2 still indexed after forget")
	}
	if _, ok := x.lookup(same.hash); !ok {
		t.Error("runtime forgotten with its pod")
	}
	if _, ok := x.lookup("sha256:" + shaB[:60] + "0000"); ok {
		t.Error("lookup of an unknown hash succeeded")
	}
}

// TestRuntimeIndexRegisterRuntimes pins the early registration the syncer
// does from an event handler: the hashes resolve, and no worker's entry
// appears or changes until its file is written.
func TestRuntimeIndexRegisterRuntimes(t *testing.T) {
	const (
		shaA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		shaB = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
		shaC = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	)
	x := newRuntimeIndex()
	gvisor := v1alpha1.SandboxClassGvisor
	old := hashedTestVersion(t, "v1", true, shaA)
	x.register("pod-1", advertisedClass{class: gvisor, config: "config-x", hashed: []hashedVersion{old}})

	next := hashedTestVersion(t, "v2", true, shaB)
	x.registerRuntimes("config-x", []hashedVersion{next})
	rt, ok := x.lookup(next.hash)
	if !ok || rt.version != "v2" || rt.config != "config-x" {
		t.Fatalf("lookup(%s) = %+v, %v; want v2 of config-x", next.hash, rt, ok)
	}
	wr, ok := x.worker("pod-1")
	if !ok {
		t.Fatal("worker pod-1 gone after registerRuntimes")
	}
	if cr, _ := wr.class(gvisor); cr.defaultHash != old.hash {
		t.Errorf("pod-1 default = %s after registerRuntimes, want the written file's %s", cr.defaultHash, old.hash)
	}
	x.registerRuntimes("config-y", []hashedVersion{hashedTestVersion(t, "v1", true, shaC)})
	if _, ok := x.worker("pod-2"); ok {
		t.Error("registerRuntimes created a worker entry")
	}
}

func TestResolveRuntime(t *testing.T) {
	const sha = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	s := &Atelet{runtimes: newRuntimeIndex()}
	advertised := hashedTestVersion(t, "v1", true, sha)

	// The request names the same binaries from another bucket: same runtime.
	requested := &sandboxAssetsRecord{
		SandboxClass: "gvisor",
		PauseImage:   testPauseImage,
		Assets:       map[string]assetEntry{gvisorAssetName: {URL: "gs://mirror/v1", SHA256: sha}},
	}
	if _, err := s.advertisedSandboxAssets(t.Context(), requested); apierror.Code(err) != codes.FailedPrecondition {
		t.Fatalf("advertisedSandboxAssets before any worker advertises it = %v, want %s", err, codes.FailedPrecondition)
	}
	s.runtimes.register("pod-1", advertisedClass{class: v1alpha1.SandboxClassGvisor, config: "config-x", hashed: []hashedVersion{advertised}})
	rec, err := s.advertisedSandboxAssets(t.Context(), requested)
	if err != nil {
		t.Fatalf("advertisedSandboxAssets: %v", err)
	}
	if rec != advertised.rec {
		t.Errorf("advertisedSandboxAssets returned %+v, want the advertised record (the index's URL, not the request's)", rec)
	}

	unpinned := &sandboxAssetsRecord{SandboxClass: "gvisor", PauseImage: "registry.k8s.io/pause:3.10", Assets: requested.Assets}
	if _, err := s.advertisedSandboxAssets(t.Context(), unpinned); apierror.Code(err) != codes.InvalidArgument {
		t.Errorf("advertisedSandboxAssets with an unpinned pause image = %v, want %s", err, codes.InvalidArgument)
	}
}
