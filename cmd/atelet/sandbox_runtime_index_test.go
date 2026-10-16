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
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/hardware"
	"go.ate.dev/substrate/internal/proto/ateletpb"
	"go.ate.dev/substrate/pkg/api/v1alpha1"
	"google.golang.org/grpc/codes"
)

// hashedTestVersion is a version of config whose runtime is identified by
// its one asset's digest.
func hashedTestVersion(t *testing.T, name string, isDefault bool, sha string) hashedVersion {
	t.Helper()
	rec := &sandboxAssetsRecord{
		SandboxClass: "gvisor",
		Assets:       map[string]assetEntry{gvisorAssetName: {URL: "gs://bucket/" + name, SHA256: sha}},
	}
	return hashedVersion{versionPlan: versionPlan{name: name, isDefault: isDefault, rec: rec}, hash: bundleHash(rec)}
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
	// runtime; the entry keeps the record it was first registered with,
	// which is the one the first worker's file was built from.
	same := hashedTestVersion(t, "release-1", true, shaA)
	x.register("pod-2", advertisedClass{class: gvisor, config: "config-y", hashed: []hashedVersion{same}})
	if same.hash != v1.hash {
		t.Fatal("identical binaries hashed differently")
	}
	if rt, _ := x.lookup(v1.hash); rt.config != "config-x" || rt.version != "v1" {
		t.Errorf("shared runtime provenance = %s/%s, want the first registration's config-x/v1", rt.config, rt.version)
	}
	if wr, _ := x.worker("pod-2"); wr.classes[gvisor].defaultHash != same.hash {
		t.Error("pod-2 does not advertise the shared runtime as its default")
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
	runtimeWith := func(class string, attrs ...*ateletpb.AttributeEntry) *ateletpb.SandboxRuntime {
		return &ateletpb.SandboxRuntime{SandboxClass: class, Version: &ateletpb.VersionedSandboxCompat{SchemaVersion: hardware.SchemaVersionV1, Attributes: attrs}}
	}
	placed := runtimeWith("gvisor",
		&ateletpb.AttributeEntry{Key: hardware.AttrArchitecture, Value: "amd64"},
		&ateletpb.AttributeEntry{Key: hardware.AttrSandboxBundleHash, Value: advertised.hash})

	if _, err := s.advertisedRuntime(t.Context(), placed); apierror.Code(err) != codes.FailedPrecondition {
		t.Fatalf("advertisedRuntime before any worker advertises it = %v, want %s", err, codes.FailedPrecondition)
	}
	s.runtimes.register("pod-1", advertisedClass{class: v1alpha1.SandboxClassGvisor, config: "config-x", hashed: []hashedVersion{advertised}})
	rec, err := s.advertisedRuntime(t.Context(), placed)
	if err != nil {
		t.Fatalf("advertisedRuntime: %v", err)
	}
	if rec != advertised.rec {
		t.Errorf("advertisedRuntime returned %+v, want the advertised record", rec)
	}

	for name, rt := range map[string]*ateletpb.SandboxRuntime{
		"no runtime":    nil,
		"no attributes": runtimeWith("gvisor"),
		"no bundle hash": runtimeWith("gvisor",
			&ateletpb.AttributeEntry{Key: hardware.AttrArchitecture, Value: "amd64"}),
		"empty bundle hash": runtimeWith("gvisor",
			&ateletpb.AttributeEntry{Key: hardware.AttrSandboxBundleHash, Value: ""}),
	} {
		if _, err := s.advertisedRuntime(t.Context(), rt); apierror.Code(err) != codes.InvalidArgument {
			t.Errorf("advertisedRuntime with %s = %v, want %s", name, err, codes.InvalidArgument)
		}
	}
	otherClass := runtimeWith("microvm", &ateletpb.AttributeEntry{Key: hardware.AttrSandboxBundleHash, Value: advertised.hash})
	if _, err := s.advertisedRuntime(t.Context(), otherClass); apierror.Code(err) != codes.FailedPrecondition {
		t.Errorf("advertisedRuntime with the hash under another class = %v, want %s", err, codes.FailedPrecondition)
	}
}

// TestRuntimeIndexPersists pins what survives an atelet restart: every
// runtime registered, with its provenance, through a file rewritten before
// the entry exists in memory; an entry whose record does not hash to its
// key is dropped on load without taking the rest with it, and a file that
// cannot be decoded starts the index empty rather than failing startup.
func TestRuntimeIndexPersists(t *testing.T) {
	const (
		shaA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		shaB = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	)
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sandbox-runtime-index.json")
	x, err := openRuntimeIndex(ctx, path)
	if err != nil {
		t.Fatalf("openRuntimeIndex with no file: %v", err)
	}
	v1, v2 := hashedTestVersion(t, "v1", true, shaA), hashedTestVersion(t, "v2", false, shaB)
	if err := x.registerRuntimes("config-x", []hashedVersion{v1}); err != nil {
		t.Fatal(err)
	}
	if err := x.register("pod-1", advertisedClass{class: v1alpha1.SandboxClassGvisor, config: "config-x", hashed: []hashedVersion{v1, v2}}); err != nil {
		t.Fatal(err)
	}

	// Restart: a new index over the same file resolves both, with what they
	// were registered as, and knows no worker until the syncer runs.
	reopened, err := openRuntimeIndex(ctx, path)
	if err != nil {
		t.Fatalf("openRuntimeIndex over the written file: %v", err)
	}
	for _, v := range []hashedVersion{v1, v2} {
		rt, ok := reopened.lookup(v.hash)
		if !ok {
			t.Fatalf("runtime %s not loaded", v.name)
		}
		if rt.config != "config-x" || rt.version != v.name || rt.rec.Assets[gvisorAssetName].SHA256 != v.rec.Assets[gvisorAssetName].SHA256 {
			t.Errorf("runtime %s loaded as %s/%s with digest %s; want config-x/%s with the registered digest", v.name, rt.config, rt.version, rt.rec.Assets[gvisorAssetName].SHA256, v.name)
		}
	}
	if _, ok := reopened.worker("pod-1"); ok {
		t.Error("a worker entry survived the restart; only the syncer knows what a worker advertises now")
	}

	// An entry filed under the wrong hash is dropped; the other survives.
	// Here v1's record is moved under a hash it does not match.
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var f runtimeIndexFile
	if err := json.Unmarshal(data, &f); err != nil {
		t.Fatal(err)
	}
	const wrongHash = "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	f.Runtimes[wrongHash] = f.Runtimes[v1.hash]
	delete(f.Runtimes, v1.hash)
	mismatched, err := json.Marshal(f)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, mismatched, 0o600); err != nil {
		t.Fatal(err)
	}
	damaged, err := openRuntimeIndex(ctx, path)
	if err != nil {
		t.Fatalf("openRuntimeIndex over a file with a bad entry: %v", err)
	}
	if _, ok := damaged.lookup(v2.hash); !ok {
		t.Error("the intact entry was not loaded")
	}
	if _, ok := damaged.lookup(wrongHash); ok {
		t.Error("an entry whose record does not hash to its key was loaded")
	}
	if _, ok := damaged.lookup(v1.hash); ok {
		t.Error("v1 loaded though the file no longer holds it under its hash")
	}
	// Registering v1 again rewrites the file with the good entries only.
	if err := damaged.registerRuntimes("config-x", []hashedVersion{v1}); err != nil {
		t.Fatal(err)
	}
	rewritten, err := openRuntimeIndex(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range []hashedVersion{v1, v2} {
		if _, ok := rewritten.lookup(v.hash); !ok {
			t.Errorf("runtime %s missing after the rewrite", v.name)
		}
	}
	if len(rewritten.runtimes) != 2 {
		t.Errorf("rewritten index holds %d runtimes, want 2 (the bad entry gone)", len(rewritten.runtimes))
	}

	// A file that does not decode starts the index empty, not atelet failed.
	if err := os.WriteFile(path, []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	empty, err := openRuntimeIndex(ctx, path)
	if err != nil {
		t.Fatalf("openRuntimeIndex over a corrupt file: %v", err)
	}
	if len(empty.runtimes) != 0 {
		t.Errorf("corrupt file loaded %d runtimes, want 0", len(empty.runtimes))
	}
}

// TestRuntimeIndexPersistFailureIsReported pins that an index that cannot
// write its file reports it and does not index the runtime, so the sync
// that asked retries rather than advertising a hash a restart would lose.
func TestRuntimeIndexPersistFailureIsReported(t *testing.T) {
	x := newRuntimeIndex()
	x.path = filepath.Join(t.TempDir(), "missing", "sandbox-runtime-index.json") // dir never created
	v := hashedTestVersion(t, "v1", true, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	if err := x.registerRuntimes("config-x", []hashedVersion{v}); err == nil {
		t.Fatal("registerRuntimes with an unwritable path = nil, want an error")
	}
	if _, ok := x.lookup(v.hash); ok {
		t.Error("a runtime whose file could not be written was indexed")
	}
}
