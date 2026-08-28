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
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/internal/ateerrors"
	"github.com/agent-substrate/substrate/internal/atelet"
	"github.com/agent-substrate/substrate/internal/ateompath"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/internal/serverboot"
	"github.com/google/go-cmp/cmp"
	"github.com/klauspost/compress/zstd"
	"github.com/spf13/pflag"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/emptypb"
	certsv1beta1 "k8s.io/api/certificates/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const testPauseImage = "registry.k8s.io/pause:3.10.2@sha256:f548e0e8e3dc1896ca956272154dde3314e8cc4fde0a57577ee9fa1c63f5baf4"

// TestPortFlagDefault verifies the default value of the --port flag.
func TestPortFlagDefault(t *testing.T) {
	f := pflag.Lookup("port")
	if f == nil {
		t.Fatal("no --port flag registered")
	}
	if want := strconv.Itoa(atelet.DefaultPort); f.DefValue != want {
		t.Errorf("--port default = %q, want %q", f.DefValue, want)
	}
}

func TestSnapshotManifestActorMetadata(t *testing.T) {
	rec := sandboxAssetsRecord{
		Atespace:               "team-a",
		ActorName:              "actor-1",
		ActorUID:               "actor-uid",
		ActorTemplateNamespace: "templates",
		ActorTemplateName:      "agent",
		Scope:                  ateattr.SnapshotScopeFull,
	}
	got, err := json.Marshal(rec)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`"atespace":"team-a"`, `"actorName":"actor-1"`, `"actorUid":"actor-uid"`, `"actorTemplateNamespace":"templates"`, `"actorTemplateName":"agent"`, `"scope":"full"`} {
		if !bytes.Contains(got, []byte(want)) {
			t.Errorf("manifest %s missing %s", got, want)
		}
	}
}

// TestSnapshotManifestScopeAbsent pins backward compatibility: manifests
// written before the scope field existed must still parse, reporting an empty
// scope, and a scope-less record must not serialize a scope key at all.
func TestSnapshotManifestScopeAbsent(t *testing.T) {
	legacy := []byte(`{"sandboxClass":"gvisor","pauseImage":"` + testPauseImage + `","snapshotFiles":["checkpoint.img"]}`)
	rec, err := unmarshalSandboxRecord(legacy)
	if err != nil {
		t.Fatalf("unmarshalSandboxRecord(legacy manifest): %v", err)
	}
	if rec.Scope != "" {
		t.Errorf("legacy manifest scope = %q, want empty", rec.Scope)
	}

	got, err := json.Marshal(sandboxAssetsRecord{SandboxClass: "gvisor"})
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(got, []byte(`"scope"`)) {
		t.Errorf("scope-less record serialized a scope key: %s", got)
	}
}

// TestSnapshotManifestRequiresPauseImage pins that a manifest without a pause
// image is rejected outright rather than yielding an empty image that would
// fail later, deep in the image pull.
func TestSnapshotManifestRequiresPauseImage(t *testing.T) {
	noPause := []byte(`{"sandboxClass":"gvisor","snapshotFiles":["checkpoint.img"]}`)
	if _, err := unmarshalSandboxRecord(noPause); err == nil {
		t.Fatal("unmarshalSandboxRecord accepted a manifest with no pauseImage")
	} else if !strings.Contains(err.Error(), "pauseImage") {
		t.Errorf("error = %v, want it to name pauseImage", err)
	}
}

func TestWriteSystemInfoVolume(t *testing.T) {
	ctx := context.Background()
	root := filepath.Join(t.TempDir(), "system-info", "vol1")
	si := &ateletpb.SystemInfoVolume{
		DataSources: []*ateletpb.SystemInfoDataSource{
			{DataSource: &ateletpb.SystemInfoDataSource_ActorMetadata{
				ActorMetadata: &ateletpb.ActorMetadataDataSource{
					Items: []*ateletpb.ActorMetadataItem{
						{Field: ateletpb.ActorMetadataField_ACTOR_METADATA_FIELD_NAME, Path: "actor-name"},
						{Field: ateletpb.ActorMetadataField_ACTOR_METADATA_FIELD_ATESPACE, Path: "atespace"},
						{Field: ateletpb.ActorMetadataField_ACTOR_METADATA_FIELD_UID, Path: "identity/actor-uid"},
					},
				},
			}},
		},
	}

	golden := resources.ActorRef{Atespace: "ate-e2e-probe", Name: "golden-actor"}
	if err := writeSystemInfoVolume(ctx, root, golden, "uid-golden", nil, si); err != nil {
		t.Fatalf("writeSystemInfoVolume: %v", err)
	}

	// Overwrite with a different actor, as happens when a snapshot taken from
	// one actor seeds another on resume: files must carry the new values.
	alpha := resources.ActorRef{Atespace: "ate-e2e-probe", Name: "probe-alpha"}
	if err := writeSystemInfoVolume(ctx, root, alpha, "uid-alpha", nil, si); err != nil {
		t.Fatalf("writeSystemInfoVolume (rewrite): %v", err)
	}

	// Values are written raw, no trailing newline.
	for path, want := range map[string]string{
		"actor-name":         "probe-alpha",
		"atespace":           "ate-e2e-probe",
		"identity/actor-uid": "uid-alpha",
	} {
		t.Run(path, func(t *testing.T) {
			target := filepath.Join(root, path)
			got, err := os.ReadFile(target)
			if err != nil {
				t.Fatalf("reading %q: %v", target, err)
			}
			if string(got) != want {
				t.Errorf("content = %q, want %q", got, want)
			}
			info, err := os.Stat(target)
			if err != nil {
				t.Fatalf("stat %q: %v", target, err)
			}
			if perm := info.Mode().Perm(); perm != 0o644 {
				t.Errorf("perm = %o, want 644", perm)
			}
		})
	}
}

// TestWriteSystemInfoVolume_StableRealPaths pins the path-stability contract
// the restore paths depend on: the micro-VM virtiofsds run in find-paths
// migration mode, which re-binds the guest's FUSE state to files by the paths
// recorded at suspend, and gVisor's gofer likewise re-opens files by path on
// restore. Projected files must therefore be plain files at stable real
// paths — no symlink indirection — and regenerating the volume must not move
// or delete a path that guest state may reference.
func TestWriteSystemInfoVolume_StableRealPaths(t *testing.T) {
	ctx := context.Background()
	root := filepath.Join(t.TempDir(), "system-info", "vol1")
	si := &ateletpb.SystemInfoVolume{
		DataSources: []*ateletpb.SystemInfoDataSource{
			{DataSource: &ateletpb.SystemInfoDataSource_ActorMetadata{
				ActorMetadata: &ateletpb.ActorMetadataDataSource{
					Items: []*ateletpb.ActorMetadataItem{
						{Field: ateletpb.ActorMetadataField_ACTOR_METADATA_FIELD_NAME, Path: "actor-name"},
						{Field: ateletpb.ActorMetadataField_ACTOR_METADATA_FIELD_UID, Path: "identity/actor-uid"},
					},
				},
			}},
		},
	}

	golden := resources.ActorRef{Atespace: "ate-e2e-probe", Name: "golden-actor"}
	if err := writeSystemInfoVolume(ctx, root, golden, "uid-golden", nil, si); err != nil {
		t.Fatalf("writeSystemInfoVolume: %v", err)
	}

	realBefore := map[string]string{}
	for _, p := range []string{"actor-name", "identity/actor-uid"} {
		visible := filepath.Join(root, p)
		fi, err := os.Lstat(visible)
		if err != nil {
			t.Fatalf("lstat %q: %v", visible, err)
		}
		if !fi.Mode().IsRegular() {
			t.Errorf("%q is %v, want a regular file: symlink indirection moves the real path on regeneration, which find-paths cannot re-bind", visible, fi.Mode().Type())
		}
		real, err := filepath.EvalSymlinks(visible)
		if err != nil {
			t.Fatalf("eval symlinks %q: %v", visible, err)
		}
		realBefore[p] = real
	}

	// Regenerate for a different actor, as a restore from a shared golden
	// snapshot does.
	alpha := resources.ActorRef{Atespace: "ate-e2e-probe", Name: "probe-alpha"}
	if err := writeSystemInfoVolume(ctx, root, alpha, "uid-alpha", nil, si); err != nil {
		t.Fatalf("writeSystemInfoVolume (rewrite): %v", err)
	}

	for _, p := range []string{"actor-name", "identity/actor-uid"} {
		real, err := filepath.EvalSymlinks(filepath.Join(root, p))
		if err != nil {
			t.Fatalf("eval symlinks after rewrite %q: %v", p, err)
		}
		if real != realBefore[p] {
			t.Errorf("%q real path moved on regeneration: %q -> %q; guest state recorded at suspend cannot re-bind", p, realBefore[p], real)
		}
		if _, err := os.Stat(realBefore[p]); err != nil {
			t.Errorf("pre-rewrite real path %q gone after regeneration: %v; find-paths re-open of a suspend-time path would fail", realBefore[p], err)
		}
	}
}

func TestWriteSystemInfoVolume_TrustBundle(t *testing.T) {
	ctx := context.Background()
	certPEM := testCertPEM(t)
	si := &ateletpb.SystemInfoVolume{
		DataSources: []*ateletpb.SystemInfoDataSource{
			{DataSource: &ateletpb.SystemInfoDataSource_TrustBundle{
				TrustBundle: &ateletpb.TrustBundleDataSource{Name: EgressTrustBundleName, Path: "trust/ca.pem"},
			}},
		},
	}
	lister := ctbLister(t, &certsv1beta1.ClusterTrustBundle{
		ObjectMeta: metav1.ObjectMeta{Name: egressTrustBundleObjectName},
		Spec:       certsv1beta1.ClusterTrustBundleSpec{TrustBundle: string(certPEM)},
	})

	root := filepath.Join(t.TempDir(), "system-info", "vol1")
	ref := resources.ActorRef{Atespace: "team-a", Name: "actor-1"}
	if err := writeSystemInfoVolume(ctx, root, ref, "uid-1", lister, si); err != nil {
		t.Fatalf("writeSystemInfoVolume: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(root, "trust/ca.pem"))
	if err != nil {
		t.Fatalf("reading projected bundle: %v", err)
	}
	if string(got) != string(certPEM) {
		t.Errorf("content = %q, want the sanitized bundle", got)
	}

	t.Run("resolution failure fails the write rather than produce an empty trust file", func(t *testing.T) {
		err := writeSystemInfoVolume(ctx, filepath.Join(t.TempDir(), "vol2"), ref, "uid-1", ctbLister(t), si)
		if err == nil || !strings.Contains(err.Error(), "not found") {
			t.Errorf("writeSystemInfoVolume = %v, want not-found resolution error", err)
		}
	})
}

func TestWriteFileAtomic(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "actor-id")

	// One shared write over an existing value, as happens on every resume;
	// each subtest checks one postcondition.
	if err := os.WriteFile(target, []byte("golden-id"), 0o600); err != nil {
		t.Fatalf("seeding target: %v", err)
	}
	if err := writeFileAtomic(target, []byte("counter-1"), 0o644); err != nil {
		t.Fatalf("writeFileAtomic: %v", err)
	}

	t.Run("replaces content", func(t *testing.T) {
		got, err := os.ReadFile(target)
		if err != nil {
			t.Fatalf("reading target: %v", err)
		}
		if string(got) != "counter-1" {
			t.Errorf("content = %q, want %q", got, "counter-1")
		}
	})

	t.Run("sets permissions", func(t *testing.T) {
		info, err := os.Stat(target)
		if err != nil {
			t.Fatalf("stat target: %v", err)
		}
		if perm := info.Mode().Perm(); perm != 0o644 {
			t.Errorf("perm = %o, want 644", perm)
		}
	})

	t.Run("leaves no temp files", func(t *testing.T) {
		// The directory is visible inside the actor.
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatalf("reading dir: %v", err)
		}
		if len(entries) != 1 {
			names := make([]string, 0, len(entries))
			for _, e := range entries {
				names = append(names, e.Name())
			}
			t.Errorf("leftover files in identity dir: %v", names)
		}
	})
}

func TestCopyFile(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src")
	want := []byte("checkpoint pages")
	if err := os.WriteFile(src, want, 0o600); err != nil {
		t.Fatalf("seeding src: %v", err)
	}

	dst := filepath.Join(dir, "dst")
	n, err := copyFile(src, dst)
	if err != nil {
		t.Fatalf("copyFile: %v", err)
	}
	if n != int64(len(want)) {
		t.Errorf("copied %d bytes, want %d", n, len(want))
	}
	got, err := os.ReadFile(dst)
	if err != nil {
		t.Fatalf("reading dst: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("dst content = %q, want %q", got, want)
	}

	if _, err := copyFile(dir, filepath.Join(dir, "dst2")); err == nil {
		t.Error("copyFile(directory, ...) succeeded, want error")
	}
}

type failingCloseFile struct{ *os.File }

func (f failingCloseFile) Close() error {
	_ = f.File.Close()
	return errors.New("deferred flush failed")
}

func TestCopyFile_CloseError(t *testing.T) {
	orig := createDestFile
	createDestFile = func(name string) (io.WriteCloser, error) {
		f, err := os.Create(name)
		if err != nil {
			return nil, err
		}
		return failingCloseFile{f}, nil
	}
	t.Cleanup(func() { createDestFile = orig })

	dir := t.TempDir()
	src := filepath.Join(dir, "src")
	if err := os.WriteFile(src, []byte("checkpoint pages"), 0o600); err != nil {
		t.Fatalf("seeding src: %v", err)
	}
	if _, err := copyFile(src, filepath.Join(dir, "dst")); err == nil {
		t.Error("copyFile with failing destination Close = nil, want error")
	}
}

// validRunRequest, validCheckpointRequest, and validRestoreRequest build
// requests whose every field passes validation; the per-request tests below
// break one field per case.
func validRunRequest() *ateletpb.RunRequest {
	return &ateletpb.RunRequest{
		Atespace:               "ate-demo",
		ActorName:              "counter-1",
		ActorTemplateNamespace: "ate-demo",
		ActorTemplateName:      "counter",
		TargetAteomUid:         "422938ba-8860-4983-a25d-d6bcb0a69d4e",
		ActorUid:               "123e4567-e89b-12d3-a456-426614174000",
		Spec:                   &ateletpb.WorkloadSpec{Containers: []*ateletpb.Container{{Name: "worker"}}},
	}
}

func validCheckpointRequest() *ateletpb.CheckpointRequest {
	return &ateletpb.CheckpointRequest{
		Atespace:               "ate-demo",
		ActorName:              "counter-1",
		ActorTemplateNamespace: "ate-demo",
		ActorTemplateName:      "counter",
		TargetAteomUid:         "422938ba-8860-4983-a25d-d6bcb0a69d4e",
		ActorUid:               "123e4567-e89b-12d3-a456-426614174000",
		Spec:                   &ateletpb.WorkloadSpec{Containers: []*ateletpb.Container{{Name: "worker"}}},
		Type:                   ateletpb.CheckpointType_CHECKPOINT_TYPE_EXTERNAL,
		Config: &ateletpb.CheckpointRequest_ExternalConfig{
			ExternalConfig: &ateletpb.ExternalCheckpointConfiguration{
				SnapshotUri: "gs://bucket/root/snapshots/ate-demo/counter-1-snap",
			},
		},
		Scope: ateletpb.SnapshotScope_SNAPSHOT_SCOPE_FULL,
	}
}

func validRestoreRequest() *ateletpb.RestoreRequest {
	return &ateletpb.RestoreRequest{
		Atespace:               "ate-demo",
		ActorName:              "counter-1",
		ActorTemplateNamespace: "ate-demo",
		ActorTemplateName:      "counter",
		TargetAteomUid:         "422938ba-8860-4983-a25d-d6bcb0a69d4e",
		ActorUid:               "123e4567-e89b-12d3-a456-426614174000",
		Spec:                   &ateletpb.WorkloadSpec{Containers: []*ateletpb.Container{{Name: "worker"}}},
		Type:                   ateletpb.CheckpointType_CHECKPOINT_TYPE_EXTERNAL,
		Config: &ateletpb.RestoreRequest_ExternalConfig{
			ExternalConfig: &ateletpb.ExternalCheckpointConfiguration{
				SnapshotUri: "gs://bucket/root/snapshots/ate-demo/counter-1-snap",
			},
		},
		Scope: ateletpb.SnapshotScope_SNAPSHOT_SCOPE_FULL,
	}
}

func TestValidateRunRequest(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*ateletpb.RunRequest)
		wantErr bool
	}{
		{"valid", func(*ateletpb.RunRequest) {}, false},
		{"invalid ateom uid", func(r *ateletpb.RunRequest) { r.TargetAteomUid = "../escape" }, true},
		{"invalid atespace", func(r *ateletpb.RunRequest) { r.Atespace = "../escape" }, true},
		{"invalid actor name", func(r *ateletpb.RunRequest) { r.ActorName = "../escape" }, true},
		{"invalid actor uid", func(r *ateletpb.RunRequest) { r.ActorUid = "../escape" }, true},
		{"any actor template identity accepted", func(r *ateletpb.RunRequest) { r.ActorTemplateNamespace, r.ActorTemplateName = "Not_Valid", "Not_Valid" }, false},
		{"empty actor template identity accepted", func(r *ateletpb.RunRequest) { r.ActorTemplateNamespace, r.ActorTemplateName = "", "" }, false},
		{"invalid container name", func(r *ateletpb.RunRequest) {
			r.Spec.Containers = []*ateletpb.Container{{Name: "../escape"}}
		}, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := validRunRequest()
			tc.mutate(req)
			if err := validateRunRequest(req); (err != nil) != tc.wantErr {
				t.Errorf("validateRunRequest err = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

// Checkpoint and Restore must reject a bad snapshot URI even when
// every common field is valid.
func TestValidateCheckpointRequest(t *testing.T) {
	makeReq := func(opts ...func(*ateletpb.CheckpointRequest)) *ateletpb.CheckpointRequest {
		r := validCheckpointRequest()
		for _, opt := range opts {
			opt(r)
		}
		return r
	}

	tests := []struct {
		name    string
		req     *ateletpb.CheckpointRequest
		wantErr bool
	}{
		{"valid", makeReq(), false},
		{"empty snapshot uri", makeReq(func(r *ateletpb.CheckpointRequest) { r.GetExternalConfig().SnapshotUri = "" }), true},
		{"bucketless snapshot uri", makeReq(func(r *ateletpb.CheckpointRequest) { r.GetExternalConfig().SnapshotUri = "relative/path" }), true},
		{"invalid ateom uid", makeReq(func(r *ateletpb.CheckpointRequest) { r.TargetAteomUid = "../escape" }), true},
		{"invalid atespace", makeReq(func(r *ateletpb.CheckpointRequest) { r.Atespace = "../escape" }), true},
		{"invalid actor name", makeReq(func(r *ateletpb.CheckpointRequest) { r.ActorName = "../escape" }), true},
		{"invalid actor uid", makeReq(func(r *ateletpb.CheckpointRequest) { r.ActorUid = "../escape" }), true},
		{"any actor template identity accepted", makeReq(func(r *ateletpb.CheckpointRequest) {
			r.ActorTemplateNamespace, r.ActorTemplateName = "Not_Valid", "Not_Valid"
		}), false},
		{"empty actor template identity accepted", makeReq(func(r *ateletpb.CheckpointRequest) { r.ActorTemplateNamespace, r.ActorTemplateName = "", "" }), false},
		{"invalid container name", makeReq(func(r *ateletpb.CheckpointRequest) {
			r.Spec.Containers = []*ateletpb.Container{{Name: "../escape"}}
		}), true},
		{"invalid local snapshot prefix", makeReq(func(r *ateletpb.CheckpointRequest) {
			r.Type = ateletpb.CheckpointType_CHECKPOINT_TYPE_LOCAL
			r.Config = &ateletpb.CheckpointRequest_LocalConfig{LocalConfig: &ateletpb.LocalCheckpointConfiguration{SnapshotName: ""}}
		}), true},
		{"local snapshot name escapes its directory", makeReq(func(r *ateletpb.CheckpointRequest) {
			r.Type = ateletpb.CheckpointType_CHECKPOINT_TYPE_LOCAL
			r.Config = &ateletpb.CheckpointRequest_LocalConfig{LocalConfig: &ateletpb.LocalCheckpointConfiguration{SnapshotName: "../escape"}}
		}), true},
		{"nested local snapshot prefix", makeReq(func(r *ateletpb.CheckpointRequest) {
			r.Type = ateletpb.CheckpointType_CHECKPOINT_TYPE_LOCAL
			r.Config = &ateletpb.CheckpointRequest_LocalConfig{LocalConfig: &ateletpb.LocalCheckpointConfiguration{SnapshotName: "pause/2"}}
		}), true},
		{"traversal local snapshot prefix", makeReq(func(r *ateletpb.CheckpointRequest) {
			r.Type = ateletpb.CheckpointType_CHECKPOINT_TYPE_LOCAL
			r.Config = &ateletpb.CheckpointRequest_LocalConfig{LocalConfig: &ateletpb.LocalCheckpointConfiguration{SnapshotName: ".."}}
		}), true},
		{"unspecified snapshot type", makeReq(func(r *ateletpb.CheckpointRequest) { r.Type = ateletpb.CheckpointType_CHECKPOINT_TYPE_UNSPECIFIED }), true},
		{"unspecified snapshot scope", makeReq(func(r *ateletpb.CheckpointRequest) { r.Scope = ateletpb.SnapshotScope_SNAPSHOT_SCOPE_UNSPECIFIED }), true},
		{"invalid snapshot scope", makeReq(func(r *ateletpb.CheckpointRequest) { r.Scope = ateletpb.SnapshotScope(23) }), true},
		// DATA_ON_GOLDEN is a restore-only scope: checkpoints only ever
		// capture FULL or DATA, so a checkpoint carrying it is a bug upstream.
		{"data-on-golden scope is restore-only", makeReq(func(r *ateletpb.CheckpointRequest) { r.Scope = ateletpb.SnapshotScope_SNAPSHOT_SCOPE_DATA_ON_GOLDEN }), true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := validateCheckpointRequest(tc.req); (err != nil) != tc.wantErr {
				t.Errorf("validateCheckpointRequest err = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

func TestValidateRestoreRequest(t *testing.T) {
	makeReq := func(opts ...func(*ateletpb.RestoreRequest)) *ateletpb.RestoreRequest {
		r := validRestoreRequest()
		for _, opt := range opts {
			opt(r)
		}
		return r
	}

	tests := []struct {
		name    string
		req     *ateletpb.RestoreRequest
		wantErr bool
	}{
		{"valid", makeReq(), false},
		{"empty snapshot uri", makeReq(func(r *ateletpb.RestoreRequest) { r.GetExternalConfig().SnapshotUri = "" }), true},
		{"bucketless snapshot uri", makeReq(func(r *ateletpb.RestoreRequest) { r.GetExternalConfig().SnapshotUri = "relative/path" }), true},
		{"invalid ateom uid", makeReq(func(r *ateletpb.RestoreRequest) { r.TargetAteomUid = "../escape" }), true},
		{"invalid atespace", makeReq(func(r *ateletpb.RestoreRequest) { r.Atespace = "../escape" }), true},
		{"invalid actor name", makeReq(func(r *ateletpb.RestoreRequest) { r.ActorName = "../escape" }), true},
		{"invalid actor uid", makeReq(func(r *ateletpb.RestoreRequest) { r.ActorUid = "../escape" }), true},
		{"any actor template identity accepted", makeReq(func(r *ateletpb.RestoreRequest) {
			r.ActorTemplateNamespace, r.ActorTemplateName = "Not_Valid", "Not_Valid"
		}), false},
		{"empty actor template identity accepted", makeReq(func(r *ateletpb.RestoreRequest) { r.ActorTemplateNamespace, r.ActorTemplateName = "", "" }), false},
		{"invalid container name", makeReq(func(r *ateletpb.RestoreRequest) {
			r.Spec.Containers = []*ateletpb.Container{{Name: "../escape"}}
		}), true},
		{"invalid local snapshot prefix", makeReq(func(r *ateletpb.RestoreRequest) {
			r.Type = ateletpb.CheckpointType_CHECKPOINT_TYPE_LOCAL
			r.Config = &ateletpb.RestoreRequest_LocalConfig{LocalConfig: &ateletpb.LocalCheckpointConfiguration{SnapshotName: ""}}
		}), true},
		{"local snapshot name escapes its directory", makeReq(func(r *ateletpb.RestoreRequest) {
			r.Type = ateletpb.CheckpointType_CHECKPOINT_TYPE_LOCAL
			r.Config = &ateletpb.RestoreRequest_LocalConfig{LocalConfig: &ateletpb.LocalCheckpointConfiguration{SnapshotName: "../escape"}}
		}), true},
		{"nested local snapshot prefix", makeReq(func(r *ateletpb.RestoreRequest) {
			r.Type = ateletpb.CheckpointType_CHECKPOINT_TYPE_LOCAL
			r.Config = &ateletpb.RestoreRequest_LocalConfig{LocalConfig: &ateletpb.LocalCheckpointConfiguration{SnapshotName: "pause/2"}}
		}), true},
		{"traversal local snapshot prefix", makeReq(func(r *ateletpb.RestoreRequest) {
			r.Type = ateletpb.CheckpointType_CHECKPOINT_TYPE_LOCAL
			r.Config = &ateletpb.RestoreRequest_LocalConfig{LocalConfig: &ateletpb.LocalCheckpointConfiguration{SnapshotName: ".."}}
		}), true},
		{"unspecified snapshot type", makeReq(func(r *ateletpb.RestoreRequest) { r.Type = ateletpb.CheckpointType_CHECKPOINT_TYPE_UNSPECIFIED }), true},
		{"unspecified snapshot scope", makeReq(func(r *ateletpb.RestoreRequest) { r.Scope = ateletpb.SnapshotScope_SNAPSHOT_SCOPE_UNSPECIFIED }), true},
		{"invalid snapshot scope", makeReq(func(r *ateletpb.RestoreRequest) { r.Scope = ateletpb.SnapshotScope(23) }), true},
		{"data-on-golden with golden uri", makeReq(func(r *ateletpb.RestoreRequest) {
			r.Scope = ateletpb.SnapshotScope_SNAPSHOT_SCOPE_DATA_ON_GOLDEN
			r.GoldenSnapshotUri = "gs://bucket/golden-root/snapshots/ate-golden/golden-1"
		}), false},
		{"data-on-golden without golden uri", makeReq(func(r *ateletpb.RestoreRequest) {
			r.Scope = ateletpb.SnapshotScope_SNAPSHOT_SCOPE_DATA_ON_GOLDEN
		}), true},
		{"data-on-golden with bucketless golden uri", makeReq(func(r *ateletpb.RestoreRequest) {
			r.Scope = ateletpb.SnapshotScope_SNAPSHOT_SCOPE_DATA_ON_GOLDEN
			r.GoldenSnapshotUri = "relative/path"
		}), true},
		// A pause (local) checkpoint may combine with the golden snapshot:
		// the golden URI is a top-level field precisely so LOCAL restores
		// can carry it.
		{"data-on-golden with local checkpoint type", makeReq(func(r *ateletpb.RestoreRequest) {
			r.Scope = ateletpb.SnapshotScope_SNAPSHOT_SCOPE_DATA_ON_GOLDEN
			r.GoldenSnapshotUri = "gs://bucket/golden-root/snapshots/ate-golden/golden-1"
			r.Type = ateletpb.CheckpointType_CHECKPOINT_TYPE_LOCAL
			r.Config = &ateletpb.RestoreRequest_LocalConfig{LocalConfig: &ateletpb.LocalCheckpointConfiguration{SnapshotName: "local-snap-1"}}
		}), false},
		{"golden uri with non-data-on-golden scope", makeReq(func(r *ateletpb.RestoreRequest) {
			r.GoldenSnapshotUri = "gs://bucket/golden-root/snapshots/ate-golden/golden-1"
		}), true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := validateRestoreRequest(tc.req); (err != nil) != tc.wantErr {
				t.Errorf("validateRestoreRequest err = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

// Every valid atelet scope must map to its ateom counterpart; in particular
// DATA_ON_GOLDEN must never silently degrade to FULL.
func TestToAteomSnapshotScope(t *testing.T) {
	tests := []struct {
		in   ateletpb.SnapshotScope
		want ateompb.SnapshotScope
	}{
		{ateletpb.SnapshotScope_SNAPSHOT_SCOPE_FULL, ateompb.SnapshotScope_SNAPSHOT_SCOPE_FULL},
		{ateletpb.SnapshotScope_SNAPSHOT_SCOPE_DATA, ateompb.SnapshotScope_SNAPSHOT_SCOPE_DATA},
		{ateletpb.SnapshotScope_SNAPSHOT_SCOPE_DATA_ON_GOLDEN, ateompb.SnapshotScope_SNAPSHOT_SCOPE_DATA_ON_GOLDEN},
	}
	for _, tc := range tests {
		if got := toAteomSnapshotScope(tc.in); got != tc.want {
			t.Errorf("toAteomSnapshotScope(%v) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

// TestFetchAssetRejectsBadHash confirms fetchAsset validates the asset hash
// before the cache-hit os.Stat/early-return, not merely "at some point". To
// prove the ordering, it plants a real file at the exact path an invalid hash
// resolves to: a correctly-ordered fetchAsset validates first and returns an
// error, while a regression that stats first would find this file and return it
// with a nil error, failing the test. StaticFilesDir is redirected to a temp
// dir so the planted path is writable and isolated.
func TestFetchAssetRejectsBadHash(t *testing.T) {
	orig := ateompath.StaticFilesDir
	ateompath.StaticFilesDir = t.TempDir()
	t.Cleanup(func() { ateompath.StaticFilesDir = orig })

	// Invalid (8 chars, not 64) but separator-free, so it resolves to a normal
	// filename inside the temp StaticFilesDir.
	const badHash = "deadbeef"
	if err := os.WriteFile(ateompath.RunSCBinaryPath(badHash), []byte("planted"), 0o755); err != nil {
		t.Fatalf("planting cache file: %v", err)
	}

	s := &AteomHerder{}
	_, err := s.fetchAsset(context.Background(), assetEntry{SHA256: badHash})
	if err == nil {
		t.Fatal("fetchAsset returned a cache hit for an invalid hash; validation must run before the os.Stat early return")
	}
	// The error must come from the validation step, proving it ran before the
	// cache-hit stat could return the planted file.
	if !strings.Contains(err.Error(), "while validating asset hash") {
		t.Errorf("error did not come from hash validation: %v", err)
	}
}

// fakeObjectStorage serves fixed bytes for GetObject so fetchAsset can be tested.
type fakeObjectStorage struct {
	data []byte
	err  error
}

func (f fakeObjectStorage) GetObject(_ context.Context, _, _ string) (io.ReadCloser, error) {
	if f.err != nil {
		return nil, f.err
	}
	return io.NopCloser(bytes.NewReader(f.data)), nil
}

func (fakeObjectStorage) PutObject(_ context.Context, _, _ string, _ io.Reader) error { return nil }

// TestFetchAssetStreaming covers the streamed download: good asset cached,
// over-cap rejected, hash mismatch rejected (failures leave no cache file).
func TestFetchAssetStreaming(t *testing.T) {
	origDir, origCap := ateompath.StaticFilesDir, maxAssetBytes
	t.Cleanup(func() { ateompath.StaticFilesDir, maxAssetBytes = origDir, origCap })

	content := []byte("micro-vm kernel bytes")
	goodHash := fmt.Sprintf("%x", sha256.Sum256(content))
	const url = "gs://test-bucket/asset"

	t.Run("good asset is cached", func(t *testing.T) {
		ateompath.StaticFilesDir = t.TempDir()
		s := &AteomHerder{anonGCSClient: fakeObjectStorage{data: content}}
		path, err := s.fetchAsset(context.Background(), assetEntry{URL: url, SHA256: goodHash})
		if err != nil {
			t.Fatalf("fetchAsset: %v", err)
		}
		got, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("reading cached asset: %v", err)
		}
		if !bytes.Equal(got, content) {
			t.Errorf("cached bytes = %q, want %q", got, content)
		}
	})

	t.Run("over-cap asset rejected, cache not written", func(t *testing.T) {
		ateompath.StaticFilesDir = t.TempDir()
		maxAssetBytes = 4 // content is longer than this
		s := &AteomHerder{anonGCSClient: fakeObjectStorage{data: content}}
		_, err := s.fetchAsset(context.Background(), assetEntry{URL: url, SHA256: goodHash})
		if err == nil {
			t.Fatal("fetchAsset accepted an over-cap asset")
		}
		if !errors.Is(err, ateerrors.ReasonInvalidSandboxAsset) {
			t.Errorf("over-cap error not tagged terminal: %v", err)
		}
		if _, err := os.Stat(ateompath.RunSCBinaryPath(goodHash)); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("over-cap download left a file at the cache path (stat err = %v)", err)
		}
	})

	t.Run("hash mismatch rejected, cache not written", func(t *testing.T) {
		ateompath.StaticFilesDir = t.TempDir()
		maxAssetBytes = origCap
		wrongHash := strings.Repeat("a", 64) // valid 64-hex format, wrong value
		s := &AteomHerder{anonGCSClient: fakeObjectStorage{data: content}}
		_, err := s.fetchAsset(context.Background(), assetEntry{URL: url, SHA256: wrongHash})
		if err == nil {
			t.Fatal("fetchAsset accepted a hash mismatch")
		}
		if !errors.Is(err, ateerrors.ReasonInvalidSandboxAsset) {
			t.Errorf("hash-mismatch error not tagged terminal: %v", err)
		}
		if _, err := os.Stat(ateompath.RunSCBinaryPath(wrongHash)); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("mismatched download left a file at the cache path (stat err = %v)", err)
		}
	})

	t.Run("missing object is terminal", func(t *testing.T) {
		ateompath.StaticFilesDir = t.TempDir()
		maxAssetBytes = origCap
		// The ategcs clients tag a missing object with ReasonFailedGetExternalObject.
		notFound := fmt.Errorf("%w: no such object", ateerrors.ReasonFailedGetExternalObject)
		s := &AteomHerder{anonGCSClient: fakeObjectStorage{err: notFound}}
		_, err := s.fetchAsset(context.Background(), assetEntry{URL: url, SHA256: goodHash})
		if !errors.Is(err, ateerrors.ReasonFailedGetExternalObject) {
			t.Errorf("missing-object error not tagged terminal: %v", err)
		}
		if errors.Is(err, ateerrors.ReasonInvalidSandboxAsset) {
			t.Errorf("missing-object error wrongly tagged ReasonInvalidSandboxAsset: %v", err)
		}
		// The extracted (outermost) Reason drives CrashIfReason's ErrorInfo;
		// it must be the client tag, not a fetchAsset blanket wrap.
		if r, ok := errors.AsType[ateerrors.Reason](err); !ok || r != ateerrors.ReasonFailedGetExternalObject {
			t.Errorf("extracted reason = %v (ok=%v), want ReasonFailedGetExternalObject", r, ok)
		}
	})

	t.Run("malformed url is terminal", func(t *testing.T) {
		ateompath.StaticFilesDir = t.TempDir()
		maxAssetBytes = origCap
		s := &AteomHerder{anonGCSClient: fakeObjectStorage{data: content}}
		// Invalid percent-escape: url.Parse rejects it inside ategcs.Open, which
		// tags the failure with ReasonInvalidObjectURL.
		_, err := s.fetchAsset(context.Background(), assetEntry{URL: "gs://bucket/%zz", SHA256: goodHash})
		if !errors.Is(err, ateerrors.ReasonInvalidObjectURL) {
			t.Errorf("malformed-url error not tagged terminal: %v", err)
		}
	})

	t.Run("network error stays untagged (retriable)", func(t *testing.T) {
		ateompath.StaticFilesDir = t.TempDir()
		maxAssetBytes = origCap
		s := &AteomHerder{anonGCSClient: fakeObjectStorage{err: errors.New("connection refused")}}
		_, err := s.fetchAsset(context.Background(), assetEntry{URL: url, SHA256: goodHash})
		if err == nil {
			t.Fatal("fetchAsset accepted a failing open")
		}
		// A transient open failure must carry no Reason at all: any tag here
		// is claimed by CrashIfReason in Checkpoint/Restore and would mark a
		// recoverable actor CRASHED instead of letting the control plane retry.
		if r, ok := errors.AsType[ateerrors.Reason](err); ok {
			t.Errorf("network error wrongly tagged with reason %v: %v", r, err)
		}
		if !strings.Contains(err.Error(), "while fetching") {
			t.Errorf("open failure lost its context wrap: %v", err)
		}
	})
}

// TestRPCBoundariesReject confirms each of the three RPCs validates path inputs
// before touching its (here nil) dependencies. A traversal value must be
// rejected as InvalidArgument rather than panicking or surfacing as
// Internal. Guards against a future removal or reordering of the validation
// call at any boundary.
func TestRPCBoundariesReject(t *testing.T) {
	s := &AteomHerder{}
	ctx := context.Background()
	badUID := "../escape" // valid actor ref, invalid ateom UID
	const okAtespace, okID, okActorUID = "ate-demo", "counter-1", "123e4567-e89b-12d3-a456-426614174000"
	okSpec := &ateletpb.WorkloadSpec{Containers: []*ateletpb.Container{{Name: "worker"}}}

	wantInvalidArgument := func(t *testing.T, rpc string, err error) {
		t.Helper()
		if err == nil {
			t.Errorf("%s accepted an invalid target ateom UID", rpc)
			return
		}
		if code := status.Code(err); code != codes.InvalidArgument {
			t.Errorf("%s returned code %v, want InvalidArgument", rpc, code)
		}
	}

	t.Run("Run", func(t *testing.T) {
		_, err := s.Run(ctx, &ateletpb.RunRequest{
			Atespace: okAtespace, ActorName: okID,
			ActorUid: okActorUID, TargetAteomUid: badUID, Spec: okSpec,
		})
		wantInvalidArgument(t, "Run", err)
	})
	t.Run("Checkpoint", func(t *testing.T) {
		_, err := s.Checkpoint(ctx, &ateletpb.CheckpointRequest{
			Atespace: okAtespace, ActorName: okID,
			ActorUid: okActorUID, TargetAteomUid: badUID, Spec: okSpec,
		})
		wantInvalidArgument(t, "Checkpoint", err)
	})
	t.Run("Restore", func(t *testing.T) {
		_, err := s.Restore(ctx, &ateletpb.RestoreRequest{
			Atespace: okAtespace, ActorName: okID,
			ActorUid: okActorUID, TargetAteomUid: badUID, Spec: okSpec,
		})
		wantInvalidArgument(t, "Restore", err)
	})
	t.Run("Terminate", func(t *testing.T) {
		t.Run("invalid ateom UID", func(t *testing.T) {
			_, err := s.Terminate(ctx, &ateletpb.TerminateRequest{
				Atespace: okAtespace, ActorName: okID,
				ActorUid: okActorUID, ActorTemplateNamespace: "default", ActorTemplateName: "template",
				TargetAteomUid: badUID, Spec: okSpec,
			})
			wantInvalidArgument(t, "Terminate", err)
		})
		t.Run("missing target ateom UID", func(t *testing.T) {
			_, err := s.Terminate(ctx, &ateletpb.TerminateRequest{
				Atespace: okAtespace, ActorName: okID,
				ActorUid: okActorUID, ActorTemplateNamespace: "default", ActorTemplateName: "template",
				Spec: okSpec,
			})
			wantInvalidArgument(t, "Terminate", err)
		})
	})
}

func TestBuildAteomWorkloadSpecForwardsReadyz(t *testing.T) {
	in := &ateletpb.WorkloadSpec{
		Containers: []*ateletpb.Container{
			{
				Name:  "with-probe",
				Image: "main",
				Readyz: &ateletpb.Readyz{
					HttpGet:        &ateletpb.HTTPGetAction{Path: "/health", Port: 8080},
					TimeoutSeconds: 45,
				},
			},
			{
				Name: "without-probe",
			},
		},
	}
	want := &ateompb.WorkloadSpec{
		Containers: []*ateompb.Container{
			{
				Name: "with-probe",
				Readyz: &ateompb.Readyz{
					HttpGet:        &ateompb.HTTPGetAction{Path: "/health", Port: 8080},
					TimeoutSeconds: 45,
				},
			},
			{Name: "without-probe"},
		},
	}
	got, err := buildAteomWorkloadSpec(in)
	if err != nil {
		t.Fatalf("buildAteomWorkloadSpec failed: %v", err)
	}
	if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
		t.Errorf("buildAteomWorkloadSpec mismatch (-want +got):\n%s", diff)
	}
}

func TestBuildAteomWorkloadSpecForwardsDurableDirMounts(t *testing.T) {
	in := &ateletpb.WorkloadSpec{
		Volumes: []*ateletpb.Volume{
			{Name: "data", Source: &ateletpb.Volume_DurableDir{DurableDir: &ateletpb.DurableDirVolume{}}},
			{Name: "cache", Source: &ateletpb.Volume_DurableDir{DurableDir: &ateletpb.DurableDirVolume{}}},
			{Name: "scratch", Source: &ateletpb.Volume_External{External: &ateletpb.ExternalVolumeSource{}}},
			{Name: "system-info", Source: &ateletpb.Volume_SystemInfo{SystemInfo: &ateletpb.SystemInfoVolume{}}},
		},
		Containers: []*ateletpb.Container{
			{
				Name: "main",
				VolumeMounts: []*ateletpb.VolumeMount{
					{Name: "data", MountPath: "/home/counter"},
					{Name: "cache", MountPath: "/var/cache"},
					{Name: "scratch", MountPath: "/scratch"},
					{Name: "system-info", MountPath: "/run/ate"},
				},
			},
			{
				Name: "sidecar",
				VolumeMounts: []*ateletpb.VolumeMount{
					{Name: "data", MountPath: "/shared"},
				},
			},
			{Name: "no-volumes"},
		},
	}
	// ateom needs the volume NAME as well as the path: the name selects the
	// per-volume directory on the host, and an actor may have several.
	want := &ateompb.WorkloadSpec{
		Containers: []*ateompb.Container{
			{
				Name: "main",
				DurableDirVolumeMounts: []*ateompb.DurableDirVolumeMount{
					{VolumeName: "data", MountPath: "/home/counter"},
					{VolumeName: "cache", MountPath: "/var/cache"},
				},
				CsiVolumeMounts: []*ateompb.VolumeMount{
					{VolumeName: "scratch", MountPath: "/scratch"},
				},
				SystemInfoVolumeMounts: []*ateompb.SystemInfoVolumeMount{
					{VolumeName: "system-info", MountPath: "/run/ate"},
				},
			},
			{
				Name: "sidecar",
				DurableDirVolumeMounts: []*ateompb.DurableDirVolumeMount{
					{VolumeName: "data", MountPath: "/shared"},
				},
			},
			{Name: "no-volumes"},
		},
	}
	got, err := buildAteomWorkloadSpec(in)
	if err != nil {
		t.Fatalf("buildAteomWorkloadSpec failed: %v", err)
	}
	if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
		t.Errorf("buildAteomWorkloadSpec mismatch (-want +got):\n%s", diff)
	}
}

func TestBuildAteomWorkloadSpecValidation(t *testing.T) {
	tests := []struct {
		name    string
		in      *ateletpb.WorkloadSpec
		wantErr string
	}{
		{
			name: "missing volume definition",
			in: &ateletpb.WorkloadSpec{
				Volumes: []*ateletpb.Volume{
					{Name: "data", Source: &ateletpb.Volume_DurableDir{DurableDir: &ateletpb.DurableDirVolume{}}},
				},
				Containers: []*ateletpb.Container{
					{
						Name: "ctr",
						VolumeMounts: []*ateletpb.VolumeMount{
							{Name: "missing-vol", MountPath: "/data"},
						},
					},
				},
			},
			wantErr: `container "ctr" mounts volume "missing-vol" which is not defined in workload volumes`,
		},
		{
			name: "unsupported volume source",
			in: &ateletpb.WorkloadSpec{
				Volumes: []*ateletpb.Volume{
					{Name: "data"},
				},
				Containers: []*ateletpb.Container{
					{
						Name: "ctr",
						VolumeMounts: []*ateletpb.VolumeMount{
							{Name: "data", MountPath: "/data"},
						},
					},
				},
			},
			wantErr: `container "ctr" mounts volume "data" with unsupported source <nil>`,
		},
		{
			name: "duplicate volume names",
			in: &ateletpb.WorkloadSpec{
				Volumes: []*ateletpb.Volume{
					{Name: "data", Source: &ateletpb.Volume_DurableDir{DurableDir: &ateletpb.DurableDirVolume{}}},
					{Name: "data", Source: &ateletpb.Volume_External{External: &ateletpb.ExternalVolumeSource{}}},
				},
				Containers: []*ateletpb.Container{
					{
						Name: "ctr",
						VolumeMounts: []*ateletpb.VolumeMount{
							{Name: "data", MountPath: "/data"},
						},
					},
				},
			},
			wantErr: `duplicate volume name "data" in workload spec`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := buildAteomWorkloadSpec(tc.in)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if got, want := err.Error(), tc.wantErr; !strings.Contains(got, want) {
				t.Errorf("error mismatch:\nwant: %s\ngot:  %s", want, got)
			}
		})
	}
}

func TestToAteomEgressGateway(t *testing.T) {
	if got := toAteomEgressGateway(nil); got != nil {
		t.Fatalf("toAteomEgressGateway(nil) = %v, want nil", got)
	}
	want := &ateompb.EgressGateway{Address: "egress.example:443"}
	got := toAteomEgressGateway(&ateletpb.EgressGateway{Address: want.Address})
	if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
		t.Errorf("toAteomEgressGateway mismatch (-want +got):\n%s", diff)
	}
}

func TestIsTerminalFileErr(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"not exist", os.ErrNotExist, true},
		{"permission", os.ErrPermission, true},
		{"is a directory", syscall.EISDIR, true},
		{"not a directory", syscall.ENOTDIR, true},
		{"name too long", syscall.ENAMETOOLONG, true},
		{"symlink loop", syscall.ELOOP, true},
		{"read-only filesystem", syscall.EROFS, true},
		{"no space left on device", syscall.ENOSPC, true},
		{"disk quota exceeded", syscall.EDQUOT, true},
		{"wrapped not exist", fmt.Errorf("while reading: %w", os.ErrNotExist), true},
		{"path error no space", &os.PathError{Op: "write", Path: "/var/lib/atelet/x", Err: syscall.ENOSPC}, true},
		{"too many open files", syscall.EMFILE, false},
		{"stale nfs handle", syscall.ESTALE, false},
		{"try again", syscall.EAGAIN, false},
		{"io error", syscall.EIO, false},
		{"nil", nil, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isTerminalFileSystemErr(tt.err); got != tt.want {
				t.Errorf("isTerminalFileSystemErr(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

// TestGoldenOnlyFiles verifies the DataOnGolden combine rule: the actor's own
// snapshot files shadow same-named golden files (the durable-dir tar), and the
// golden snapshot supplies the rest.
func TestGoldenOnlyFiles(t *testing.T) {
	tests := []struct {
		name        string
		actorFiles  []string
		goldenFiles []string
		want        []string
	}{
		{
			name:        "durable tar shadowed, guest files kept",
			actorFiles:  []string{"durable-dir.tar"},
			goldenFiles: []string{"config.json", "state.json", "memory-ranges", "base-id", "durable-dir.tar"},
			want:        []string{"config.json", "state.json", "memory-ranges", "base-id"},
		},
		{
			name:        "golden without durable tar is kept whole",
			actorFiles:  []string{"durable-dir.tar"},
			goldenFiles: []string{"config.json", "state.json"},
			want:        []string{"config.json", "state.json"},
		},
		{
			name:        "no actor files keeps everything",
			actorFiles:  nil,
			goldenFiles: []string{"config.json"},
			want:        []string{"config.json"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := goldenOnlyFiles(tc.actorFiles, tc.goldenFiles)
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("goldenOnlyFiles diff (-want +got):\n%s", diff)
			}
		})
	}
}

func TestWrapFileSystemErrAttachesTerminalReason(t *testing.T) {
	tests := []struct {
		name         string
		err          error
		wantTerminal bool
	}{
		{"no space left on device", &os.PathError{Op: "write", Path: "/x", Err: syscall.ENOSPC}, true},
		{"disk quota exceeded", syscall.EDQUOT, true},
		{"not exist", os.ErrNotExist, true},
		{"io error", syscall.EIO, false},
		{"try again", syscall.EAGAIN, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wrapped := wrapFileSystemErr("while writing asset", tt.err)
			if got := errors.Is(wrapped, ateerrors.ReasonTerminalFileSystemError); got != tt.wantTerminal {
				t.Errorf("errors.Is(wrapFileSystemErr(%v), ReasonTerminalFileSystemError) = %v, want %v", tt.err, got, tt.wantTerminal)
			}
			if !errors.Is(wrapped, tt.err) {
				t.Errorf("wrapFileSystemErr(%v) lost the original error: %v", tt.err, wrapped)
			}
		})
	}
}

// mapObjectStorage serves per-object bytes so multi-object downloads can be
// tested; the key is "<bucket>/<object>".
type mapObjectStorage struct {
	objects map[string][]byte
}

func (m mapObjectStorage) GetObject(_ context.Context, bucket, object string) (io.ReadCloser, error) {
	data, ok := m.objects[bucket+"/"+object]
	if !ok {
		return nil, fmt.Errorf("object %s/%s not found", bucket, object)
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (mapObjectStorage) PutObject(_ context.Context, _, _ string, _ io.Reader) error { return nil }

// TestDownloadCombinedCheckpoint verifies a DataOnGolden restore stages one
// folder holding the actor snapshot's durable-dir tar and the golden
// snapshot's remaining files — and that the golden's own durable-dir tar is
// the one that loses the name collision.
func TestDownloadCombinedCheckpoint(t *testing.T) {
	zstdBytes := func(t *testing.T, s string) []byte {
		t.Helper()
		var buf bytes.Buffer
		zw, err := zstd.NewWriter(&buf)
		if err != nil {
			t.Fatalf("zstd.NewWriter: %v", err)
		}
		if _, err := zw.Write([]byte(s)); err != nil {
			t.Fatalf("zstd write: %v", err)
		}
		if err := zw.Close(); err != nil {
			t.Fatalf("zstd close: %v", err)
		}
		return buf.Bytes()
	}

	store := mapObjectStorage{objects: map[string][]byte{
		"bucket/root/snapshots/ate-demo/counter-1-snap/durable-dir.tar.zstd":    zstdBytes(t, "actor durable data"),
		"bucket/golden-root/snapshots/ate-golden/golden-1/config.json.zstd":     zstdBytes(t, "golden config"),
		"bucket/golden-root/snapshots/ate-golden/golden-1/memory-ranges.zstd":   zstdBytes(t, "golden memory"),
		"bucket/golden-root/snapshots/ate-golden/golden-1/durable-dir.tar.zstd": zstdBytes(t, "golden durable data (must not be downloaded)"),
	}}
	s := &AteomHerder{gcsClient: store}

	dstDir := t.TempDir()
	err := s.downloadCombinedCheckpoint(context.Background(),
		"gs://bucket/root/snapshots/ate-demo/counter-1-snap",
		"gs://bucket/golden-root/snapshots/ate-golden/golden-1",
		dstDir,
		[]string{"durable-dir.tar"},
		[]string{"config.json", "memory-ranges", "durable-dir.tar"})
	if err != nil {
		t.Fatalf("downloadCombinedCheckpoint: %v", err)
	}

	want := map[string]string{
		"durable-dir.tar": "actor durable data",
		"config.json":     "golden config",
		"memory-ranges":   "golden memory",
	}
	entries, err := os.ReadDir(dstDir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(entries) != len(want) {
		t.Errorf("staged %d files, want %d", len(entries), len(want))
	}
	for name, content := range want {
		got, err := os.ReadFile(filepath.Join(dstDir, name))
		if err != nil {
			t.Fatalf("ReadFile(%s): %v", name, err)
		}
		if string(got) != content {
			t.Errorf("%s content = %q, want %q", name, got, content)
		}
	}
}

// blockerDesc registers a single unary method whose handler blocks until block
// is closed (or the RPC context is cancelled). It lets a test hold one RPC
// "in-flight" across a drain without any generated proto.
func blockerDesc(block <-chan struct{}) grpc.ServiceDesc {
	return grpc.ServiceDesc{
		ServiceName: "drain.test.Blocker",
		HandlerType: (*any)(nil),
		Methods: []grpc.MethodDesc{{
			MethodName: "Block",
			Handler: func(_ any, ctx context.Context, dec func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
				if err := dec(new(emptypb.Empty)); err != nil {
					return nil, err
				}
				select {
				case <-block:
					return new(emptypb.Empty), nil
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			},
		}},
	}
}

// newBlockingTestServer starts a gRPC server on a loopback port exposing the
// blocker service and returns it with a connected client.
func newBlockingTestServer(t *testing.T, block <-chan struct{}) (*grpc.Server, *grpc.ClientConn) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	desc := blockerDesc(block)
	srv.RegisterService(&desc, nil)
	go func() { _ = srv.Serve(lis) }()

	conn, err := grpc.NewClient(
		lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return srv, conn
}

func callBlock(conn *grpc.ClientConn) <-chan error {
	rpcErr := make(chan error, 1)
	go func() {
		rpcErr <- conn.Invoke(context.Background(), "/drain.test.Blocker/Block",
			new(emptypb.Empty), new(emptypb.Empty))
	}()
	return rpcErr
}

// TestDrainOnShutdownInFlightFinishes asserts that an RPC already in flight when
// SIGTERM arrives is allowed to complete (GracefulStop waits for it) and that
// readiness flips to not-ready.
func TestDrainOnShutdownInFlightFinishes(t *testing.T) {
	*drainDelay = 0
	*drainTimeout = 5 * time.Second

	block := make(chan struct{})
	srv, conn := newBlockingTestServer(t, block)

	rpcErr := callBlock(conn)
	time.Sleep(100 * time.Millisecond) // let the RPC reach the handler

	readiness := &serverboot.Readiness{}
	ctx, cancel := context.WithCancel(context.Background())
	drainDone := drainOnShutdown(ctx, srv, readiness)

	cancel() // simulate SIGTERM

	// Release the handler shortly after the drain begins; a graceful drain must
	// wait for it rather than abort it.
	time.Sleep(100 * time.Millisecond)
	close(block)

	select {
	case err := <-rpcErr:
		if err != nil {
			t.Fatalf("in-flight RPC should complete during graceful drain, got: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("in-flight RPC did not complete")
	}

	select {
	case <-drainDone:
	case <-time.After(3 * time.Second):
		t.Fatal("drain did not complete")
	}
	if readiness.Ready() {
		t.Fatal("readiness should be not-ready after drain")
	}
}

// TestDrainOnShutdownForceStopsAfterTimeout asserts that an RPC still running
// past drain-timeout is forcefully cancelled by Stop().
func TestDrainOnShutdownForceStopsAfterTimeout(t *testing.T) {
	*drainDelay = 0
	*drainTimeout = 200 * time.Millisecond

	block := make(chan struct{}) // never closed → handler blocks past the timeout
	srv, conn := newBlockingTestServer(t, block)

	rpcErr := callBlock(conn)
	time.Sleep(100 * time.Millisecond)

	readiness := &serverboot.Readiness{}
	ctx, cancel := context.WithCancel(context.Background())
	start := time.Now()
	drainDone := drainOnShutdown(ctx, srv, readiness)
	cancel()

	select {
	case <-drainDone:
	case <-time.After(3 * time.Second):
		t.Fatal("drain did not force-stop within deadline")
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("force stop took too long (%v); expected ~drain-timeout", elapsed)
	}

	select {
	case err := <-rpcErr:
		if err == nil {
			t.Fatal("in-flight RPC should have been aborted by force stop")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("in-flight RPC did not return after force stop")
	}
	if readiness.Ready() {
		t.Fatal("readiness should be not-ready after drain")
	}
}

// Image volumes appear on their own ImageVolumeMounts field, separate from durable-dir mounts.
func TestBuildAteomWorkloadSpec_ImageVolumeMounts(t *testing.T) {
	spec := &ateletpb.WorkloadSpec{
		Volumes: []*ateletpb.Volume{
			{Name: "agent", Source: &ateletpb.Volume_Image{Image: &ateletpb.ImageVolumeSource{}}},
			{Name: "data", Source: &ateletpb.Volume_DurableDir{DurableDir: &ateletpb.DurableDirVolume{}}},
			{Name: "ext", Source: &ateletpb.Volume_External{External: &ateletpb.ExternalVolumeSource{}}},
		},
		Containers: []*ateletpb.Container{{
			Name: "app",
			VolumeMounts: []*ateletpb.VolumeMount{
				{Name: "agent", MountPath: "/ate"},
				{Name: "data", MountPath: "/var/data"},
				{Name: "ext", MountPath: "/mnt/ext"},
			},
		}},
	}

	got, err := buildAteomWorkloadSpec(spec)
	if err != nil {
		t.Fatalf("buildAteomWorkloadSpec: %v", err)
	}
	if len(got.GetContainers()) != 1 {
		t.Fatalf("containers = %d, want 1", len(got.GetContainers()))
	}
	ctr := got.GetContainers()[0]

	if len(ctr.GetImageVolumeMounts()) != 1 {
		t.Fatalf("image volume mounts = %v, want 1", ctr.GetImageVolumeMounts())
	}
	if name, path := ctr.GetImageVolumeMounts()[0].GetVolumeName(), ctr.GetImageVolumeMounts()[0].GetMountPath(); name != "agent" || path != "/ate" {
		t.Errorf("image volume mount = (%q, %q), want (agent, /ate)", name, path)
	}
	if len(ctr.GetDurableDirVolumeMounts()) != 1 || ctr.GetDurableDirVolumeMounts()[0].GetVolumeName() != "data" {
		t.Errorf("durable mounts = %v, want just data", ctr.GetDurableDirVolumeMounts())
	}
}

// allocatedBytes reports how much disk a file actually occupies, which is less than its
// size when it has holes.
func allocatedBytes(t *testing.T, path string) int64 {
	t.Helper()
	var st syscall.Stat_t
	if err := syscall.Stat(path, &st); err != nil {
		t.Fatalf("stat %q: %v", path, err)
	}
	return st.Blocks * 512
}

// noFdFile hides the descriptor of an *os.File, forcing copySparse down its
// userspace path.
type noFdFile struct {
	f *os.File
}

func (n noFdFile) Write(b []byte) (int, error)              { return n.f.Write(b) }
func (n noFdFile) WriteAt(b []byte, off int64) (int, error) { return n.f.WriteAt(b, off) }
func (n noFdFile) Truncate(size int64) error                { return n.f.Truncate(size) }
func (n noFdFile) Close() error                             { return n.f.Close() }

func TestCopyFilePreservesHoles(t *testing.T) {
	const (
		size     = 32 << 20
		markerAt = 16 << 20
	)
	dir := t.TempDir()
	src := filepath.Join(dir, "memory-ranges")

	// A stand-in for a guest memory image: mostly hole, with data at both the start
	// and the middle.
	f, err := os.Create(src)
	if err != nil {
		t.Fatalf("creating src: %v", err)
	}
	if err := f.Truncate(size); err != nil {
		t.Fatalf("sizing src: %v", err)
	}
	head := bytes.Repeat([]byte{0xAB}, 4<<10)
	middle := bytes.Repeat([]byte{0xCD}, 4<<10)
	if _, err := f.WriteAt(head, 0); err != nil {
		t.Fatalf("writing head: %v", err)
	}
	if _, err := f.WriteAt(middle, markerAt); err != nil {
		t.Fatalf("writing middle: %v", err)
	}
	if err := errors.Join(f.Sync(), f.Close()); err != nil {
		t.Fatalf("flushing src: %v", err)
	}

	dst := filepath.Join(dir, "copied")
	n, err := copyFile(src, dst)
	if err != nil {
		t.Fatalf("copyFile: %v", err)
	}
	if n != size {
		t.Errorf("copied %d logical bytes, want %d", n, size)
	}

	// The copy must be byte-identical, holes included.
	want, err := os.ReadFile(src)
	if err != nil {
		t.Fatalf("reading src: %v", err)
	}
	got, err := os.ReadFile(dst)
	if err != nil {
		t.Fatalf("reading dst: %v", err)
	}
	if !bytes.Equal(want, got) {
		t.Fatal("copy differs from source")
	}

	srcAlloc, dstAlloc := allocatedBytes(t, src), allocatedBytes(t, dst)
	if srcAlloc >= size/2 {
		t.Skipf("source did not end up sparse (%d of %d bytes allocated); "+
			"this filesystem cannot report holes", srcAlloc, int64(size))
	}
	// A dense copy would allocate the full logical size; a hole-preserving one stays
	// near the source's footprint.
	if dstAlloc > srcAlloc*4 {
		t.Errorf("copy allocated %d bytes for a %d-byte source (logical %d): holes were filled in",
			dstAlloc, srcAlloc, int64(size))
	}
}

func TestCopyFileAllHoles(t *testing.T) {
	const size = 8 << 20
	dir := t.TempDir()
	src := filepath.Join(dir, "empty")
	f, err := os.Create(src)
	if err != nil {
		t.Fatalf("creating src: %v", err)
	}
	if err := errors.Join(f.Truncate(size), f.Close()); err != nil {
		t.Fatalf("sizing src: %v", err)
	}

	dst := filepath.Join(dir, "copied")
	if _, err := copyFile(src, dst); err != nil {
		t.Fatalf("copyFile: %v", err)
	}
	st, err := os.Stat(dst)
	if err != nil {
		t.Fatalf("stat dst: %v", err)
	}
	if st.Size() != size {
		t.Errorf("copy is %d bytes, want %d", st.Size(), int64(size))
	}
}

// TestCopyFilePreservesHolesUserspace covers the fallback taken when the destination
// does not expose a descriptor, so copy_file_range is unavailable.
func TestCopyFilePreservesHolesUserspace(t *testing.T) {
	orig := createDestFile
	createDestFile = func(name string) (io.WriteCloser, error) {
		f, err := os.Create(name)
		if err != nil {
			return nil, err
		}
		return noFdFile{f: f}, nil
	}
	t.Cleanup(func() { createDestFile = orig })

	const size = 32 << 20
	dir := t.TempDir()
	src := filepath.Join(dir, "memory-ranges")
	f, err := os.Create(src)
	if err != nil {
		t.Fatalf("creating src: %v", err)
	}
	if err := f.Truncate(size); err != nil {
		t.Fatalf("sizing src: %v", err)
	}
	marker := bytes.Repeat([]byte{0xEF}, 4<<10)
	if _, err := f.WriteAt(marker, 8<<20); err != nil {
		t.Fatalf("writing marker: %v", err)
	}
	if err := errors.Join(f.Sync(), f.Close()); err != nil {
		t.Fatalf("flushing src: %v", err)
	}

	dst := filepath.Join(dir, "copied")
	if _, err := copyFile(src, dst); err != nil {
		t.Fatalf("copyFile: %v", err)
	}

	want, err := os.ReadFile(src)
	if err != nil {
		t.Fatalf("reading src: %v", err)
	}
	got, err := os.ReadFile(dst)
	if err != nil {
		t.Fatalf("reading dst: %v", err)
	}
	if !bytes.Equal(want, got) {
		t.Fatal("userspace copy differs from source")
	}

	srcAlloc, dstAlloc := allocatedBytes(t, src), allocatedBytes(t, dst)
	if srcAlloc >= size/2 {
		t.Skipf("source did not end up sparse (%d of %d bytes allocated)", srcAlloc, int64(size))
	}
	if dstAlloc > srcAlloc*4 {
		t.Errorf("userspace copy allocated %d bytes for a %d-byte source: holes were filled in",
			dstAlloc, srcAlloc)
	}
}

// recordingObjectStorage serves gets from and records puts into one map, so
// upload tests can assert exactly which objects landed.
type recordingObjectStorage struct {
	mu      sync.Mutex
	objects map[string][]byte
	putErr  error
}

func (r *recordingObjectStorage) GetObject(_ context.Context, bucket, object string) (io.ReadCloser, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	b, ok := r.objects[bucket+"/"+object]
	if !ok {
		return nil, fmt.Errorf("%w: Bucket:%q, Object:%q", ateerrors.ReasonFailedGetExternalObject, bucket, object)
	}
	return io.NopCloser(bytes.NewReader(b)), nil
}

func (r *recordingObjectStorage) PutObject(_ context.Context, bucket, object string, reader io.Reader) error {
	if r.putErr != nil {
		return r.putErr
	}
	b, err := io.ReadAll(reader)
	if err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.objects == nil {
		r.objects = map[string][]byte{}
	}
	r.objects[bucket+"/"+object] = b
	return nil
}

func (r *recordingObjectStorage) keys() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	keys := make([]string, 0, len(r.objects))
	for k := range r.objects {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

// writeLocalSnapshot lays a local pause snapshot out in dir: the given files
// plus the marshaled manifest beside them.
func writeLocalSnapshot(t *testing.T, dir string, rec sandboxAssetsRecord, contents map[string]string) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatalf("creating snapshot dir: %v", err)
	}
	for name, body := range contents {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600); err != nil {
			t.Fatalf("writing snapshot file %s: %v", name, err)
		}
	}
	manifest, err := json.Marshal(rec)
	if err != nil {
		t.Fatalf("marshaling manifest: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, sandboxManifestName), manifest, 0o600); err != nil {
		t.Fatalf("writing manifest: %v", err)
	}
}

func validUploadPausedCheckpointRequest() *ateletpb.UploadPausedCheckpointRequest {
	return &ateletpb.UploadPausedCheckpointRequest{
		Atespace:               "ate-demo",
		ActorName:              "counter-1",
		ActorUid:               "123e4567-e89b-12d3-a456-426614174000",
		ActorTemplateNamespace: "ate-demo",
		ActorTemplateName:      "counter",
		LocalSnapshotName:      "pause-snap-1",
		DestinationSnapshotUri: "gs://bucket/root/snapshots/ate-demo/snap-1",
		DesiredScope:           ateletpb.SnapshotScope_SNAPSHOT_SCOPE_FULL,
	}
}

func TestUploadLocalCheckpointDir(t *testing.T) {
	ctx := context.Background()
	uri, err := resources.ParseSnapshotURI("gs://bucket/root/snapshots/ate-demo/snap-1")
	if err != nil {
		t.Fatalf("ParseSnapshotURI: %v", err)
	}
	fullRec := func(class string) sandboxAssetsRecord {
		return sandboxAssetsRecord{
			SandboxClass:  class,
			PauseImage:    testPauseImage,
			SnapshotFiles: []string{"config.json", "memory-ranges", ateompath.DurableDirTarFile},
			Scope:         ateattr.SnapshotScopeFull,
		}
	}

	remoteManifest := func(t *testing.T, store *recordingObjectStorage) sandboxAssetsRecord {
		t.Helper()
		b, ok := store.objects["bucket/root/snapshots/ate-demo/snap-1/manifest.json"]
		if !ok {
			t.Fatal("no manifest uploaded")
		}
		rec, err := unmarshalSandboxRecord(b)
		if err != nil {
			t.Fatalf("parsing uploaded manifest: %v", err)
		}
		return *rec
	}

	t.Run("matching scope uploads all files", func(t *testing.T) {
		store := &recordingObjectStorage{}
		s := &AteomHerder{gcsClient: store}
		dir := filepath.Join(t.TempDir(), "pause-snap-1")
		writeLocalSnapshot(t, dir, fullRec("microvm"), map[string]string{
			"config.json": "cfg", "memory-ranges": "mem", ateompath.DurableDirTarFile: "data",
		})

		if _, err := s.uploadLocalCheckpointDir(ctx, validUploadPausedCheckpointRequest(), dir, uri); err != nil {
			t.Fatalf("uploadLocalCheckpointDir: %v", err)
		}
		want := []string{
			"bucket/root/snapshots/ate-demo/snap-1/config.json.zstd",
			"bucket/root/snapshots/ate-demo/snap-1/durable-dir.tar.zstd",
			"bucket/root/snapshots/ate-demo/snap-1/manifest.json",
			"bucket/root/snapshots/ate-demo/snap-1/memory-ranges.zstd",
		}
		if got := store.keys(); !slices.Equal(got, want) {
			t.Errorf("uploaded objects = %v, want %v", got, want)
		}
		if rec := remoteManifest(t, store); rec.Scope != ateattr.SnapshotScopeFull {
			t.Errorf("uploaded manifest scope = %q, want %q", rec.Scope, ateattr.SnapshotScopeFull)
		}
	})

	t.Run("microvm full capture uploads durable tar alone as data", func(t *testing.T) {
		store := &recordingObjectStorage{}
		s := &AteomHerder{gcsClient: store}
		dir := filepath.Join(t.TempDir(), "pause-snap-1")
		writeLocalSnapshot(t, dir, fullRec("microvm"), map[string]string{
			"config.json": "cfg", "memory-ranges": "mem", ateompath.DurableDirTarFile: "data",
		})

		req := validUploadPausedCheckpointRequest()
		req.DesiredScope = ateletpb.SnapshotScope_SNAPSHOT_SCOPE_DATA
		if _, err := s.uploadLocalCheckpointDir(ctx, req, dir, uri); err != nil {
			t.Fatalf("uploadLocalCheckpointDir: %v", err)
		}
		want := []string{
			"bucket/root/snapshots/ate-demo/snap-1/durable-dir.tar.zstd",
			"bucket/root/snapshots/ate-demo/snap-1/manifest.json",
		}
		if got := store.keys(); !slices.Equal(got, want) {
			t.Errorf("uploaded objects = %v, want %v", got, want)
		}
		rec := remoteManifest(t, store)
		if rec.Scope != ateattr.SnapshotScopeData {
			t.Errorf("uploaded manifest scope = %q, want %q", rec.Scope, ateattr.SnapshotScopeData)
		}
		if want := []string{ateompath.DurableDirTarFile}; !slices.Equal(rec.SnapshotFiles, want) {
			t.Errorf("uploaded manifest files = %v, want %v", rec.SnapshotFiles, want)
		}
	})

	t.Run("gvisor full capture cannot become data yet", func(t *testing.T) {
		s := &AteomHerder{gcsClient: &recordingObjectStorage{}}
		dir := filepath.Join(t.TempDir(), "pause-snap-1")
		writeLocalSnapshot(t, dir, sandboxAssetsRecord{
			SandboxClass:  "gvisor",
			PauseImage:    testPauseImage,
			SnapshotFiles: []string{"checkpoint.img"},
			Scope:         ateattr.SnapshotScopeFull,
		}, map[string]string{"checkpoint.img": "img"})

		req := validUploadPausedCheckpointRequest()
		req.DesiredScope = ateletpb.SnapshotScope_SNAPSHOT_SCOPE_DATA
		_, err := s.uploadLocalCheckpointDir(ctx, req, dir, uri)
		if got := status.Code(err); got != codes.Unimplemented {
			t.Fatalf("status.Code = %v (err %v), want Unimplemented", got, err)
		}
	})

	t.Run("microvm full capture without durable tar has no data", func(t *testing.T) {
		s := &AteomHerder{gcsClient: &recordingObjectStorage{}}
		dir := filepath.Join(t.TempDir(), "pause-snap-1")
		writeLocalSnapshot(t, dir, sandboxAssetsRecord{
			SandboxClass:  "microvm",
			PauseImage:    testPauseImage,
			SnapshotFiles: []string{"config.json", "memory-ranges"},
			Scope:         ateattr.SnapshotScopeFull,
		}, map[string]string{"config.json": "cfg", "memory-ranges": "mem"})

		req := validUploadPausedCheckpointRequest()
		req.DesiredScope = ateletpb.SnapshotScope_SNAPSHOT_SCOPE_DATA
		_, err := s.uploadLocalCheckpointDir(ctx, req, dir, uri)
		if got := status.Code(err); got != codes.FailedPrecondition {
			t.Fatalf("status.Code = %v (err %v), want FailedPrecondition", got, err)
		}
	})

	t.Run("unknown sandbox class cannot convert", func(t *testing.T) {
		s := &AteomHerder{gcsClient: &recordingObjectStorage{}}
		dir := filepath.Join(t.TempDir(), "pause-snap-1")
		writeLocalSnapshot(t, dir, sandboxAssetsRecord{
			SandboxClass:  "mystery",
			PauseImage:    testPauseImage,
			SnapshotFiles: []string{ateompath.DurableDirTarFile},
			Scope:         ateattr.SnapshotScopeFull,
		}, map[string]string{ateompath.DurableDirTarFile: "data"})

		req := validUploadPausedCheckpointRequest()
		req.DesiredScope = ateletpb.SnapshotScope_SNAPSHOT_SCOPE_DATA
		_, err := s.uploadLocalCheckpointDir(ctx, req, dir, uri)
		if got := status.Code(err); got != codes.FailedPrecondition {
			t.Fatalf("status.Code = %v (err %v), want FailedPrecondition", got, err)
		}
	})

	t.Run("data capture cannot become full", func(t *testing.T) {
		s := &AteomHerder{gcsClient: &recordingObjectStorage{}}
		dir := filepath.Join(t.TempDir(), "pause-snap-1")
		writeLocalSnapshot(t, dir, sandboxAssetsRecord{
			SandboxClass:  "microvm",
			PauseImage:    testPauseImage,
			SnapshotFiles: []string{ateompath.DurableDirTarFile},
			Scope:         ateattr.SnapshotScopeData,
		}, map[string]string{ateompath.DurableDirTarFile: "data"})

		_, err := s.uploadLocalCheckpointDir(ctx, validUploadPausedCheckpointRequest(), dir, uri)
		if got := status.Code(err); got != codes.FailedPrecondition {
			t.Fatalf("status.Code = %v (err %v), want FailedPrecondition", got, err)
		}
	})

	t.Run("manifest without scope is rejected", func(t *testing.T) {
		store := &recordingObjectStorage{}
		s := &AteomHerder{gcsClient: store}
		dir := filepath.Join(t.TempDir(), "pause-snap-1")
		writeLocalSnapshot(t, dir, sandboxAssetsRecord{
			SandboxClass:  "microvm",
			PauseImage:    testPauseImage,
			SnapshotFiles: []string{ateompath.DurableDirTarFile},
		}, map[string]string{ateompath.DurableDirTarFile: "data"})

		req := validUploadPausedCheckpointRequest()
		req.DesiredScope = ateletpb.SnapshotScope_SNAPSHOT_SCOPE_DATA
		_, err := s.uploadLocalCheckpointDir(ctx, req, dir, uri)
		if got := status.Code(err); got != codes.FailedPrecondition {
			t.Fatalf("status.Code = %v (err %v), want FailedPrecondition for a scope-less manifest", got, err)
		}
		if ateerrors.ActorCrashRequested(err) {
			t.Error("scope-less manifest requests an actor crash; the actor is still resumable")
		}
		if len(store.keys()) != 0 {
			t.Errorf("objects uploaded despite rejection: %v", store.keys())
		}
	})

	t.Run("gone locally but already uploaded succeeds", func(t *testing.T) {
		store := &recordingObjectStorage{objects: map[string][]byte{
			"bucket/root/snapshots/ate-demo/snap-1/manifest.json": []byte(`{"sandboxClass":"microvm"}`),
		}}
		s := &AteomHerder{gcsClient: store}

		if _, err := s.uploadLocalCheckpointDir(ctx, validUploadPausedCheckpointRequest(), filepath.Join(t.TempDir(), "never-created"), uri); err != nil {
			t.Fatalf("uploadLocalCheckpointDir: %v", err)
		}
	})

	t.Run("gone locally and remotely crashes the actor", func(t *testing.T) {
		s := &AteomHerder{gcsClient: &recordingObjectStorage{}}

		_, err := s.uploadLocalCheckpointDir(ctx, validUploadPausedCheckpointRequest(), filepath.Join(t.TempDir(), "never-created"), uri)
		if got := status.Code(err); got != codes.DataLoss {
			t.Fatalf("status.Code = %v (err %v), want DataLoss", got, err)
		}
		if !ateerrors.ActorCrashRequested(err) {
			t.Error("error does not request an actor crash")
		}
		if got := ateerrors.ExtractReason(err); got != string(ateerrors.ReasonLocalSnapshotGone) {
			t.Errorf("reason = %q, want %q", got, ateerrors.ReasonLocalSnapshotGone)
		}
	})

	t.Run("upload failure is a plain retryable error", func(t *testing.T) {
		s := &AteomHerder{gcsClient: &recordingObjectStorage{putErr: errors.New("boom")}}
		dir := filepath.Join(t.TempDir(), "pause-snap-1")
		writeLocalSnapshot(t, dir, fullRec("microvm"), map[string]string{
			"config.json": "cfg", "memory-ranges": "mem", ateompath.DurableDirTarFile: "data",
		})

		_, err := s.uploadLocalCheckpointDir(ctx, validUploadPausedCheckpointRequest(), dir, uri)
		if err == nil {
			t.Fatal("uploadLocalCheckpointDir succeeded, want error")
		}
		if ateerrors.ActorCrashRequested(err) {
			t.Error("upload failure requests an actor crash; must stay retryable")
		}
	})
}

func TestValidateUploadPausedCheckpointRequest(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*ateletpb.UploadPausedCheckpointRequest)
		wantErr bool
	}{
		{"valid", func(*ateletpb.UploadPausedCheckpointRequest) {}, false},
		{"valid data scope", func(r *ateletpb.UploadPausedCheckpointRequest) {
			r.DesiredScope = ateletpb.SnapshotScope_SNAPSHOT_SCOPE_DATA
		}, false},
		{"invalid atespace", func(r *ateletpb.UploadPausedCheckpointRequest) { r.Atespace = "../escape" }, true},
		{"golden atespace rejected", func(r *ateletpb.UploadPausedCheckpointRequest) { r.Atespace = resources.GoldenActorAtespace }, true},
		{"invalid actor name", func(r *ateletpb.UploadPausedCheckpointRequest) { r.ActorName = "UPPER" }, true},
		{"invalid actor uid", func(r *ateletpb.UploadPausedCheckpointRequest) { r.ActorUid = "" }, true},
		{"any actor template identity accepted", func(r *ateletpb.UploadPausedCheckpointRequest) { r.ActorTemplateNamespace = "no/slashes" }, false},
		{"empty actor template identity accepted", func(r *ateletpb.UploadPausedCheckpointRequest) {
			r.ActorTemplateNamespace, r.ActorTemplateName = "", ""
		}, false},
		{"invalid snapshot name", func(r *ateletpb.UploadPausedCheckpointRequest) { r.LocalSnapshotName = "../escape" }, true},
		{"invalid snapshot uri", func(r *ateletpb.UploadPausedCheckpointRequest) { r.DestinationSnapshotUri = "not-a-uri" }, true},
		{"unspecified scope", func(r *ateletpb.UploadPausedCheckpointRequest) {
			r.DesiredScope = ateletpb.SnapshotScope_SNAPSHOT_SCOPE_UNSPECIFIED
		}, true},
		{"data-on-golden scope", func(r *ateletpb.UploadPausedCheckpointRequest) {
			r.DesiredScope = ateletpb.SnapshotScope_SNAPSHOT_SCOPE_DATA_ON_GOLDEN
		}, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := validUploadPausedCheckpointRequest()
			tc.mutate(req)
			if err := validateUploadPausedCheckpointRequest(req); (err != nil) != tc.wantErr {
				t.Errorf("validateUploadPausedCheckpointRequest err = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

func TestShouldHaveSnapshots(t *testing.T) {
	tests := []struct {
		name string
		req  *ateletpb.CheckpointRequest
		want bool
	}{
		{
			name: "full scope always expects snapshots",
			req: &ateletpb.CheckpointRequest{
				Scope: ateletpb.SnapshotScope_SNAPSHOT_SCOPE_FULL,
			},
			want: true,
		},
		{
			name: "data scope with durable volumes expects snapshots",
			req: &ateletpb.CheckpointRequest{
				Scope: ateletpb.SnapshotScope_SNAPSHOT_SCOPE_DATA,
				Spec: &ateletpb.WorkloadSpec{
					Volumes: []*ateletpb.Volume{
						{Name: "durable", Source: &ateletpb.Volume_DurableDir{DurableDir: &ateletpb.DurableDirVolume{}}},
					},
				},
			},
			want: true,
		},
		{
			name: "data scope with only CSI volumes does not expect snapshots",
			req: &ateletpb.CheckpointRequest{
				Scope: ateletpb.SnapshotScope_SNAPSHOT_SCOPE_DATA,
				Spec: &ateletpb.WorkloadSpec{
					Volumes: []*ateletpb.Volume{
						{Name: "csi", Source: &ateletpb.Volume_External{External: &ateletpb.ExternalVolumeSource{}}},
					},
				},
			},
			want: false,
		},
		{
			name: "data scope with both durable and CSI volumes expects snapshots",
			req: &ateletpb.CheckpointRequest{
				Scope: ateletpb.SnapshotScope_SNAPSHOT_SCOPE_DATA,
				Spec: &ateletpb.WorkloadSpec{
					Volumes: []*ateletpb.Volume{
						{Name: "durable", Source: &ateletpb.Volume_DurableDir{DurableDir: &ateletpb.DurableDirVolume{}}},
						{Name: "csi", Source: &ateletpb.Volume_External{External: &ateletpb.ExternalVolumeSource{}}},
					},
				},
			},
			want: true,
		},
		{
			name: "data scope with no volumes does not expect snapshots",
			req: &ateletpb.CheckpointRequest{
				Scope: ateletpb.SnapshotScope_SNAPSHOT_SCOPE_DATA,
				Spec:  &ateletpb.WorkloadSpec{},
			},
			want: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := shouldHaveSnapshots(tc.req); got != tc.want {
				t.Errorf("shouldHaveSnapshots() = %v, want %v", got, tc.want)
			}
		})
	}
}
