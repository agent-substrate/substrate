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
	"encoding/pem"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/internal/resources"
	certsv1beta1 "k8s.io/api/certificates/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	certlisters "k8s.io/client-go/listers/certificates/v1beta1"
	"k8s.io/client-go/tools/cache"
)

// ctbStore is a mutable stand-in for the informer cache: tests rotate the
// backing ClusterTrustBundle by replacing it in the indexer, the way watch
// events replace it in production.
type ctbStore struct {
	indexer cache.Indexer
	lister  certlisters.ClusterTrustBundleLister
}

func newCTBStore(t *testing.T) *ctbStore {
	t.Helper()
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	return &ctbStore{indexer: indexer, lister: certlisters.NewClusterTrustBundleLister(indexer)}
}

func (s *ctbStore) object(raw string) *certsv1beta1.ClusterTrustBundle {
	return &certsv1beta1.ClusterTrustBundle{
		ObjectMeta: metav1.ObjectMeta{Name: egressTrustBundleObjectName},
		Spec:       certsv1beta1.ClusterTrustBundleSpec{TrustBundle: raw},
	}
}

func (s *ctbStore) set(t *testing.T, raw string) *certsv1beta1.ClusterTrustBundle {
	t.Helper()
	obj := s.object(raw)
	if err := s.indexer.Add(obj); err != nil {
		t.Fatal(err)
	}
	return obj
}

func (s *ctbStore) remove(t *testing.T) {
	t.Helper()
	if err := s.indexer.Delete(s.object("")); err != nil {
		t.Fatal(err)
	}
}

func trustVolumeSpec(relPath string) *ateletpb.SystemInfoVolume {
	return &ateletpb.SystemInfoVolume{
		DataSources: []*ateletpb.SystemInfoDataSource{
			{DataSource: &ateletpb.SystemInfoDataSource_TrustBundle{
				TrustBundle: &ateletpb.TrustBundleDataSource{Name: EgressTrustBundleName, Path: relPath},
			}},
		},
	}
}

func metadataVolumeSpec() *ateletpb.SystemInfoVolume {
	return &ateletpb.SystemInfoVolume{
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
}

// registerTrustVolume registers actorUID with one volume projecting the
// egress bundle at trust/ca.pem, rooted under a temp actors dir mirroring
// ateompath's <actorsDir>/<uid>/system-info/<volume> shape.
func registerTrustVolume(t *testing.T, r *systemInfoVolumeRefresher, dir, actorUID string) {
	t.Helper()
	vol := systemInfoVolume{
		Name: "trust",
		Root: filepath.Join(dir, actorUID, "system-info", "trust"),
		Spec: trustVolumeSpec("ca.pem"),
	}
	if err := r.Register(actorUID, resources.ActorRef{Atespace: "team-a", Name: actorUID}, []systemInfoVolume{vol}); err != nil {
		t.Fatalf("Register(%s): %v", actorUID, err)
	}
}

func readProjected(t *testing.T, dir, actorUID, volume, relPath string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, actorUID, "system-info", volume, relPath))
	if err != nil {
		t.Fatalf("reading projected file: %v", err)
	}
	return string(b)
}

func TestSystemInfoVolumeRefresher_RefreshesRunningActorsOnChange(t *testing.T) {
	ctx := context.Background()
	certA, certB := string(testCertPEM(t)), string(testCertPEM(t))
	store := newCTBStore(t)
	store.set(t, certA)
	r := newSystemInfoVolumeRefresher(store.lister)
	dir := t.TempDir()

	// Two running actors project the same bundle; a rotation must rewrite
	// both files.
	for _, uid := range []string{"uid-1", "uid-2"} {
		registerTrustVolume(t, r, dir, uid)
		if got := readProjected(t, dir, uid, "trust", "ca.pem"); got != certA {
			t.Fatalf("actor %s: projected file = %q, want the initial bundle", uid, got)
		}
	}

	store.set(t, certB)
	if err := r.refreshBundle(ctx, EgressTrustBundleName); err != nil {
		t.Fatalf("refreshBundle: %v", err)
	}
	for _, uid := range []string{"uid-1", "uid-2"} {
		if got := readProjected(t, dir, uid, "trust", "ca.pem"); got != certB {
			t.Errorf("actor %s: projected file = %q, want the rotated bundle", uid, got)
		}
	}

	// A replayed event for unchanged contents (relist after a watch
	// reconnect, the daily resync) must not rewrite the files: a no-op
	// rewrite still replaces the inode a suspended guest may need to re-bind.
	// The sentinel write stands in for inode identity.
	sentinelPath := filepath.Join(dir, "uid-1", "system-info", "trust", "ca.pem")
	if err := os.WriteFile(sentinelPath, []byte("sentinel"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := r.refreshBundle(ctx, EgressTrustBundleName); err != nil {
		t.Fatalf("refreshBundle (replay): %v", err)
	}
	if got := readProjected(t, dir, "uid-1", "trust", "ca.pem"); got != "sentinel" {
		t.Errorf("projected file rewritten on a no-change event (got %q)", got)
	}
}

func TestSystemInfoVolumeRefresher_KeepsLastGoodOnFailure(t *testing.T) {
	ctx := context.Background()
	certA, certB := string(testCertPEM(t)), string(testCertPEM(t))
	store := newCTBStore(t)
	store.set(t, certA)
	r := newSystemInfoVolumeRefresher(store.lister)
	dir := t.TempDir()
	registerTrustVolume(t, r, dir, "uid-1")

	t.Run("backing object deleted", func(t *testing.T) {
		store.remove(t)
		if err := r.refreshBundle(ctx, EgressTrustBundleName); err != nil {
			t.Errorf("refreshBundle = %v, want nil: resolution failures wait for the next event, not the queue", err)
		}
		if got := readProjected(t, dir, "uid-1", "trust", "ca.pem"); got != certA {
			t.Errorf("projected file = %q, want the last good bundle to survive deletion", got)
		}
	})

	t.Run("backing object unusable", func(t *testing.T) {
		store.set(t, "no certificates here")
		r.refreshBundle(ctx, EgressTrustBundleName)
		if got := readProjected(t, dir, "uid-1", "trust", "ca.pem"); got != certA {
			t.Errorf("projected file = %q, want the last good bundle to survive junk contents", got)
		}
	})

	t.Run("recovers when the bundle heals", func(t *testing.T) {
		store.set(t, certB)
		if err := r.refreshBundle(ctx, EgressTrustBundleName); err != nil {
			t.Fatalf("refreshBundle: %v", err)
		}
		if got := readProjected(t, dir, "uid-1", "trust", "ca.pem"); got != certB {
			t.Errorf("projected file = %q, want the healed bundle", got)
		}
	})
}

func TestSystemInfoVolumeRefresher_Deregister(t *testing.T) {
	ctx := context.Background()
	certA, certB := string(testCertPEM(t)), string(testCertPEM(t))
	store := newCTBStore(t)
	store.set(t, certA)
	r := newSystemInfoVolumeRefresher(store.lister)
	dir := t.TempDir()
	registerTrustVolume(t, r, dir, "uid-1")

	r.Deregister("uid-1")
	store.set(t, certB)
	r.refreshBundle(ctx, EgressTrustBundleName)
	if got := readProjected(t, dir, "uid-1", "trust", "ca.pem"); got != certA {
		t.Errorf("projected file = %q, refreshed after deregistration", got)
	}
}

func TestSystemInfoVolumeRefresher_RegisterEmptyClearsStaleRegistration(t *testing.T) {
	ctx := context.Background()
	certA, certB := string(testCertPEM(t)), string(testCertPEM(t))
	store := newCTBStore(t)
	store.set(t, certA)
	r := newSystemInfoVolumeRefresher(store.lister)
	dir := t.TempDir()
	registerTrustVolume(t, r, dir, "uid-1")

	// The actor comes back under a spec with no system-info volumes.
	if err := r.Register("uid-1", resources.ActorRef{Atespace: "team-a", Name: "uid-1"}, nil); err != nil {
		t.Fatalf("Register(empty): %v", err)
	}
	store.set(t, certB)
	r.refreshBundle(ctx, EgressTrustBundleName)
	if got := readProjected(t, dir, "uid-1", "trust", "ca.pem"); got != certA {
		t.Errorf("projected file = %q, refreshed after an empty registration", got)
	}
}

// TestSystemInfoVolumeRefresher_RegisterRewritesFromCurrentState pins the
// no-persistence restart story: a fresh refresher (as after an atelet
// restart) knows nothing of past writes, and the next Run/Restore's Register
// applies any rotation missed in between.
func TestSystemInfoVolumeRefresher_RegisterRewritesFromCurrentState(t *testing.T) {
	certA, certB := string(testCertPEM(t)), string(testCertPEM(t))
	store := newCTBStore(t)
	store.set(t, certA)
	dir := t.TempDir()
	registerTrustVolume(t, newSystemInfoVolumeRefresher(store.lister), dir, "uid-1")

	store.set(t, certB)
	registerTrustVolume(t, newSystemInfoVolumeRefresher(store.lister), dir, "uid-1")
	if got := readProjected(t, dir, "uid-1", "trust", "ca.pem"); got != certB {
		t.Errorf("projected file = %q, want the rotation missed while down applied at registration", got)
	}
}

func TestSystemInfoVolumeRefresher_WriteFailureIsolatedAndRetried(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("permission-based write-failure injection needs non-root")
	}
	ctx := context.Background()
	certA, certB := string(testCertPEM(t)), string(testCertPEM(t))
	store := newCTBStore(t)
	store.set(t, certA)
	r := newSystemInfoVolumeRefresher(store.lister)
	dir := t.TempDir()
	for _, uid := range []string{"uid-1", "uid-2"} {
		registerTrustVolume(t, r, dir, uid)
	}

	// One actor's volume root refuses writes; the rotation must still reach
	// the other actor, and the error must surface for the queue to redrive.
	blocked := filepath.Join(dir, "uid-1", "system-info", "trust")
	if err := os.Chmod(blocked, 0o555); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(blocked, 0o755) })
	store.set(t, certB)
	if err := r.refreshBundle(ctx, EgressTrustBundleName); err == nil {
		t.Error("refreshBundle = nil, want the write failure surfaced for requeue")
	}
	if got := readProjected(t, dir, "uid-2", "trust", "ca.pem"); got != certB {
		t.Errorf("healthy actor's file = %q, want the rotation despite the sibling's write failure", got)
	}
	if got := readProjected(t, dir, "uid-1", "trust", "ca.pem"); got != certA {
		t.Errorf("blocked actor's file = %q, want last good contents", got)
	}

	// The failed write left the applied hash stale, so the queue's redrive
	// retries exactly it.
	if err := os.Chmod(blocked, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := r.refreshBundle(ctx, EgressTrustBundleName); err != nil {
		t.Fatalf("refreshBundle (retry): %v", err)
	}
	if got := readProjected(t, dir, "uid-1", "trust", "ca.pem"); got != certB {
		t.Errorf("blocked actor's file = %q after retry, want the rotation", got)
	}
}

// TestSystemInfoVolumeRefresher_EventPipelineRetriesFailedWrites drives the
// informer-to-workqueue path end to end: events only enqueue, the run loop
// writes, and a failed write requeues with backoff until it lands.
func TestSystemInfoVolumeRefresher_EventPipelineRetriesFailedWrites(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("permission-based write-failure injection needs non-root")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	certA, certB := string(testCertPEM(t)), string(testCertPEM(t))
	store := newCTBStore(t)
	store.set(t, certA)
	r := newSystemInfoVolumeRefresher(store.lister)
	dir := t.TempDir()
	registerTrustVolume(t, r, dir, "uid-1")

	blocked := filepath.Join(dir, "uid-1", "system-info", "trust")
	if err := os.Chmod(blocked, 0o555); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(blocked, 0o755) })

	done := make(chan struct{})
	go func() { defer close(done); r.run(ctx) }()
	t.Cleanup(func() { cancel(); <-done })

	r.eventHandler().OnUpdate(store.object(certA), store.set(t, certB))

	// Attempts against the read-only directory keep last-good contents; once
	// it heals, a rate-limited retry must deliver the rotation with no
	// further event.
	time.Sleep(50 * time.Millisecond)
	if got := readProjected(t, dir, "uid-1", "trust", "ca.pem"); got != certA {
		t.Fatalf("projected file = %q, want last-good contents while writes fail", got)
	}
	if err := os.Chmod(blocked, 0o755); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for readProjected(t, dir, "uid-1", "trust", "ca.pem") != certB {
		if time.Now().After(deadline) {
			t.Fatal("rotation never applied by the retry loop")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestSystemInfoVolumeRefresher_RotationLeavesUnchangedFilesAlone pins the
// identical-contents skip in write: a rotation rewrites the bundle file but
// not the volume's metadata files, whose inodes suspended guests may need to
// re-bind. mtime stands in for inode identity.
func TestSystemInfoVolumeRefresher_RotationLeavesUnchangedFilesAlone(t *testing.T) {
	ctx := context.Background()
	certA, certB := string(testCertPEM(t)), string(testCertPEM(t))
	store := newCTBStore(t)
	store.set(t, certA)
	r := newSystemInfoVolumeRefresher(store.lister)
	root := filepath.Join(t.TempDir(), "system-info", "vol1")
	spec := &ateletpb.SystemInfoVolume{
		DataSources: append(metadataVolumeSpec().GetDataSources(), trustVolumeSpec("trust/ca.pem").GetDataSources()...),
	}
	if err := r.Register("uid-1", resources.ActorRef{Atespace: "team-a", Name: "actor-1"}, []systemInfoVolume{{Name: "vol1", Root: root, Spec: spec}}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	metadataPath := filepath.Join(root, "actor-name")
	old := time.Now().Add(-time.Hour)
	if err := os.Chtimes(metadataPath, old, old); err != nil {
		t.Fatal(err)
	}
	store.set(t, certB)
	if err := r.refreshBundle(ctx, EgressTrustBundleName); err != nil {
		t.Fatalf("refreshBundle: %v", err)
	}
	if got, err := os.ReadFile(filepath.Join(root, "trust", "ca.pem")); err != nil || string(got) != certB {
		t.Errorf("bundle file = %q (err %v), want the rotation", got, err)
	}
	info, err := os.Stat(metadataPath)
	if err != nil {
		t.Fatal(err)
	}
	if !info.ModTime().Equal(old) {
		t.Errorf("metadata file rewritten by a bundle rotation although its contents never changed")
	}
}

func TestSystemInfoVolumeRegister_WritesActorMetadata(t *testing.T) {
	root := filepath.Join(t.TempDir(), "system-info", "vol1")
	r := newSystemInfoVolumeRefresher(nil)
	vol := func() systemInfoVolume {
		return systemInfoVolume{Name: "vol1", Root: root, Spec: metadataVolumeSpec()}
	}

	golden := resources.ActorRef{Atespace: "ate-e2e-probe", Name: "golden-actor"}
	if err := r.Register("uid-golden", golden, []systemInfoVolume{vol()}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	// Overwrite with a different actor, as happens when a snapshot taken from
	// one actor seeds another on resume: files must carry the new values.
	alpha := resources.ActorRef{Atespace: "ate-e2e-probe", Name: "probe-alpha"}
	if err := r.Register("uid-alpha", alpha, []systemInfoVolume{vol()}); err != nil {
		t.Fatalf("Register (rewrite): %v", err)
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

// TestSystemInfoVolumeRegister_StableRealPaths pins the path-stability
// contract the restore paths depend on: the micro-VM virtiofsds run in
// find-paths migration mode, which re-binds the guest's FUSE state to files
// by the paths recorded at suspend, and gVisor's gofer likewise re-opens
// files by path on restore. Projected files must therefore be plain files at
// stable real paths — no symlink indirection — and regenerating the volume
// must not move or delete a path that guest state may reference.
func TestSystemInfoVolumeRegister_StableRealPaths(t *testing.T) {
	root := filepath.Join(t.TempDir(), "system-info", "vol1")
	r := newSystemInfoVolumeRefresher(nil)
	vol := func() systemInfoVolume {
		return systemInfoVolume{Name: "vol1", Root: root, Spec: metadataVolumeSpec()}
	}

	golden := resources.ActorRef{Atespace: "ate-e2e-probe", Name: "golden-actor"}
	if err := r.Register("uid-golden", golden, []systemInfoVolume{vol()}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	paths := []string{"actor-name", "atespace", "identity/actor-uid"}
	realBefore := map[string]string{}
	for _, p := range paths {
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
	if err := r.Register("uid-alpha", alpha, []systemInfoVolume{vol()}); err != nil {
		t.Fatalf("Register (rewrite): %v", err)
	}

	for _, p := range paths {
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

func TestSystemInfoVolumeRegister_TrustBundle(t *testing.T) {
	certPEM := testCertPEM(t)
	// Junk around the certificate proves kubelet-style sanitization: only the
	// CERTIFICATE block survives, and the duplicate is dropped.
	junk := "garbage\n" + string(pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: []byte("x")}))
	store := newCTBStore(t)
	store.set(t, junk+string(certPEM)+string(certPEM))
	dir := t.TempDir()
	registerTrustVolume(t, newSystemInfoVolumeRefresher(store.lister), dir, "uid-1")
	if got := readProjected(t, dir, "uid-1", "trust", "ca.pem"); got != string(certPEM) {
		t.Errorf("content = %q, want the sanitized bundle", got)
	}

	t.Run("resolution failure fails the start rather than produce an empty trust file", func(t *testing.T) {
		r := newSystemInfoVolumeRefresher(ctbLister(t))
		vol := systemInfoVolume{Name: "trust", Root: filepath.Join(t.TempDir(), "trust"), Spec: trustVolumeSpec("ca.pem")}
		err := r.Register("uid-2", resources.ActorRef{Atespace: "team-a", Name: "actor-2"}, []systemInfoVolume{vol})
		if err == nil || !strings.Contains(err.Error(), "not found") || !strings.Contains(err.Error(), `"trust"`) {
			t.Errorf("Register = %v, want not-found error naming the volume", err)
		}
	})
}
