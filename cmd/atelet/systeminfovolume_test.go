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
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/cmd/atelet/internal/ateletpath"
	"github.com/agent-substrate/substrate/cmd/atelet/internal/trustbundle"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/internal/resources"
	certsv1 "k8s.io/api/certificates/v1"
	certsv1beta1 "k8s.io/api/certificates/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	certlisters "k8s.io/client-go/listers/certificates/v1"
	"k8s.io/client-go/tools/cache"
)

// testCertPEM mints a throwaway self-signed certificate, PEM-encoded.
func testCertPEM(t *testing.T) []byte {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.CreateCertificate(rand.Reader, &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "test"},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(time.Hour),
	}, &x509.Certificate{SerialNumber: big.NewInt(1)}, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func ctbLister(t *testing.T, bundles ...*certsv1beta1.ClusterTrustBundle) certlisters.ClusterTrustBundleLister {
	t.Helper()
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	for _, b := range bundles {
		if err := indexer.Add(b); err != nil {
			t.Fatal(err)
		}
	}
	return certlisters.NewClusterTrustBundleLister(indexer)
}

// ctbStore stands in for the informer cache; set replaces the backing
// ClusterTrustBundle the way a watch event would.
type ctbStore struct {
	indexer cache.Indexer
	lister  certlisters.ClusterTrustBundleLister
}

func newCTBStore(t *testing.T) *ctbStore {
	t.Helper()
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	return &ctbStore{indexer: indexer, lister: certlisters.NewClusterTrustBundleLister(indexer)}
}

func (s *ctbStore) object(raw string) *certsv1.ClusterTrustBundle {
	return &certsv1.ClusterTrustBundle{
		ObjectMeta: metav1.ObjectMeta{Name: trustbundle.EgressCTB},
		Spec:       certsv1.ClusterTrustBundleSpec{TrustBundle: raw},
	}
}

func (s *ctbStore) set(t *testing.T, raw string) *certsv1.ClusterTrustBundle {
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
			{TrustBundle: &ateletpb.TrustBundleDataSource{Names: []string{trustbundle.EgressName}, Path: relPath}},
		},
	}
}

func metadataVolumeSpec() *ateletpb.SystemInfoVolume {
	return &ateletpb.SystemInfoVolume{
		DataSources: []*ateletpb.SystemInfoDataSource{
			{ActorMetadata: &ateletpb.ActorMetadataDataSource{
				Items: []*ateletpb.ActorMetadataItem{
					{Field: ateletpb.ActorMetadataField_ACTOR_METADATA_FIELD_NAME, Path: "actor-name"},
					{Field: ateletpb.ActorMetadataField_ACTOR_METADATA_FIELD_ATESPACE, Path: "atespace"},
					{Field: ateletpb.ActorMetadataField_ACTOR_METADATA_FIELD_UID, Path: "identity/actor-uid"},
				},
			}},
		},
	}
}

// registerTrustVolume registers actorUID with one volume projecting the
// egress bundle at <dir>/<uid>/system-info/trust/ca.pem.
func registerTrustVolume(t *testing.T, r *systemInfoVolumeRefresher, dir, actorUID string) {
	t.Helper()
	vol := &systemInfoVolume{
		Name: "trust",
		Root: filepath.Join(dir, actorUID, "system-info", "trust"),
		Spec: trustVolumeSpec("ca.pem"),
	}
	if _, err := r.Register(actorUID, resources.ActorRef{Atespace: "team-a", Name: actorUID}, []*systemInfoVolume{vol}); err != nil {
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
	r := newSystemInfoVolumeRefresher(trustbundle.NewSource(store.lister.Get, nil), nil)
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
	if err := r.refreshBundle(ctx, trustbundle.EgressName); err != nil {
		t.Fatalf("refreshBundle: %v", err)
	}
	for _, uid := range []string{"uid-1", "uid-2"} {
		if got := readProjected(t, dir, uid, "trust", "ca.pem"); got != certB {
			t.Errorf("actor %s: projected file = %q, want the rotated bundle", uid, got)
		}
	}

	// A replayed event for unchanged contents (relist, resync) must not
	// rewrite the file, since a rewrite replaces the inode.
	projectedPath := filepath.Join(dir, "uid-1", "system-info", "trust", "ca.pem")
	before, err := os.Stat(projectedPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.refreshBundle(ctx, trustbundle.EgressName); err != nil {
		t.Fatalf("refreshBundle (replay): %v", err)
	}
	after, err := os.Stat(projectedPath)
	if err != nil {
		t.Fatal(err)
	}
	if !os.SameFile(before, after) {
		t.Error("projected file replaced on a no-change event")
	}
}

func TestSystemInfoVolumeRefresher_KeepsLastGoodOnFailure(t *testing.T) {
	ctx := context.Background()
	certA, certB := string(testCertPEM(t)), string(testCertPEM(t))
	store := newCTBStore(t)
	store.set(t, certA)
	r := newSystemInfoVolumeRefresher(trustbundle.NewSource(store.lister.Get, nil), nil)
	dir := t.TempDir()
	registerTrustVolume(t, r, dir, "uid-1")

	t.Run("backing object deleted", func(t *testing.T) {
		store.remove(t)
		if err := r.refreshBundle(ctx, trustbundle.EgressName); err != nil {
			t.Errorf("refreshBundle = %v, want nil: resolution failures wait for the next event, not the queue", err)
		}
		if got := readProjected(t, dir, "uid-1", "trust", "ca.pem"); got != certA {
			t.Errorf("projected file = %q, want the last good bundle to survive deletion", got)
		}
	})

	t.Run("backing object unusable", func(t *testing.T) {
		store.set(t, "no certificates here")
		r.refreshBundle(ctx, trustbundle.EgressName)
		if got := readProjected(t, dir, "uid-1", "trust", "ca.pem"); got != certA {
			t.Errorf("projected file = %q, want the last good bundle to survive junk contents", got)
		}
	})

	t.Run("recovers when the bundle heals", func(t *testing.T) {
		store.set(t, certB)
		if err := r.refreshBundle(ctx, trustbundle.EgressName); err != nil {
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
	r := newSystemInfoVolumeRefresher(trustbundle.NewSource(store.lister.Get, nil), nil)
	dir := t.TempDir()
	registerTrustVolume(t, r, dir, "uid-1")

	r.Deregister("uid-1")
	store.set(t, certB)
	r.refreshBundle(ctx, trustbundle.EgressName)
	if got := readProjected(t, dir, "uid-1", "trust", "ca.pem"); got != certA {
		t.Errorf("projected file = %q, refreshed after deregistration", got)
	}
}

func TestSystemInfoVolumeRefresher_RegisterEmptyStopsRefreshing(t *testing.T) {
	ctx := context.Background()
	certA, certB := string(testCertPEM(t)), string(testCertPEM(t))
	store := newCTBStore(t)
	store.set(t, certA)
	r := newSystemInfoVolumeRefresher(trustbundle.NewSource(store.lister.Get, nil), nil)
	dir := t.TempDir()
	registerTrustVolume(t, r, dir, "uid-1")

	// The actor comes back under a spec with no system-info volumes: it stays
	// tracked, but its former volumes stop refreshing.
	r.Deregister("uid-1")
	if _, err := r.Register("uid-1", resources.ActorRef{Atespace: "team-a", Name: "uid-1"}, nil); err != nil {
		t.Fatalf("Register(empty): %v", err)
	}
	if r.actors["uid-1"] == nil {
		t.Error("actor dropped from the registry by a volume-less registration; every running actor stays tracked")
	}
	store.set(t, certB)
	r.refreshBundle(ctx, trustbundle.EgressName)
	if got := readProjected(t, dir, "uid-1", "trust", "ca.pem"); got != certA {
		t.Errorf("projected file = %q, refreshed after an empty registration", got)
	}
}

// A fresh refresher (atelet restarted) applies any rotation it missed while
// down at the next Register.
func TestSystemInfoVolumeRefresher_RegisterRewritesFromCurrentState(t *testing.T) {
	certA, certB := string(testCertPEM(t)), string(testCertPEM(t))
	store := newCTBStore(t)
	store.set(t, certA)
	dir := t.TempDir()
	registerTrustVolume(t, newSystemInfoVolumeRefresher(trustbundle.NewSource(store.lister.Get, nil), nil), dir, "uid-1")

	store.set(t, certB)
	registerTrustVolume(t, newSystemInfoVolumeRefresher(trustbundle.NewSource(store.lister.Get, nil), nil), dir, "uid-1")
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
	r := newSystemInfoVolumeRefresher(trustbundle.NewSource(store.lister.Get, nil), nil)
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
	if err := r.refreshBundle(ctx, trustbundle.EgressName); err == nil {
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
	if err := r.refreshBundle(ctx, trustbundle.EgressName); err != nil {
		t.Fatalf("refreshBundle (retry): %v", err)
	}
	if got := readProjected(t, dir, "uid-1", "trust", "ca.pem"); got != certB {
		t.Errorf("blocked actor's file = %q after retry, want the rotation", got)
	}
}

// An event goes through the queue and run loop, and a failed write requeues
// until it lands.
func TestSystemInfoVolumeRefresher_EventPipelineRetriesFailedWrites(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("permission-based write-failure injection needs non-root")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	certA, certB := string(testCertPEM(t)), string(testCertPEM(t))
	store := newCTBStore(t)
	store.set(t, certA)
	r := newSystemInfoVolumeRefresher(trustbundle.NewSource(store.lister.Get, nil), nil)
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

// A rotation rewrites the bundle file but not the volume's other files. mtime
// stands in for the inode.
func TestSystemInfoVolumeRefresher_RotationLeavesUnchangedFilesAlone(t *testing.T) {
	ctx := context.Background()
	certA, certB := string(testCertPEM(t)), string(testCertPEM(t))
	store := newCTBStore(t)
	store.set(t, certA)
	r := newSystemInfoVolumeRefresher(trustbundle.NewSource(store.lister.Get, nil), nil)
	root := filepath.Join(t.TempDir(), "system-info", "vol1")
	spec := &ateletpb.SystemInfoVolume{
		DataSources: append(metadataVolumeSpec().GetDataSources(), trustVolumeSpec("trust/ca.pem").GetDataSources()...),
	}
	if _, err := r.Register("uid-1", resources.ActorRef{Atespace: "team-a", Name: "actor-1"}, []*systemInfoVolume{{Name: "vol1", Root: root, Spec: spec}}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	metadataPath := filepath.Join(root, "actor-name")
	old := time.Now().Add(-time.Hour)
	if err := os.Chtimes(metadataPath, old, old); err != nil {
		t.Fatal(err)
	}
	store.set(t, certB)
	if err := r.refreshBundle(ctx, trustbundle.EgressName); err != nil {
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
	r := newSystemInfoVolumeRefresher(trustbundle.NewSource(nil, nil), nil)
	vol := func() *systemInfoVolume {
		return &systemInfoVolume{Name: "vol1", Root: root, Spec: metadataVolumeSpec()}
	}

	golden := resources.ActorRef{Atespace: "ate-e2e-probe", Name: "golden-actor"}
	if _, err := r.Register("uid-golden", golden, []*systemInfoVolume{vol()}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	// Overwrite with a different actor, as happens when a snapshot taken from
	// one actor seeds another on resume: files must carry the new values.
	alpha := resources.ActorRef{Atespace: "ate-e2e-probe", Name: "probe-alpha"}
	if _, err := r.Register("uid-alpha", alpha, []*systemInfoVolume{vol()}); err != nil {
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

// Restores re-bind guest state by the paths recorded at suspend, so
// regeneration must not move or drop real paths.
func TestSystemInfoVolumeRegister_StableRealPaths(t *testing.T) {
	root := filepath.Join(t.TempDir(), "system-info", "vol1")
	r := newSystemInfoVolumeRefresher(trustbundle.NewSource(nil, nil), nil)
	vol := func() *systemInfoVolume {
		return &systemInfoVolume{Name: "vol1", Root: root, Spec: metadataVolumeSpec()}
	}

	golden := resources.ActorRef{Atespace: "ate-e2e-probe", Name: "golden-actor"}
	if _, err := r.Register("uid-golden", golden, []*systemInfoVolume{vol()}); err != nil {
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
	if _, err := r.Register("uid-alpha", alpha, []*systemInfoVolume{vol()}); err != nil {
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

// A refresh stuck writing one actor must not block Register or Deregister of
// other actors.
func TestSystemInfoVolumeRefresher_LifecycleUnblockedDuringRefresh(t *testing.T) {
	ctx := context.Background()
	certA, certB := string(testCertPEM(t)), string(testCertPEM(t))
	store := newCTBStore(t)
	store.set(t, certA)
	r := newSystemInfoVolumeRefresher(trustbundle.NewSource(store.lister.Get, nil), nil)
	dir := t.TempDir()
	registerTrustVolume(t, r, dir, "uid-a")
	registerTrustVolume(t, r, dir, "uid-b")

	// Pin uid-a's lock so the rotation's refresh blocks mid-walk on it.
	stuck := r.actors["uid-a"]
	stuck.mu.Lock()
	store.set(t, certB)
	refreshDone := make(chan error, 1)
	go func() { refreshDone <- r.refreshBundle(ctx, trustbundle.EgressName) }()

	lifecycleDone := make(chan struct{})
	go func() {
		defer close(lifecycleDone)
		registerTrustVolume(t, r, dir, "uid-c")
		r.Deregister("uid-b")
	}()
	select {
	case <-lifecycleDone:
	case <-time.After(10 * time.Second):
		t.Fatal("Register/Deregister of other actors blocked behind an in-flight refresh")
	}
	select {
	case err := <-refreshDone:
		t.Fatalf("refreshBundle returned (err=%v) while uid-a's lock was still held", err)
	default:
	}

	stuck.mu.Unlock()
	if err := <-refreshDone; err != nil {
		t.Fatalf("refreshBundle: %v", err)
	}
	if got := readProjected(t, dir, "uid-a", "trust", "ca.pem"); got != certB {
		t.Errorf("stuck actor's file = %q, want the rotation applied once unblocked", got)
	}
}

// A refresh that snapshotted an entry before Deregister removed it must see
// it marked stale and skip it.
func TestSystemInfoVolumeRefresher_DeregisterMarksStale(t *testing.T) {
	store := newCTBStore(t)
	store.set(t, string(testCertPEM(t)))
	r := newSystemInfoVolumeRefresher(trustbundle.NewSource(store.lister.Get, nil), nil)
	dir := t.TempDir()

	registerTrustVolume(t, r, dir, "uid-1")
	cur := r.actors["uid-1"]
	r.Deregister("uid-1")
	if !cur.stale {
		t.Error("Deregister left the removed entry unfenced")
	}
}

func TestSystemInfoVolumeRefresher_RegisterTwiceSupersedes(t *testing.T) {
	store := newCTBStore(t)
	store.set(t, string(testCertPEM(t)))
	r := newSystemInfoVolumeRefresher(trustbundle.NewSource(store.lister.Get, nil), nil)
	dir := t.TempDir()
	registerTrustVolume(t, r, dir, "uid-1")
	first := r.actors["uid-1"]

	if _, err := r.Register("uid-1", resources.ActorRef{Atespace: "team-a", Name: "uid-1"}, nil); err != nil {
		t.Fatalf("second Register: %v", err)
	}
	if !first.stale {
		t.Error("superseded entry was not marked stale")
	}
	if r.actors["uid-1"] == first {
		t.Error("superseded entry was not replaced in r.actors")
	}
}

func TestSystemInfoVolumeRefresher_StaleDeregisterPreservesNewerRegistration(t *testing.T) {
	store := newCTBStore(t)
	store.set(t, string(testCertPEM(t)))
	r := newSystemInfoVolumeRefresher(trustbundle.NewSource(store.lister.Get, nil), nil)

	firstDir := t.TempDir()
	firstVol := &systemInfoVolume{
		Name: "trust", Root: filepath.Join(firstDir, "uid-1", "system-info", "trust"), Spec: trustVolumeSpec("ca.pem"),
	}
	first, err := r.Register("uid-1", resources.ActorRef{Atespace: "team-a", Name: "uid-1"}, []*systemInfoVolume{firstVol})
	if err != nil {
		t.Fatalf("first Register: %v", err)
	}
	registerTrustVolume(t, r, t.TempDir(), "uid-1")
	newer := r.actors["uid-1"]

	// This models cleanup from the first registration after the second one has
	// become live. It must not remove the newer registration.
	r.DeregisterOwned(first)
	if got := r.actors["uid-1"]; got != newer {
		t.Fatalf("stale cleanup removed the newer registration: got %p, want %p", got, newer)
	}
	r.DeregisterOwned(newer)
	if got := r.actors["uid-1"]; got != nil {
		t.Fatalf("owned cleanup left registration B live: got %p", got)
	}
}

func TestSystemInfoVolumeRefresher_DeregisterOwnedNil(t *testing.T) {
	for _, registered := range []bool{false, true} {
		t.Run(fmt.Sprintf("registered=%v", registered), func(t *testing.T) {
			r := newSystemInfoVolumeRefresher(trustbundle.NewSource(ctbLister(t).Get, nil), nil)
			var current *registeredActor
			if registered {
				var err error
				current, err = r.Register("uid-1", resources.ActorRef{}, nil)
				if err != nil {
					t.Fatal(err)
				}
			}
			defer func() {
				if p := recover(); p != nil {
					t.Errorf("nil owner cleanup panicked: %v", p)
				}
			}()
			r.DeregisterOwned(nil)
			if r.actors["uid-1"] != current {
				t.Error("nil owner cleanup removed an existing registration")
			}
		})
	}
}

func TestSystemInfoVolumeRefresher_FailedReplacementDropsRegistration(t *testing.T) {
	store := newCTBStore(t)
	store.set(t, string(testCertPEM(t)))
	r := newSystemInfoVolumeRefresher(trustbundle.NewSource(store.lister.Get, nil), nil)
	dir := t.TempDir()
	registerTrustVolume(t, r, dir, "uid-1")
	previous := r.actors["uid-1"]
	// A file in place of the new volume directory forces a real initial-write
	// failure after the previous registration has been superseded.
	blockedRoot := filepath.Join(dir, "blocked")
	if err := os.WriteFile(blockedRoot, []byte("not a directory"), 0o600); err != nil {
		t.Fatal(err)
	}
	owner, err := r.Register("uid-1", resources.ActorRef{}, []*systemInfoVolume{{
		Name: "trust", Root: blockedRoot, Spec: trustVolumeSpec("ca.pem"),
	}})
	if err == nil || !strings.Contains(err.Error(), "while creating") {
		t.Fatalf("Register error = %v, want initial filesystem write failure", err)
	}
	if owner != nil {
		t.Error("failed Register returned an owner; want nil after self-cleanup")
	}
	if r.actors["uid-1"] != nil || !previous.stale {
		t.Error("failed replacement must drop its entry and leave the superseded registration stale")
	}
}

func TestSystemInfoVolumesFor(t *testing.T) {
	spec := &ateletpb.WorkloadSpec{Volumes: []*ateletpb.Volume{
		{Name: "data", DurableDir: &ateletpb.DurableDirVolume{}},
		{Name: "trust", SystemInfo: trustVolumeSpec("ca.pem")},
		{Name: "meta", SystemInfo: metadataVolumeSpec()},
	}}
	got := systemInfoVolumesFor("uid-1", spec)
	if len(got) != 2 || got[0].Name != "trust" || got[1].Name != "meta" {
		t.Fatalf("systemInfoVolumesFor = %+v, want the two system-info volumes in spec order", got)
	}
	for _, v := range got {
		if want := ateletpath.SystemInfoVolumeRoot("uid-1", v.Name); v.Root != want || v.Spec == nil {
			t.Errorf("volume %q: root %q spec %v, want root %q and a spec", v.Name, v.Root, v.Spec, want)
		}
	}
	if got := systemInfoVolumesFor("uid-1", &ateletpb.WorkloadSpec{}); len(got) != 0 {
		t.Errorf("systemInfoVolumesFor(no volumes) = %+v, want none", got)
	}
}

// Register, Deregister, and refresh run concurrently for the race detector.
func TestSystemInfoVolumeRefresher_ConcurrentLifecycle(t *testing.T) {
	ctx := context.Background()
	certA, certB := string(testCertPEM(t)), string(testCertPEM(t))
	store := newCTBStore(t)
	store.set(t, certA)
	r := newSystemInfoVolumeRefresher(trustbundle.NewSource(store.lister.Get, nil), nil)
	dir := t.TempDir()

	var wg sync.WaitGroup
	for i := range 4 {
		uid := fmt.Sprintf("uid-%d", i)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 20 {
				vol := &systemInfoVolume{
					Name: "trust",
					Root: filepath.Join(dir, uid, "system-info", "trust"),
					Spec: trustVolumeSpec("ca.pem"),
				}
				if _, err := r.Register(uid, resources.ActorRef{Atespace: "team-a", Name: uid}, []*systemInfoVolume{vol}); err != nil {
					t.Errorf("Register(%s): %v", uid, err)
					return
				}
				r.Deregister(uid)
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for n := range 40 {
			cert := certA
			if n%2 == 0 {
				cert = certB
			}
			_ = store.indexer.Add(store.object(cert))
			_ = r.refreshBundle(ctx, trustbundle.EgressName)
		}
	}()
	wg.Wait()
}

// A symlink inside the volume must not route a projected file outside it;
// path validation alone cannot catch that.
func TestWriteSystemInfoFile_ConfinedToRoot(t *testing.T) {
	dir := t.TempDir()
	volRoot := filepath.Join(dir, "vol")
	outside := filepath.Join(dir, "outside")
	for _, d := range []string{volRoot, outside} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Symlink(outside, filepath.Join(volRoot, "escape")); err != nil {
		t.Fatal(err)
	}
	root, err := os.OpenRoot(volRoot)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()

	if _, err := writeSystemInfoFile(root, "escape/ca.pem", []byte("x")); err == nil {
		t.Error("writeSystemInfoFile = nil, want refusal to write through a symlink leaving the volume root")
	}
	if _, err := os.Stat(filepath.Join(outside, "ca.pem")); !os.IsNotExist(err) {
		t.Errorf("projected file escaped the volume root (stat err = %v)", err)
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
	registerTrustVolume(t, newSystemInfoVolumeRefresher(trustbundle.NewSource(store.lister.Get, nil), nil), dir, "uid-1")
	if got := readProjected(t, dir, "uid-1", "trust", "ca.pem"); got != string(certPEM) {
		t.Errorf("content = %q, want the sanitized bundle", got)
	}

	t.Run("resolution failure fails the start rather than produce an empty trust file", func(t *testing.T) {
		r := newSystemInfoVolumeRefresher(trustbundle.NewSource(ctbLister(t).Get, nil), nil)
		vol := &systemInfoVolume{Name: "trust", Root: filepath.Join(t.TempDir(), "trust"), Spec: trustVolumeSpec("ca.pem")}
		owner, err := r.Register("uid-2", resources.ActorRef{Atespace: "team-a", Name: "actor-2"}, []*systemInfoVolume{vol})
		if err == nil || !strings.Contains(err.Error(), "not found") || !strings.Contains(err.Error(), `"trust"`) {
			t.Errorf("Register = %v, want not-found error naming the volume", err)
		}
		if owner != nil {
			t.Error("failed Register returned an owner; want nil after self-cleanup")
		}
		if got := r.actors["uid-2"]; got != nil {
			t.Errorf("failed initial Register left an entry without caller cleanup: %p", got)
		}
	})

	t.Run("failed initial write cleanup preserves a newer registration", func(t *testing.T) {
		r := newSystemInfoVolumeRefresher(trustbundle.NewSource(ctbLister(t).Get, nil), nil)
		bad := &systemInfoVolume{Name: "trust", Root: filepath.Join(t.TempDir(), "trust"), Spec: trustVolumeSpec("ca.pem")}
		owner, err := r.Register("uid-3", resources.ActorRef{Atespace: "team-a", Name: "actor-3"}, []*systemInfoVolume{bad})
		if err == nil {
			t.Fatal("Register succeeded without the projected trust bundle")
		}
		newer, err := r.Register("uid-3", resources.ActorRef{Atespace: "team-a", Name: "actor-3"}, []*systemInfoVolume{{
			Name: "metadata", Root: filepath.Join(t.TempDir(), "metadata"), Spec: metadataVolumeSpec(),
		}})
		if err != nil {
			t.Fatalf("newer Register: %v", err)
		}
		r.DeregisterOwned(owner)
		if got := r.actors["uid-3"]; got != newer {
			t.Errorf("failed initial write cleanup removed newer registration: got %p, want %p", got, newer)
		}
	})

	t.Run("re-registration supersedes stale entry without panicking", func(t *testing.T) {
		r := newSystemInfoVolumeRefresher(trustbundle.NewSource(store.lister.Get, nil), nil)
		dir1 := t.TempDir()
		dir2 := t.TempDir()
		registerTrustVolume(t, r, dir1, "uid-rereg")
		registerTrustVolume(t, r, dir2, "uid-rereg")
		if got := readProjected(t, dir2, "uid-rereg", "trust", "ca.pem"); got != string(certPEM) {
			t.Errorf("content = %q, want the sanitized bundle", got)
		}
	})
}

func TestSystemInfoVolumeRefresher_APIVersionEvents(t *testing.T) {
	for _, tc := range []struct {
		version string
		bundle  metav1.Object
	}{
		{"v1", &certsv1.ClusterTrustBundle{ObjectMeta: metav1.ObjectMeta{Name: trustbundle.EgressCTB}}},
		{"v1beta1", &certsv1beta1.ClusterTrustBundle{ObjectMeta: metav1.ObjectMeta{Name: trustbundle.EgressCTB}}},
	} {
		for _, event := range []string{"add", "update", "delete", "tombstone"} {
			t.Run(tc.version+"/"+event, func(t *testing.T) {
				r := newSystemInfoVolumeRefresher(nil, nil)
				defer r.queue.ShutDown()
				h := r.eventHandler()
				switch event {
				case "add":
					h.OnAdd(tc.bundle, false)
				case "update":
					h.OnUpdate(tc.bundle, tc.bundle)
				case "delete":
					h.OnDelete(tc.bundle)
				case "tombstone":
					h.OnDelete(cache.DeletedFinalStateUnknown{Key: tc.bundle.GetName(), Obj: tc.bundle})
				}
				if r.queue.Len() != 1 {
					t.Fatal("event did not enqueue bundle")
				}
				key, _ := r.queue.Get()
				r.queue.Done(key)
				if key != trustbundle.EgressName {
					t.Fatalf("queued %q", key)
				}
			})
		}
	}
}
