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
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/ateompath"
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

// testRefresher returns a refresher whose path layout lives under a temp
// actors dir mirroring ateompath's <actorsDir>/<uid>/system-info shape.
func testRefresher(t *testing.T, lister certlisters.ClusterTrustBundleLister) (*trustBundleRefresher, string) {
	t.Helper()
	dir := t.TempDir()
	r := newTrustBundleRefresher(lister)
	r.actorsDir = dir
	r.stateFile = func(actorUID string) string {
		return filepath.Join(dir, actorUID, "system-info", trustBundleProjectionsFileName)
	}
	return r, dir
}

// projectTrustBundle runs the production initial-write path
// (writeSystemInfoVolume) for one actor volume projecting the egress bundle
// at relPath, returning the projections Run/Restore would register.
func projectTrustBundle(t *testing.T, lister certlisters.ClusterTrustBundleLister, actorsDir, actorUID, volume, relPath string) []trustBundleProjection {
	t.Helper()
	root := filepath.Join(actorsDir, actorUID, "system-info", volume)
	si := &ateletpb.SystemInfoVolume{
		DataSources: []*ateletpb.SystemInfoDataSource{
			{DataSource: &ateletpb.SystemInfoDataSource_TrustBundle{
				TrustBundle: &ateletpb.TrustBundleDataSource{Name: EgressTrustBundleName, Path: relPath},
			}},
		},
	}
	projections, err := writeSystemInfoVolume(context.Background(), root, resources.ActorRef{Atespace: "team-a", Name: actorUID}, actorUID, lister, si)
	if err != nil {
		t.Fatalf("writeSystemInfoVolume: %v", err)
	}
	return projections
}

func readProjected(t *testing.T, actorsDir, actorUID, volume, relPath string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(actorsDir, actorUID, "system-info", volume, relPath))
	if err != nil {
		t.Fatalf("reading projected file: %v", err)
	}
	return string(b)
}

func TestTrustBundleRefresher_RefreshesRunningActorsOnChange(t *testing.T) {
	ctx := context.Background()
	certA, certB := string(testCertPEM(t)), string(testCertPEM(t))
	store := newCTBStore(t)
	store.set(t, certA)
	r, dir := testRefresher(t, store.lister)

	// Two running actors project the same bundle; the informer event must
	// rewrite both files.
	for _, uid := range []string{"uid-1", "uid-2"} {
		if err := r.Register(ctx, uid, projectTrustBundle(t, store.lister, dir, uid, "trust", "ca.pem")); err != nil {
			t.Fatalf("Register(%s): %v", uid, err)
		}
	}

	// Rotate and deliver the update the way the informer does.
	handler := r.eventHandler(ctx)
	handler.OnUpdate(store.object(certA), store.set(t, certB))
	for _, uid := range []string{"uid-1", "uid-2"} {
		if got := readProjected(t, dir, uid, "trust", "ca.pem"); got != certB {
			t.Errorf("actor %s: projected file = %q, want the rotated bundle", uid, got)
		}
	}

	// A replayed event for unchanged contents (relist after a watch
	// reconnect) must not rewrite the files: a no-op rewrite still replaces
	// the inode a suspended guest may need to re-bind. The sentinel write
	// stands in for inode identity.
	sentinelPath := filepath.Join(dir, "uid-1", "system-info", "trust", "ca.pem")
	if err := os.WriteFile(sentinelPath, []byte("sentinel"), 0o644); err != nil {
		t.Fatal(err)
	}
	handler.OnAdd(store.object(certB), true)
	if got := readProjected(t, dir, "uid-1", "trust", "ca.pem"); got != "sentinel" {
		t.Errorf("projected file rewritten on a no-change event (got %q)", got)
	}
}

func TestTrustBundleRefresher_KeepsLastGoodOnFailure(t *testing.T) {
	ctx := context.Background()
	certA, certB := string(testCertPEM(t)), string(testCertPEM(t))
	store := newCTBStore(t)
	store.set(t, certA)
	r, dir := testRefresher(t, store.lister)
	if err := r.Register(ctx, "uid-1", projectTrustBundle(t, store.lister, dir, "uid-1", "trust", "ca.pem")); err != nil {
		t.Fatalf("Register: %v", err)
	}

	t.Run("backing object deleted", func(t *testing.T) {
		store.remove(t)
		r.eventHandler(ctx).OnDelete(store.object(certA))
		r.refreshBundle(ctx, EgressTrustBundleName)
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
		r.refreshBundle(ctx, EgressTrustBundleName)
		if got := readProjected(t, dir, "uid-1", "trust", "ca.pem"); got != certB {
			t.Errorf("projected file = %q, want the healed bundle", got)
		}
	})
}

func TestTrustBundleRefresher_RegisterClosesTheUpdateWindow(t *testing.T) {
	ctx := context.Background()
	certA, certB := string(testCertPEM(t)), string(testCertPEM(t))
	store := newCTBStore(t)
	store.set(t, certA)
	r, dir := testRefresher(t, store.lister)

	// The bundle rotates after the initial projection write but before the
	// actor is registered: no handler saw the event, but the lister already
	// has the new contents, and Register must re-project from them.
	projections := projectTrustBundle(t, store.lister, dir, "uid-1", "trust", "ca.pem")
	store.set(t, certB)
	if err := r.Register(ctx, "uid-1", projections); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if got := readProjected(t, dir, "uid-1", "trust", "ca.pem"); got != certB {
		t.Errorf("projected file = %q, want the update that raced registration", got)
	}
}

func TestTrustBundleRefresher_Deregister(t *testing.T) {
	ctx := context.Background()
	certA, certB := string(testCertPEM(t)), string(testCertPEM(t))
	store := newCTBStore(t)
	store.set(t, certA)
	r, dir := testRefresher(t, store.lister)
	if err := r.Register(ctx, "uid-1", projectTrustBundle(t, store.lister, dir, "uid-1", "trust", "ca.pem")); err != nil {
		t.Fatalf("Register: %v", err)
	}

	r.Deregister(ctx, "uid-1")
	if _, err := os.Stat(r.stateFile("uid-1")); !os.IsNotExist(err) {
		t.Errorf("state file survives deregistration (stat err: %v)", err)
	}
	store.set(t, certB)
	r.refreshBundle(ctx, EgressTrustBundleName)
	if got := readProjected(t, dir, "uid-1", "trust", "ca.pem"); got != certA {
		t.Errorf("projected file = %q, refreshed after deregistration", got)
	}
}

func TestTrustBundleRefresher_RegisterEmptyClearsStaleRegistration(t *testing.T) {
	ctx := context.Background()
	certA, certB := string(testCertPEM(t)), string(testCertPEM(t))
	store := newCTBStore(t)
	store.set(t, certA)
	r, dir := testRefresher(t, store.lister)
	if err := r.Register(ctx, "uid-1", projectTrustBundle(t, store.lister, dir, "uid-1", "trust", "ca.pem")); err != nil {
		t.Fatalf("Register: %v", err)
	}

	// The actor comes back under a spec that no longer projects any bundle:
	// prepareOCIBundles registers whatever was written, here nothing.
	if err := r.Register(ctx, "uid-1", nil); err != nil {
		t.Fatalf("Register(empty): %v", err)
	}
	if _, err := os.Stat(r.stateFile("uid-1")); !os.IsNotExist(err) {
		t.Errorf("state file survives an empty registration (stat err: %v)", err)
	}
	store.set(t, certB)
	r.refreshBundle(ctx, EgressTrustBundleName)
	if got := readProjected(t, dir, "uid-1", "trust", "ca.pem"); got != certA {
		t.Errorf("projected file = %q, refreshed after an empty registration", got)
	}
}

func TestTrustBundleRefresher_WriteFailureIsolatedAndRetried(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("permission-based write-failure injection needs non-root")
	}
	ctx := context.Background()
	certA, certB := string(testCertPEM(t)), string(testCertPEM(t))
	store := newCTBStore(t)
	store.set(t, certA)
	r, dir := testRefresher(t, store.lister)
	for _, uid := range []string{"uid-1", "uid-2"} {
		if err := r.Register(ctx, uid, projectTrustBundle(t, store.lister, dir, uid, "trust", "ca.pem")); err != nil {
			t.Fatalf("Register(%s): %v", uid, err)
		}
	}

	// One actor's volume root refuses writes; the rotation must still reach
	// the other actor.
	blocked := filepath.Join(dir, "uid-1", "system-info", "trust")
	if err := os.Chmod(blocked, 0o555); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(blocked, 0o755) })
	store.set(t, certB)
	r.refreshBundle(ctx, EgressTrustBundleName)
	if got := readProjected(t, dir, "uid-2", "trust", "ca.pem"); got != certB {
		t.Errorf("healthy actor's file = %q, want the rotation despite the sibling's write failure", got)
	}
	if got := readProjected(t, dir, "uid-1", "trust", "ca.pem"); got != certA {
		t.Errorf("blocked actor's file = %q, want last good contents", got)
	}

	// The failed write left AppliedHash stale, so the next replay of the same
	// contents (in production, the informer resync) retries exactly it.
	if err := os.Chmod(blocked, 0o755); err != nil {
		t.Fatal(err)
	}
	r.refreshBundle(ctx, EgressTrustBundleName)
	if got := readProjected(t, dir, "uid-1", "trust", "ca.pem"); got != certB {
		t.Errorf("blocked actor's file = %q after retry, want the rotation", got)
	}
}

// TestNewTrustBundleRefresherPathLayout pins the production layout the tests
// otherwise replace with temp-dir equivalents.
func TestNewTrustBundleRefresherPathLayout(t *testing.T) {
	r := newTrustBundleRefresher(nil)
	if r.actorsDir != ateompath.ActorsDir {
		t.Errorf("actorsDir = %q, want ateompath.ActorsDir %q", r.actorsDir, ateompath.ActorsDir)
	}
	want := filepath.Join(ateompath.SystemInfoVolumeRootsDir("uid-1"), trustBundleProjectionsFileName)
	if got := r.stateFile("uid-1"); got != want {
		t.Errorf("stateFile = %q, want %q", got, want)
	}
}

func TestTrustBundleRefresher_RecoverFromDisk(t *testing.T) {
	ctx := context.Background()
	certA, certB := string(testCertPEM(t)), string(testCertPEM(t))
	store := newCTBStore(t)
	store.set(t, certA)
	r1, dir := testRefresher(t, store.lister)
	if err := r1.Register(ctx, "uid-1", projectTrustBundle(t, store.lister, dir, "uid-1", "trust", "ca.pem")); err != nil {
		t.Fatalf("Register: %v", err)
	}

	restarted := func(t *testing.T) *trustBundleRefresher {
		t.Helper()
		r := newTrustBundleRefresher(store.lister)
		r.actorsDir, r.stateFile = r1.actorsDir, r1.stateFile
		return r
	}

	t.Run("unchanged bundle is not rewritten", func(t *testing.T) {
		// mtime stands in for inode identity: recovery must not replace a
		// file whose contents already match the backing bundle.
		path := filepath.Join(dir, "uid-1", "system-info", "trust", "ca.pem")
		old := time.Now().Add(-time.Hour)
		if err := os.Chtimes(path, old, old); err != nil {
			t.Fatal(err)
		}
		restarted(t).recoverFromDisk(ctx)
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if !info.ModTime().Equal(old) {
			t.Errorf("projected file rewritten on recovery although the bundle never changed")
		}
	})

	t.Run("rotation missed while down is applied", func(t *testing.T) {
		store.set(t, certB)
		restarted(t).recoverFromDisk(ctx)
		if got := readProjected(t, dir, "uid-1", "trust", "ca.pem"); got != certB {
			t.Errorf("projected file = %q, want the rotation applied at recovery", got)
		}
	})

	t.Run("recovered registration stays live", func(t *testing.T) {
		r2 := restarted(t)
		r2.recoverFromDisk(ctx)
		next := string(testCertPEM(t))
		store.set(t, next)
		r2.refreshBundle(ctx, EgressTrustBundleName)
		if got := readProjected(t, dir, "uid-1", "trust", "ca.pem"); got != next {
			t.Errorf("projected file = %q, want post-recovery rotations applied", got)
		}
	})

	t.Run("unparsable record is skipped", func(t *testing.T) {
		if err := os.MkdirAll(filepath.Join(dir, "uid-corrupt", "system-info"), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(r1.stateFile("uid-corrupt"), []byte("not json"), 0o600); err != nil {
			t.Fatal(err)
		}
		r2 := restarted(t)
		r2.recoverFromDisk(ctx)
		r2.mu.Lock()
		_, recovered := r2.actors["uid-corrupt"]
		r2.mu.Unlock()
		if recovered {
			t.Errorf("corrupt record produced a registration")
		}
	})
}
