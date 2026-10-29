//go:build linux

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
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"

	"go.ate.dev/substrate/cmd/ateworker-microvm/internal/kata"
	"go.ate.dev/substrate/internal/actorlock"
	"go.ate.dev/substrate/internal/actorlog"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/ateattr"
	"go.ate.dev/substrate/internal/atunnel"
	"go.ate.dev/substrate/internal/atunnel/egress"
	"go.ate.dev/substrate/internal/atunnel/ingress"
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/internal/workerstats/workerstatstest"
)

// checkpointFixture is testActor running on a fake VM, ready for a ROOTFS
// checkpoint. With async set, the only teardown slot is taken, so a
// background teardown waits until the test frees it.
type checkpointFixture struct {
	s    *AteWorkerService
	rec  *workerstatstest.Recorder
	req  *ateworkerpb.CheckpointWorkloadRequest
	dirs *ateworkerpb.ActorDirs
}

const (
	testActorMemory = 1 << 30
	testPodLimit    = 64 << 30
)

func newCheckpointFixture(t *testing.T, async bool) *checkpointFixture {
	t.Helper()
	s := &AteWorkerService{
		locks:       actorlock.New(),
		inFlight:    actorlock.NewInFlight(),
		actors:      map[string]*hostedActor{},
		maxActors:   2,
		tunnel:      &atunnel.Tunnel{Ingress: &ingress.Server{}, Egress: &egress.Proxy{}},
		actorLogger: actorlog.NewActorLogger(io.Discard, false),
	}
	if async {
		s.teardownSlots = make(chan struct{}, 1)
		s.teardownSlots <- struct{}{}
	}
	s.epoch.Store(1)
	rec := withUsageRecorder(s)

	root := t.TempDir()
	dirs := testDirs()
	dirs.RootDir = root
	dirs.OciBundleDir = filepath.Join(root, "bundle")
	dirs.CheckpointDir = filepath.Join(root, "checkpoint-state")
	upper, _ := kata.UpperWorkDirs(rootfsUpperDir(dirs), "app")
	if err := os.MkdirAll(upper, 0o700); err != nil {
		t.Fatal(err)
	}

	hostTestActor(s, testActor, nil).memoryBytes = testActorMemory
	ra := fakeVM(t)
	ra.apiSocket = fakeCH(t)
	s.setRunningVM(testActor.UID, ra)
	return &checkpointFixture{s: s, rec: rec, dirs: dirs, req: &ateworkerpb.CheckpointWorkloadRequest{
		Atespace:  testActor.Ref.Atespace,
		ActorName: testActor.Ref.Name,
		ActorUid:  testActor.UID,
		Spec:      testSpec(),
		ActorDirs: dirs,
		Fidelity:  ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS,
	}}
}

// fakeCH serves the cloud-hypervisor API calls a checkpoint makes.
func fakeCH(t *testing.T) string {
	t.Helper()
	sock := filepath.Join(t.TempDir(), "ch.sock")
	l, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/vmm.ping" {
			_, _ = w.Write([]byte("{}"))
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})}
	go func() { _ = srv.Serve(l) }()
	t.Cleanup(func() { _ = srv.Close() })
	return sock
}

func (f *checkpointFixture) checkpoint(t *testing.T) {
	t.Helper()
	if _, err := f.s.CheckpointWorkload(t.Context(), f.req); err != nil {
		t.Fatalf("CheckpointWorkload() = %v", err)
	}
}

// finishTeardown frees the teardown slot and waits for the actor's lock,
// which the background teardown releases last.
func (f *checkpointFixture) finishTeardown(t *testing.T) {
	t.Helper()
	<-f.s.teardownSlots
	if !f.s.locks.Lock(t.Context(), testActor.UID) {
		t.Fatal("background teardown never released the actor's lock")
	}
	f.s.locks.Unlock(testActor.UID)
}

func (f *checkpointFixture) draining() int {
	f.s.actorsMu.RLock()
	defer f.s.actorsMu.RUnlock()
	return f.s.draining
}

func (f *checkpointFixture) asideUppers(t *testing.T) []string {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join(f.dirs.GetRootDir(), rootfsUpperDeletingPrefix+"*"))
	if err != nil {
		t.Fatal(err)
	}
	return paths
}

func finalRecords(rec *workerstatstest.Recorder) int {
	n := 0
	for _, k := range rec.Kinds() {
		if k == ateattr.StatsKindFinal {
			n++
		}
	}
	return n
}

func TestCheckpointTeardown(t *testing.T) {
	for _, tc := range []struct {
		name  string
		async bool
	}{
		{name: "before the reply"},
		{name: "in the background", async: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newCheckpointFixture(t, tc.async)
			hostedUnbooked := f.s.unbookedMemory(testPodLimit)
			f.checkpoint(t)

			if f.s.lookupActor(testActor.UID) != nil {
				t.Error("actor still hosted after the checkpoint")
			}
			if got := finalRecords(f.rec); got != 1 {
				t.Errorf("final usage records = %d, want 1", got)
			}
			if _, err := os.Stat(rootfsUpperDir(f.dirs)); !errors.Is(err, os.ErrNotExist) {
				t.Errorf("live rootfs upper after the checkpoint: %v, want it gone", err)
			}
			wantDraining, wantAside := 0, 0
			if tc.async {
				wantDraining, wantAside = 1, 1
			}
			if got := f.draining(); got != wantDraining {
				t.Errorf("draining = %d, want %d", got, wantDraining)
			}
			if got := len(f.asideUppers(t)); got != wantAside {
				t.Errorf("moved-aside uppers = %d, want %d", got, wantAside)
			}
			// The killed VM may still hold its memory until the teardown ends.
			wantUnbooked := hostedUnbooked + actorBooking(testActorMemory)
			if tc.async {
				wantUnbooked = hostedUnbooked
			}
			if got := f.s.unbookedMemory(testPodLimit); got != wantUnbooked {
				t.Errorf("unbookedMemory() = %d, want %d", got, wantUnbooked)
			}
			if got := f.s.locks.Busy(testActor.UID); got != tc.async {
				t.Errorf("actor's lock busy = %t, want %t", got, tc.async)
			}
			canceled, cancel := context.WithCancel(t.Context())
			cancel()
			if got := f.s.inFlight.WaitIdle(canceled); got == tc.async {
				t.Errorf("WaitIdle() = %t, want %t", got, !tc.async)
			}
			// The pending teardown keeps its slot, so a full worker turns
			// others away until it ends.
			f.s.maxActors = 1
			_, _, err := f.s.admitActor(resources.ActorAttribution{UID: otherActorUID}, false, 0)
			if tc.async && (apierror.Code(err) != codes.ResourceExhausted || !strings.Contains(err.Error(), "worker is full")) {
				t.Errorf("admitActor() during the teardown = %v, want ResourceExhausted", err)
			}
			if !tc.async {
				if err != nil {
					t.Errorf("admitActor() = %v, want nil", err)
				}
				return
			}

			f.finishTeardown(t)
			if got := f.draining(); got != 0 {
				t.Errorf("draining after the teardown = %d, want 0", got)
			}
			if got, want := f.s.unbookedMemory(testPodLimit), hostedUnbooked+actorBooking(testActorMemory); got != want {
				t.Errorf("unbookedMemory() after the teardown = %d, want %d", got, want)
			}
			if got := f.asideUppers(t); len(got) != 0 {
				t.Errorf("moved-aside uppers after the teardown = %v, want none", got)
			}
			if !f.s.inFlight.WaitIdle(canceled) {
				t.Error("WaitIdle() = false after the teardown")
			}
			if _, _, err := f.s.admitActor(resources.ActorAttribution{UID: otherActorUID}, false, 0); err != nil {
				t.Errorf("admitActor() after the teardown = %v, want nil", err)
			}
			if got := finalRecords(f.rec); got != 1 {
				t.Errorf("final usage records after the teardown = %d, want 1", got)
			}
		})
	}
}

// The actor's next RPC waits for the background teardown. One whose caller
// gives up first returns the context error and leaves nothing hosted.
func TestSameActorRPCsWaitForCheckpointTeardown(t *testing.T) {
	for _, tc := range []struct {
		name string
		call func(context.Context, *checkpointFixture) error
		// after is the code once the teardown is done. The worker is shutting
		// down, so Run and Restore stop right after the lock.
		after codes.Code
	}{
		{
			name: "RestoreWorkload",
			call: func(ctx context.Context, f *checkpointFixture) error {
				_, err := f.s.RestoreWorkload(ctx, validRestoreRequest())
				return err
			},
			after: codes.Unavailable,
		},
		{
			name: "RunWorkload",
			call: func(ctx context.Context, f *checkpointFixture) error {
				_, err := f.s.RunWorkload(ctx, validRunRequest())
				return err
			},
			after: codes.Unavailable,
		},
		{
			name: "TerminateWorkload",
			call: func(ctx context.Context, f *checkpointFixture) error {
				_, err := f.s.TerminateWorkload(ctx, &ateworkerpb.TerminateWorkloadRequest{
					Atespace: testActor.Ref.Atespace, ActorName: testActor.Ref.Name, ActorUid: testActor.UID, Spec: testSpec(), ActorDirs: f.dirs,
				})
				return err
			},
			after: codes.OK,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newCheckpointFixture(t, true)
			f.checkpoint(t)
			f.s.shuttingDown.Store(true)

			ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
			defer cancel()
			err := tc.call(ctx, f)
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Errorf("%s() during the teardown = %v, want it to wrap %v", tc.name, err, context.DeadlineExceeded)
			}
			if got := apierror.Code(err); got != codes.DeadlineExceeded {
				t.Errorf("%s() during the teardown code = %v, want %v", tc.name, got, codes.DeadlineExceeded)
			}
			if f.s.lookupActor(testActor.UID) != nil || f.draining() != 1 {
				t.Errorf("%s() changed the actor's state while its teardown was pending", tc.name)
			}

			<-f.s.teardownSlots
			err = tc.call(t.Context(), f)
			if got := apierror.Code(err); got != tc.after {
				t.Errorf("%s() after the teardown = %v, want code %v", tc.name, err, tc.after)
			}
			if got := f.draining(); got != 0 {
				t.Errorf("draining once %s() had the lock = %d, want 0", tc.name, got)
			}
		})
	}
}

// The background teardown removes only uppers moved aside: never the live one
// a later activation made, nor a moved-aside path that now holds another dir.
func TestCheckpointTeardownRemovesOnlyMovedAsideUppers(t *testing.T) {
	f := newCheckpointFixture(t, true)
	stale := filepath.Join(f.dirs.GetRootDir(), rootfsUpperDeletingPrefix+"stale")
	if err := os.Mkdir(stale, 0o700); err != nil {
		t.Fatal(err)
	}
	f.checkpoint(t)

	var aside string
	for _, p := range f.asideUppers(t) {
		if p != stale {
			aside = p
		}
	}
	if aside == "" {
		t.Fatal("checkpoint moved no rootfs upper aside")
	}
	if err := os.Rename(aside, aside+".old"); err != nil {
		t.Fatal(err)
	}
	for _, dir := range []string{aside, rootfsUpperDir(f.dirs)} {
		if err := os.Mkdir(dir, 0o700); err != nil {
			t.Fatal(err)
		}
	}

	f.finishTeardown(t)
	if _, err := os.Stat(stale); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("stale moved-aside upper: %v, want it removed", err)
	}
	for _, dir := range []string{aside, rootfsUpperDir(f.dirs)} {
		if _, err := os.Stat(dir); err != nil {
			t.Errorf("%s: %v, want it left in place", dir, err)
		}
	}
}

func TestCheckpointTeardownReleasesAfterPanic(t *testing.T) {
	s := &AteWorkerService{actors: map[string]*hostedActor{}, teardownSlots: make(chan struct{}, 1)}
	hostTestActor(s, testActor, nil).memoryBytes = testActorMemory
	hosted := s.detachForTeardown(testActor.UID)
	if hosted == nil {
		t.Fatal("test actor not hosted")
	}

	var released []string
	done := make(chan struct{})
	s.startTeardown(t.Context(), hosted,
		func() {
			released = append(released, "lock")
			close(done)
		},
		func() {
			s.actorsMu.RLock()
			released = append(released, "inFlight")
			if s.draining != 0 || s.teardownBooked != 0 {
				released = append(released, "draining slot or memory still held")
			}
			s.actorsMu.RUnlock()
		},
		func() { panic("teardown failed") })
	<-done

	if want := []string{"inFlight", "lock"}; !slices.Equal(released, want) {
		t.Errorf("released %v, want %v", released, want)
	}
	if len(s.teardownSlots) != 0 {
		t.Error("teardown slot still taken")
	}
}
