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
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sfake "k8s.io/client-go/kubernetes/fake"
)

const (
	gcUIDLive   = "11111111-1111-4111-8111-111111111111"
	gcUIDOrphan = "22222222-2222-4222-8222-222222222222"
)

// gcFixture is a janitor over a temp ateoms directory with scripted pod-list
// and probe results.
type gcFixture struct {
	g       *ateomGC
	dir     string
	pods    map[string]struct{}
	listErr error
	alive   map[string]bool
	now     time.Time
	probes  int
	// probesWithoutDeadline counts probes whose context carried no deadline.
	probesWithoutDeadline int
	// probeErr replaces the default transport failure for a not-alive probe.
	probeErr error
}

func newGCFixture(t *testing.T) *gcFixture {
	t.Helper()
	f := &gcFixture{
		dir:   t.TempDir(),
		pods:  map[string]struct{}{},
		alive: map[string]bool{},
		now:   time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC),
	}
	f.g = &ateomGC{
		ateomsDir: f.dir,
		minAge:    10 * time.Minute,
		listNodePodUIDs: func(context.Context) (map[string]struct{}, error) {
			if f.listErr != nil {
				return nil, f.listErr
			}
			return f.pods, nil
		},
		probe: func(ctx context.Context, uid string) error {
			f.probes++
			if _, ok := ctx.Deadline(); !ok {
				f.probesWithoutDeadline++
			}
			if f.alive[uid] {
				return nil
			}
			if f.probeErr != nil {
				return f.probeErr
			}
			return status.Error(codes.Unavailable, "connect: no such file or directory")
		},
		now:     func() time.Time { return f.now },
		strikes: map[string]int{},
	}
	return f
}

// mkdir creates an ateom directory aged relative to the fixture clock.
func (f *gcFixture) mkdir(t *testing.T, uid string, age time.Duration) {
	t.Helper()
	dir := filepath.Join(f.dir, uid)
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatalf("mkdir %s: %v", uid, err)
	}
	mtime := f.now.Add(-age)
	if err := os.Chtimes(dir, mtime, mtime); err != nil {
		t.Fatalf("chtimes %s: %v", uid, err)
	}
}

func (f *gcFixture) exists(uid string) bool {
	_, err := os.Stat(filepath.Join(f.dir, uid))
	return err == nil
}

func (f *gcFixture) passes(n int) {
	for range n {
		f.g.runPass(context.Background())
	}
}

func TestAteomGCRemovesOrphanAfterStrikes(t *testing.T) {
	f := newGCFixture(t)
	f.mkdir(t, gcUIDOrphan, time.Hour)

	f.passes(ateomGCStrikes - 1)
	if !f.exists(gcUIDOrphan) {
		t.Fatalf("directory removed after %d passes, want it kept until %d", ateomGCStrikes-1, ateomGCStrikes)
	}
	f.passes(1)
	if f.exists(gcUIDOrphan) {
		t.Errorf("directory still present after %d orphaned passes", ateomGCStrikes)
	}
	if _, ok := f.g.strikes[gcUIDOrphan]; ok {
		t.Errorf("strike entry kept after removal")
	}
	if f.probesWithoutDeadline != 0 {
		t.Errorf("%d probes ran without a deadline; every probe must be bounded", f.probesWithoutDeadline)
	}
}

// TestAteomGCLivePodNeverCandidate: a listed pod is never probed or struck.
func TestAteomGCLivePodNeverCandidate(t *testing.T) {
	f := newGCFixture(t)
	f.mkdir(t, gcUIDLive, time.Hour)
	f.pods[gcUIDLive] = struct{}{}

	f.passes(ateomGCStrikes + 2)
	if !f.exists(gcUIDLive) {
		t.Fatal("live pod's directory removed")
	}
	if f.probes != 0 {
		t.Errorf("live pod probed %d times, want 0", f.probes)
	}
}

// TestAteomGCSocketAnswerVetoes: an ateom that answers is kept and its
// strikes reset, whatever the pod list said.
func TestAteomGCSocketAnswerVetoes(t *testing.T) {
	f := newGCFixture(t)
	f.mkdir(t, gcUIDOrphan, time.Hour)

	f.passes(ateomGCStrikes - 1) // two strikes accrued
	f.alive[gcUIDOrphan] = true
	f.passes(1)
	if got := f.g.strikes[gcUIDOrphan]; got != 0 {
		t.Errorf("strikes after an answering probe = %d, want 0", got)
	}
	f.alive[gcUIDOrphan] = false
	f.passes(ateomGCStrikes - 1)
	if !f.exists(gcUIDOrphan) {
		t.Error("directory removed without the full run of consecutive strikes after the veto")
	}
}

// TestAteomGCListErrorAbortsPass: a failed pod list advances no strikes.
func TestAteomGCListErrorAbortsPass(t *testing.T) {
	f := newGCFixture(t)
	f.mkdir(t, gcUIDOrphan, time.Hour)

	f.listErr = errors.New("apiserver unavailable")
	f.passes(ateomGCStrikes * 2)
	if !f.exists(gcUIDOrphan) {
		t.Fatal("directory removed while the pod list was failing")
	}
	if got := f.g.strikes[gcUIDOrphan]; got != 0 {
		t.Errorf("strikes advanced to %d during list failures, want 0", got)
	}
	if f.probes != 0 {
		t.Errorf("probed %d times during list failures, want 0", f.probes)
	}
}

func TestAteomGCMinAgeVetoes(t *testing.T) {
	f := newGCFixture(t)
	f.mkdir(t, gcUIDOrphan, time.Minute) // younger than the 10m min-age

	f.passes(ateomGCStrikes + 1)
	if !f.exists(gcUIDOrphan) {
		t.Fatal("young directory removed")
	}
	// Strikes are at the threshold, so aging it past the cutoff removes it.
	f.now = f.now.Add(time.Hour)
	f.passes(1)
	if f.exists(gcUIDOrphan) {
		t.Error("aged directory not removed")
	}
}

func TestAteomGCSkipsNonUUIDEntries(t *testing.T) {
	f := newGCFixture(t)
	// A file, an operator directory, and two UUID spellings no ateom uses.
	dirs := []string{"lost+found", "{" + gcUIDOrphan + "}", "22222222222242228222222222222222"}
	for _, name := range dirs {
		if err := os.Mkdir(filepath.Join(f.dir, name), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(f.dir, "notes.txt"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}

	f.passes(ateomGCStrikes + 1)
	for _, name := range append(dirs, "notes.txt") {
		if _, err := os.Stat(filepath.Join(f.dir, name)); err != nil {
			t.Errorf("%s removed; only pod-UID directories are the janitor's", name)
		}
	}
	if f.probes != 0 {
		t.Errorf("non-UUID entries probed %d times, want 0", f.probes)
	}
}

// TestAteomGCStrikesPrunedWithDirectory: a strike entry does not outlive its
// directory.
func TestAteomGCStrikesPrunedWithDirectory(t *testing.T) {
	f := newGCFixture(t)
	f.mkdir(t, gcUIDOrphan, time.Hour)

	f.passes(1)
	if got := f.g.strikes[gcUIDOrphan]; got != 1 {
		t.Fatalf("strikes = %d, want 1", got)
	}
	if err := os.RemoveAll(filepath.Join(f.dir, gcUIDOrphan)); err != nil {
		t.Fatal(err)
	}
	f.passes(1)
	if _, ok := f.g.strikes[gcUIDOrphan]; ok {
		t.Error("strike entry kept for a directory that no longer exists")
	}
}

// TestAteomGCMissingAteomsDirIsQuiet: no ateoms directory is empty work, not
// an error.
func TestAteomGCMissingAteomsDirIsQuiet(t *testing.T) {
	f := newGCFixture(t)
	f.g.ateomsDir = filepath.Join(f.dir, "absent")
	f.g.listNodePodUIDs = func(context.Context) (map[string]struct{}, error) {
		t.Fatal("pod list taken with no ateoms directory to reconcile against")
		return nil, nil
	}

	f.passes(1)
	if f.probes != 0 || len(f.g.strikes) != 0 {
		t.Errorf("probes = %d, strikes = %v; want none", f.probes, f.g.strikes)
	}
}

func TestAteomGCRunPassRecoversPanic(t *testing.T) {
	f := newGCFixture(t)
	f.mkdir(t, gcUIDOrphan, time.Hour)
	f.g.probe = func(context.Context, string) error { panic("probe bug") }
	f.passes(1) // must return
}

func TestNodePodUIDLister(t *testing.T) {
	client := k8sfake.NewSimpleClientset(
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "a", Namespace: "ns", UID: gcUIDLive}},
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "b", Namespace: "other", UID: gcUIDOrphan}},
	)
	// The fake clientset ignores field selectors; this pins the UID keying
	// across namespaces.
	got, err := nodePodUIDLister(client, "node-1")(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, uid := range []string{gcUIDLive, gcUIDOrphan} {
		if _, ok := got[uid]; !ok {
			t.Errorf("uid %s missing from %v", uid, got)
		}
	}
}

// TestAteomGCStatusErrorIsAnAnswer: a live ateom whose sandbox read fails
// answers Internal, which is a bound server and must veto.
func TestAteomGCStatusErrorIsAnAnswer(t *testing.T) {
	f := newGCFixture(t)
	f.mkdir(t, gcUIDOrphan, time.Hour)
	f.probeErr = status.Error(codes.Internal, "reading sandbox cgroup: permission denied")

	f.passes(ateomGCStrikes + 2)
	if !f.exists(gcUIDOrphan) {
		t.Fatal("directory removed although its ateom answered with a status error")
	}
	if got := f.g.strikes[gcUIDOrphan]; got != 0 {
		t.Errorf("strikes = %d after answering probes, want 0", got)
	}
}

func TestAteomAnswered(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil", err: nil, want: true},
		{name: "internal is a bound server", err: status.Error(codes.Internal, "x"), want: true},
		{name: "unimplemented is a bound server", err: status.Error(codes.Unimplemented, "x"), want: true},
		{name: "failed precondition is a bound server", err: status.Error(codes.FailedPrecondition, "x"), want: true},
		{name: "unavailable is no listener", err: status.Error(codes.Unavailable, "x"), want: false},
		{name: "deadline exceeded is no answer", err: status.Error(codes.DeadlineExceeded, "x"), want: false},
		{name: "canceled is no answer", err: status.Error(codes.Canceled, "x"), want: false},
		// status.Code reports Unknown for a plain error, which only a server
		// produces.
		{name: "plain error reads as unknown, a server", err: errors.New("x"), want: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := ateomAnswered(tc.err); got != tc.want {
				t.Errorf("ateomAnswered(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

// TestAteomGCRunWaitsOnePeriod: no pass before the first tick, then one per
// period.
func TestAteomGCRunWaitsOnePeriod(t *testing.T) {
	f := newGCFixture(t)
	f.g.period = 50 * time.Millisecond
	lists := make(chan struct{}, 16)
	f.g.listNodePodUIDs = func(context.Context) (map[string]struct{}, error) {
		lists <- struct{}{}
		return nil, nil
	}
	f.mkdir(t, gcUIDOrphan, time.Hour) // so a pass reaches the list

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go f.g.Run(ctx)

	select {
	case <-lists:
		t.Fatal("a pass ran before the first period elapsed")
	case <-time.After(10 * time.Millisecond):
	}
	for i := range 2 {
		select {
		case <-lists:
		case <-time.After(time.Second):
			t.Fatalf("pass %d did not run on the period", i+1)
		}
	}
}

// TestAteomGCCancelDoesNotStrike: a probe cut short by our own context aborts
// the pass with strikes unchanged.
func TestAteomGCCancelDoesNotStrike(t *testing.T) {
	f := newGCFixture(t)
	f.mkdir(t, gcUIDOrphan, time.Hour)
	f.passes(ateomGCStrikes - 1) // one strike short of removal

	ctx, cancel := context.WithCancel(context.Background())
	f.g.probe = func(context.Context, string) error {
		cancel() // shutdown lands while this probe is in flight
		return status.Error(codes.Canceled, "context canceled")
	}
	f.g.runPass(ctx)

	if !f.exists(gcUIDOrphan) {
		t.Fatal("directory removed on a probe cut off by our own cancellation")
	}
	if got := f.g.strikes[gcUIDOrphan]; got != ateomGCStrikes-1 {
		t.Errorf("strikes = %d after a canceled pass, want unchanged %d", got, ateomGCStrikes-1)
	}
}
