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
	"slices"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/ateompath"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc"
)

// fakeLiveActors is a live set, or the failure to read one.
type fakeLiveActors struct {
	uids []string
	err  error
}

func (f fakeLiveActors) liveActorUIDs(context.Context) (map[string]bool, error) {
	if f.err != nil {
		return nil, f.err
	}
	live := map[string]bool{}
	for _, uid := range f.uids {
		live[uid] = true
	}
	return live, nil
}

// seedActorDir creates an actor's state directory with a payload in it, aged
// to look like state left behind rather than state being set up right now.
func seedActorDir(t *testing.T, uid string, age time.Duration) string {
	t.Helper()
	dir := ateompath.ActorPath(uid)
	payload := filepath.Join(dir, "durable-dir", "vol")
	if err := os.MkdirAll(payload, 0o755); err != nil {
		t.Fatalf("seeding %s: %v", dir, err)
	}
	if err := os.WriteFile(filepath.Join(payload, "data"), []byte("actor state"), 0o600); err != nil {
		t.Fatalf("seeding %s: %v", dir, err)
	}
	aged := time.Now().Add(-age)
	if err := os.Chtimes(dir, aged, aged); err != nil {
		t.Fatalf("aging %s: %v", dir, err)
	}
	return dir
}

func newTestActorGC(t *testing.T, live liveActorLister, resident func() []string) *actorGC {
	t.Helper()
	return &actorGC{
		actorsDir: ateompath.ActorsDir,
		live:      live,
		resident:  resident,
		minAge:    10 * time.Minute,
	}
}

// The sweep is the safety net for every path that cannot terminate
// gracefully — an evicted node, a crashed atelet, an uninstall on top of live
// state — so what it keeps matters as much as what it reclaims. Each row is
// one reason a directory is or is not the sweep's to take.
func TestActorGCSweep(t *testing.T) {
	const (
		orphan   = "actor-orphan"
		assigned = "actor-assigned"
	)

	tests := []struct {
		name string
		// seed arranges the tree and returns the dirs that must survive the
		// pass; every other seeded dir must be gone.
		seed     func(t *testing.T) (keep []string)
		live     fakeLiveActors
		resident []string
		dryRun   bool
	}{
		{
			name: "an orphan is reclaimed",
			seed: func(t *testing.T) []string {
				seedActorDir(t, orphan, time.Hour)
				return nil
			},
		},
		{
			name: "an actor the control plane placed here is kept",
			seed: func(t *testing.T) []string {
				return []string{seedActorDir(t, assigned, time.Hour)}
			},
			live: fakeLiveActors{uids: []string{assigned}},
		},
		{
			// The control plane does not name it, but this atelet is running
			// it: a bind that landed after the listing, or a teardown in
			// flight.
			name: "an actor this atelet is hosting is kept",
			seed: func(t *testing.T) []string {
				return []string{seedActorDir(t, orphan, time.Hour)}
			},
			resident: []string{orphan},
		},
		{
			// The window between atelet creating the directory and the
			// control plane recording where the actor was placed.
			name: "a directory younger than min-age is kept",
			seed: func(t *testing.T) []string {
				return []string{seedActorDir(t, orphan, time.Minute)}
			},
		},
		{
			// A PAUSED actor holds no worker assignment, so it is absent from
			// the live set by construction; its pause snapshot is node-pinned
			// state held deliberately, and deleting it is unrecoverable.
			name: "an actor holding a local snapshot is kept",
			seed: func(t *testing.T) []string {
				dir := seedActorDir(t, orphan, time.Hour)
				snap := ateompath.LocalSnapshotDir(orphan, "pause-1")
				if err := os.MkdirAll(snap, 0o755); err != nil {
					t.Fatalf("seeding %s: %v", snap, err)
				}
				return []string{dir}
			},
		},
		{
			// Without a root set an orphan is indistinguishable from a
			// running actor, so the pass deletes nothing at all.
			name: "nothing is swept when the live set cannot be read",
			seed: func(t *testing.T) []string {
				return []string{seedActorDir(t, orphan, time.Hour)}
			},
			live: fakeLiveActors{err: errors.New("control plane is down")},
		},
		{
			name: "a dry run reclaims nothing",
			seed: func(t *testing.T) []string {
				return []string{seedActorDir(t, orphan, time.Hour)}
			},
			dryRun: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			useTempNodeDirs(t)
			if err := os.MkdirAll(ateompath.ActorsDir, 0o755); err != nil {
				t.Fatalf("creating the actors dir: %v", err)
			}
			keep := tt.seed(t)

			gc := newTestActorGC(t, tt.live, func() []string { return tt.resident })
			gc.dryRun = tt.dryRun
			gc.runPass(t.Context())

			entries, err := os.ReadDir(ateompath.ActorsDir)
			if err != nil {
				t.Fatalf("reading the actors dir: %v", err)
			}
			var left []string
			for _, e := range entries {
				left = append(left, filepath.Join(ateompath.ActorsDir, e.Name()))
			}
			slices.Sort(left)
			slices.Sort(keep)
			if !slices.Equal(left, keep) {
				t.Errorf("actors dir holds %v after the pass, want %v", left, keep)
			}
		})
	}
}

// A pass that dies between the rename and the delete leaves the tree out of
// the UID namespace but still on disk. It is already unreachable, so the next
// pass finishes it without consulting anything.
func TestActorGCFinishesRetiredDirs(t *testing.T) {
	useTempNodeDirs(t)
	if err := os.MkdirAll(ateompath.ActorsDir, 0o755); err != nil {
		t.Fatalf("creating the actors dir: %v", err)
	}
	retired := filepath.Join(ateompath.ActorsDir, retiredActorPrefix+"actor-1-12345")
	if err := os.MkdirAll(filepath.Join(retired, "durable-dir"), 0o755); err != nil {
		t.Fatalf("seeding %s: %v", retired, err)
	}

	newTestActorGC(t, fakeLiveActors{}, nil).runPass(t.Context())

	if _, err := os.Stat(retired); !os.IsNotExist(err) {
		t.Errorf("retired dir survived the pass (stat err = %v)", err)
	}
}

// The live set is the actors assigned to the workers on this node, and only
// this node: another node's workers hold actors whose directories live over
// there, and counting them here would say nothing about this disk.
func TestControlPlaneActorsLiveSetIsNodeScoped(t *testing.T) {
	client := &fakeControlClient{
		workers: []*ateapipb.Worker{
			{Metadata: &ateapipb.ResourceMetadata{Name: "worker-here"}, NodeName: "node-1"},
			{Metadata: &ateapipb.ResourceMetadata{Name: "worker-elsewhere"}, NodeName: "node-2"},
		},
		assignments: map[string][]string{
			"worker-here":      {"actor-a", "actor-b"},
			"worker-elsewhere": {"actor-c"},
		},
	}

	live, err := (&controlPlaneActors{client: client, nodeName: "node-1"}).liveActorUIDs(context.Background())
	if err != nil {
		t.Fatalf("liveActorUIDs: %v", err)
	}
	want := map[string]bool{"actor-a": true, "actor-b": true}
	if len(live) != len(want) {
		t.Fatalf("live set = %v, want %v", live, want)
	}
	for uid := range want {
		if !live[uid] {
			t.Errorf("live set %v is missing %q", live, uid)
		}
	}
}

// A listing that fails partway fails the whole call: a half-read set looks
// exactly like a set of actors that no longer exist.
func TestControlPlaneActorsFailsClosedOnAssignmentError(t *testing.T) {
	client := &fakeControlClient{
		workers:       []*ateapipb.Worker{{Metadata: &ateapipb.ResourceMetadata{Name: "worker-here"}, NodeName: "node-1"}},
		assignmentErr: errors.New("db is down"),
	}

	if _, err := (&controlPlaneActors{client: client, nodeName: "node-1"}).liveActorUIDs(context.Background()); err == nil {
		t.Fatal("liveActorUIDs() = nil error, want the listing failure reported")
	}
}

// fakeControlClient answers the two Control API reads the live set is built
// from and nothing else.
type fakeControlClient struct {
	ateapipb.ControlClient
	workers       []*ateapipb.Worker
	assignments   map[string][]string
	assignmentErr error
}

func (f *fakeControlClient) ListWorkers(context.Context, *ateapipb.ListWorkersRequest, ...grpc.CallOption) (*ateapipb.ListWorkersResponse, error) {
	return &ateapipb.ListWorkersResponse{Workers: f.workers}, nil
}

func (f *fakeControlClient) ListWorkerActorAssignments(_ context.Context, req *ateapipb.ListWorkerActorAssignmentsRequest, _ ...grpc.CallOption) (*ateapipb.ListWorkerActorAssignmentsResponse, error) {
	if f.assignmentErr != nil {
		return nil, f.assignmentErr
	}
	resp := &ateapipb.ListWorkerActorAssignmentsResponse{}
	for _, uid := range f.assignments[req.GetWorker().GetName()] {
		resp.ActorAssignments = append(resp.ActorAssignments, &ateapipb.ActorAssignment{ActorUid: uid})
	}
	return resp, nil
}
