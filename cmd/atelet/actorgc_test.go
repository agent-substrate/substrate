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
	"maps"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/cmd/atelet/internal/ateletpath"
	"github.com/agent-substrate/substrate/internal/nodepath"
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
	dir := ateletpath.ActorPath(uid)
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

// seedVolume puts an external volume's contents under an actor's directory at
// volumes/<name>, leaving the directory's age as it was.
func seedVolume(t *testing.T, dir, name string) {
	t.Helper()
	info, err := os.Stat(dir)
	if err != nil {
		t.Fatalf("seeding a volume under %s: %v", dir, err)
	}
	vol := filepath.Join(dir, "volumes", name)
	if err := os.MkdirAll(vol, 0o755); err != nil {
		t.Fatalf("seeding %s: %v", vol, err)
	}
	if err := os.WriteFile(filepath.Join(vol, "file"), []byte("volume contents"), 0o600); err != nil {
		t.Fatalf("seeding %s: %v", vol, err)
	}
	if err := os.Chtimes(dir, info.ModTime(), info.ModTime()); err != nil {
		t.Fatalf("aging %s: %v", dir, err)
	}
}

func newTestActorGC(t *testing.T, live liveActorLister, resident func() []string) *actorGC {
	t.Helper()
	return &actorGC{
		actorsDir:   nodepath.ActorsDir,
		live:        live,
		resident:    resident,
		mountPoints: func() ([]string, error) { return nil, nil },
		minAge:      10 * time.Minute,
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
		// mounts are the mount points atelet sees, relative to the actors
		// dir; mountsErr is the failure to read them.
		mounts    []string
		mountsErr error
		dryRun    bool
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
				snap := ateletpath.LocalSnapshotDir(orphan, "pause-1")
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
			// An external volume whose unmount failed is still mounted there,
			// and atelet sees it once it restarts. Deleting the directory
			// would delete the volume's contents.
			name: "an orphan with a mount under it is kept",
			seed: func(t *testing.T) []string {
				dir := seedActorDir(t, orphan, time.Hour)
				seedVolume(t, dir, "data")
				return []string{dir}
			},
			mounts: []string{orphan + "/volumes/data"},
		},
		{
			// Mounts match on whole path components, so another actor's
			// volume keeps only that actor.
			name: "a mount under another actor's directory keeps only that one",
			seed: func(t *testing.T) []string {
				seedActorDir(t, orphan, time.Hour)
				return []string{seedActorDir(t, orphan+"-2", time.Hour)}
			},
			mounts: []string{orphan + "-2/volumes/data"},
		},
		{
			// Without the mount table, a mounted volume looks like any other
			// directory, so the pass deletes nothing at all.
			name: "nothing is swept when the mount table cannot be read",
			seed: func(t *testing.T) []string {
				return []string{seedActorDir(t, orphan, time.Hour)}
			},
			mountsErr: errors.New("permission denied"),
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
			if err := os.MkdirAll(nodepath.ActorsDir, 0o755); err != nil {
				t.Fatalf("creating the actors dir: %v", err)
			}
			keep := tt.seed(t)

			gc := newTestActorGC(t, tt.live, func() []string { return tt.resident })
			gc.dryRun = tt.dryRun
			gc.mountPoints = func() ([]string, error) {
				if tt.mountsErr != nil {
					return nil, tt.mountsErr
				}
				var mounts []string
				for _, m := range tt.mounts {
					mounts = append(mounts, filepath.Join(nodepath.ActorsDir, m))
				}
				return mounts, nil
			}
			gc.runPass(t.Context())

			entries, err := os.ReadDir(nodepath.ActorsDir)
			if err != nil {
				t.Fatalf("reading the actors dir: %v", err)
			}
			var left []string
			for _, e := range entries {
				left = append(left, filepath.Join(nodepath.ActorsDir, e.Name()))
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
// pass finishes it without consulting the control plane.
func TestActorGCFinishesRetiredDirs(t *testing.T) {
	useTempNodeDirs(t)
	if err := os.MkdirAll(nodepath.ActorsDir, 0o755); err != nil {
		t.Fatalf("creating the actors dir: %v", err)
	}
	retired := filepath.Join(nodepath.ActorsDir, retiredActorPrefix+"actor-1-12345")
	if err := os.MkdirAll(filepath.Join(retired, "durable-dir"), 0o755); err != nil {
		t.Fatalf("seeding %s: %v", retired, err)
	}

	newTestActorGC(t, fakeLiveActors{}, nil).runPass(t.Context())

	if _, err := os.Stat(retired); !os.IsNotExist(err) {
		t.Errorf("retired dir survived the pass (stat err = %v)", err)
	}
}

// Retired debris gets the same mount check as any other directory: a volume
// still mounted under it holds data that is not the actor's to lose.
func TestActorGCKeepsRetiredDirWithAMount(t *testing.T) {
	useTempNodeDirs(t)
	if err := os.MkdirAll(nodepath.ActorsDir, 0o755); err != nil {
		t.Fatalf("creating the actors dir: %v", err)
	}
	retired := filepath.Join(nodepath.ActorsDir, retiredActorPrefix+"actor-1-12345")
	if err := os.MkdirAll(retired, 0o755); err != nil {
		t.Fatalf("seeding %s: %v", retired, err)
	}
	seedVolume(t, retired, "data")

	gc := newTestActorGC(t, fakeLiveActors{}, nil)
	gc.mountPoints = func() ([]string, error) {
		return []string{filepath.Join(retired, "volumes", "data")}, nil
	}
	gc.runPass(t.Context())

	if _, err := os.Stat(filepath.Join(retired, "volumes", "data", "file")); err != nil {
		t.Errorf("the volume's contents did not survive the pass: %v", err)
	}
}

func TestParseMountPoints(t *testing.T) {
	mountinfo := "22 1 8:1 / / rw,relatime shared:1 - ext4 /dev/sda1 rw\n" +
		"1021 22 8:1 /var/lib/ate /var/lib/ate rw,relatime - ext4 /dev/sda1 rw\n" +
		`1077 1021 0:52 / /var/lib/ate/actors/uid-1/volumes/my\040data rw,relatime - nfs4 srv:/export rw` + "\n" +
		"truncated line\n"
	want := []string{"/", "/var/lib/ate", "/var/lib/ate/actors/uid-1/volumes/my data"}
	if got := parseMountPoints(mountinfo); !slices.Equal(got, want) {
		t.Errorf("parseMountPoints() = %q, want %q", got, want)
	}
}

// The live set is what the control plane lists for this node, read in one
// call per pass. Scoping is not atelet's to get wrong: the control plane takes
// the node from atelet's certificate.
func TestControlPlaneActorsLiveSet(t *testing.T) {
	client := &fakeNodeActorsClient{uids: []string{"actor-a", "actor-b"}}

	live, err := (&controlPlaneActors{client: client}).liveActorUIDs(context.Background())
	if err != nil {
		t.Fatalf("liveActorUIDs: %v", err)
	}
	if want := map[string]bool{"actor-a": true, "actor-b": true}; !maps.Equal(live, want) {
		t.Errorf("live set = %v, want %v", live, want)
	}
	if client.calls != 1 {
		t.Errorf("ListNodeActorUIDs called %d times, want once per pass", client.calls)
	}
	// Passes are serialized, so a call that never returns would stall every
	// pass after it.
	if !client.hadDeadline {
		t.Error("ListNodeActorUIDs was called without a deadline")
	}
}

// A failed call fails the live set: an empty set looks exactly like a node
// with no actors placed on it.
func TestControlPlaneActorsFailsClosedOnRPCError(t *testing.T) {
	client := &fakeNodeActorsClient{uids: []string{"actor-a"}, err: errors.New("worker cache not ready")}

	if live, err := (&controlPlaneActors{client: client}).liveActorUIDs(context.Background()); err == nil {
		t.Fatalf("liveActorUIDs() = %v, nil error; want the RPC failure reported", live)
	}
}

// fakeNodeActorsClient answers the one WorkerService read the live set is
// built from.
type fakeNodeActorsClient struct {
	uids        []string
	err         error
	calls       int
	hadDeadline bool
}

func (f *fakeNodeActorsClient) ListNodeActorUIDs(ctx context.Context, _ *ateapipb.ListNodeActorUIDsRequest, _ ...grpc.CallOption) (*ateapipb.ListNodeActorUIDsResponse, error) {
	f.calls++
	_, f.hadDeadline = ctx.Deadline()
	if f.err != nil {
		return nil, f.err
	}
	return &ateapipb.ListNodeActorUIDsResponse{ActorUids: f.uids}, nil
}
