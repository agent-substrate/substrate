//go:build linux

// Copyright 2026 The Agent Substrate Authors
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
	"strings"
	"testing"

	"go.ate.dev/substrate/internal/proto/ateworkerpb"
)

// fakeRunsc stands in for *runsc. calls logs every command, and cmdDelete
// returns deleteErr.
type fakeRunsc struct {
	calls     []string
	deleteErr error
}

func (f *fakeRunsc) cmdDelete(_ context.Context, names ...string) error {
	f.calls = append(f.calls, "delete "+strings.Join(names, " "))
	return f.deleteErr
}

func (f *fakeRunsc) cmdKill(_ context.Context, name, signal string) error {
	f.calls = append(f.calls, "kill "+name+" "+signal)
	return nil
}

func (f *fakeRunsc) cmdWait(_ context.Context, name string) error {
	f.calls = append(f.calls, "wait "+name)
	return nil
}

func assertCalls(t *testing.T, f *fakeRunsc, want ...string) {
	t.Helper()
	if !slices.Equal(f.calls, want) {
		t.Errorf("runsc calls = %v, want %v", f.calls, want)
	}
}

var appContainers = []*ateworkerpb.Container{{Name: "app"}}

// teardownContainers deletes the application containers before the sandbox,
// with one runsc command.
func TestTeardownContainers(t *testing.T) {
	for _, tc := range []struct {
		name      string
		deleteErr error
		// leaf, if set, holds the files the sandbox's cgroup leaf starts with.
		leaf map[string]string
		// wantKilled is what cgroup.kill holds afterwards.
		wantKilled string
		// wantReleased is whether runsc's records are dropped.
		wantReleased bool
		// wantAppLeafRemoved is whether the app container's empty leaf is
		// removed.
		wantAppLeafRemoved bool
		// wantCkptKilled is what the checkpoint cgroup's cgroup.kill holds
		// afterwards.
		wantCkptKilled string
		wantErr        bool
	}{{
		name:         "deleted",
		leaf:         map[string]string{"cgroup.kill": ""},
		wantReleased: true,
	}, {
		name:               "delete failed, no sandbox leaf",
		deleteErr:          errors.New("exit status 128"),
		wantReleased:       true,
		wantAppLeafRemoved: true,
		wantCkptKilled:     "1",
	}, {
		// A missing cgroup.events fails the wait for the leaf to empty, as a
		// sandbox that does not die would. The app leaf is still removed.
		name:               "delete and kill failed",
		deleteErr:          errors.New("exit status 128"),
		leaf:               map[string]string{"cgroup.kill": ""},
		wantKilled:         "1",
		wantAppLeafRemoved: true,
		wantCkptKilled:     "1",
		wantErr:            true,
	}} {
		t.Run(tc.name, func(t *testing.T) {
			s := &AteWorkerService{cgroupRoot: t.TempDir()}
			leaf := s.sandboxLeaf(testActor.UID)
			if tc.leaf != nil {
				if err := os.Mkdir(leaf, 0o755); err != nil {
					t.Fatal(err)
				}
				writeCgroupFiles(t, leaf, tc.leaf)
			}
			appLeaf := s.containerLeaf(testActor.UID, "app")
			if err := os.Mkdir(appLeaf, 0o755); err != nil {
				t.Fatal(err)
			}
			// The checkpoint cgroup of a checkpoint in progress, which empties
			// once killed.
			ckpt := checkpointCgroupDir(s.cgroupRoot, testActor.UID)
			if err := os.Mkdir(ckpt, 0o755); err != nil {
				t.Fatal(err)
			}
			writeCgroupFiles(t, ckpt, map[string]string{"cgroup.kill": "", "cgroup.events": "populated 0\n"})
			actorDirs := &ateworkerpb.ActorDirs{RootDir: t.TempDir(), OciBundleDir: t.TempDir()}
			record := filepath.Join(runscStateDir(actorDirs), "app")
			if err := os.MkdirAll(record, 0o700); err != nil {
				t.Fatal(err)
			}

			f := &fakeRunsc{deleteErr: tc.deleteErr}
			err := s.teardownContainers(context.Background(), f, testActor.UID, actorDirs, appContainers)
			if (err != nil) != tc.wantErr {
				t.Fatalf("teardownContainers() = %v, want error %t", err, tc.wantErr)
			}
			assertCalls(t, f, "delete app _sandbox")
			if tc.leaf != nil {
				if got, err := os.ReadFile(filepath.Join(leaf, "cgroup.kill")); err != nil || string(got) != tc.wantKilled {
					t.Errorf("cgroup.kill = %q, %v; want %q", got, err, tc.wantKilled)
				}
			}
			if _, err := os.Stat(record); os.IsNotExist(err) != tc.wantReleased {
				t.Errorf("runsc record stat error = %v, want released %t", err, tc.wantReleased)
			}
			if _, err := os.Stat(appLeaf); os.IsNotExist(err) != tc.wantAppLeafRemoved {
				t.Errorf("app leaf stat error = %v, want removed %t", err, tc.wantAppLeafRemoved)
			}
			if got, err := os.ReadFile(filepath.Join(ckpt, "cgroup.kill")); err != nil || string(got) != tc.wantCkptKilled {
				t.Errorf("checkpoint cgroup.kill = %q, %v; want %q", got, err, tc.wantCkptKilled)
			}
		})
	}
}
