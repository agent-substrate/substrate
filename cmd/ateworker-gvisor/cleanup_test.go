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
	"maps"
	"slices"
	"strings"
	"testing"

	"go.ate.dev/substrate/internal/proto/ateworkerpb"
)

// fakeRunsc stands in for *runsc. containers is the set runsc has a record of;
// calls logs every command.
type fakeRunsc struct {
	containers map[string]bool
	calls      []string
}

func newFakeRunsc(containers ...string) *fakeRunsc {
	f := &fakeRunsc{containers: map[string]bool{}}
	for _, name := range containers {
		f.containers[name] = true
	}
	return f
}

// cmdDelete succeeds for a container runsc has no record of, as
// runsc delete -force does.
func (f *fakeRunsc) cmdDelete(_ context.Context, names ...string) error {
	f.calls = append(f.calls, "delete "+strings.Join(names, " "))
	for _, name := range names {
		delete(f.containers, name)
	}
	return nil
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

// cleanupContainers deletes the application containers before the sandbox,
// with one runsc command, whatever runsc still has a record of.
func TestCleanupContainers(t *testing.T) {
	tests := []struct {
		name    string
		present []string
	}{
		{name: "every container present", present: []string{"app", "_sandbox"}},
		{name: "sandbox already gone", present: []string{"app"}},
		{name: "everything already gone"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newFakeRunsc(tc.present...)
			if err := cleanupContainers(context.Background(), f, appContainers); err != nil {
				t.Fatalf("cleanupContainers: %v", err)
			}
			assertCalls(t, f, "delete app _sandbox")
			if len(f.containers) != 0 {
				t.Errorf("containers survived cleanup: %v", slices.Sorted(maps.Keys(f.containers)))
			}
		})
	}
}

// The sandbox must outlive the deletes of the
// containers inside it, so stopping the actor leaves it to cleanup.
func TestStopContainersLeavesTheSandboxToCleanup(t *testing.T) {
	f := newFakeRunsc()
	stopContainers(context.Background(), f, appContainers)
	var want []string
	for _, c := range appContainers {
		want = append(want, "kill "+c.GetName()+" SIGKILL", "wait "+c.GetName())
	}
	assertCalls(t, f, want...)
}
