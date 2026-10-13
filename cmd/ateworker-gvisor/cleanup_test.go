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
	"maps"
	"slices"
	"testing"

	"go.ate.dev/substrate/internal/proto/ateworkerpb"
)

// fakeRunsc stands in for *runsc. containers is the set runsc has a record of;
// calls logs every command.
type fakeRunsc struct {
	containers map[string]bool
	calls      []string
	// deleteFailures makes cmdDelete fail that many times for a container;
	// a failure with removeOnFailure set still drops the record, like a
	// delete killed after its work.
	deleteFailures  map[string]int
	removeOnFailure bool
}

func newFakeRunsc(containers ...string) *fakeRunsc {
	f := &fakeRunsc{containers: map[string]bool{}}
	for _, name := range containers {
		f.containers[name] = true
	}
	return f
}

func (f *fakeRunsc) cmdState(_ context.Context, name string) error {
	f.calls = append(f.calls, "state "+name)
	if !f.containers[name] {
		return errors.New("exit status 128")
	}
	return nil
}

func (f *fakeRunsc) cmdDelete(_ context.Context, name string) error {
	f.calls = append(f.calls, "delete "+name)
	if !f.containers[name] {
		return errors.New("exit status 128")
	}
	if f.deleteFailures[name] > 0 {
		f.deleteFailures[name]--
		if f.removeOnFailure {
			delete(f.containers, name)
		}
		return errors.New("signal: killed")
	}
	delete(f.containers, name)
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

func (f *fakeRunsc) cmdList(context.Context) ([]string, error) {
	f.calls = append(f.calls, "list")
	return slices.Sorted(maps.Keys(f.containers)), nil
}

func assertCalls(t *testing.T, f *fakeRunsc, want ...string) {
	t.Helper()
	if !slices.Equal(f.calls, want) {
		t.Errorf("runsc calls = %v, want %v", f.calls, want)
	}
}

var appContainers = []*ateworkerpb.Container{{Name: "app"}}

func TestCleanupContainers_DeletesEveryContainerRunscKnows(t *testing.T) {
	f := newFakeRunsc("app", "_pause")

	if err := cleanupContainers(context.Background(), f, appContainers); err != nil {
		t.Fatalf("cleanupContainers: %v", err)
	}

	// Happy path: no `runsc list` at all.
	assertCalls(t, f, "state app", "state _pause", "delete app", "delete _pause")
	if len(f.containers) != 0 {
		t.Errorf("containers survived cleanup: %v", slices.Sorted(maps.Keys(f.containers)))
	}
	if err := cleanupContainers(context.Background(), f, appContainers); err != nil {
		t.Fatalf("repeated cleanupContainers: %v", err)
	}
}

func TestCleanupContainers_SkipsContainersRunscHasNoRecordOf(t *testing.T) {
	f := newFakeRunsc("app")

	if err := cleanupContainers(context.Background(), f, appContainers); err != nil {
		t.Fatalf("cleanupContainers: %v", err)
	}

	assertCalls(t, f, "state app", "state _pause", "list", "delete app")
}

func TestCleanupContainers_SucceedsWhenEverythingIsAlreadyGone(t *testing.T) {
	f := newFakeRunsc()

	if err := cleanupContainers(context.Background(), f, appContainers); err != nil {
		t.Fatalf("cleanupContainers: %v", err)
	}

	assertCalls(t, f, "state app", "list", "state _pause", "list")
}

// The pause container is the sandbox: it must outlive the deletes of the
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

func TestCleanupContainers_RetriesAFailedDelete(t *testing.T) {
	f := newFakeRunsc("app", "_pause")
	f.deleteFailures = map[string]int{"_pause": 1}

	if err := cleanupContainers(context.Background(), f, []*ateworkerpb.Container{{Name: "app"}}); err != nil {
		t.Fatalf("cleanupContainers: %v", err)
	}
	if got := countCalls(f.calls, "delete "+"_pause"); got != 2 {
		t.Errorf("pause container deleted %d times, want 2 (one retry)", got)
	}
	if len(f.containers) != 0 {
		t.Errorf("containers left: %v", f.containers)
	}
}

func TestCleanupContainers_AcceptsAFailedDeleteThatRemovedTheContainer(t *testing.T) {
	f := newFakeRunsc("_pause")
	f.deleteFailures = map[string]int{"_pause": 1}
	f.removeOnFailure = true

	if err := cleanupContainers(context.Background(), f, nil); err != nil {
		t.Fatalf("cleanupContainers: %v", err)
	}
	if got := countCalls(f.calls, "delete "+"_pause"); got != 1 {
		t.Errorf("pause container deleted %d times, want 1 (gone after the failure)", got)
	}
}

func TestCleanupContainers_ReportsADeleteThatFailsTwice(t *testing.T) {
	f := newFakeRunsc("_pause")
	f.deleteFailures = map[string]int{"_pause": 2}

	if err := cleanupContainers(context.Background(), f, nil); err == nil {
		t.Fatal("cleanupContainers succeeded with a container runsc could not delete")
	}
}

func countCalls(calls []string, want string) int {
	n := 0
	for _, c := range calls {
		if c == want {
			n++
		}
	}
	return n
}
