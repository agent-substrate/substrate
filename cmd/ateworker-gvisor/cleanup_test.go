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
	"maps"
	"slices"
	"testing"

	"go.ate.dev/substrate/internal/proto/ateworkerpb"
)

// fakeRunsc stands in for *runsc. containers is the set runsc has a record of;
// calls logs every command.
type fakeRunsc struct {
	containers          map[string]bool
	calls               []string
	stateErrors         map[string]error
	deleteErrors        map[string]error
	listError           error
	removeOnDeleteError bool
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
	if err := f.stateErrors[name]; err != nil {
		return err
	}
	if !f.containers[name] {
		return errors.New("exit status 128")
	}
	return nil
}

func (f *fakeRunsc) cmdDelete(_ context.Context, name string) error {
	f.calls = append(f.calls, "delete "+name)
	if err := f.deleteErrors[name]; err != nil {
		if f.removeOnDeleteError {
			delete(f.containers, name)
		}
		return err
	}
	if !f.containers[name] {
		return errors.New("exit status 128")
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
	return slices.Sorted(maps.Keys(f.containers)), f.listError
}

func assertCalls(t *testing.T, f *fakeRunsc, want ...string) {
	t.Helper()
	if !slices.Equal(f.calls, want) {
		t.Errorf("runsc calls = %v, want %v", f.calls, want)
	}
}

var appContainers = []*ateworkerpb.Container{{Name: "app"}}

func TestCleanupContainers_DeletesEveryContainerRunscKnows(t *testing.T) {
	f := newFakeRunsc("app", "_sandbox")

	if err := cleanupContainers(context.Background(), f, appContainers); err != nil {
		t.Fatalf("cleanupContainers: %v", err)
	}

	// Happy path: no `runsc list` at all.
	assertCalls(t, f, "state app", "state _sandbox", "delete app", "delete _sandbox")
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

	assertCalls(t, f, "state app", "state _sandbox", "list", "delete app")
}

func TestCleanupContainers_SucceedsWhenEverythingIsAlreadyGone(t *testing.T) {
	f := newFakeRunsc()

	if err := cleanupContainers(context.Background(), f, appContainers); err != nil {
		t.Fatalf("cleanupContainers: %v", err)
	}

	assertCalls(t, f, "state app", "list", "state _sandbox", "list")
}

func TestCleanupContainers_ContinuesAfterApplicationErrors(t *testing.T) {
	stateErr := errors.New("state failed")
	listErr := errors.New("list failed")
	deleteErr := errors.New("delete failed")
	containers := []*ateworkerpb.Container{{Name: "app"}, {Name: "sidecar"}}
	for _, tc := range []struct {
		name                string
		stateError          error
		listError           error
		deleteError         error
		removeOnDeleteError bool
	}{
		{name: "state error", stateError: stateErr},
		{name: "state and list errors", stateError: stateErr, listError: listErr},
		{name: "delete error", deleteError: deleteErr},
		{name: "record removed before delete error", deleteError: deleteErr, removeOnDeleteError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFakeRunsc("app", "sidecar", "_sandbox")
			f.stateErrors = map[string]error{"app": tc.stateError}
			f.deleteErrors = map[string]error{"app": tc.deleteError}
			f.listError = tc.listError
			f.removeOnDeleteError = tc.removeOnDeleteError
			err := cleanupContainers(context.Background(), f, containers)
			for _, want := range []error{tc.stateError, tc.listError, tc.deleteError} {
				if want != nil && !errors.Is(err, want) {
					t.Errorf("cleanupContainers = %v, want %v", err, want)
				}
			}
			for _, name := range []string{"sidecar", "_sandbox"} {
				if f.containers[name] {
					t.Errorf("%s survived application cleanup error; calls: %v", name, f.calls)
				}
			}
			if got := f.calls[len(f.calls)-1]; got != "delete _sandbox" {
				t.Errorf("last call = %q, want sandbox deletion", got)
			}
			if tc.removeOnDeleteError {
				if err := cleanupContainers(context.Background(), f, containers); err != nil {
					t.Errorf("retry after records were removed: %v", err)
				}
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
