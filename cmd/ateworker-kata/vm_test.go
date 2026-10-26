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
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"go.ate.dev/substrate/internal/proto/ateworkerpb"
)

func testVM(f *fakeShim, names ...string) *vm {
	v := &vm{shim: f, sandboxID: testUID}
	for i, name := range names {
		id := "c_" + name
		if i == 0 {
			id = testUID
		}
		v.containers = append(v.containers, vmContainer{name: name, id: id})
	}
	return v
}

// kills returns the signals the fake shim was sent, in order, as "container
// signal". The VM's wait runs in a goroutine, so the order of the other calls
// is not fixed.
func kills(f *fakeShim) []string {
	var out []string
	for _, call := range f.log() {
		if strings.HasPrefix(call, "Kill ") {
			out = append(out, strings.TrimPrefix(call, "Kill "))
		}
	}
	return out
}

func TestStop(t *testing.T) {
	tests := []struct {
		name string
		// endOn are the signals that end the VM.
		endOn []syscall.Signal
		// grace is how long the VM gets before it is killed.
		grace     time.Duration
		wantErr   bool
		wantKills []string
	}{
		{
			name:      "exits on SIGTERM",
			endOn:     []syscall.Signal{syscall.SIGTERM},
			grace:     time.Minute,
			wantKills: []string{testUID + " 15", "c_sidecar 15"},
		},
		{
			name:      "killed once the grace period is over",
			endOn:     []syscall.Signal{syscall.SIGKILL},
			grace:     20 * time.Millisecond,
			wantKills: []string{testUID + " 15", "c_sidecar 15", testUID + " 9", "c_sidecar 9"},
		},
		{
			name:      "survives SIGKILL",
			grace:     20 * time.Millisecond,
			wantErr:   true,
			wantKills: []string{testUID + " 15", "c_sidecar 15", testUID + " 9", "c_sidecar 9"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFakeShim(tt.endOn...)
			err := testVM(f, "app", "sidecar").stop(t.Context(), time.Now().Add(tt.grace), 50*time.Millisecond)
			if (err != nil) != tt.wantErr {
				t.Fatalf("stop() = %v, want error %v", err, tt.wantErr)
			}
			if diff := cmp.Diff(tt.wantKills, kills(f)); diff != "" {
				t.Errorf("signals mismatch (-want +got):\n%s", diff)
			}
			if !slices.Contains(f.log(), "Wait "+testUID) {
				t.Errorf("the VM was not waited for: %v", f.log())
			}
		})
	}
}

func TestStopStopsWhenTheCallerGivesUp(t *testing.T) {
	f := newFakeShim()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	err := testVM(f, "app").stop(ctx, time.Now().Add(time.Hour), time.Second)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("stop() = %v, want the context's error", err)
	}
	for _, kill := range kills(f) {
		if strings.HasSuffix(kill, " 9") {
			t.Errorf("stop() sent SIGKILL after the caller gave up: %v", kills(f))
		}
	}
}

func TestWaitGone(t *testing.T) {
	t.Run("the container exits", func(t *testing.T) {
		f := newFakeShim()
		f.end()
		if err := testVM(f, "app").waitGone(t.Context()); err != nil {
			t.Errorf("waitGone() = %v", err)
		}
	})
	t.Run("the shim exits", func(t *testing.T) {
		f := newFakeShim()
		_ = f.Shutdown(t.Context(), testUID, true)
		if err := testVM(f, "app").waitGone(t.Context()); err != nil {
			t.Errorf("waitGone() = %v", err)
		}
	})
	t.Run("the caller gives up", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
		defer cancel()
		if err := testVM(newFakeShim(), "app").waitGone(ctx); !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("waitGone() = %v, want the deadline", err)
		}
	})
}

func TestSignalAllJoinsErrors(t *testing.T) {
	f := newFakeShim()
	f.killErrOn = map[syscall.Signal]error{syscall.SIGTERM: errors.New("not found")}
	err := testVM(f, "app", "sidecar").signalAll(t.Context(), syscall.SIGTERM)
	if err == nil || strings.Count(err.Error(), "not found") != 2 {
		t.Fatalf("signalAll() = %v, want one error per container", err)
	}
	if got := len(f.log()); got != 2 {
		t.Errorf("signalAll() stopped at the first error: %d calls, want 2", got)
	}
}

// teardown ends everything in order, the containers last to first, and closes
// what the worker holds.
func TestTeardown(t *testing.T) {
	f := newFakeShim(syscall.SIGKILL)
	v := testVM(f, "app", "sidecar")
	v.bundle = t.TempDir()
	var closed []string
	for _, name := range []string{"first", "second"} {
		v.closers = append(v.closers, closerFunc(func() error { closed = append(closed, name); return nil }))
	}

	// /bin/true stands in for the shim binary, whose delete command removes
	// nothing here.
	if err := v.teardown(t.Context(), "/bin/true"); err != nil {
		t.Fatalf("teardown() = %v", err)
	}
	// Everything but the wait, which runs in a goroutine, is in order.
	var got []string
	for _, call := range f.log() {
		if !strings.HasPrefix(call, "Wait ") {
			got = append(got, call)
		}
	}
	want := []string{
		"Kill " + testUID + " 9",
		"Kill c_sidecar 9",
		"Delete c_sidecar",
		"Delete " + testUID,
		"Shutdown " + testUID + " now=true",
		"Close",
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("shim calls mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff([]string{"second", "first"}, closed); diff != "" {
		t.Errorf("closers mismatch (-want +got):\n%s", diff)
	}
}

// A VM this process never started is cleared by the actor UID alone.
func TestTeardownWithoutAShim(t *testing.T) {
	v := &vm{sandboxID: testUID, bundle: t.TempDir()}
	if err := v.teardown(t.Context(), "/bin/true"); err != nil {
		t.Fatalf("teardown() = %v", err)
	}
	if err := v.teardown(t.Context(), "/nonexistent/shim"); err == nil {
		t.Fatal("teardown() with no shim binary = nil, want the failed delete command")
	}
}

func TestHasVolumes(t *testing.T) {
	image := appContainer()
	image.ImageVolumeMounts = []*ateworkerpb.VolumeMount{{Name: "agent", MountPath: "/ate"}}
	tests := []struct {
		name       string
		containers []*ateworkerpb.Container
		want       bool
	}{
		{"none", []*ateworkerpb.Container{appContainer()}, false},
		{"durable", []*ateworkerpb.Container{appContainer(), volumeContainer()}, true},
		{"image volume", []*ateworkerpb.Container{image}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := hasVolumes(tt.containers); got != tt.want {
				t.Errorf("hasVolumes() = %v, want %v", got, tt.want)
			}
		})
	}
}
