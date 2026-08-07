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
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

// TestIdleFlusherFiresAfterIdle asserts the watcher flushes once the idle
// timeout elapses with no activity, which is the core of the #503 mitigation.
func TestIdleFlusherFiresAfterIdle(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var flushes atomic.Int64
	done := make(chan struct{})
	go func() {
		idleFlusher(ctx, 20*time.Millisecond, make(chan struct{}), func(context.Context) {
			flushes.Add(1)
			select {
			case <-done:
			default:
				close(done)
			}
		})
	}()

	select {
	case <-done:
		if got := flushes.Load(); got < 1 {
			t.Fatalf("expected at least one flush, got %d", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("idle flush did not fire within deadline")
	}
}

// TestIdleFlusherResetsOnActivity asserts that a steady stream of activity
// keeps the timer from firing: telemetry is only flushed once traffic stops.
func TestIdleFlusherResetsOnActivity(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var flushes atomic.Int64
	activity := make(chan struct{})
	go idleFlusher(ctx, 50*time.Millisecond, activity, func(context.Context) {
		flushes.Add(1)
	})

	// Keep it busy for ~250ms with pokes every 10ms (< the 50ms timeout).
	deadline := time.After(250 * time.Millisecond)
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
busy:
	for {
		select {
		case <-deadline:
			break busy
		case <-tick.C:
			activity <- struct{}{}
		}
	}
	if got := flushes.Load(); got != 0 {
		t.Fatalf("expected no flush while busy, got %d", got)
	}

	// Now go quiet and confirm a flush lands.
	time.Sleep(150 * time.Millisecond)
	if got := flushes.Load(); got < 1 {
		t.Fatalf("expected a flush after going idle, got %d", got)
	}
}

// TestIdleFlusherDisabled asserts that timeout <= 0 never flushes, which is how
// the demo reproduces the telemetry loss (nothing flushed before suspend).
func TestIdleFlusherDisabled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var flushes atomic.Int64
	activity := make(chan struct{}, 1)
	go idleFlusher(ctx, 0, activity, func(context.Context) {
		flushes.Add(1)
	})

	activity <- struct{}{}
	time.Sleep(100 * time.Millisecond)
	if got := flushes.Load(); got != 0 {
		t.Fatalf("expected no flush when disabled, got %d", got)
	}
}

// TestActorNameFromIdentityFile asserts actorName reads the per-actor name
// fresh from the bind-mounted identity file, and falls back to "unknown" when
// the file is absent (rather than returning the useless "runsc" hostname).
func TestActorNameFromIdentityFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "actor-id")
	orig := identityFile
	identityFile = path
	t.Cleanup(func() { identityFile = orig })

	// Missing file -> "unknown".
	if got := actorName(); got != "unknown" {
		t.Fatalf("actorName() with no file = %q, want %q", got, "unknown")
	}

	// Present file (raw name, no trailing newline) -> that name. A trailing
	// newline, if present, must be trimmed.
	if err := os.WriteFile(path, []byte("my-telemetry-actor-1\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if got := actorName(); got != "my-telemetry-actor-1" {
		t.Fatalf("actorName() = %q, want %q", got, "my-telemetry-actor-1")
	}
}
