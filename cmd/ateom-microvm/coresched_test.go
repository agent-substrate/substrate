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
	"errors"
	"runtime"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestKeepCoreSchedulingEnabledHoldsCookie(t *testing.T) {
	tid, err := keepCoreSchedulingEnabled(t.Context())
	if err != nil {
		// Without SMT or kernel support there is no core scheduling to keep on.
		if !errors.Is(err, unix.ENODEV) && !errors.Is(err, unix.EINVAL) {
			t.Fatalf("keepCoreSchedulingEnabled() error = %v, want nil, ENODEV or EINVAL", err)
		}
		return
	}
	// The thread must keep its cookie after the call returns, through whatever
	// the runtime does with idle threads meanwhile.
	runtime.GC()
	time.Sleep(50 * time.Millisecond)
	cookie, err := coreSchedulingCookie(tid)
	if err != nil {
		t.Fatalf("reading the cookie of holder thread %d: %v", tid, err)
	}
	if cookie == 0 {
		t.Fatalf("holder thread %d has no core-scheduling cookie", tid)
	}
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	if mine, err := coreSchedulingCookie(unix.Gettid()); err != nil || mine == cookie {
		t.Errorf("test thread cookie = %#x (err %v), want it unaffected by the holder's %#x", mine, err, cookie)
	}
}
