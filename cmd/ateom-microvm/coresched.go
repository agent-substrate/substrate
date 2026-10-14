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
	"log/slog"
	"runtime"
	"unsafe"

	"golang.org/x/sys/unix"
)

// cloud-hypervisor gives each VM a core-scheduling cookie as its vCPUs start
// (core_scheduling=vm, its default), so no other VM shares a physical core
// with them. The kernel only schedules by cookie while some task holds one:
// creating the node's first cookie enables core scheduling, which includes a
// synchronize_rcu(), and dropping its last one disables it again. Each VMM
// exits at suspend, so on a node with no other running VM every restore paid
// for that enable: ~19ms of vm_restore on the counter-microvm demo.
//
// keepCoreSchedulingEnabled holds a cookie of its own for the life of the
// process, on an OS thread that does nothing else, so core scheduling stays
// enabled and a VMM's cookie is cheap to create. The idle thread's cookie
// constrains only that thread, which never runs.
//
// It returns the holding thread's id, or the error from creating the cookie:
// ENODEV without SMT and EINVAL without kernel support, where cloud-hypervisor
// gets no core scheduling either.
func keepCoreSchedulingEnabled(ctx context.Context) (int, error) {
	type result struct {
		tid int
		err error
	}
	done := make(chan result, 1)
	go func() {
		// Never unlocked: the goroutine parks below forever and keeps its
		// thread, and with it the cookie. On error, returning while locked
		// ends the thread.
		runtime.LockOSThread()
		err := unix.Prctl(unix.PR_SCHED_CORE, unix.PR_SCHED_CORE_CREATE, 0, unix.PR_SCHED_CORE_SCOPE_THREAD, 0)
		done <- result{tid: unix.Gettid(), err: err}
		if err != nil {
			return
		}
		select {}
	}()
	r := <-done
	if r.err != nil {
		level := slog.LevelWarn
		if errors.Is(r.err, unix.ENODEV) || errors.Is(r.err, unix.EINVAL) {
			level = slog.LevelInfo
		}
		slog.Log(ctx, level, "Not holding a core-scheduling cookie; VM restores may pay to enable core scheduling", slog.Any("err", r.err))
		return 0, r.err
	}
	return r.tid, nil
}

// coreSchedulingCookie returns the core-scheduling cookie of thread tid.
func coreSchedulingCookie(tid int) (uint64, error) {
	var cookie uint64
	err := unix.Prctl(unix.PR_SCHED_CORE, unix.PR_SCHED_CORE_GET, uintptr(tid), unix.PR_SCHED_CORE_SCOPE_THREAD, uintptr(unsafe.Pointer(&cookie)))
	return cookie, err
}
