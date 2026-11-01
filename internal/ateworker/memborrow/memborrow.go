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

// Package memborrow lends snapshot writes the worker pod's unbooked, free
// memory for their page cache, and holds back placements that would grow into
// it until it is written back.
package memborrow

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"go.ate.dev/substrate/internal/ateworker/cgroup"
)

// margin is pod memory a write leaves free when it borrows.
const margin = 32 << 20

// Ledger is memory snapshot writes hold for their page cache beyond what their
// actors booked: capacity no actor has booked, that the pod also has free.
type Ledger struct {
	// Unbooked is the worker's reported memory capacity, for a pod memory
	// limit, less what its hosted actors booked.
	Unbooked func(podLimit int64) int64
	// PodMemory replaces cgroup.PodMemory in tests.
	PodMemory func() (limit, used int64, err error)

	mu    sync.Mutex
	bytes int64
	// returned is closed and replaced whenever bytes drops
	returned chan struct{}
}

func (l *Ledger) podMemory() (limit, used int64, err error) {
	if l.PodMemory != nil {
		return l.PodMemory()
	}
	return cgroup.PodMemory()
}

// Take borrows up to want. The caller passes what it got to Return once the
// pages it held are on disk. A nil Ledger lends nothing. Without a pod memory
// limit it returns all of want, there's no limit to run into.
func (l *Ledger) Take(want int64) int64 {
	if l == nil {
		return 0
	}
	limit, used, err := l.podMemory()
	if err != nil {
		return 0
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	n := want
	if limit != 0 {
		n = min(want, l.Unbooked(limit)-l.bytes, limit-used-margin)
	}
	if n <= 0 {
		return 0
	}
	l.bytes += n
	return n
}

// Return gives back n taken by Take.
func (l *Ledger) Return(n int64) {
	if l == nil || n <= 0 {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.bytes -= n
	if l.returned != nil {
		close(l.returned)
	}
	l.returned = make(chan struct{})
}

// Wait holds a newly hosted actor, already counted by Unbooked, until what is
// borrowed fits in what is still unbooked, so it doesn't grow into dirty page
// cache.
func (l *Ledger) Wait(ctx context.Context) error {
	if l == nil {
		return nil
	}
	logged := false
	for {
		l.mu.Lock()
		if l.bytes == 0 {
			l.mu.Unlock()
			return nil
		}
		limit, _, err := l.podMemory()
		if err != nil || limit == 0 || l.bytes <= l.Unbooked(limit) {
			l.mu.Unlock()
			return nil
		}
		if l.returned == nil {
			l.returned = make(chan struct{})
		}
		returned := l.returned
		borrowed := l.bytes
		l.mu.Unlock()
		if !logged {
			slog.InfoContext(ctx, "Waiting for snapshot writeback before booting", slog.Int64("borrowedBytes", borrowed))
			logged = true
		}
		select {
		case <-returned:
		case <-ctx.Done():
			return fmt.Errorf("while waiting for snapshot writeback: %w", ctx.Err())
		}
	}
}
