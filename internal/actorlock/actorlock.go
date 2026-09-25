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

// Package actorlock serializes lifecycle RPCs per actor.
package actorlock

import (
	"context"
	"sync"
)

// CancelableMutex supports context cancellation while waiting for a lock.
type CancelableMutex struct {
	ch chan struct{}
}

// NewCancelableMutex returns an unlocked CancelableMutex.
func NewCancelableMutex() *CancelableMutex {
	ch := make(chan struct{}, 1)
	ch <- struct{}{}
	return &CancelableMutex{ch: ch}
}

// Lock acquires the mutex, blocking until it is free.
func (m *CancelableMutex) Lock() { <-m.ch }

// Unlock releases the mutex.
func (m *CancelableMutex) Unlock() { m.ch <- struct{}{} }

// LockContext returns false on cancellation without acquiring the mutex.
func (m *CancelableMutex) LockContext(ctx context.Context) bool {
	select {
	case <-m.ch:
		return true
	case <-ctx.Done():
		return false
	}
}

// Locks serializes lifecycle RPCs per actor.
type Locks struct {
	mu sync.Mutex
	// References include holders and waiters; unused entries are removed.
	held map[string]*heldLock
}

type heldLock struct {
	mutex *CancelableMutex
	refs  int
}

// New returns an empty set of per-actor locks.
func New() *Locks {
	return &Locks{held: map[string]*heldLock{}}
}

// Lock returns false on cancellation without acquiring the actor's lock.
func (l *Locks) Lock(ctx context.Context, actorUID string) bool {
	l.mu.Lock()
	entry, ok := l.held[actorUID]
	if !ok {
		entry = &heldLock{mutex: NewCancelableMutex()}
		l.held[actorUID] = entry
	}
	entry.refs++
	l.mu.Unlock()

	if entry.mutex.LockContext(ctx) {
		return true
	}
	l.release(actorUID, false)
	return false
}

// Unlock releases the named actor's lock.
func (l *Locks) Unlock(actorUID string) {
	l.release(actorUID, true)
}

// release drops a reference and removes unused entries.
func (l *Locks) release(actorUID string, wasHeld bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	entry, ok := l.held[actorUID]
	if !ok {
		return
	}
	if wasHeld {
		entry.mutex.Unlock()
	}
	entry.refs--
	if entry.refs <= 0 {
		delete(l.held, actorUID)
	}
}
