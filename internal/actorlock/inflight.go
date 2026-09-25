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

package actorlock

import (
	"context"
	"sync"
)

// InFlight tracks lifecycle RPCs by actor for shutdown cancellation and draining.
type InFlight struct {
	mu sync.Mutex
	// Callers must serialize RPCs for each actor with Locks.
	rpcs map[string]*rpc
	// Closed and replaced on each registry change to wake waiters.
	changed chan struct{}
}

type rpc struct {
	name string
	// Nil for an RPC that must run to completion.
	cancel context.CancelFunc
}

// NewInFlight returns an empty set.
func NewInFlight() *InFlight {
	return &InFlight{rpcs: map[string]*rpc{}, changed: make(chan struct{})}
}

// Add records an RPC for actorUID and returns the function that clears it.
// cancel is nil for an RPC that must finish, such as a checkpoint saving state.
func (f *InFlight) Add(actorUID, name string, cancel context.CancelFunc) func() {
	f.mu.Lock()
	f.rpcs[actorUID] = &rpc{name: name, cancel: cancel}
	f.notifyLocked()
	f.mu.Unlock()

	return func() {
		f.mu.Lock()
		defer f.mu.Unlock()
		delete(f.rpcs, actorUID)
		f.notifyLocked()
	}
}

// CancelStartups cancels startup RPCs and returns their actor UIDs.
func (f *InFlight) CancelStartups() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var canceled []string
	for actorUID, r := range f.rpcs {
		if r.cancel != nil {
			r.cancel()
			canceled = append(canceled, actorUID)
		}
	}
	return canceled
}

// WaitIdle waits for all RPCs to finish, returning false on cancellation.
func (f *InFlight) WaitIdle(ctx context.Context) bool {
	for {
		f.mu.Lock()
		if len(f.rpcs) == 0 {
			f.mu.Unlock()
			return true
		}
		changed := f.changed
		f.mu.Unlock()

		select {
		case <-changed:
		case <-ctx.Done():
			return false
		}
	}
}

// Names returns the RPC name for each active actor.
func (f *InFlight) Names() map[string]string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make(map[string]string, len(f.rpcs))
	for actorUID, r := range f.rpcs {
		out[actorUID] = r.name
	}
	return out
}

func (f *InFlight) notifyLocked() {
	close(f.changed)
	f.changed = make(chan struct{})
}
