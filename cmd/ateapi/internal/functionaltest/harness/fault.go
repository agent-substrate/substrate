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

package harness

import (
	"context"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// gateTimeout bounds how long Gate.Wait waits for the blocked call to arrive.
const gateTimeout = 10 * time.Second

// Fault decides how the calls to one operation of a fake behave. Its faults
// apply to the next calls, one each, in the order they were added. A call with
// no fault left succeeds.
type Fault struct {
	mu    sync.Mutex
	queue []*action
}

// action is one queued fault: fail with err, hold the call at gate, or hold
// its reply at reply.
type action struct {
	err   error
	gate  *Gate
	reply *Gate
}

// FailNext fails the next call with err, without applying its effect.
func (f *Fault) FailNext(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.queue = append(f.queue, &action{err: err})
}

// Block holds the next call until the returned Gate is released.
func (f *Fault) Block() *Gate {
	g := &Gate{arrived: make(chan struct{}, 1), released: make(chan struct{}, 1)}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.queue = append(f.queue, &action{gate: g})
	return g
}

// HoldReply lets the next call take effect, then holds its reply until the
// returned Gate is released, as when the reply is slow to reach the caller.
func (f *Fault) HoldReply() *Gate {
	g := &Gate{arrived: make(chan struct{}, 1), released: make(chan struct{}, 1)}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.queue = append(f.queue, &action{reply: g})
	return g
}

// run makes one call to the faulted operation through call, as the next
// queued fault says: it fails the call without making it, holds the call until
// released, or makes the call and holds its reply. With no fault queued it
// makes the call. A held call or reply fails with the context's error if ctx
// ends first.
func (f *Fault) run(ctx context.Context, call func() (any, error)) (any, error) {
	f.mu.Lock()
	var next *action
	if len(f.queue) > 0 {
		next = f.queue[0]
		f.queue = f.queue[1:]
	}
	f.mu.Unlock()

	switch {
	case next == nil:
		return call()
	case next.gate != nil:
		if err := next.gate.hold(ctx); err != nil {
			return nil, err
		}
		return call()
	case next.reply != nil:
		resp, err := call()
		if holdErr := next.reply.hold(ctx); holdErr != nil {
			return nil, holdErr
		}
		return resp, err
	default:
		return nil, next.err
	}
}

// Gate holds one call to a faulted operation, or its reply, until the test
// releases it.
type Gate struct {
	arrived  chan struct{}
	released chan struct{}
}

// Wait blocks until the held call, or its reply, arrives.
func (g *Gate) Wait(t testing.TB) {
	t.Helper()
	select {
	case <-g.arrived:
	case <-time.After(gateTimeout):
		t.Fatalf("no call reached the gate within %v", gateTimeout)
	}
}

// Release lets the held call proceed as if it had not been faulted.
func (g *Gate) Release() {
	g.released <- struct{}{}
}

// hold signals that a call reached g, and waits until g is released or ctx
// ends.
func (g *Gate) hold(ctx context.Context) error {
	g.arrived <- struct{}{}
	select {
	case <-g.released:
		return nil
	case <-ctx.Done():
		return status.FromContextError(ctx.Err()).Err()
	}
}

// ErrUnavailable is the transport failing: the atelet, the object-store plugin
// or the volume plugin is restarting or unreachable.
var ErrUnavailable = status.Error(codes.Unavailable, "harness: unavailable")
