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
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// actorLocks serializes node-local operations that would otherwise run against
// one actor's on-node state concurrently.
//
// Recovering a lost checkpoint response makes a re-entered attempt succeed
// rather than fail, which is the point — but it also means two attempts at one
// actor can now both reach the teardown that follows a checkpoint. That
// teardown removes the actor's checkpoint dir, which is the source the other
// attempt may still be uploading from. Recognizing a repeated operation and
// excluding a concurrent one are separate problems, and solving the first does
// not solve the second.
//
// Every RPC that clears an actor's on-node state takes it, not only Checkpoint:
// UploadPausedCheckpoint prunes every local snapshot, and Run and Restore reset
// the actor's directories. Each of those destroys what an in-flight checkpoint
// is still reading from, and the lease expiry that lets two checkpoints overlap
// lets a checkpoint overlap with any of them just as easily. A lock one caller
// can walk around is not a lock.
//
// The zero value is ready to use. Entries are dropped on release, so this
// holds one entry per in-flight operation rather than one per actor the node
// has ever seen.
type actorLocks struct {
	mu   sync.Mutex
	held map[string]struct{}
}

// tryLock claims actorUID for the caller, reporting whether it got it. It does
// not wait: a caller that finds the actor busy has nothing useful to do
// meanwhile, since the operation it would run is the one already in progress.
//
// release is always non-nil, so `defer release()` is safe on either outcome,
// and is idempotent.
func (l *actorLocks) tryLock(actorUID string) (release func(), ok bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, busy := l.held[actorUID]; busy {
		return func() {}, false
	}
	if l.held == nil {
		l.held = map[string]struct{}{}
	}
	l.held[actorUID] = struct{}{}
	var once sync.Once
	return func() {
		once.Do(func() {
			l.mu.Lock()
			defer l.mu.Unlock()
			delete(l.held, actorUID)
		})
	}, true
}

// lockActorFor claims actorUID for the named operation, or refuses it.
//
// The refusal is Aborted: retriable, and carrying no crash directive, because
// nothing is wrong with the actor — another operation simply holds it. It is
// deliberately not a queued wait: the caller has nothing to contribute while
// the holder moves gigabytes, and keeping the RPC open for that long only
// risks a timeout on a call that was never doing anything. By the time the
// control plane retries, the holder has usually finished, and a re-entered
// checkpoint's fast-forward answers immediately.
func (s *AteomHerder) lockActorFor(operation, actorUID string) (release func(), _ error) {
	release, ok := s.actorLocks.tryLock(actorUID)
	if !ok {
		return nil, status.Errorf(codes.Aborted, "cannot start %s for actor %s: another operation on this actor is already in progress on this node", operation, actorUID)
	}
	return release, nil
}
