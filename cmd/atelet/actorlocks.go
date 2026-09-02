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

// actorLocks serializes concurrent node-local operations for the same actor.
// The zero value is ready to use.
type actorLocks struct {
	mu   sync.Mutex
	held map[string]struct{}
}

// tryLock attempts to acquire the lock for actorUID without blocking.
// It returns ok=true and an idempotent release func on success, or a no-op func and ok=false if busy.
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

// lockActorFor acquires the lock for actorUID or returns codes.Aborted if another
// operation for the same actor is already in progress.
func (s *AteomHerder) lockActorFor(operation, actorUID string) (release func(), _ error) {
	release, ok := s.actorLocks.tryLock(actorUID)
	if !ok {
		return nil, status.Errorf(codes.Aborted, "cannot start %s for actor %s: another operation on this actor is already in progress on this node", operation, actorUID)
	}
	return release, nil
}
