// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
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
	"fmt"
	"sync"

	"github.com/agent-substrate/substrate/internal/actoroperation"
	"github.com/agent-substrate/substrate/internal/ateompath"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// actorOperationLocks prevents overlapping lifecycle operations from mutating
// the same actor's node-local files. Its zero value is ready for use.
type actorOperationLocks struct {
	mu     sync.Mutex
	locked map[string]*actorOperationLease
}

// actorOperationLease gives each acquisition its own identity. A stale or
// duplicate release therefore cannot unlock a newer operation for the same
// actor.
//
// The byte makes this type non-zero-sized. Go permits pointers to distinct
// zero-sized variables to compare equal, which would defeat the identity
// check below.
type actorOperationLease struct {
	_ byte
}

func (l *actorOperationLocks) tryLock(actorUID string) (func(), bool) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if _, ok := l.locked[actorUID]; ok {
		return nil, false
	}
	if l.locked == nil {
		l.locked = make(map[string]*actorOperationLease)
	}
	lease := &actorOperationLease{}
	l.locked[actorUID] = lease

	return func() {
		l.mu.Lock()
		defer l.mu.Unlock()

		if l.locked[actorUID] == lease {
			delete(l.locked, actorUID)
		}
	}, true
}

type actorOperation struct {
	actorUID   string
	id         string
	fileLock   *actoroperation.Lock
	endProcess func()
}

func (s *AteomHerder) beginActorOperation(actorUID string) (*actorOperation, error) {
	endProcess, ok := s.actorOperations.tryLock(actorUID)
	if !ok {
		return nil, status.Error(codes.Aborted, "another operation is in progress for this actor")
	}

	fileLock, err := actoroperation.TryAcquire(ateompath.ActorPath(actorUID))
	if err != nil {
		endProcess()
		if errors.Is(err, actoroperation.ErrLocked) {
			return nil, status.Error(codes.Aborted, "another operation is in progress for this actor")
		}
		return nil, fmt.Errorf("while acquiring actor file lock: %w", err)
	}

	operation := &actorOperation{
		actorUID:   actorUID,
		id:         uuid.NewString(),
		fileLock:   fileLock,
		endProcess: endProcess,
	}
	if err := fileLock.SetOperationID(operation.id); err != nil {
		_ = fileLock.Release()
		endProcess()
		return nil, fmt.Errorf("while recording actor operation: %w", err)
	}
	return operation, nil
}

func (o *actorOperation) releaseFiles() error {
	if o.fileLock == nil {
		return nil
	}
	err := o.fileLock.Release()
	o.fileLock = nil
	return err
}

func (o *actorOperation) reacquireFiles(ctx context.Context) error {
	if o.fileLock != nil {
		return errors.New("actor file lock is already held")
	}

	fileLock, err := actoroperation.Acquire(ctx, ateompath.ActorPath(o.actorUID))
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return status.FromContextError(err).Err()
		}
		return fmt.Errorf("while reacquiring actor file lock: %w", err)
	}
	if err := fileLock.CheckOperationID(o.id); err != nil {
		_ = fileLock.Release()
		if errors.Is(err, actoroperation.ErrOperationChanged) {
			return status.Error(codes.Aborted, "actor operation was superseded")
		}
		return fmt.Errorf("while checking actor operation: %w", err)
	}
	o.fileLock = fileLock
	return nil
}

func (o *actorOperation) end() {
	_ = o.releaseFiles()
	o.endProcess()
}
