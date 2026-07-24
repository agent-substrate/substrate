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

// Package actoroperation coordinates access to an actor's node-local files
// across atelet and ateom processes.
package actoroperation

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"golang.org/x/sys/unix"
)

const (
	lockFileName      = ".operation.lock"
	operationIDName   = ".operation-id"
	lockRetryInterval = 10 * time.Millisecond
)

var (
	// ErrLocked means another process currently owns the actor's file lock.
	ErrLocked = errors.New("actor files are locked")

	// ErrOperationChanged means the files were prepared by a different
	// operation than the caller expected.
	ErrOperationChanged = errors.New("actor operation changed")
)

// Lock is an exclusive advisory lock for one actor directory.
type Lock struct {
	file      *os.File
	actorPath string

	releaseOnce sync.Once
	releaseErr  error
}

// TryAcquire acquires actorPath's lock without waiting.
func TryAcquire(actorPath string) (*Lock, error) {
	if err := os.MkdirAll(actorPath, 0o700); err != nil {
		return nil, fmt.Errorf("while creating actor directory: %w", err)
	}

	file, err := os.OpenFile(filepath.Join(actorPath, lockFileName), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("while opening actor operation lock: %w", err)
	}
	if err := unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = file.Close()
		if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
			return nil, ErrLocked
		}
		return nil, fmt.Errorf("while locking actor operation file: %w", err)
	}

	return &Lock{file: file, actorPath: actorPath}, nil
}

// Acquire waits until actorPath's lock is available or ctx is done.
func Acquire(ctx context.Context, actorPath string) (*Lock, error) {
	ticker := time.NewTicker(lockRetryInterval)
	defer ticker.Stop()

	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		lock, err := TryAcquire(actorPath)
		if !errors.Is(err, ErrLocked) {
			return lock, err
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
		}
	}
}

// SetOperationID records which operation prepared the actor's files. The
// caller must hold l until this method returns.
func (l *Lock) SetOperationID(operationID string) error {
	if operationID == "" {
		return errors.New("operation ID is empty")
	}

	file, err := os.CreateTemp(l.actorPath, ".operation-id-")
	if err != nil {
		return fmt.Errorf("while creating temporary operation ID file: %w", err)
	}
	tmpPath := file.Name()
	defer os.Remove(tmpPath)

	if _, err := fmt.Fprintln(file, operationID); err != nil {
		_ = file.Close()
		return fmt.Errorf("while writing operation ID: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("while closing operation ID: %w", err)
	}
	if err := os.Rename(tmpPath, filepath.Join(l.actorPath, operationIDName)); err != nil {
		return fmt.Errorf("while installing operation ID: %w", err)
	}
	return nil
}

// CheckOperationID verifies that no newer operation prepared the actor's
// files. The caller must hold l until this method returns.
func (l *Lock) CheckOperationID(operationID string) error {
	got, err := os.ReadFile(filepath.Join(l.actorPath, operationIDName))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("%w: operation ID is missing", ErrOperationChanged)
		}
		return fmt.Errorf("while reading operation ID: %w", err)
	}
	if string(got) != operationID+"\n" {
		return fmt.Errorf("%w: got %q, want %q", ErrOperationChanged, string(got), operationID)
	}
	return nil
}

// HasOperationID reports whether a generation-aware atelet has prepared this
// actor before. The caller must hold l until this method returns.
func (l *Lock) HasOperationID() (bool, error) {
	_, err := os.Stat(filepath.Join(l.actorPath, operationIDName))
	switch {
	case err == nil:
		return true, nil
	case errors.Is(err, os.ErrNotExist):
		return false, nil
	default:
		return false, fmt.Errorf("while checking operation ID file: %w", err)
	}
}

// Release unlocks and closes the lock. It is safe to call more than once.
func (l *Lock) Release() error {
	l.releaseOnce.Do(func() {
		unlockErr := unix.Flock(int(l.file.Fd()), unix.LOCK_UN)
		closeErr := l.file.Close()
		l.releaseErr = errors.Join(unlockErr, closeErr)
	})
	return l.releaseErr
}
