//go:build linux

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
	"log/slog"

	"github.com/agent-substrate/substrate/internal/actoroperation"
	"github.com/agent-substrate/substrate/internal/ateompath"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func acquireActorOperation(ctx context.Context, actorUID, operationID string) (*actoroperation.Lock, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}

	fileLock, err := actoroperation.TryAcquire(ateompath.ActorPath(actorUID))
	if err != nil {
		if errors.Is(err, actoroperation.ErrLocked) {
			return nil, status.Error(codes.Aborted, "another operation is in progress for this actor")
		}
		return nil, fmt.Errorf("while acquiring actor file lock: %w", err)
	}

	// Empty operation IDs come from an older atelet during a rolling upgrade.
	// Accept one only if no generation-aware atelet has prepared this actor;
	// otherwise a queued legacy request could mutate a newer operation's files.
	if operationID == "" {
		hasOperationID, err := fileLock.HasOperationID()
		if err != nil {
			_ = fileLock.Release()
			return nil, err
		}
		if hasOperationID {
			_ = fileLock.Release()
			return nil, status.Error(codes.Aborted, "actor operation ID is missing")
		}
		return fileLock, nil
	}
	if err := fileLock.CheckOperationID(operationID); err != nil {
		_ = fileLock.Release()
		if errors.Is(err, actoroperation.ErrOperationChanged) {
			return nil, status.Error(codes.Aborted, "actor operation was superseded")
		}
		return nil, fmt.Errorf("while checking actor operation: %w", err)
	}
	return fileLock, nil
}

func releaseActorOperation(ctx context.Context, fileLock *actoroperation.Lock) {
	if err := fileLock.Release(); err != nil {
		slog.ErrorContext(ctx, "Failed to release actor file lock", "err", err)
	}
}
