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

package checkpointabort

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"go.ate.dev/substrate/internal/ateinterceptors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestIs(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"Errorf", Errorf("freeze failed"), true},
		{"wrapped Errorf", fmt.Errorf("while checkpointing: %w", Errorf("freeze failed")), true},
		{"received status", status.Error(codes.Aborted, "freeze failed"), true},
		{"nil", nil, false},
		{"plain error", errors.New("freeze failed"), false},
		{"other status", status.Error(codes.Internal, "freeze failed"), false},
		{"unavailable", status.Error(codes.Unavailable, "atelet restarting"), false},
		{"context canceled", context.Canceled, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := Is(tc.err); got != tc.want {
				t.Errorf("Is(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

// TestPassesThroughServers follows an abort the way it travels: the worker's
// handler returns it, the worker's interceptor encodes it, atelet sees the
// status, returns its own Errorf, and atelet's interceptor encodes that for
// ate-api-server. Both the worker and atelet serve with
// InternalServerUnaryInterceptor.
func TestPassesThroughServers(t *testing.T) {
	serve := func(handlerErr error) error {
		_, err := ateinterceptors.InternalServerUnaryInterceptor(t.Context(), nil,
			&grpc.UnaryServerInfo{FullMethod: "/test/Checkpoint"},
			func(context.Context, any) (any, error) { return nil, handlerErr })
		return err
	}

	fromWorker := serve(fmt.Errorf("while freezing: %w", Errorf("guest container %q did not freeze", "a")))
	if !Is(fromWorker) {
		t.Fatalf("worker returned %v, want an aborted checkpoint", fromWorker)
	}
	fromAtelet := serve(Errorf("while calling worker CheckpointWorkload: %s", status.Convert(fromWorker).Message()))
	if !Is(fromAtelet) {
		t.Fatalf("atelet returned %v, want an aborted checkpoint", fromAtelet)
	}

	// A plain error from either server must not read as an abort.
	if err := serve(errors.New("freeze failed")); Is(err) {
		t.Errorf("plain error reached the caller as %v, an aborted checkpoint", err)
	}
	// Nor must a status received from upstream and returned wrapped: only an
	// explicit Errorf crosses a server.
	if err := serve(fmt.Errorf("while calling worker: %w", fromWorker)); Is(err) {
		t.Errorf("wrapped upstream abort reached the caller as %v, want it reported as Internal", err)
	}
}
