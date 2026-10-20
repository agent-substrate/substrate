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

package main

import (
	"context"
	"errors"
	"testing"
)

func TestPauseGuest(t *testing.T) {
	pauseFailed := errors.New("PUT /api/v1/vm.pause: status 500")
	for _, tt := range []struct {
		name      string
		failures  int
		wantCalls int
		wantErr   error
	}{
		{name: "first call", wantCalls: 1},
		{name: "late vCPU acknowledges the second call", failures: 1, wantCalls: 2},
		{name: "gives up", failures: pauseAttempts, wantCalls: pauseAttempts, wantErr: pauseFailed},
	} {
		t.Run(tt.name, func(t *testing.T) {
			calls := 0
			err := pauseGuest(context.Background(), func(context.Context) error {
				calls++
				if calls <= tt.failures {
					return pauseFailed
				}
				return nil
			})
			if !errors.Is(err, tt.wantErr) || calls != tt.wantCalls {
				t.Fatalf("got %v after %d calls, want %v after %d", err, calls, tt.wantErr, tt.wantCalls)
			}
		})
	}
}

func TestPauseGuestStopsOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	err := pauseGuest(ctx, func(context.Context) error {
		calls++
		cancel()
		return ctx.Err()
	})
	if !errors.Is(err, context.Canceled) || calls != 1 {
		t.Fatalf("got %v after %d calls, want context.Canceled after 1", err, calls)
	}
}
