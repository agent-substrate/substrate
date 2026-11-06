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
	"bytes"
	"context"
	"testing"

	"go.ate.dev/substrate/internal/actorlog"
	"go.ate.dev/substrate/internal/resources"
)

type fakeExitWaiter struct {
	status int
	err    error
}

func (f *fakeExitWaiter) cmdWaitExit(ctx context.Context, containerName string) (int, error) {
	if f.err != nil {
		return 0, f.err
	}
	return f.status, nil
}

func TestWatchContainerExit(t *testing.T) {
	tests := []struct {
		name      string
		status    int
		waitErr   error
		logLine   string
		cancelCtx bool
		wantErr   string
	}{
		{
			name:    "non-zero exit with logs",
			status:  2,
			logLine: "flag provided but not defined: -invalid-flag-exit-immediately\n",
			wantErr: "exit status 2: flag provided but not defined: -invalid-flag-exit-immediately",
		},
		{
			name:    "zero exit status before wakeup probe",
			status:  0,
			wantErr: "exit status 0 before wakeup probe succeeded",
		},
		{
			name:      "context cancelled",
			waitErr:   context.Canceled,
			cancelCtx: true,
			wantErr:   "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			logger := actorlog.NewActorLogger(&buf, false)
			attr := resources.ActorAttribution{UID: "test-actor"}

			if tc.logLine != "" {
				pw, err := logger.StartJSONLogPipe(attr, "counter")
				if err != nil {
					t.Fatalf("StartJSONLogPipe: %v", err)
				}
				if _, err := pw.Write([]byte(tc.logLine)); err != nil {
					t.Fatalf("write log line: %v", err)
				}
				pw.Close()
			}

			s := &AteWorkerService{actorLogger: logger}
			fake := &fakeExitWaiter{status: tc.status, err: tc.waitErr}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tc.cancelCtx {
				cancel()
			}

			err := s.watchContainerExit(ctx, attr, fake, "counter")
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("watchContainerExit = %v, want nil", err)
				}
				return
			}
			if err == nil || err.Error() != tc.wantErr {
				t.Errorf("watchContainerExit = %v, want %q", err, tc.wantErr)
			}
		})
	}
}
