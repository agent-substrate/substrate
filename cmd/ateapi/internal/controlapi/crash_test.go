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

package controlapi

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store"
	"github.com/agent-substrate/substrate/cmd/ateapi/internal/store/storetest"
	"github.com/agent-substrate/substrate/internal/ateerrors"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// seedActor stores a running actor with all worker-binding fields populated, so
// tests can assert they are cleared when the actor crashes.
func seedActor(t *testing.T, ctx context.Context, st store.Interface, atespace, actorName string) {
	t.Helper()
	if _, err := st.CreateActor(ctx, &ateapipb.Actor{
		Metadata:           &ateapipb.ResourceMetadata{Name: actorName, Atespace: atespace},
		Status:             ateapipb.Actor_STATUS_RUNNING,
		AteomPodNamespace:  "ns",
		AteomPodName:       "pod",
		AteomPodIp:         "1.2.3.4",
		AteomPodUid:        "uid",
		WorkerPoolName:     "pool",
		InProgressSnapshot: "gs://snapshots/actor-1/reserved",
	}); err != nil {
		t.Fatalf("seed actor: %v", err)
	}
}

// assertCrashed reloads the actor and verifies it is CRASHED.
func assertCrashed(t *testing.T, ctx context.Context, st store.Interface, atespace, actorName string) {
	t.Helper()
	got, err := st.GetActor(ctx, atespace, actorName)
	if err != nil {
		t.Fatalf("GetActor(%q, %q) = %v, want nil", atespace, actorName, err)
	}
	if got.GetStatus() != ateapipb.Actor_STATUS_CRASHED {
		t.Errorf("status = %v, want %v", got.GetStatus(), ateapipb.Actor_STATUS_CRASHED)
	}
	// Keep the snapshot uri for debugging.
	if got.GetInProgressSnapshot() == "" {
		t.Error(`InProgressSnapshot = "", want preserved`)
	}
}

func TestCrashActor(t *testing.T) {
	const (
		atespace  = "team-a"
		actorName = "actor-1"
	)

	tests := []struct {
		name string
		seed bool
		// check inspects the returned error; nil-safe.
		check func(t *testing.T, ctx context.Context, st store.Interface, err error)
	}{
		{
			name: "crashes running actor",
			seed: true,
			check: func(t *testing.T, ctx context.Context, st store.Interface, err error) {
				if err != nil {
					t.Fatalf("crashActor() = %v, want nil", err)
				}
				assertCrashed(t, ctx, st, atespace, actorName)
			},
		},
		{
			name: "actor not found",
			seed: false,
			check: func(t *testing.T, ctx context.Context, st store.Interface, err error) {
				if err == nil {
					t.Fatal("crashActor() = nil, want error")
				}
				if !errors.Is(err, store.ErrNotFound) {
					t.Errorf("crashActor() error = %v, want errors.Is(store.ErrNotFound)", err)
				}
				if !strings.Contains(err.Error(), "while loading actor to crash") {
					t.Errorf("crashActor() error = %q, want it to contain %q", err, "while loading actor to crash")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			st, cleanup := storetest.SetupTestStore(t)
			defer cleanup()

			if tt.seed {
				seedActor(t, ctx, st, atespace, actorName)
			}

			err := crashActor(ctx, st, atespace, actorName)
			tt.check(t, ctx, st, err)
		})
	}
}

func TestMaybeCrashActor(t *testing.T) {
	const (
		atespace  = "team-a"
		actorName = "actor-1"
		wrapMsg   = "calling atelet"
	)

	crashErr := ateerrors.NewGRPCError(context.Background(), codes.NotFound, ateerrors.ReasonTerminalFileSystemError, ateerrors.ActorCrashedMetadata(), errors.New("boom"))
	// A structured error carrying a reason but no actorCrashed directive must be
	// wrapped, not crash the actor.
	noCrashErr := ateerrors.NewGRPCError(context.Background(), codes.NotFound, ateerrors.ReasonFailedGetExternalObject, nil, errors.New("infra"))
	plainErr := errors.New("transient")

	tests := []struct {
		name string
		seed bool
		err  error
		// check inspects the returned error and store state.
		check func(t *testing.T, ctx context.Context, st store.Interface, err error)
	}{
		{
			name: "nil error returns nil",
			seed: false,
			err:  nil,
			check: func(t *testing.T, ctx context.Context, st store.Interface, err error) {
				if err != nil {
					t.Fatalf("maybeCrashActor() = %v, want nil", err)
				}
			},
		},
		{
			name: "crash reason crashes actor",
			seed: true,
			err:  crashErr,
			check: func(t *testing.T, ctx context.Context, st store.Interface, err error) {
				if err == nil {
					t.Fatal("maybeCrashActor() = nil, want error")
				}
				if got := status.Code(err); got != codes.DataLoss {
					t.Errorf("status code = %v, want %v", got, codes.DataLoss)
				}
				assertCrashed(t, ctx, st, atespace, actorName)
			},
		},
		{
			name: "crash reason but actor missing returns load error",
			seed: false,
			err:  crashErr,
			check: func(t *testing.T, ctx context.Context, st store.Interface, err error) {
				if err == nil {
					t.Fatal("maybeCrashActor() = nil, want error")
				}
				if got := status.Code(err); got == codes.DataLoss {
					t.Errorf("status code = %v, want it not to be DataLoss", got)
				}
				if !errors.Is(err, store.ErrNotFound) {
					t.Errorf("maybeCrashActor() error = %v, want errors.Is(store.ErrNotFound)", err)
				}
			},
		},
		{
			name: "status error without crash directive is wrapped",
			seed: true,
			err:  noCrashErr,
			check: func(t *testing.T, ctx context.Context, st store.Interface, err error) {
				if err == nil {
					t.Fatal("maybeCrashActor() = nil, want error")
				}
				if !errors.Is(err, noCrashErr) {
					t.Errorf("maybeCrashActor() error = %v, want errors.Is(noCrashErr)", err)
				}
				if !strings.HasPrefix(err.Error(), wrapMsg) {
					t.Errorf("maybeCrashActor() error = %q, want prefix %q", err, wrapMsg)
				}
				// The actor must not have been crashed.
				got, gerr := st.GetActor(ctx, atespace, actorName)
				if gerr != nil {
					t.Fatalf("GetActor() = %v, want nil", gerr)
				}
				if got.GetStatus() == ateapipb.Actor_STATUS_CRASHED {
					t.Errorf("status = CRASHED, want it unchanged")
				}
			},
		},
		{
			name: "non-crash error is wrapped",
			seed: true,
			err:  plainErr,
			check: func(t *testing.T, ctx context.Context, st store.Interface, err error) {
				if err == nil {
					t.Fatal("maybeCrashActor() = nil, want error")
				}
				if !errors.Is(err, plainErr) {
					t.Errorf("maybeCrashActor() error = %v, want errors.Is(plainErr)", err)
				}
				if !strings.HasPrefix(err.Error(), wrapMsg) {
					t.Errorf("maybeCrashActor() error = %q, want prefix %q", err, wrapMsg)
				}
				// The actor must not have been crashed.
				got, gerr := st.GetActor(ctx, atespace, actorName)
				if gerr != nil {
					t.Fatalf("GetActor() = %v, want nil", gerr)
				}
				if got.GetStatus() == ateapipb.Actor_STATUS_CRASHED {
					t.Errorf("status = CRASHED, want it unchanged")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			st, cleanup := storetest.SetupTestStore(t)
			defer cleanup()

			if tt.seed {
				seedActor(t, ctx, st, atespace, actorName)
			}

			err := maybeCrashActor(ctx, st, atespace, actorName, tt.err, wrapMsg)
			tt.check(t, ctx, st, err)
		})
	}
}
