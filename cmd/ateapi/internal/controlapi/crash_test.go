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
func seedActor(t *testing.T, ctx context.Context, st store.Interface, atespace, id string) {
	t.Helper()
	if err := st.CreateActor(ctx, &ateapipb.Actor{
		ActorId:           id,
		Atespace:          atespace,
		Status:            ateapipb.Actor_STATUS_RUNNING,
		AteomPodNamespace: "ns",
		AteomPodName:      "pod",
		AteomPodIp:        "1.2.3.4",
		AteomPodUid:       "uid",
		WorkerPoolName:    "pool",
	}); err != nil {
		t.Fatalf("seed actor: %v", err)
	}
}

// assertCrashed reloads the actor and verifies it is CRASHED with all
// worker-binding fields cleared.
func assertCrashed(t *testing.T, ctx context.Context, st store.Interface, atespace, id string) {
	t.Helper()
	got, err := st.GetActor(ctx, atespace, id)
	if err != nil {
		t.Fatalf("GetActor(%q, %q) = %v, want nil", atespace, id, err)
	}
	if got.GetStatus() != ateapipb.Actor_STATUS_CRASHED {
		t.Errorf("status = %v, want %v", got.GetStatus(), ateapipb.Actor_STATUS_CRASHED)
	}
	if got.GetAteomPodNamespace() != "" || got.GetAteomPodName() != "" ||
		got.GetAteomPodIp() != "" || got.GetAteomPodUid() != "" ||
		got.GetWorkerPoolName() != "" {
		t.Errorf("worker fields not cleared: ns=%q name=%q ip=%q uid=%q pool=%q",
			got.GetAteomPodNamespace(), got.GetAteomPodName(), got.GetAteomPodIp(),
			got.GetAteomPodUid(), got.GetWorkerPoolName())
	}
}

func TestCrashActor(t *testing.T) {
	const (
		atespace = "team-a"
		actorID  = "actor-1"
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
				assertCrashed(t, ctx, st, atespace, actorID)
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
				seedActor(t, ctx, st, atespace, actorID)
			}

			err := crashActor(ctx, st, atespace, actorID)
			tt.check(t, ctx, st, err)
		})
	}
}

func TestMaybeCrashActor(t *testing.T) {
	const (
		atespace = "team-a"
		actorID  = "actor-1"
		wrapMsg  = "calling atelet"
	)

	crashErr := ateerrors.NewGRPCError(codes.NotFound, ateerrors.ErrReasonCrashActor, errors.New("boom"))
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
				assertCrashed(t, ctx, st, atespace, actorID)
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
				got, gerr := st.GetActor(ctx, atespace, actorID)
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
				seedActor(t, ctx, st, atespace, actorID)
			}

			err := maybeCrashActor(ctx, st, atespace, actorID, tt.err, wrapMsg)
			tt.check(t, ctx, st, err)
		})
	}
}
