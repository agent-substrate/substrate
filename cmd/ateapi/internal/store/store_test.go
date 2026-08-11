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

package store

import (
	"errors"
	"testing"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

func TestWithPrecondition(t *testing.T) {
	const (
		storedUID = "stored-uid"
		staleUID  = "stale-uid"
		storedVer = int64(7)
		staleVer  = int64(6)
	)
	// The actor the transaction reads, which the mutation is checked against.
	stored := &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{
			Atespace: "test-atespace",
			Name:     "actor-1",
			Uid:      storedUID,
			Version:  storedVer,
		},
	}
	errMutate := errors.New("mutate failed")

	tests := []struct {
		name        string
		observed    *ateapipb.Actor
		mutateErr   error
		wantErr     error
		wantMutated bool
	}{
		{
			name:        "the observed object is still the stored one",
			observed:    stored,
			wantErr:     nil,
			wantMutated: true,
		},
		{
			name:        "the mutate error is surfaced verbatim",
			observed:    stored,
			mutateErr:   errMutate,
			wantErr:     errMutate,
			wantMutated: true,
		},
		{
			name:        "an unguarded observed object pins nothing",
			observed:    &ateapipb.Actor{Metadata: &ateapipb.ResourceMetadata{Atespace: "test-atespace", Name: "actor-1"}},
			wantErr:     nil,
			wantMutated: true,
		},
		{
			name: "uid guarded, version waived, tolerates the moved version",
			observed: &ateapipb.Actor{
				Metadata: &ateapipb.ResourceMetadata{Uid: storedUID, Version: AnyVersion},
			},
			wantErr:     nil,
			wantMutated: true,
		},
		{
			name: "version guarded, uid waived, tolerates the new incarnation",
			observed: &ateapipb.Actor{
				Metadata: &ateapipb.ResourceMetadata{Uid: AnyUID, Version: storedVer},
			},
			wantErr:     nil,
			wantMutated: true,
		},
		{
			name: "the name now addresses a different incarnation",
			observed: &ateapipb.Actor{
				Metadata: &ateapipb.ResourceMetadata{Uid: staleUID, Version: storedVer},
			},
			wantErr:     ErrUIDConflict,
			wantMutated: false,
		},
		{
			name: "the version moved under the caller",
			observed: &ateapipb.Actor{
				Metadata: &ateapipb.ResourceMetadata{Uid: storedUID, Version: staleVer},
			},
			wantErr:     ErrVersionConflict,
			wantMutated: false,
		},
		{
			name: "uid guarded, version waived, still catches the new incarnation",
			observed: &ateapipb.Actor{
				Metadata: &ateapipb.ResourceMetadata{Uid: staleUID, Version: AnyVersion},
			},
			wantErr:     ErrUIDConflict,
			wantMutated: false,
		},
		{
			name: "version guarded, uid waived, still catches the moved version",
			observed: &ateapipb.Actor{
				Metadata: &ateapipb.ResourceMetadata{Uid: AnyUID, Version: staleVer},
			},
			wantErr:     ErrVersionConflict,
			wantMutated: false,
		},
		{
			// The uid is reported first: a new incarnation makes the version
			// meaningless, and it is the failure a retry can never resolve.
			name: "both stale reports the uid conflict",
			observed: &ateapipb.Actor{
				Metadata: &ateapipb.ResourceMetadata{Uid: staleUID, Version: staleVer},
			},
			wantErr:     ErrUIDConflict,
			wantMutated: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mutated := false
			mutate := WithPrecondition(tt.observed, func(toUpdate *ateapipb.Actor) error {
				mutated = true
				if toUpdate != stored {
					t.Errorf("mutate got actor %v, want the stored one", toUpdate)
				}
				return tt.mutateErr
			})

			if err := mutate(stored); !errors.Is(err, tt.wantErr) {
				t.Errorf("mutate(stored) = %v, want one matching %v", err, tt.wantErr)
			}
			if mutated != tt.wantMutated {
				t.Errorf("mutate ran = %t, want %t", mutated, tt.wantMutated)
			}
		})
	}
}

func TestWithWorkerPrecondition(t *testing.T) {
	const (
		storedUID = "stored-pod-uid"
		staleUID  = "stale-pod-uid"
		storedVer = int64(7)
		staleVer  = int64(6)
	)
	// The worker the transaction reads, which the mutation is checked against.
	stored := &ateapipb.Worker{
		WorkerNamespace: "worker-ns",
		WorkerPool:      "pool-1",
		WorkerPod:       "pod-1",
		WorkerPodUid:    storedUID,
		Version:         storedVer,
	}
	errMutate := errors.New("mutate failed")

	tests := []struct {
		name               string
		preconditionWorker *ateapipb.Worker
		mutateErr          error
		wantErr            error
		wantMutated        bool
	}{
		{
			name:               "the observed worker is still the stored one",
			preconditionWorker: stored,
			wantErr:            nil,
			wantMutated:        true,
		},
		{
			name:               "the mutate error is surfaced verbatim",
			preconditionWorker: stored,
			mutateErr:          errMutate,
			wantErr:            errMutate,
			wantMutated:        true,
		},
		{
			name:               "an unguarded observed worker pins nothing",
			preconditionWorker: &ateapipb.Worker{WorkerNamespace: "worker-ns", WorkerPool: "pool-1", WorkerPod: "pod-1"},
			wantErr:            nil,
			wantMutated:        true,
		},
		{
			name:               "pod uid guarded, version waived, tolerates the moved version",
			preconditionWorker: &ateapipb.Worker{WorkerPodUid: storedUID, Version: AnyVersion},
			wantErr:            nil,
			wantMutated:        true,
		},
		{
			name:               "version guarded, pod uid waived, tolerates the new pod",
			preconditionWorker: &ateapipb.Worker{WorkerPodUid: AnyUID, Version: storedVer},
			wantErr:            nil,
			wantMutated:        true,
		},
		{
			name:               "the pod name now addresses a replaced pod",
			preconditionWorker: &ateapipb.Worker{WorkerPodUid: staleUID, Version: storedVer},
			wantErr:            ErrUIDConflict,
			wantMutated:        false,
		},
		{
			name:               "the version moved under the caller",
			preconditionWorker: &ateapipb.Worker{WorkerPodUid: storedUID, Version: staleVer},
			wantErr:            ErrVersionConflict,
			wantMutated:        false,
		},
		{
			name:               "pod uid guarded, version waived, still catches the replaced pod",
			preconditionWorker: &ateapipb.Worker{WorkerPodUid: staleUID, Version: AnyVersion},
			wantErr:            ErrUIDConflict,
			wantMutated:        false,
		},
		{
			name:               "version guarded, pod uid waived, still catches the moved version",
			preconditionWorker: &ateapipb.Worker{WorkerPodUid: AnyUID, Version: staleVer},
			wantErr:            ErrVersionConflict,
			wantMutated:        false,
		},
		{
			// The pod uid is reported first: a replaced pod makes the version
			// meaningless, and it is the failure a retry can never resolve.
			name:               "both stale reports the pod uid conflict",
			preconditionWorker: &ateapipb.Worker{WorkerPodUid: staleUID, Version: staleVer},
			wantErr:            ErrUIDConflict,
			wantMutated:        false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mutated := false
			mutate := WithWorkerPrecondition(tt.preconditionWorker, func(toUpdate *ateapipb.Worker) error {
				mutated = true
				if toUpdate != stored {
					t.Errorf("mutate got worker %v, want the stored one", toUpdate)
				}
				return tt.mutateErr
			})

			if err := mutate(stored); !errors.Is(err, tt.wantErr) {
				t.Errorf("mutate(stored) = %v, want one matching %v", err, tt.wantErr)
			}
			if mutated != tt.wantMutated {
				t.Errorf("mutate ran = %t, want %t", mutated, tt.wantMutated)
			}
		})
	}
}
