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

func TestCheckActorPrecondition(t *testing.T) {
	const (
		storedUID = "stored-uid"
		staleUID  = "stale-uid"
		storedVer = int64(7)
		staleVer  = int64(6)
	)
	dbActor := &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{
			Atespace: "test-atespace",
			Name:     "actor-1",
			Uid:      storedUID,
			Version:  storedVer,
		},
	}

	tests := []struct {
		name    string
		uid     string
		version int64
		wantErr error
	}{
		{
			name:    "both waived",
			uid:     AnyUID,
			version: AnyVersion,
			wantErr: nil,
		},
		{
			name:    "both guarded and both match",
			uid:     storedUID,
			version: storedVer,
			wantErr: nil,
		},
		{
			name:    "uid guarded, version waived, tolerates the moved version",
			uid:     storedUID,
			version: AnyVersion,
			wantErr: nil,
		},
		{
			name:    "version guarded, uid waived, still catches the moved version",
			uid:     AnyUID,
			version: staleVer,
			wantErr: ErrVersionConflict,
		},
		{
			name:    "uid guarded, version waived, still catches the new incarnation",
			uid:     staleUID,
			version: AnyVersion,
			wantErr: ErrUIDConflict,
		},
		{
			// The uid is reported first: a new incarnation makes the version
			// meaningless, and it is the failure a retry can never resolve.
			name:    "both stale reports the uid conflict",
			uid:     staleUID,
			version: staleVer,
			wantErr: ErrUIDConflict,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := CheckActorPrecondition(dbActor, tt.uid, tt.version)
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("CheckActorPrecondition(dbActor, %q, %d) = %v, want one matching %v", tt.uid, tt.version, err, tt.wantErr)
			}
		})
	}
}
