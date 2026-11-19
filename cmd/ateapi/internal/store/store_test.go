// Copyright 2026 The Agent Substrate Authors
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

	"go.ate.dev/substrate/api/ateapi/v1"
)

const (
	storedUID = "stored-uid"
	staleUID  = "stale-uid"
	storedVer = int64(7)
	staleVer  = int64(6)
)

// storedMetadata is the metadata of the object the transaction reads, which a
// precondition is checked against.
var storedMetadata = &ateapi.ResourceMetadata{
	Atespace: "test-atespace",
	Name:     "actor-1",
	Uid:      storedUID,
	Version:  storedVer,
}

func TestPreconditionCheck(t *testing.T) {
	tests := []struct {
		name         string
		precondition Precondition
		wantRequired error
		wantOptional error
	}{
		{
			name:         "the guarded object is still the stored one",
			precondition: Precondition{UID: storedUID, Version: storedVer},
		},
		{
			name:         "the name now addresses a different incarnation",
			precondition: Precondition{UID: staleUID, Version: storedVer},
			wantRequired: ErrUIDConflict,
			wantOptional: ErrUIDConflict,
		},
		{
			name:         "the version moved under the caller",
			precondition: Precondition{UID: storedUID, Version: staleVer},
			wantRequired: ErrVersionConflict,
			wantOptional: ErrVersionConflict,
		},
		{
			// The uid is reported first: a new incarnation makes the version
			// meaningless, and it is the failure a retry can never resolve.
			name:         "both stale reports the uid conflict",
			precondition: Precondition{UID: staleUID, Version: staleVer},
			wantRequired: ErrUIDConflict,
			wantOptional: ErrUIDConflict,
		},
		{
			name:         "no guards",
			wantRequired: ErrPreconditionRequired,
		},
		{
			name:         "uid only",
			precondition: Precondition{UID: storedUID},
			wantRequired: ErrPreconditionRequired,
		},
		{
			name:         "stale version only",
			precondition: Precondition{Version: staleVer},
			wantRequired: ErrPreconditionRequired,
			wantOptional: ErrVersionConflict,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.precondition.CheckRequired(storedMetadata); !errors.Is(err, tt.wantRequired) {
				t.Errorf("CheckRequired(storedMetadata) = %v, want one matching %v", err, tt.wantRequired)
			}
			if err := tt.precondition.CheckOptional(storedMetadata); !errors.Is(err, tt.wantOptional) {
				t.Errorf("CheckOptional(storedMetadata) = %v, want one matching %v", err, tt.wantOptional)
			}
		})
	}
}

func TestPreconditionValidateRequired(t *testing.T) {
	tests := []struct {
		name         string
		precondition Precondition
		wantErr      error
	}{
		{
			name:         "guarding on both uid and version is what an update requires",
			precondition: Precondition{UID: storedUID, Version: storedVer},
			wantErr:      nil,
		},
		{
			name:         "guarding on nothing is a blind write",
			precondition: Precondition{},
			wantErr:      ErrPreconditionRequired,
		},
		{
			name:         "a uid alone does not guard a revision",
			precondition: Precondition{UID: storedUID},
			wantErr:      ErrPreconditionRequired,
		},
		{
			name:         "a version alone does not guard an incarnation",
			precondition: Precondition{Version: storedVer},
			wantErr:      ErrPreconditionRequired,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.precondition.ValidateRequired(); !errors.Is(err, tt.wantErr) {
				t.Errorf("ValidateRequired() = %v, want one matching %v", err, tt.wantErr)
			}
		})
	}
}

func TestPreconditionFrom(t *testing.T) {
	tests := []struct {
		name            string
		observed        *ateapi.Actor
		want            Precondition
		wantValidateErr error
	}{
		{
			name:            "the observed object supplies its uid and version as guards",
			observed:        &ateapi.Actor{Metadata: storedMetadata},
			want:            Precondition{UID: storedUID, Version: storedVer},
			wantValidateErr: nil,
		},
		{
			name:            "an unguarded object with no uid or version is rejected",
			observed:        &ateapi.Actor{Metadata: &ateapi.ResourceMetadata{Atespace: "test-atespace", Name: "actor-1"}},
			want:            Precondition{},
			wantValidateErr: ErrPreconditionRequired,
		},
		{
			name:            "an object carrying only a uid is rejected",
			observed:        &ateapi.Actor{Metadata: &ateapi.ResourceMetadata{Uid: storedUID}},
			want:            Precondition{UID: storedUID},
			wantValidateErr: ErrPreconditionRequired,
		},
		{
			name:            "an object carrying only a version is rejected",
			observed:        &ateapi.Actor{Metadata: &ateapi.ResourceMetadata{Version: storedVer}},
			want:            Precondition{Version: storedVer},
			wantValidateErr: ErrPreconditionRequired,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := PreconditionFrom(tt.observed)
			if got != tt.want {
				t.Errorf("PreconditionFrom(observed) = %+v, want %+v", got, tt.want)
			}
			if err := got.ValidateRequired(); !errors.Is(err, tt.wantValidateErr) {
				t.Errorf("PreconditionFrom(observed).ValidateRequired() = %v, want one matching %v", err, tt.wantValidateErr)
			}
		})
	}
}

func TestNormalizeListOptions(t *testing.T) {
	got, err := NormalizeListOptions(ListOptions{})
	if err != nil {
		t.Fatalf("NormalizeListOptions(zero) error = %v", err)
	}
	if got.PageSize != DefaultPageSize {
		t.Errorf("NormalizeListOptions(zero).PageSize = %d, want %d", got.PageSize, DefaultPageSize)
	}
	if _, err := NormalizeListOptions(ListOptions{PageSize: -1}); !errors.Is(err, ErrInvalidPageSize) {
		t.Errorf("NormalizeListOptions(negative) error = %v, want ErrInvalidPageSize", err)
	}
}
