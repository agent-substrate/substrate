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
	"strings"
	"testing"

	"google.golang.org/grpc/codes"

	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
)

// TestValidateFidelity pins what this runtime serves.
func TestValidateFidelity(t *testing.T) {
	for _, tc := range []struct {
		name     string
		fidelity ateworkerpb.SnapshotFidelity
		wantErr  bool
	}{
		{"volumes", ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES, false},
		{"rootfs", ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS, false},
		{"memory", ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, false},
		{"unspecified", ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_UNSPECIFIED, true},
		{"outside the enum", ateworkerpb.SnapshotFidelity(99), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := validateFidelity(tc.fidelity)
			if gotErr := err != nil; gotErr != tc.wantErr {
				t.Fatalf("validateFidelity(%v) = %v, wantErr %t", tc.fidelity, err, tc.wantErr)
			}
			if err != nil && apierror.Code(err) != codes.InvalidArgument {
				t.Errorf("code = %v, want InvalidArgument", apierror.Code(err))
			}
		})
	}
	err := validateFidelity(ateworkerpb.SnapshotFidelity(99))
	for _, want := range []string{"SNAPSHOT_FIDELITY_VOLUMES", "SNAPSHOT_FIDELITY_ROOTFS", "SNAPSHOT_FIDELITY_MEMORY"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("invalid fidelity rejection %q does not list %s as supported", err, want)
		}
	}
}

// TestRPCsRejectInvalidFidelityBeforeTouchingTheSandbox pins that an invalid
// fidelity checkpoint or restore fails validation up front, before the actor
// lock or any file system work, so a misrouted request cannot leave the actor
// half-handled.
func TestRPCsRejectInvalidFidelityBeforeTouchingTheSandbox(t *testing.T) {
	s := &AteWorkerService{}
	unspecified := ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_UNSPECIFIED

	ckpt := validCheckpointRequest()
	ckpt.Fidelity = unspecified
	_, err := s.CheckpointWorkload(context.Background(), ckpt)
	if apierror.Code(err) != codes.InvalidArgument || !strings.Contains(err.Error(), "fidelity") {
		t.Errorf("CheckpointWorkload(UNSPECIFIED) = %v, want InvalidArgument naming the fidelity", err)
	}
	restore := validRestoreRequest()
	restore.Fidelity = unspecified
	_, err = s.RestoreWorkload(context.Background(), restore)
	if apierror.Code(err) != codes.InvalidArgument || !strings.Contains(err.Error(), "fidelity") {
		t.Errorf("RestoreWorkload(UNSPECIFIED) = %v, want InvalidArgument naming the fidelity", err)
	}
}
