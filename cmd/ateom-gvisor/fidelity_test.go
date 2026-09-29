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

	"github.com/agent-substrate/substrate/internal/apierror"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
)

// TestValidateFidelity pins what this runtime serves. gVisor keeps rootfs
// changes inside its memory checkpoint, so ROOTFS is refused until it can
// capture them on their own; the error names the levels it does serve.
func TestValidateFidelity(t *testing.T) {
	for _, tc := range []struct {
		name     string
		fidelity ateompb.SnapshotFidelity
		wantErr  bool
	}{
		{"volumes", ateompb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES, false},
		{"memory", ateompb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, false},
		{"rootfs is not served", ateompb.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS, true},
		{"unspecified", ateompb.SnapshotFidelity_SNAPSHOT_FIDELITY_UNSPECIFIED, true},
		{"outside the enum", ateompb.SnapshotFidelity(99), true},
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
	err := validateFidelity(ateompb.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS)
	for _, want := range []string{"SNAPSHOT_FIDELITY_VOLUMES", "SNAPSHOT_FIDELITY_MEMORY"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("rootfs rejection %q does not list %s as supported", err, want)
		}
	}
}

// TestRPCsRejectRootfsBeforeTouchingTheSandbox pins that a ROOTFS checkpoint
// or restore fails validation up front, before the actor lock or any file
// system work, so a misrouted request cannot leave the actor half-handled.
func TestRPCsRejectRootfsBeforeTouchingTheSandbox(t *testing.T) {
	s := &AteomService{}
	rootfs := ateompb.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS

	ckpt := validCheckpointRequest()
	ckpt.Fidelity = rootfs
	_, err := s.CheckpointWorkload(context.Background(), ckpt)
	if apierror.Code(err) != codes.InvalidArgument || !strings.Contains(err.Error(), "fidelity") {
		t.Errorf("CheckpointWorkload(ROOTFS) = %v, want InvalidArgument naming the fidelity", err)
	}
	restore := validRestoreRequest()
	restore.Fidelity = rootfs
	_, err = s.RestoreWorkload(context.Background(), restore)
	if apierror.Code(err) != codes.InvalidArgument || !strings.Contains(err.Error(), "fidelity") {
		t.Errorf("RestoreWorkload(ROOTFS) = %v, want InvalidArgument naming the fidelity", err)
	}
}
