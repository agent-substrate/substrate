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
	"strings"
	"testing"

	"google.golang.org/grpc/codes"

	"github.com/agent-substrate/substrate/internal/apierror"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
)

// TestValidateFidelity pins what this runtime serves: every level of the
// ladder, ROOTFS included, since the rootfs upper is host-backed and ships
// with or without guest memory. Anything else is refused before the request
// touches the guest.
func TestValidateFidelity(t *testing.T) {
	for _, tc := range []struct {
		name     string
		fidelity ateompb.SnapshotFidelity
		wantErr  bool
	}{
		{"volumes", ateompb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES, false},
		{"rootfs", ateompb.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS, false},
		{"memory", ateompb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY, false},
		{"unspecified", ateompb.SnapshotFidelity_SNAPSHOT_FIDELITY_UNSPECIFIED, true},
		{"outside the enum", ateompb.SnapshotFidelity(99), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := validateFidelity(tc.fidelity)
			if gotErr := err != nil; gotErr != tc.wantErr {
				t.Fatalf("validateFidelity(%v) = %v, wantErr %t", tc.fidelity, err, tc.wantErr)
			}
			if err != nil {
				if apierror.Code(err) != codes.InvalidArgument {
					t.Errorf("code = %v, want InvalidArgument", apierror.Code(err))
				}
				if !strings.Contains(err.Error(), "fidelity") {
					t.Errorf("error %q does not name the fidelity field", err)
				}
			}
		})
	}
}
