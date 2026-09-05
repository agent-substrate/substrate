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
	"testing"

	ateompb "github.com/agent-substrate/substrate/internal/proto/ateompb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestCheckpointWorkloadRejectsSkipDurableDirTar pins the rejection, and pins
// that it happens before the sandbox is touched: a zero-value service has no
// actor to check point, so reaching any of the real work would not return
// InvalidArgument.
func TestCheckpointWorkloadRejectsSkipDurableDirTar(t *testing.T) {
	s := &AteomService{}
	_, err := s.CheckpointWorkload(t.Context(), &ateompb.CheckpointWorkloadRequest{
		ActorUid:          "actor-1",
		SkipDurableDirTar: true,
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("CheckpointWorkload err = %v (code %v), want InvalidArgument", err, status.Code(err))
	}
}
