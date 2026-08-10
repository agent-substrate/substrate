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
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/agent-substrate/substrate/internal/proto/ateompb"
)

// TestGetWorkloadStatsUnimplemented pins the stub's advertised contract. The
// cgroup read replaces this body; until then a caller that gets any other code back
// would be reading numbers that are not there.
//
// The retention this stub will eventually read — s.activeActor, set by
// RunWorkload / RestoreWorkload and cleared by CheckpointWorkload — has no unit
// test, because those three RPCs each reach for netlink, runsc, and the worker
// pod's netns within a few lines of entry and cannot be driven from `go test`.
// Its mapping is covered in internal/ateomstats; the transitions are verified
// end to end once GetWorkloadStats returns real data.
func TestGetWorkloadStatsUnimplemented(t *testing.T) {
	s := &AteomService{}

	resp, err := s.GetWorkloadStats(context.Background(), &ateompb.GetWorkloadStatsRequest{ActorUid: "uid-c"})
	if resp != nil {
		t.Errorf("GetWorkloadStats() returned response %v, want nil", resp)
	}
	if got := status.Code(err); got != codes.Unimplemented {
		t.Errorf("GetWorkloadStats() error code = %v, want %v (err: %v)", got, codes.Unimplemented, err)
	}
}

// TestAteomServiceStartsAvailable checks that a freshly constructed service
// retains no attribution. GetWorkloadStats's NOT_FOUND-when-available behavior
// is built on this: a non-nil zero value here would make an idle ateom report
// an empty actor's usage instead of refusing.
func TestAteomServiceStartsAvailable(t *testing.T) {
	if s := (&AteomService{}); s.activeActor.Load() != nil {
		t.Errorf("new AteomService.activeActor = %v, want nil", s.activeActor.Load())
	}
}
