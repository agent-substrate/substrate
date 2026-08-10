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

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/agent-substrate/substrate/internal/proto/ateompb"
)

// GetWorkloadStats implements ateompb.Ateom/GetWorkloadStats.
//
// The attribution half is wired up (see AteomService.activeActor); the measurement
// half is not. The micro-VM read goes to the guest agent's StatsContainer
// rather than the host cgroup — guest RAM is a fixed allocation, so the host
// cgroup barely moves with the workload — and lands in the follow-up to
// https://github.com/agent-substrate/substrate/issues/594, at which point this
// stops returning Unimplemented.
func (s *AteomService) GetWorkloadStats(ctx context.Context, req *ateompb.GetWorkloadStatsRequest) (*ateompb.GetWorkloadStatsResponse, error) {
	return nil, status.Error(codes.Unimplemented, "GetWorkloadStats is not implemented yet")
}
