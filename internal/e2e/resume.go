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

package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// resumeCapacityWait bounds how long ResumeActorAwaitCapacity waits for a
// worker to free up. Neighboring suites hold workers for the length of a
// test, so this covers several of their actor lifetimes.
const resumeCapacityWait = 90 * time.Second

// ResumeActorAwaitCapacity resumes the actor, retrying while its worker pool
// is saturated. The control plane returns ResourceExhausted when no worker
// is free and expects callers to wait (the router's parking resumer retries
// the same condition); suites running concurrently against shared pools make
// that a normal state in e2e, not a failure. Any other error, or saturation
// outlasting the wait budget, is returned to the caller. Each retry is
// logged so a pass that had to wait stays visible in the test output.
func ResumeActorAwaitCapacity(t *testing.T, ctx context.Context, clients *Clients, req *ateapipb.ResumeActorRequest) (*ateapipb.ResumeActorResponse, error) {
	t.Helper()
	deadline := time.Now().Add(resumeCapacityWait)
	for {
		resp, err := clients.SubstrateAPI.ResumeActor(ctx, req)
		if status.Code(err) != codes.ResourceExhausted || time.Now().After(deadline) {
			return resp, err
		}
		t.Logf("ResumeActor %s/%s: pool saturated (%v); retrying", req.GetActor().GetAtespace(), req.GetActor().GetName(), err)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}
