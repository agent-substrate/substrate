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

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// abortedRetryWait bounds how long RetryAborted retries.
const abortedRetryWait = 30 * time.Second

// RetryAborted calls rpc with req, retrying while it fails with Aborted. The
// control plane returns Aborted while another operation holds the actor's
// lease, such as applying atelet's report of a background snapshot upload.
// Tests call an actor's RPCs through it after an asynchronous suspend, since
// that report can come at any time. Any other result, or Aborted outlasting
// the wait, is returned to the caller.
func RetryAborted[Req, Resp any](t *testing.T, ctx context.Context, rpc func(context.Context, Req, ...grpc.CallOption) (Resp, error), req Req) (Resp, error) {
	t.Helper()
	deadline := time.Now().Add(abortedRetryWait)
	for {
		resp, err := rpc(ctx, req)
		if status.Code(err) != codes.Aborted || time.Now().After(deadline) {
			return resp, err
		}
		t.Logf("%T: %v; retrying", req, err)
		select {
		case <-ctx.Done():
			var zero Resp
			return zero, ctx.Err()
		case <-time.After(time.Second):
		}
	}
}
