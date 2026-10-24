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

package ingress

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/atenet/internal/router/extproc"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/resources"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// withReason builds the status ateapi sends: code, free text, and the reason
// as an ErrorInfo detail.
func withReason(code codes.Code, msg, reason string) error {
	st, err := status.New(code, msg).WithDetails(&errdetails.ErrorInfo{Reason: reason, Domain: apierror.ErrorInfoDomain})
	if err != nil {
		panic(err)
	}
	return st.Err()
}

func TestParkingFullErrReason(t *testing.T) {
	t.Parallel()
	var reqErr *extproc.ReqError
	if err := parkingFullErr("team-a/ctr6", status.Error(codes.ResourceExhausted, "full")); !errors.As(err, &reqErr) || reqErr.Reason != apierror.ReasonLotFull {
		t.Errorf("parkingFullErr reason = %q, want %q", reqErr.Reason, apierror.ReasonLotFull)
	}
}

// A TERMINAL_STATE refusal is not parked: only a revert changes that state.
func TestActorResumer_TerminalStateIsNotParked(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var calls atomic.Int32
		mock := &resumerMockClient{
			resumeFn: func(context.Context, *ateapi.ResumeActorRequest, ...grpc.CallOption) (*ateapi.ResumeActorResponse, error) {
				calls.Add(1)
				return nil, withReason(codes.FailedPrecondition, "AssignWorker prerequisite not met", apierror.ReasonTerminalState)
			},
		}
		resumer := NewActorResumer(mock, withParking(ParkedRequestConfig{Max: 1, Budget: 5 * time.Second}))
		_, outcome, err := resumer.ResumeActor(context.Background(), resources.ActorRef{Atespace: "team-a", Name: "crashed"})
		if got := status.Code(err); got != codes.FailedPrecondition {
			t.Errorf("code = %v, want FailedPrecondition (err=%v)", got, err)
		}
		if outcome != ResumeOutcomeUnknown {
			t.Errorf("outcome = %q, want %q", outcome, ResumeOutcomeUnknown)
		}
		if n := calls.Load(); n != 1 {
			t.Errorf("resume attempts = %d, want 1 (no parking on a terminal state)", n)
		}
	})
}
