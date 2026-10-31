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
	"errors"
	"testing"

	"go.ate.dev/substrate/cmd/atenet/internal/router/extproc"
	"go.ate.dev/substrate/internal/apierror"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
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
