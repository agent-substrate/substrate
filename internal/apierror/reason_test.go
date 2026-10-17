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

package apierror

import (
	"errors"
	"fmt"
	"testing"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
)

func TestWithReason(t *testing.T) {
	err := fmt.Errorf("wrap: %w", WithReason(ResourceExhausted("full"), ReasonNoCapacity))
	if got := Reason(err); got != ReasonNoCapacity {
		t.Errorf("Reason() = %q, want %q", got, ReasonNoCapacity)
	}
	st, _ := FromError(err)
	if st.Message() != "full" {
		t.Errorf("Message() = %q, want %q", st.Message(), "full")
	}
	var infos []*errdetails.ErrorInfo
	for _, d := range st.Details() {
		if info, ok := d.(*errdetails.ErrorInfo); ok {
			infos = append(infos, info)
		}
	}
	if len(infos) != 1 || infos[0].GetReason() != ReasonNoCapacity || infos[0].GetDomain() != ErrorInfoDomain {
		t.Errorf("details = %v, want one ErrorInfo %s/%s", st.Details(), ErrorInfoDomain, ReasonNoCapacity)
	}

	if st, _ := FromError(NotFound("x")); len(st.Details()) != 0 {
		t.Errorf("details without a reason = %v, want none", st.Details())
	}
	plain := errors.New("plain")
	if got := WithReason(plain, ReasonNoCapacity); got != plain || Reason(got) != "" {
		t.Errorf("WithReason(plain) = %v, want the error unchanged and no reason", got)
	}
}
