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

package objectstoreplugin

import (
	"errors"
	"testing"

	"github.com/agent-substrate/substrate/internal/apierror"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestCallError(t *testing.T) {
	if err := CallError(nil); err != nil {
		t.Errorf("CallError(nil) = %v, want nil", err)
	}
	unavailable := status.Error(codes.Unavailable, "connection refused")
	if got := apierror.Code(CallError(unavailable)); got != codes.Unavailable {
		t.Errorf("apierror.Code(CallError(Unavailable)) = %s, want %s", got, codes.Unavailable)
	}
	for _, code := range []codes.Code{codes.NotFound, codes.Internal, codes.FailedPrecondition} {
		in := status.Error(code, "plugin error")
		got := CallError(in)
		if !errors.Is(got, in) || status.Code(got) != code {
			t.Errorf("CallError(%s) = %v, want the plugin's status unchanged", code, got)
		}
	}
}
