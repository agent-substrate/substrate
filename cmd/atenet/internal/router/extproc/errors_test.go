//  Copyright 2026 Google LLC
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

package extproc

import (
	"errors"
	"testing"

	envoy_type "github.com/envoyproxy/go-control-plane/envoy/type/v3"
)

func TestNewReqError(t *testing.T) {
	t.Parallel()

	err := NewReqError(envoy_type.StatusCode_BadRequest, "actor %q is %s", "abc", "bad")
	if err == nil {
		t.Fatal("NewReqError returned nil")
	}
	var reqErr *ReqError
	if !errors.As(err, &reqErr) {
		t.Fatalf("errors.As(*ReqError) = false, want true; err type = %T", err)
	}
	if reqErr.StatusCode != int(envoy_type.StatusCode_BadRequest) {
		t.Errorf("StatusCode = %d, want %d", reqErr.StatusCode, envoy_type.StatusCode_BadRequest)
	}
	if got, want := err.Error(), `actor "abc" is bad`; got != want {
		t.Errorf("Error() = %q, want %q", got, want)
	}
}

// TestImmediateResponseHeaderEncoding pins the RawValue encoding: Envoy drops
// plain Value in ext_proc header mutations, so a Value-encoded header reaches
// the client with an empty value (found live — content-type on every immediate
// response had been arriving empty).
func TestImmediateResponseHeaderEncoding(t *testing.T) {
	t.Parallel()

	resp := ImmediateResponse(envoy_type.StatusCode_ServiceUnavailable, "body")
	set := resp.GetImmediateResponse().GetHeaders().GetSetHeaders()
	if len(set) != 1 {
		t.Fatalf("SetHeaders count = %d, want 1", len(set))
	}
	h := set[0].GetHeader()
	if h.GetKey() != "content-type" || string(h.GetRawValue()) != "text/plain" {
		t.Errorf("header = %q:%q (RawValue), want content-type:text/plain", h.GetKey(), h.GetRawValue())
	}
	if h.GetValue() != "" {
		t.Errorf("header uses Value (%q); must use RawValue only", h.GetValue())
	}
}
