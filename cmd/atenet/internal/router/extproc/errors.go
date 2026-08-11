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

package extproc

import (
	"fmt"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	envoy_type "github.com/envoyproxy/go-control-plane/envoy/type/v3"
)

// ReqError is a handler's denial of a request: an HTTP-mappable status code
// and a client-safe message the mux turns into an immediate response. The
// underlying cause (if any) is preserved via Unwrap so logs can inspect the
// full chain without leaking server-side detail into the response body.
type ReqError struct {
	Msg        string
	Cause      error
	StatusCode int
}

func (e *ReqError) Error() string { return e.Msg }
func (e *ReqError) Unwrap() error { return e.Cause }

// NewReqError builds a ReqError whose body is the formatted message and no
// wrapped cause. Use WrapReqError when a cause is available.
func NewReqError(code envoy_type.StatusCode, format string, args ...any) error {
	return &ReqError{
		Msg:        fmt.Sprintf(format, args...),
		StatusCode: int(code),
	}
}

// WrapReqError builds a ReqError that keeps cause reachable through Unwrap
// while answering the client with only the formatted message.
func WrapReqError(code envoy_type.StatusCode, cause error, format string, args ...any) error {
	return &ReqError{
		Msg:        fmt.Sprintf(format, args...),
		Cause:      cause,
		StatusCode: int(code),
	}
}

// ImmediateResponse tells the dataplane to answer the request itself, without
// going upstream.
func ImmediateResponse(statusCode envoy_type.StatusCode, message string) *extprocv3.ProcessingResponse {
	return &extprocv3.ProcessingResponse{
		Response: &extprocv3.ProcessingResponse_ImmediateResponse{
			ImmediateResponse: &extprocv3.ImmediateResponse{
				Status: &envoy_type.HttpStatus{
					Code: statusCode,
				},
				Body: []byte(message),
				Headers: &extprocv3.HeaderMutation{
					SetHeaders: []*corev3.HeaderValueOption{
						{
							// Using RawValues instead of Value: newer versions of Envoy
							// drop Value and use RawValue
							Header: &corev3.HeaderValue{
								Key:      "content-type",
								RawValue: []byte("text/plain"),
							},
						},
					},
				},
			},
		},
	}
}
