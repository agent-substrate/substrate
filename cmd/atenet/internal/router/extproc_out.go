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

package router

import (
	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	extproc "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	envoy_type "github.com/envoyproxy/go-control-plane/envoy/type/v3"
)

// reqError carries an HTTP-mappable status code and a client-safe message.
// The underlying cause (if any) is preserved via Unwrap so logs can inspect
// the full chain without leaking server-side detail into the response body.
type reqError struct {
	msg        string
	cause      error
	statusCode int
}

func (e *reqError) Error() string { return e.msg }
func (e *reqError) Unwrap() error { return e.cause }

func addAuthorityMutation(auth string, mut *extproc.HeaderMutation) {
	mut.SetHeaders = append(mut.SetHeaders,
		&corev3.HeaderValueOption{
			Header: &corev3.HeaderValue{
				Key:      ":authority",
				RawValue: []byte(auth),
			},
		},
	)
}

func immediateResponse(statusCode envoy_type.StatusCode, message string) *extproc.ProcessingResponse {
	return &extproc.ProcessingResponse{
		Response: &extproc.ProcessingResponse_ImmediateResponse{
			ImmediateResponse: &extproc.ImmediateResponse{
				Status: &envoy_type.HttpStatus{
					Code: statusCode,
				},
				Body: []byte(message),
				Headers: &extproc.HeaderMutation{
					SetHeaders: []*corev3.HeaderValueOption{
						{
							Header: &corev3.HeaderValue{
								Key:   "content-type",
								Value: "text/plain",
							},
						},
					},
				},
			},
		},
	}
}
