// Copyright 2026 The Agent Substrate Authors
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

package extproc_test

import (
	"context"
	"testing"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	extprocv3filter "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/http/ext_proc/v3"
	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	envoy_type "github.com/envoyproxy/go-control-plane/envoy/type/v3"
	"google.golang.org/protobuf/types/known/structpb"

	"go.ate.dev/substrate/cmd/atenet/internal/router/extproc"
)

type responseHook = func(*extproc.RequestMetadata) *extprocv3.ProcessingResponse

// hookHandler routes every request and asks for its response headers.
type hookHandler struct{ hook responseHook }

func (hookHandler) Direction() extproc.Direction { return extproc.DirectionIngress }

func (h hookHandler) HandleRequestHeaders(context.Context, *extproc.RequestMetadata) (extproc.Result, error) {
	return extproc.Result{Response: &extprocv3.HeadersResponse{}, OnResponseHeaders: h.hook}, nil
}

func TestProcessResponseHeaders(t *testing.T) {
	tests := []struct {
		name          string
		hook          responseHook
		wantImmediate bool
	}{
		{name: "no hook lets the response through"},
		{
			name: "hook returning nil lets the response through",
			hook: func(*extproc.RequestMetadata) *extprocv3.ProcessingResponse { return nil },
		},
		{
			name: "hook answers in the response's place",
			hook: func(resp *extproc.RequestMetadata) *extprocv3.ProcessingResponse {
				if resp.Header(":status") != "421" || resp.Attribute("response.code_details") != "via_upstream" {
					return nil
				}
				return extproc.ImmediateResponse(envoy_type.StatusCode_ServiceUnavailable, "stale")
			},
			wantImmediate: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			stream := &websocketProcessStream{ctx: context.Background(), requests: []*extprocv3.ProcessingRequest{
				{Request: &extprocv3.ProcessingRequest_RequestHeaders{RequestHeaders: &extprocv3.HttpHeaders{}}},
				{
					Request: &extprocv3.ProcessingRequest_ResponseHeaders{ResponseHeaders: &extprocv3.HttpHeaders{
						Headers: &corev3.HeaderMap{Headers: []*corev3.HeaderValue{{Key: ":status", RawValue: []byte("421")}}},
					}},
					Attributes: map[string]*structpb.Struct{
						"envoy.filters.http.ext_proc": {Fields: map[string]*structpb.Value{
							"response.code_details": structpb.NewStringValue("via_upstream"),
						}},
					},
				},
			}}
			server := extproc.NewServer(0, nil, extproc.Handlers{extproc.DirectionIngress: hookHandler{hook: tc.hook}})

			if err := server.Process(stream); err != nil {
				t.Fatalf("Process() error = %v", err)
			}
			if len(stream.responses) != 2 {
				t.Fatalf("responses = %d, want 2", len(stream.responses))
			}
			override := stream.responses[0].GetModeOverride()
			if tc.hook == nil {
				if override != nil {
					t.Errorf("RequestHeaders mode_override = %v, want nil when no response hook is set", override)
				}
			} else if got := override.GetResponseHeaderMode(); got != extprocv3filter.ProcessingMode_SEND {
				t.Errorf("RequestHeaders mode_override.response_header_mode = %v, want SEND", got)
			}
			got := stream.responses[1]
			if tc.wantImmediate {
				if code := got.GetImmediateResponse().GetStatus().GetCode(); code != envoy_type.StatusCode_ServiceUnavailable {
					t.Errorf("response = %v, want an immediate 503", got)
				}
			} else if got.GetResponseHeaders() == nil {
				t.Errorf("response = %v, want the response headers let through", got)
			}
		})
	}
}
