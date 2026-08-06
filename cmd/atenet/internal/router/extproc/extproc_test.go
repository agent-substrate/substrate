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
	"context"
	"strings"
	"testing"

	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	envoy_type "github.com/envoyproxy/go-control-plane/envoy/type/v3"
)

// stubHandler records that it ran and returns an empty successful Result.
type stubHandler struct {
	direction Direction
	called    bool
}

func (h *stubHandler) Direction() Direction { return h.direction }

func (h *stubHandler) HandleRequestHeaders(context.Context, *RequestMetadata) (Result, error) {
	h.called = true
	return Result{Response: &extprocv3.HeadersResponse{Response: &extprocv3.CommonResponse{}}}, nil
}

// The mux must pick the handler by the Envoy-asserted filter chain, and refuse
// outright when this instance was not started to serve that direction (--mode).
// Falling back to the other handler would run the request through the opposite
// trust model.
func TestProcessRequestHeadersDispatchesByMode(t *testing.T) {
	tests := []struct {
		name       string
		registered []Direction
		chain      string
		wantRan    Direction
		wantStatus envoy_type.StatusCode // 0 means "expect success"
	}{
		{
			name:       "both directions served, egress chain",
			registered: []Direction{DirectionIngress, DirectionEgress},
			chain:      EgressFilterChainName,
			wantRan:    DirectionEgress,
		},
		{
			name:       "both directions served, ingress chain",
			registered: []Direction{DirectionIngress, DirectionEgress},
			chain:      ingressHTTPListener,
			wantRan:    DirectionIngress,
		},
		{
			name:       "egress-only instance refuses ingress traffic",
			registered: []Direction{DirectionEgress},
			chain:      ingressHTTPListener,
			wantStatus: envoy_type.StatusCode_NotFound,
		},
		{
			name:       "ingress-only instance refuses egress traffic",
			registered: []Direction{DirectionIngress},
			chain:      EgressFilterChainName,
			wantStatus: envoy_type.StatusCode_NotFound,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			handlers := Handlers{}
			stubs := map[Direction]*stubHandler{}
			for _, d := range tc.registered {
				stubs[d] = &stubHandler{direction: d}
				handlers[d] = stubs[d]
			}

			s := NewServer(50051, nil, handlers)
			req := connectRequest("envoy.filters.http.ext_proc", tc.chain)
			resp := s.processRequestHeaders(context.Background(), req, req.GetRequestHeaders())

			if tc.wantStatus != 0 {
				ir := resp.GetImmediateResponse()
				if ir == nil {
					t.Fatalf("expected an immediate response, got %v", resp)
				}
				if got := ir.GetStatus().GetCode(); got != tc.wantStatus {
					t.Errorf("status = %v, want %v", got, tc.wantStatus)
				}
				if !strings.Contains(string(ir.GetBody()), "does not serve") {
					t.Errorf("body = %q, want it to say the direction is not served", ir.GetBody())
				}
				for d, h := range stubs {
					if h.called {
						t.Errorf("%s handler ran for a direction this instance does not serve", d)
					}
				}
				return
			}

			if resp.GetImmediateResponse() != nil {
				t.Fatalf("unexpected immediate response: %v", resp.GetImmediateResponse())
			}
			for d, h := range stubs {
				if want := d == tc.wantRan; h.called != want {
					t.Errorf("%s handler called = %v, want %v", d, h.called, want)
				}
			}
		})
	}
}
