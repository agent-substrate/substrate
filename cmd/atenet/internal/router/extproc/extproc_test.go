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
	"reflect"
	"strings"
	"testing"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	envoy_type "github.com/envoyproxy/go-control-plane/envoy/type/v3"
	"google.golang.org/protobuf/types/known/structpb"
)

// stubHandler records that it ran and returns an empty successful Result.
type stubHandler struct {
	direction Direction
	called    bool
	metadata  *RequestMetadata
}

func (h *stubHandler) Direction() Direction { return h.direction }

func (h *stubHandler) HandleRequestHeaders(_ context.Context, md *RequestMetadata) (Result, error) {
	h.called = true
	h.metadata = md
	return Result{Response: &extprocv3.HeadersResponse{Response: &extprocv3.CommonResponse{}}}, nil
}

func TestProcessRequestHeadersForwardsDynamicMetadata(t *testing.T) {
	h := &stubHandler{direction: DirectionEgress}
	s := NewServer(50051, nil, Handlers{DirectionEgress: h})
	req := connectRequest("envoy.filters.http.ext_proc", EgressFilterChainName)
	req.MetadataContext = &corev3.Metadata{FilterMetadata: map[string]*structpb.Struct{
		"dev.ate.egress.peer_certificate": {Fields: map[string]*structpb.Value{
			"chain": structpb.NewStringValue("encoded-peer-chain"),
		}},
	}}
	attrs := req.Attributes
	s.processRequestHeaders(context.Background(), req, req.GetRequestHeaders())
	if h.metadata.DynamicMetadata["dev.ate.egress.peer_certificate"] != req.MetadataContext.FilterMetadata["dev.ate.egress.peer_certificate"] {
		t.Fatal("dynamic metadata was copied instead of forwarded")
	}
	if !reflect.DeepEqual(h.metadata.Attributes, attrs) {
		t.Fatalf("attributes changed: got %#v want %#v", h.metadata.Attributes, attrs)
	}
	if _, ok := h.metadata.Headers["x-forwarded-client-cert"]; ok {
		t.Fatal("certificate header was copied into request metadata")
	}
	if got := h.metadata.DynamicMetadata["dev.ate.egress.peer_certificate"].GetFields()["chain"].GetStringValue(); got != "encoded-peer-chain" {
		t.Errorf("chain metadata = %q, want encoded-peer-chain", got)
	}
	if got := h.metadata.Attribute(FilterChainNameAttribute); got != EgressFilterChainName {
		t.Fatalf("filter chain attribute = %q, want %q", got, EgressFilterChainName)
	}
	h = &stubHandler{direction: DirectionEgress}
	req = connectRequest("envoy.filters.http.ext_proc", EgressFilterChainName)
	s = NewServer(50051, nil, Handlers{DirectionEgress: h})
	s.processRequestHeaders(context.Background(), req, req.GetRequestHeaders())
	if h.metadata.DynamicMetadata != nil {
		t.Fatalf("nil metadata context became %#v", h.metadata.DynamicMetadata)
	}
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
