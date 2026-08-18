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
	"testing"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	"google.golang.org/protobuf/types/known/structpb"
)

// connectRequest builds a RequestHeaders ProcessingRequest for a CONNECT,
// optionally attributed to a filter chain. filterKey is the ext_proc filter
// name Envoy keys the attributes map by.
func connectRequest(filterKey, chain string) *extprocv3.ProcessingRequest {
	req := &extprocv3.ProcessingRequest{
		Request: &extprocv3.ProcessingRequest_RequestHeaders{
			RequestHeaders: &extprocv3.HttpHeaders{
				Headers: &corev3.HeaderMap{
					Headers: []*corev3.HeaderValue{
						{Key: ":method", RawValue: []byte("CONNECT")},
						{Key: ":authority", RawValue: []byte("10.0.0.9:443")},
					},
				},
			},
		},
	}
	if chain != "" {
		req.Attributes = map[string]*structpb.Struct{
			filterKey: {
				Fields: map[string]*structpb.Value{
					FilterChainNameAttribute: structpb.NewStringValue(chain),
				},
			},
		}
	}
	return req
}

// The ingress listener names the router's xDS server assigns. Spelled out here
// rather than imported: the point of this package is that the mux knows nothing
// about either direction's configuration, so the test may not reach for it
// either.
const (
	ingressHTTPListener  = "ingress_http_listener"
	ingressHTTPSListener = "ingress_https_listener"
)

func TestDirectionOf(t *testing.T) {
	tests := []struct {
		name      string
		filterKey string
		chain     string
		want      Direction
	}{
		{
			name:      "egress filter chain",
			filterKey: "envoy.filters.http.ext_proc",
			chain:     EgressFilterChainName,
			want:      DirectionEgress,
		},
		{
			name:      "egress filter chain under a renamed filter",
			filterKey: "some.custom.ext_proc.name",
			chain:     EgressFilterChainName,
			want:      DirectionEgress,
		},
		{
			// The pre-listener-dispatch hole: an external client sending
			// CONNECT to the ingress gateway must not reach the egress handler,
			// whose denials would otherwise report whether an arbitrary actor
			// exists and is running.
			name:      "CONNECT on the ingress HTTP listener",
			filterKey: "envoy.filters.http.ext_proc",
			chain:     ingressHTTPListener,
			want:      DirectionIngress,
		},
		{
			name:      "CONNECT on the ingress HTTPS listener",
			filterKey: "envoy.filters.http.ext_proc",
			chain:     ingressHTTPSListener,
			want:      DirectionIngress,
		},
		{
			// A listener that never requested the attribute falls back to
			// ingress, the fail-safe direction.
			name:  "no attributes at all",
			chain: "",
			want:  DirectionIngress,
		},
		{
			name:      "unrecognised filter chain",
			filterKey: "envoy.filters.http.ext_proc",
			chain:     "some-other-chain",
			want:      DirectionIngress,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := directionOf(connectRequest(tc.filterKey, tc.chain)); got != tc.want {
				t.Errorf("directionOf() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestDirectionOfAgentgatewayAttribute(t *testing.T) {
	req := connectRequest("", "")
	req.Attributes = map[string]*structpb.Struct{
		"envoy.filters.http.ext_proc": {
			Fields: map[string]*structpb.Value{
				directionAttribute: structpb.NewStringValue(string(DirectionEgress)),
			},
		},
	}
	if got := directionOf(req); got != DirectionEgress {
		t.Errorf("directionOf() = %v, want %v", got, DirectionEgress)
	}
}

// A request the client dresses up to look like egress must not be enough: only
// the Envoy-asserted filter chain name selects the egress handler.
func TestDirectionOfIgnoresClientSuppliedAttributeHeader(t *testing.T) {
	req := connectRequest("envoy.filters.http.ext_proc", ingressHTTPListener)
	rh := req.GetRequestHeaders().GetHeaders()
	rh.Headers = append(rh.Headers,
		&corev3.HeaderValue{Key: FilterChainNameAttribute, RawValue: []byte(EgressFilterChainName)},
		&corev3.HeaderValue{Key: "x-envoy-filter-chain-name", RawValue: []byte(EgressFilterChainName)},
	)

	if got := directionOf(req); got != DirectionIngress {
		t.Errorf("directionOf() = %v for a client-forged filter chain header, want %v", got, DirectionIngress)
	}
}
