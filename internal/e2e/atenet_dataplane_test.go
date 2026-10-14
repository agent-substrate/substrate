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

package e2e

import (
	"fmt"
	"net/http"
	"testing"
)

func TestAtenetDataplaneEgressPolicyDenial(t *testing.T) {
	t.Run("envoy", func(t *testing.T) {
		t.Setenv(AtenetDataplaneEnv, "")
		if !CurrentAtenetDataplane().IsEgressPolicyDenied(http.StatusBadGateway, "request failed") {
			t.Error("Envoy CONNECT refusal was not recognized as an egress-policy denial")
		}
	})
	t.Run("agentgateway", func(t *testing.T) {
		t.Setenv(AtenetDataplaneEnv, "agentgateway")
		if !CurrentAtenetDataplane().IsEgressPolicyDenied(http.StatusForbidden, "actor egress policy denied destination") {
			t.Error("AgentGateway direct policy denial was not recognized")
		}
	})
}

func TestAtenetDataplaneWebSocketEgressDenial(t *testing.T) {
	const (
		standard = "WebSocket egress is not supported"
		agw      = "missing substrate egress request policy"
	)
	for _, tt := range []struct {
		dataplane string
		status    int
		body      string
		want      bool
	}{
		{"", http.StatusForbidden, standard, true},
		{"", http.StatusForbidden, agw, false},
		{"", http.StatusOK, standard, false},
		{"agentgateway", http.StatusForbidden, standard, true},
		{"agentgateway", http.StatusForbidden, agw, true},
		{"agentgateway", http.StatusForbidden, "other", false},
		{"agentgateway", http.StatusBadGateway, agw, false},
	} {
		t.Run(fmt.Sprintf("%q/%d/%s", tt.dataplane, tt.status, tt.body), func(t *testing.T) {
			t.Setenv(AtenetDataplaneEnv, tt.dataplane)
			if got := CurrentAtenetDataplane().IsWebSocketEgressDenied(tt.status, tt.body); got != tt.want {
				t.Errorf("IsWebSocketEgressDenied(%d, %q) = %v, want %v", tt.status, tt.body, got, tt.want)
			}
		})
	}
}

func TestAtenetDataplaneEgressConnectStats(t *testing.T) {
	const before = `# TYPE envoy_http_downstream_cx_http2_total counter
envoy_http_downstream_cx_http2_total{envoy_http_conn_manager_prefix="egress_connect"} 1
envoy_http_downstream_rq_xx{envoy_response_code_class="2",envoy_http_conn_manager_prefix="egress_connect"} 2
`
	const after = `# TYPE envoy_http_downstream_cx_http2_total counter
envoy_http_downstream_cx_http2_total{envoy_http_conn_manager_prefix="egress_connect"} 4
envoy_http_downstream_rq_xx{envoy_response_code_class="2",envoy_http_conn_manager_prefix="egress_connect"} 252
`

	t.Run("envoy", func(t *testing.T) {
		t.Setenv(AtenetDataplaneEnv, "")
		got := CurrentAtenetDataplane().EgressConnectStats(before, after)
		want := EgressConnectStats{Connections: 3, Requests: 250}
		if got != want {
			t.Errorf("EgressConnectStats() = %+v, want %+v", got, want)
		}
	})
	t.Run("agentgateway", func(t *testing.T) {
		t.Setenv(AtenetDataplaneEnv, "agentgateway")
		got := CurrentAtenetDataplane().EgressConnectStats(before, after)
		want := EgressConnectStats{Connections: -1, Requests: -1}
		if got != want {
			t.Errorf("EgressConnectStats() = %+v, want %+v", got, want)
		}
	})
}
