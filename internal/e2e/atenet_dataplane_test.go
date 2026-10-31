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
		if !CurrentAtenetDataplane().IsEgressPolicyDenied(http.StatusBadGateway, `{"error":"request failed: EOF"}`) {
			t.Error("AgentGateway ClientHello refusal was not recognized")
		}
	})
}

func TestAtenetDataplaneParkingBudget(t *testing.T) {
	for _, dataplane := range []string{"envoy", "agentgateway"} {
		t.Run(dataplane, func(t *testing.T) {
			t.Setenv(AtenetDataplaneEnv, dataplane)
			d := CurrentAtenetDataplane()
			if got := d.ParkingBudgetStatus(); got != http.StatusServiceUnavailable {
				t.Errorf("parking budget status = %d, want 503", got)
			}
			if !d.IsRetryableParkingBudgetExhaustion(http.StatusServiceUnavailable, "no worker has room for the actor") {
				t.Error("parking budget exhaustion was not recognized")
			}
			if d.IsRetryableParkingBudgetExhaustion(http.StatusGatewayTimeout, "request timed out") {
				t.Error("transport timeout was recognized as parking budget exhaustion")
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

func TestAtenetDataplaneEgressRequestStats(t *testing.T) {
	const before = `# TYPE envoy_http_ext_proc_egress_policy_mitm_streams_started counter
envoy_http_ext_proc_egress_policy_mitm_streams_started{envoy_http_conn_manager_prefix="mitm_http"} 1
# TYPE envoy_dynamicmodulescustom_ate_egress_cache_hit counter
envoy_dynamicmodulescustom_ate_egress_cache_hit{} 4
# TYPE envoy_dynamicmodulescustom_ate_egress_cache_miss counter
envoy_dynamicmodulescustom_ate_egress_cache_miss{} 1
# TYPE envoy_dynamicmodulescustom_ate_egress_allowed counter
envoy_dynamicmodulescustom_ate_egress_allowed{} 3
# TYPE envoy_dynamicmodulescustom_ate_egress_rejected counter
envoy_dynamicmodulescustom_ate_egress_rejected{} 1
# TYPE envoy_dynamicmodulescustom_ate_egress_policy_change_for_active_connection counter
envoy_dynamicmodulescustom_ate_egress_policy_change_for_active_connection{} 0
`
	const after = `# TYPE envoy_http_ext_proc_egress_policy_mitm_streams_started counter
envoy_http_ext_proc_egress_policy_mitm_streams_started{envoy_http_conn_manager_prefix="mitm_http"} 3
# TYPE envoy_dynamicmodulescustom_ate_egress_cache_hit counter
envoy_dynamicmodulescustom_ate_egress_cache_hit{} 10
# TYPE envoy_dynamicmodulescustom_ate_egress_cache_miss counter
envoy_dynamicmodulescustom_ate_egress_cache_miss{} 4
# TYPE envoy_dynamicmodulescustom_ate_egress_allowed counter
envoy_dynamicmodulescustom_ate_egress_allowed{} 8
# TYPE envoy_dynamicmodulescustom_ate_egress_rejected counter
envoy_dynamicmodulescustom_ate_egress_rejected{} 2
# TYPE envoy_dynamicmodulescustom_ate_egress_policy_change_for_active_connection counter
envoy_dynamicmodulescustom_ate_egress_policy_change_for_active_connection{} 1
`

	t.Run("envoy", func(t *testing.T) {
		t.Setenv(AtenetDataplaneEnv, "")
		got := CurrentAtenetDataplane().EgressRequestStats(before, after)
		want := EgressRequestStats{
			MitmExtProcCalls:                      2,
			EgressPolicyHits:                      6,
			EgressPolicyMisses:                    3,
			EgressPolicyAllowed:                   5,
			EgressPolicyRejected:                  1,
			EgressPolicyChangeForActiveConnection: 1,
		}
		if got != want {
			t.Errorf("EgressRequestStats() = %+v, want %+v", got, want)
		}
	})
	t.Run("agentgateway", func(t *testing.T) {
		t.Setenv(AtenetDataplaneEnv, "agentgateway")
		got := CurrentAtenetDataplane().EgressRequestStats(before, after)
		want := EgressRequestStats{
			MitmExtProcCalls:                      -1,
			EgressPolicyHits:                      -1,
			EgressPolicyMisses:                    -1,
			EgressPolicyAllowed:                   -1,
			EgressPolicyRejected:                  -1,
			EgressPolicyChangeForActiveConnection: -1,
		}
		if got != want {
			t.Errorf("EgressRequestStats() = %+v, want %+v", got, want)
		}
	})
}
