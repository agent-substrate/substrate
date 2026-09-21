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
	"context"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

// AtenetDataplaneEnv selects the dataplane exercised by an e2e lane.
const AtenetDataplaneEnv = "E2E_ATENET_DATAPLANE"

// AtenetDataplane captures the observable differences between the supported
// atenet dataplanes. Suites use these operations rather than branching on the
// selected implementation.
type AtenetDataplane interface {
	NewParkingObserver(context.Context) (ParkingObserver, error)
	IsRetryableParkingBudgetExhaustion(status int, body string) bool
	ParkingBudgetStatus() int
	IsEgressPolicyDenied(status int, body string) bool
	SupportsTLSPassthroughEgressPolicy() bool
	PlatformMetricPrefixes([]string) []string
	RouteDurationSeen(context.Context, string) (bool, error)
	SupportsIngressProtocolDowngrade() bool
}

// CurrentAtenetDataplane returns the implementation selected for this test
// process. Envoy remains the default to match installation defaults.
func CurrentAtenetDataplane() AtenetDataplane {
	if os.Getenv(AtenetDataplaneEnv) == "agentgateway" {
		return agentGatewayAtenetDataplane{}
	}
	return envoyAtenetDataplane{}
}

// ParkingObserver waits for the dataplane's active parked-request gauge.
type ParkingObserver interface {
	WaitForCount(context.Context, func(int) bool) (int, error)
	Close()
}

type envoyAtenetDataplane struct{}

func (envoyAtenetDataplane) NewParkingObserver(ctx context.Context) (ParkingObserver, error) {
	return NewStatuszClient(ctx)
}

func (envoyAtenetDataplane) IsRetryableParkingBudgetExhaustion(status int, body string) bool {
	return status == http.StatusServiceUnavailable && strings.Contains(body, "no free workers available")
}

func (envoyAtenetDataplane) ParkingBudgetStatus() int { return http.StatusServiceUnavailable }

func (envoyAtenetDataplane) IsEgressPolicyDenied(status int, body string) bool {
	return (status == http.StatusForbidden && strings.Contains(body, "egress denied")) ||
		(status == http.StatusBadGateway && strings.Contains(body, "request failed"))
}

func (envoyAtenetDataplane) SupportsTLSPassthroughEgressPolicy() bool { return true }

func (envoyAtenetDataplane) PlatformMetricPrefixes(prefixes []string) []string { return prefixes }

func (envoyAtenetDataplane) RouteDurationSeen(_ context.Context, collectorScrape string) (bool, error) {
	return len(MissingPlatformMetrics(collectorScrape, []string{"atenet_router_route_duration"})) == 0, nil
}

func (envoyAtenetDataplane) SupportsIngressProtocolDowngrade() bool { return true }

type agentGatewayAtenetDataplane struct{}

func (agentGatewayAtenetDataplane) NewParkingObserver(context.Context) (ParkingObserver, error) {
	return agentGatewayParkingObserver{}, nil
}

func (agentGatewayAtenetDataplane) IsRetryableParkingBudgetExhaustion(status int, body string) bool {
	return status == http.StatusGatewayTimeout && strings.Contains(body, "request timed out")
}

func (agentGatewayAtenetDataplane) ParkingBudgetStatus() int { return http.StatusGatewayTimeout }

func (agentGatewayAtenetDataplane) IsEgressPolicyDenied(status int, body string) bool {
	return status == http.StatusForbidden && strings.Contains(body, "actor egress policy denied")
}

// TODO: Apply substrateEgress to TLS passthrough routes in AgentGateway.
func (agentGatewayAtenetDataplane) SupportsTLSPassthroughEgressPolicy() bool { return false }

func (agentGatewayAtenetDataplane) PlatformMetricPrefixes(prefixes []string) []string {
	filtered := make([]string, 0, len(prefixes))
	for _, prefix := range prefixes {
		if prefix != "atenet_router_route_duration" {
			filtered = append(filtered, prefix)
		}
	}
	return filtered
}

func (agentGatewayAtenetDataplane) RouteDurationSeen(ctx context.Context, _ string) (bool, error) {
	scrape, err := ScrapeAgentGatewayRouterMetrics(ctx)
	if err != nil {
		return false, err
	}
	return len(MissingPlatformMetrics(scrape, []string{"agentgateway_atenet_router_route_duration_seconds"})) == 0, nil
}

func (agentGatewayAtenetDataplane) SupportsIngressProtocolDowngrade() bool { return false }

type agentGatewayParkingObserver struct{}

func (agentGatewayParkingObserver) WaitForCount(ctx context.Context, cond func(int) bool) (int, error) {
	deadline := time.Now().Add(4 * time.Second)
	var last int
	for time.Now().Before(deadline) {
		active, err := agentGatewayParkingCount(ctx)
		if err == nil {
			last = active
			if cond(active) {
				return active, nil
			}
		}
		time.Sleep(150 * time.Millisecond)
	}
	return last, fmt.Errorf("timed out waiting for the parking gauge to satisfy the condition")
}

func (agentGatewayParkingObserver) Close() {}

func agentGatewayParkingCount(ctx context.Context) (int, error) {
	scrape, err := ScrapeAgentGatewayRouterMetrics(ctx)
	if err != nil {
		return 0, err
	}
	for _, line := range strings.Split(scrape, "\n") {
		fields := strings.Fields(line)
		if len(fields) != 2 || !strings.HasPrefix(fields[0], "agentgateway_substrate_request_parking_active") {
			continue
		}
		value, err := strconv.ParseFloat(fields[1], 64)
		if err != nil {
			return 0, err
		}
		return int(value), nil
	}
	return 0, fmt.Errorf("AgentGateway parking gauge not found")
}
