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
	"errors"
	"net/http"
	"testing"
	"time"
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

func TestAtenetDataplaneTLSPassthroughEgressPolicy(t *testing.T) {
	t.Run("envoy", func(t *testing.T) {
		t.Setenv(AtenetDataplaneEnv, "")
		if !CurrentAtenetDataplane().SupportsTLSPassthroughEgressPolicy() {
			t.Error("Envoy TLS passthrough egress policy was not supported")
		}
	})
	t.Run("agentgateway", func(t *testing.T) {
		t.Setenv(AtenetDataplaneEnv, "agentgateway")
		if CurrentAtenetDataplane().SupportsTLSPassthroughEgressPolicy() {
			t.Error("AgentGateway TLS passthrough egress policy unexpectedly reported support")
		}
	})
}

func TestPollParkingCount(t *testing.T) {
	reads := []struct {
		n   int
		err error
	}{{0, errors.New("scrape failed")}, {1, nil}, {2, nil}}
	var calls int
	read := func(context.Context) (int, error) {
		r := reads[min(calls, len(reads)-1)]
		calls++
		return r.n, r.err
	}

	got, err := pollParkingCount(context.Background(), read, func(n int) bool { return n == 2 }, time.Minute, 0)
	if err != nil || got != 2 || calls != 3 {
		t.Errorf("pollParkingCount = %d, %v after %d reads; want 2, nil after 3", got, err, calls)
	}

	calls = 0
	got, err = pollParkingCount(context.Background(), read, func(n int) bool { return n > 5 }, 20*time.Millisecond, time.Millisecond)
	if err == nil || got != 2 {
		t.Errorf("pollParkingCount = %d, %v; want the last read 2 and a timeout error", got, err)
	}
}
