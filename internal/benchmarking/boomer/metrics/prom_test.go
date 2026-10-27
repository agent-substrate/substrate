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

package metrics

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// The server histogram resolves a fast RPC's sub-millisecond time,
// which a first bucket at 1 ms would hide.
func TestRecordServerLatencyKeepsSubMillisecond(t *testing.T) {
	serverDuration.Reset()
	RecordServerLatency("grpc", "SubMs", "TestUser", 300*time.Microsecond)

	got := collectLines(t, "locust_server_duration_milliseconds")
	for _, line := range []string{
		`locust_server_duration_milliseconds_bucket{method="grpc",name="SubMs",user_class="TestUser",le="0.2"} 0`,
		`locust_server_duration_milliseconds_bucket{method="grpc",name="SubMs",user_class="TestUser",le="0.4"} 1`,
		`locust_server_duration_milliseconds_sum{method="grpc",name="SubMs",user_class="TestUser"} 0.3`,
	} {
		if !strings.Contains(got, line) {
			t.Errorf("missing line %q in:\n%s", line, got)
		}
	}
}

func TestRecordServerLatency(t *testing.T) {
	serverDuration.Reset()
	RecordServerLatency("grpc", "Srv", "TestUser", 1500*time.Microsecond)
	RecordServerLatency("grpc", "Srv", "TestUser", 2500*time.Microsecond)

	got := collectLines(t, "locust_server_duration_milliseconds")
	if !strings.Contains(got, `locust_server_duration_milliseconds_sum{method="grpc",name="Srv",user_class="TestUser"} 4`) {
		t.Errorf("sum is not 4 ms:\n%s", got)
	}
	if !strings.Contains(got, `locust_server_duration_milliseconds_count{method="grpc",name="Srv",user_class="TestUser"} 2`) {
		t.Errorf("count is not 2:\n%s", got)
	}
}

// collectLines scrapes the default registry and returns the lines of the
// named metric family.
func collectLines(t *testing.T, name string) string {
	t.Helper()
	rec := httptest.NewRecorder()
	promhttp.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	var b strings.Builder
	for _, line := range strings.Split(rec.Body.String(), "\n") {
		if strings.Contains(line, name) {
			b.WriteString(line)
			b.WriteString("\n")
		}
	}
	return b.String()
}
