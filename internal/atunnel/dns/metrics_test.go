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

package dns

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"golang.org/x/net/dns/dnsmessage"

	"go.ate.dev/substrate/internal/ateattr"
)

// collectQueries returns the ate.dns.queries count for each ate.dns.decision.
func collectQueries(t *testing.T, reader *sdkmetric.ManualReader) map[string]int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect: %v", err)
	}
	got := make(map[string]int64)
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != queriesMetric {
				continue
			}
			if m.Unit != "{query}" {
				t.Errorf("unit = %q, want {query}", m.Unit)
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("data type = %T, want Sum[int64]", m.Data)
			}
			if !sum.IsMonotonic {
				t.Error("IsMonotonic = false, want true")
			}
			for _, dp := range sum.DataPoints {
				if dp.Attributes.Len() != 1 {
					t.Errorf("attributes = %v, want only %s", dp.Attributes.ToSlice(), ateattr.DNSDecisionKey)
				}
				v, _ := dp.Attributes.Value(ateattr.DNSDecisionKey)
				got[v.AsString()] += dp.Value
			}
		}
	}
	return got
}

// TestQueriesCountsPolicyDecisions: every query the egress policy judges is
// counted once, on either transport. A malformed query never reaches the
// policy and is not counted.
func TestQueriesCountsPolicyDecisions(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	meter := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("ateworker-gvisor")
	inst, err := newInstruments(meter, []string{"ate-workers.svc.cluster.local", "SVC.Cluster.Local."})
	if err != nil {
		t.Fatalf("newInstruments: %v", err)
	}
	lim := newLimiter()
	allow := func(q dnsmessage.Question) bool { return !strings.HasPrefix(q.Name.String(), "blocked.") }
	h := newDNSHandler(newPendingRequests(lim), lim, allow, inst)

	for i, name := range []string{
		"example.com.",
		"example.com.svc.cluster.local.",
		"blocked.example.com.",
		"blocked.notsvc.cluster.local.",
		"blocked.example.com.ate-workers.svc.cluster.local.",
		"Blocked.Example.COM.svc.CLUSTER.local.",
	} {
		raw, _ := dnsQueryForName(t, uint16(i), name)
		h.onTCPRequest(raw)
	}
	allowed, _ := dnsQueryForName(t, 100, "example.com.")
	h.onUDPRequest(allowed, nil, []string{"10.96.0.10:53"})
	denied, _ := dnsQueryForName(t, 101, "blocked.example.com.")
	h.onUDPRequest(denied, nil, []string{"10.96.0.10:53"})
	h.onTCPRequest(buildTestMessage(t, dnsmessage.Header{ID: 102}, nil))

	want := map[string]int64{
		ateattr.DNSDecisionAllowed:            3,
		ateattr.DNSDecisionDenied:             3,
		ateattr.DNSDecisionDeniedSearchDomain: 2,
	}
	if diff := cmp.Diff(want, collectQueries(t, reader)); diff != "" {
		t.Errorf("ate.dns.queries mismatch (-want +got):\n%s", diff)
	}
}

// TestNewRelayCountsQueries: a relay from NewRelay counts on its meter and
// takes the search domains from the resolv.conf it reads.
func TestNewRelayCountsQueries(t *testing.T) {
	path := filepath.Join(t.TempDir(), "resolv.conf")
	if err := os.WriteFile(path, []byte("search ate-workers.svc.cluster.local\nnameserver 127.0.0.1\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	reader := sdkmetric.NewManualReader()
	relay, err := NewRelay(path, sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("ateworker-gvisor"))
	if err != nil {
		t.Fatalf("NewRelay: %v", err)
	}
	// The actor has no policy, so the relay answers every query itself.
	_, udpAddr, _ := serveLoopbackWithPolicy(t, relay, staticPolicy{})
	conn := dialRelay(t, "udp", udpAddr)

	for i, name := range []string{"example.com.", "example.com.ate-workers.svc.cluster.local."} {
		query, q := dnsQueryForName(t, uint16(i), name)
		if _, err := conn.Write(query); err != nil {
			t.Fatal(err)
		}
		checkAnswerForQuestion(t, readWithin(t, conn), uint16(i), q, dnsmessage.RCodeNameError)
	}

	want := map[string]int64{
		ateattr.DNSDecisionDenied:             1,
		ateattr.DNSDecisionDeniedSearchDomain: 1,
	}
	if diff := cmp.Diff(want, collectQueries(t, reader)); diff != "" {
		t.Errorf("ate.dns.queries mismatch (-want +got):\n%s", diff)
	}
}
