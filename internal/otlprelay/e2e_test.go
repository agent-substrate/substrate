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

package otlprelay

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/serverboot"
)

// TestEndToEndThroughServerboot exercises the whole path an ateom span actually
// takes, rather than the relay in isolation:
//
//	serverboot.InitTracing → OTLP exporter → unix socket → relay → collector
//
// The relay tests above speak the collector protocol directly, so they would
// still pass if TracingOptions.ExporterConn were wired up wrong and the exporter
// quietly kept dialing OTEL_EXPORTER_OTLP_ENDPOINT. This one would not: the
// endpoint variable points at the fake collector *through* the relay only, and
// the assertion is that the span arrived carrying ateom's own service.name.
//
// Run it on its own to watch the hop happen:
//
//	go test ./internal/otlprelay/ -run TestEndToEndThroughServerboot -v
func TestEndToEndThroughServerboot(t *testing.T) {
	sink, collector := startFakeCollector(t)
	sock := startRelay(t, collector)
	// Re-point the generic endpoint at an unroutable address so an exporter
	// that ignored ExporterConn would fail deterministically instead of dialing
	// the test collector directly.
	t.Setenv(endpointEnv, "http://127.0.0.1:1")
	t.Logf("fake collector on %s, relay socket %s", collector, sock)

	conn, err := Dial(context.Background(), sock)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	if conn == nil {
		t.Fatal("Dial returned no connection; the exporter would have bypassed the relay")
	}
	defer conn.Close()

	const serviceName = "ateom-microvm"
	tp, err := serverboot.InitTracing(context.Background(), serverboot.TracingOptions{
		ServiceName: serviceName,
		// Ratio 1.0: this test asserts on delivery, not on sampling.
		Sampling:     serverboot.ParentRatioSampling(1.0),
		ExporterConn: conn,
		RelayCapable: true,
	})
	if err != nil {
		t.Fatalf("InitTracing: %v", err)
	}

	_, span := tp.Tracer("relay-e2e").Start(context.Background(), "RunWorkload")
	span.End()

	// Shutdown flushes the batch processor, which is what actually puts the
	// span on the wire.
	if err := tp.Shutdown(context.Background()); err != nil {
		t.Fatalf("TracerProvider.Shutdown: %v", err)
	}

	select {
	case <-sink.got:
	case <-time.After(10 * time.Second):
		t.Fatal("collector never received a span through the relay")
	}

	sink.mu.Lock()
	defer sink.mu.Unlock()
	if len(sink.traces) == 0 {
		t.Fatal("collector recorded no trace exports")
	}

	var gotService, gotSpan, gotRelay string
	for _, req := range sink.traces {
		for _, rs := range req.GetResourceSpans() {
			for _, attr := range rs.GetResource().GetAttributes() {
				if attr.GetKey() == "service.name" {
					gotService = attr.GetValue().GetStringValue()
				}
				if attr.GetKey() == relayAttrKey {
					gotRelay = attr.GetValue().GetStringValue()
				}
			}
			for _, ss := range rs.GetScopeSpans() {
				for _, s := range ss.GetSpans() {
					gotSpan = s.GetName()
				}
			}
		}
	}
	t.Logf("collector received span %q from service %q (relay=%q)", gotSpan, gotService, gotRelay)

	// The point of forwarding the request verbatim: the span is still ateom's,
	// not atelet's.
	if gotService != serviceName {
		t.Errorf("span arrived with service.name %q, want %q; the relay must not re-attribute it", gotService, serviceName)
	}
	if gotSpan != "RunWorkload" {
		t.Errorf("span arrived named %q, want %q", gotSpan, "RunWorkload")
	}
	if gotRelay != "relay" {
		t.Errorf("span arrived with %s %q, want %q", relayAttrKey, gotRelay, "relay")
	}
}

// relayAttrKey duplicates serverboot's unexported constant. Keeping a literal
// here is the point: if serverboot renames the attribute, the dashboards and
// alerts keyed on it break too, and this test is where that shows up.
const relayAttrKey = "ate.otlp.relay"

// TestEndToEndFallsBackToDirect is the other half of TestEndToEndThroughServerboot:
// the ateom asked for the relay, atelet was not serving one, and the exporter
// must fall back to the network path rather than dropping telemetry.
//
// This is the case the ateoms degrade into instead of exiting (see the Dial call
// in cmd/ateom-*/main.go), so it needs to be more than a nil check: the span has
// to reach the collector, and it has to be distinguishable from a relayed one at
// query time — hence the "direct" attribute.
func TestEndToEndFallsBackToDirect(t *testing.T) {
	sink, collector := startFakeCollector(t)
	// No relay: the socket path is inside a fresh temp dir nothing created.
	sock := filepath.Join(t.TempDir(), "absent-atelet-otlp.sock")
	// The direct path is the exporter dialing this itself, which is exactly what
	// the relay test points at an unroutable address to rule out.
	t.Setenv(endpointEnv, "http://"+collector)
	t.Logf("fake collector on %s, absent relay socket %s", collector, sock)

	conn, err := Dial(context.Background(), sock)
	if err != nil {
		t.Fatalf("Dial with no relay present must not fail: %v", err)
	}
	if conn != nil {
		conn.Close()
		t.Fatal("Dial returned a connection for a socket that does not exist")
	}

	const serviceName = "ateom-microvm"
	tp, err := serverboot.InitTracing(context.Background(), serverboot.TracingOptions{
		ServiceName:  serviceName,
		Sampling:     serverboot.ParentRatioSampling(1.0),
		ExporterConn: conn, // nil: the fallback
		RelayCapable: true,
	})
	if err != nil {
		t.Fatalf("InitTracing: %v", err)
	}

	_, span := tp.Tracer("relay-e2e").Start(context.Background(), "RunWorkload")
	span.End()

	if err := tp.Shutdown(context.Background()); err != nil {
		t.Fatalf("TracerProvider.Shutdown: %v", err)
	}

	select {
	case <-sink.got:
	case <-time.After(10 * time.Second):
		t.Fatal("collector never received a span over the direct path")
	}

	sink.mu.Lock()
	defer sink.mu.Unlock()
	var gotService, gotRelay string
	for _, req := range sink.traces {
		for _, rs := range req.GetResourceSpans() {
			for _, attr := range rs.GetResource().GetAttributes() {
				switch attr.GetKey() {
				case "service.name":
					gotService = attr.GetValue().GetStringValue()
				case relayAttrKey:
					gotRelay = attr.GetValue().GetStringValue()
				}
			}
		}
	}
	if gotService != serviceName {
		t.Errorf("span arrived with service.name %q, want %q", gotService, serviceName)
	}
	// Without this, a node whose atelet never came up looks identical to a
	// healthy one in the trace store.
	if gotRelay != "direct" {
		t.Errorf("span arrived with %s %q, want %q", relayAttrKey, gotRelay, "direct")
	}
}
