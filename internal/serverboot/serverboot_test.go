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

package serverboot

import (
	"bytes"
	"context"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"go.ate.dev/substrate/internal/ateattr"
)

func resourceAttrs(res *resource.Resource) map[string]string {
	m := make(map[string]string)
	for _, kv := range res.Attributes() {
		m[string(kv.Key)] = kv.Value.String()
	}
	return m
}

func TestNewResourceDefaults(t *testing.T) {
	res, err := newResource(context.Background(), "ateapi")
	if err != nil {
		t.Fatalf("newResource: %v", err)
	}
	attrs := resourceAttrs(res)
	if got := attrs[string(semconv.ServiceNameKey)]; got != "ateapi" {
		t.Errorf("service.name = %q, want ateapi", got)
	}
	if attrs[string(semconv.ServiceInstanceIDKey)] == "" {
		t.Error("service.instance.id must be set")
	}
}

func TestNewResourceEnvWins(t *testing.T) {
	t.Setenv("OTEL_SERVICE_NAME", "from-env")
	t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "service.instance.id=fixed-id")
	res, err := newResource(context.Background(), "ateapi")
	if err != nil {
		t.Fatalf("newResource: %v", err)
	}
	attrs := resourceAttrs(res)
	if got := attrs[string(semconv.ServiceNameKey)]; got != "from-env" {
		t.Errorf("service.name = %q, want from-env (OTEL_SERVICE_NAME must win)", got)
	}
	if got := attrs[string(semconv.ServiceInstanceIDKey)]; got != "fixed-id" {
		t.Errorf("service.instance.id = %q, want fixed-id (OTEL_RESOURCE_ATTRIBUTES must win)", got)
	}
}

// lazyConn is a ClientConn that never dials: grpc.NewClient connects on first
// RPC, and relayAttrs only cares whether the pointer is nil.
func lazyConn(t *testing.T) *grpc.ClientConn {
	t.Helper()
	conn, err := grpc.NewClient("passthrough:///unused", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

// The three cases relayAttrs exists to separate: a component that got the
// relay, one that wanted it and fell back, and one that was never offered one.
// The last must carry no attribute at all rather than a misleading "direct".
func TestRelayAttrs(t *testing.T) {
	for _, tc := range []struct {
		name         string
		relayCapable bool
		conn         bool
		want         string // "" means the attribute must be absent
	}{
		{name: "relay capable with conn", relayCapable: true, conn: true, want: "relay"},
		{name: "relay capable fell back", relayCapable: true, conn: false, want: "direct"},
		{name: "not relay capable", relayCapable: false, conn: false},
		// atecontroller stays unlabeled even if some future caller hands it a
		// connection for another reason: capability, not the conn, is the gate.
		{name: "not relay capable with conn", relayCapable: false, conn: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var conn *grpc.ClientConn
			if tc.conn {
				conn = lazyConn(t)
			}
			res, err := newResource(context.Background(), "ateom-gvisor", relayAttrs(tc.relayCapable, conn)...)
			if err != nil {
				t.Fatalf("newResource: %v", err)
			}
			got, ok := resourceAttrs(res)[string(ateattr.OTLPRelayKey)]
			if tc.want == "" {
				if ok {
					t.Errorf("%s = %q, want absent", string(ateattr.OTLPRelayKey), got)
				}
				return
			}
			if got != tc.want {
				t.Errorf("%s = %q, want %q", string(ateattr.OTLPRelayKey), got, tc.want)
			}
		})
	}
}

// collectedResource reads back the resource a meter provider actually stamps on
// its exports, by attaching a ManualReader alongside the OTLP one. The provider
// does not expose its resource any other way, and asserting on newResource's
// return would only re-test relayAttrs.
func collectedResource(t *testing.T, relayCapable bool, conn *grpc.ClientConn) map[string]string {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	mp, err := newMeterProvider(context.Background(), "ateom-gvisor", true, relayCapable, conn, nil, reader)
	if err != nil {
		t.Fatalf("newMeterProvider: %v", err)
	}
	t.Cleanup(func() {
		// Shutdown flushes the OTLP reader too, and no collector is listening
		// here: a live context spends the exporter's full retry budget (~10s)
		// per provider. A cancelled one skips the flush, which is all this test
		// wants from Shutdown anyway.
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		mp.Shutdown(ctx)
	})
	// A meter with no instruments still collects, carrying the resource.
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	return resourceAttrs(rm.Resource)
}

// The metric path's half of the decision, asserted on what the provider exports
// rather than on what relayAttrs returns: a wiring mistake in newMeterProvider
// (passing the wrong flag, dropping the attrs) fails here and not in
// TestRelayAttrs.
func TestMeterProviderRelayAttribute(t *testing.T) {
	if got, ok := collectedResource(t, true, lazyConn(t))[string(ateattr.OTLPRelayKey)]; !ok || got != "relay" {
		t.Errorf("%s = %q (present %t), want relay", string(ateattr.OTLPRelayKey), got, ok)
	}
	if got, ok := collectedResource(t, true, nil)[string(ateattr.OTLPRelayKey)]; !ok || got != "direct" {
		t.Errorf("%s = %q (present %t), want direct", string(ateattr.OTLPRelayKey), got, ok)
	}
	// atecontroller and ateapi: no relay was ever offered, so no claim is made
	// about which path they took.
	if got, ok := collectedResource(t, false, nil)[string(ateattr.OTLPRelayKey)]; ok {
		t.Errorf("%s = %q, want absent for a component with no relay", string(ateattr.OTLPRelayKey), got)
	}
}

func TestReadyzDrainsWhileHealthzStaysUp(t *testing.T) {
	readiness := &Readiness{}
	mux := metricsMux(MetricsServerOptions{
		Readiness:     readiness,
		EnableHealthz: true,
	})

	if got := getCode(t, mux, "/readyz"); got != http.StatusOK {
		t.Errorf("/readyz before drain = %d, want %d", got, http.StatusOK)
	}
	if got := getCode(t, mux, "/healthz"); got != http.StatusOK {
		t.Errorf("/healthz before drain = %d, want %d", got, http.StatusOK)
	}

	readiness.MarkNotReady()

	if got := getCode(t, mux, "/readyz"); got != http.StatusServiceUnavailable {
		t.Errorf("/readyz during drain = %d, want %d", got, http.StatusServiceUnavailable)
	}
	if got := getCode(t, mux, "/healthz"); got != http.StatusOK {
		t.Errorf("/healthz during drain = %d, want %d (liveness must not fail while draining)", got, http.StatusOK)
	}
}

func TestReadyzStaticWithZeroValueReadiness(t *testing.T) {
	mux := metricsMux(MetricsServerOptions{Readiness: &Readiness{}})
	if got := getCode(t, mux, "/readyz"); got != http.StatusOK {
		t.Errorf("/readyz with zero-value Readiness = %d, want %d", got, http.StatusOK)
	}
}

func TestReadinessMux(t *testing.T) {
	readiness := &Readiness{}
	mux := readinessMux(readiness)

	if got := getCode(t, mux, "/readyz"); got != http.StatusOK {
		t.Errorf("/readyz before drain = %d, want %d", got, http.StatusOK)
	}
	if got := getCode(t, mux, "/metrics"); got != http.StatusNotFound {
		t.Errorf("/metrics = %d, want %d", got, http.StatusNotFound)
	}

	readiness.MarkNotReady()
	if got := getCode(t, mux, "/readyz"); got != http.StatusServiceUnavailable {
		t.Errorf("/readyz during drain = %d, want %d", got, http.StatusServiceUnavailable)
	}
}

func TestReadyzAbsentWithoutReadiness(t *testing.T) {
	mux := metricsMux(MetricsServerOptions{})
	if got := getCode(t, mux, "/readyz"); got != http.StatusNotFound {
		t.Errorf("/readyz with nil Readiness = %d, want %d", got, http.StatusNotFound)
	}
}

func TestHealthzAbsentUnlessEnabled(t *testing.T) {
	mux := metricsMux(MetricsServerOptions{Readiness: &Readiness{}})
	if got := getCode(t, mux, "/healthz"); got != http.StatusNotFound {
		t.Errorf("/healthz without EnableHealthz = %d, want %d", got, http.StatusNotFound)
	}
}

func TestInitMetricsPushOnlyHasNoPrometheusSurface(t *testing.T) {
	mp, err := InitMetricsPushOnlyVia(context.Background(), "test-pushonly", nil)
	if err != nil {
		t.Fatalf("InitMetricsPushOnlyVia: %v", err)
	}
	// Bound shutdown: the periodic reader would otherwise block flushing to the
	// unreachable default OTLP endpoint until the export timeout.
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = mp.Shutdown(ctx)
	})

	ctr, err := mp.Meter("test").Int64Counter("ate.test.pushonly.count")
	if err != nil {
		t.Fatalf("create counter: %v", err)
	}
	ctr.Add(context.Background(), 1)

	// A push-only provider registers no Prometheus reader, so what it records must
	// not surface on the default registry StartMetricsServer's /metrics serves.
	rec := httptest.NewRecorder()
	promhttp.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if strings.Contains(rec.Body.String(), "ate_test_pushonly") {
		t.Error("push-only MeterProvider must not expose a Prometheus pull surface")
	}
}

// metricsCollector is an OTLP metrics endpoint that records every export.
type metricsCollector struct {
	colmetricspb.UnimplementedMetricsServiceServer
	mu       sync.Mutex
	requests []*colmetricspb.ExportMetricsServiceRequest
}

func (c *metricsCollector) Export(_ context.Context, req *colmetricspb.ExportMetricsServiceRequest) (*colmetricspb.ExportMetricsServiceResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.requests = append(c.requests, req)
	return &colmetricspb.ExportMetricsServiceResponse{}, nil
}

// flush forces an export from mp, shuts it down, and counts the metric names in
// the first export received; nil means nothing was pushed. Only the first
// export counts, so the cumulative re-export on shutdown is not a duplicate.
func (c *metricsCollector) flush(t *testing.T, mp *sdkmetric.MeterProvider) map[string]int {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	_ = mp.ForceFlush(ctx)
	_ = mp.Shutdown(ctx)
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.requests) == 0 {
		return nil
	}
	names := map[string]int{}
	for _, rm := range c.requests[0].GetResourceMetrics() {
		for _, sm := range rm.GetScopeMetrics() {
			for _, m := range sm.GetMetrics() {
				names[m.GetName()]++
			}
		}
	}
	return names
}

// startMetricsCollector serves a metricsCollector on a loopback port, points
// OTEL_EXPORTER_OTLP_ENDPOINT at it, and sets OTEL_METRICS_EXPORTER.
func startMetricsCollector(t *testing.T, exporter string) *metricsCollector {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	collector := &metricsCollector{}
	srv := grpc.NewServer()
	colmetricspb.RegisterMetricsServiceServer(srv, collector)
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(srv.Stop)
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://"+ln.Addr().String())
	t.Setenv(metricsExporterEnv, exporter)
	return collector
}

func addOne(t *testing.T, mp *sdkmetric.MeterProvider, name string) {
	t.Helper()
	ctr, err := mp.Meter("test").Int64Counter(name)
	if err != nil {
		t.Fatalf("create counter: %v", err)
	}
	ctr.Add(t.Context(), 1)
}

func gatheredNames(t *testing.T, reg prometheus.Gatherer) map[string]bool {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	names := map[string]bool{}
	for _, f := range families {
		names[f.GetName()] = true
	}
	return names
}

func TestMetricsPushEnabled(t *testing.T) {
	for value, wantPush := range map[string]bool{"": true, "otlp": true, " OTLP ": true, "none": false, " None ": false, "prometheus": true} {
		var buf bytes.Buffer
		prev := slog.Default()
		slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
		t.Setenv(metricsExporterEnv, value)
		push := metricsPushEnabled(t.Context())
		slog.SetDefault(prev)

		if push != wantPush {
			t.Errorf("%s=%q: push = %t, want %t", metricsExporterEnv, value, push, wantPush)
		}
		if warned, wantWarn := strings.Contains(buf.String(), "level=WARN"), value == "prometheus"; warned != wantWarn {
			t.Errorf("%s=%q: warned = %t, want %t:\n%s", metricsExporterEnv, value, warned, wantWarn, buf.String())
		}
	}
}

func TestInitMetricsExportsOverOTLPByDefault(t *testing.T) {
	collector := startMetricsCollector(t, "")
	mp, err := InitMetrics(t.Context(), "test-default")
	if err != nil {
		t.Fatalf("InitMetrics: %v", err)
	}
	addOne(t, mp, "ate.test.default.count")
	if pushed := collector.flush(t, mp); pushed["ate.test.default.count"] != 1 {
		t.Errorf("the instrument was not pushed: %v", pushed)
	}
}

// With OTEL_METRICS_EXPORTER=none the instruments are still served for a
// scrape and nothing is pushed, so a scraped component reaches the backend
// once.
func TestInitMetricsExporterNoneKeepsPrometheusOnly(t *testing.T) {
	collector := startMetricsCollector(t, "none")
	mp, err := InitMetrics(t.Context(), "test-none")
	if err != nil {
		t.Fatalf("InitMetrics: %v", err)
	}
	addOne(t, mp, "ate.test.none.count")
	if !gatheredNames(t, prometheus.DefaultGatherer)["ate_test_none_count_total"] {
		t.Error("/metrics does not serve the instrument")
	}
	if pushed := collector.flush(t, mp); pushed != nil {
		t.Errorf("OTEL_METRICS_EXPORTER=none still pushed %v", pushed)
	}
}

func TestInitMetricsPushOnlyExporterNoneExportsNothing(t *testing.T) {
	collector := startMetricsCollector(t, "none")
	mp, err := InitMetricsPushOnlyVia(t.Context(), "test-pushonly-none", nil)
	if err != nil {
		t.Fatalf("InitMetricsPushOnlyVia: %v", err)
	}
	addOne(t, mp, "ate.test.pushonlynone.count")
	if pushed := collector.flush(t, mp); pushed != nil {
		t.Errorf("OTEL_METRICS_EXPORTER=none still pushed %v", pushed)
	}
}

// bridgedRegistry stands in for controller-runtime's registry: a family
// recorded outside the OTel SDK.
func bridgedRegistry(t *testing.T) *prometheus.Registry {
	t.Helper()
	reg := prometheus.NewRegistry()
	gauge := prometheus.NewGauge(prometheus.GaugeOpts{Name: "test_bridged_family", Help: "recorded outside the OTel SDK"})
	reg.MustRegister(gauge)
	gauge.Set(1)
	return reg
}

// With the OTLP export, the bridged family and the OTel instrument are each
// pushed once. The instrument must stay off the bridged registry: there the
// bridge would push it a second time, under its Prometheus name.
func TestInitMetricsBridgedPushesEachMetricOnce(t *testing.T) {
	collector := startMetricsCollector(t, "otlp")
	reg := bridgedRegistry(t)
	mp, err := InitMetricsBridged(t.Context(), "test-bridged-otlp", reg, nil)
	if err != nil {
		t.Fatalf("InitMetricsBridged: %v", err)
	}
	addOne(t, mp, "ate.test.bridged.count")
	if gatheredNames(t, reg)["ate_test_bridged_count_total"] {
		t.Error("the OTel instrument is registered on the bridged registry")
	}
	pushed := collector.flush(t, mp)
	if pushed["ate.test.bridged.count"] != 1 || pushed["test_bridged_family"] != 1 || pushed["ate_test_bridged_count_total"] != 0 {
		t.Errorf("want ate.test.bridged.count and test_bridged_family pushed once each and nothing else for them, got %v", pushed)
	}
}

// wrapProducer sits between the bridge and the OTLP push.
func TestInitMetricsBridgedWrapsPushedProducer(t *testing.T) {
	collector := startMetricsCollector(t, "otlp")
	var produced atomic.Bool
	wrap := func(inner sdkmetric.Producer) sdkmetric.Producer {
		return producerFunc(func(ctx context.Context) ([]metricdata.ScopeMetrics, error) {
			produced.Store(true)
			return inner.Produce(ctx)
		})
	}
	mp, err := InitMetricsBridged(t.Context(), "test-bridged-wrap", bridgedRegistry(t), wrap)
	if err != nil {
		t.Fatalf("InitMetricsBridged: %v", err)
	}
	pushed := collector.flush(t, mp)
	if !produced.Load() {
		t.Error("the OTLP reader did not collect through wrapProducer")
	}
	if pushed["test_bridged_family"] != 1 {
		t.Errorf("want test_bridged_family pushed once through the wrapper, got %v", pushed)
	}
}

type producerFunc func(context.Context) ([]metricdata.ScopeMetrics, error)

func (f producerFunc) Produce(ctx context.Context) ([]metricdata.ScopeMetrics, error) { return f(ctx) }

// With OTEL_METRICS_EXPORTER=none, the OTel instrument is served from the
// bridged registry next to its own families, and nothing is pushed.
func TestInitMetricsBridgedExporterNoneServesFromRegistry(t *testing.T) {
	collector := startMetricsCollector(t, "none")
	reg := bridgedRegistry(t)
	mp, err := InitMetricsBridged(t.Context(), "test-bridged-none", reg, nil)
	if err != nil {
		t.Fatalf("InitMetricsBridged: %v", err)
	}
	addOne(t, mp, "ate.test.bridgednone.count")
	names := gatheredNames(t, reg)
	if !names["ate_test_bridgednone_count_total"] || !names["test_bridged_family"] {
		t.Errorf("the bridged registry does not serve both the instrument and its own family: %v", names)
	}
	if pushed := collector.flush(t, mp); pushed != nil {
		t.Errorf("OTEL_METRICS_EXPORTER=none still pushed %v", pushed)
	}
}

func TestInitMetricsBridgedRequiresServiceName(t *testing.T) {
	if _, err := InitMetricsBridged(t.Context(), "", prometheus.NewRegistry(), nil); err == nil {
		t.Error("InitMetricsBridged(\"\") must return an error")
	}
}

func TestInitMetricsPushOnlyRequiresServiceName(t *testing.T) {
	if _, err := InitMetricsPushOnlyVia(context.Background(), "", nil); err == nil {
		t.Error("InitMetricsPushOnlyVia(\"\") must return an error")
	}
}

func TestInitMetricsRequiresServiceName(t *testing.T) {
	if _, err := InitMetrics(context.Background(), ""); err == nil {
		t.Error("InitMetrics(\"\") must return an error")
	}
}

func getCode(t *testing.T, mux *http.ServeMux, path string) int {
	t.Helper()
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	return rec.Code
}

func TestSetLogLevel(t *testing.T) {
	t.Cleanup(func() { logLevel.Set(slog.LevelInfo) })

	// The untouched default must be exactly info: every existing deployment
	// relies on this for "no behavior change without the flag".
	if got := logLevel.Level(); got != slog.LevelInfo {
		t.Fatalf("default log level = %v, want %v", got, slog.LevelInfo)
	}

	var buf bytes.Buffer
	InitLoggerWithWriter(&buf)
	t.Cleanup(InitLogger)

	slog.Info("visible at default level")
	if !strings.Contains(buf.String(), "visible at default level") {
		t.Errorf("info line not emitted at default level: %s", buf.String())
	}
	buf.Reset()
	slog.Debug("hidden at default level")
	if buf.Len() != 0 {
		t.Errorf("debug line emitted at default level: %s", buf.String())
	}

	if err := SetLogLevel("debug"); err != nil {
		t.Fatalf("SetLogLevel(debug): %v", err)
	}
	slog.Debug("visible at debug")
	if !strings.Contains(buf.String(), "visible at debug") {
		t.Errorf("debug line not emitted after SetLogLevel(debug): %s", buf.String())
	}

	// Case-insensitive, and dynamic: raising the level silences info.
	if err := SetLogLevel("WARN"); err != nil {
		t.Fatalf("SetLogLevel(WARN): %v", err)
	}
	buf.Reset()
	slog.Info("hidden at warn")
	if buf.Len() != 0 {
		t.Errorf("info line emitted at warn level: %s", buf.String())
	}

	if err := SetLogLevel("verbose"); err == nil {
		t.Error("SetLogLevel accepted an invalid level")
	}

	// Empty means unset: no error, level unchanged.
	if err := SetLogLevel(""); err != nil {
		t.Errorf("SetLogLevel(\"\") = %v, want nil", err)
	}
	if got := logLevel.Level(); got != slog.LevelWarn {
		t.Errorf("SetLogLevel(\"\") changed the level to %v", got)
	}
}
