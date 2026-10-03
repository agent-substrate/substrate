// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package main

import (
	"context"
	"log/slog"
	"slices"
	"strings"
	"testing"

	prombridge "go.opentelemetry.io/contrib/bridges/prometheus"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

func TestNewControllerRuntimeLoggerVerbosity(t *testing.T) {
	const probe = "verbosity-probe"

	tests := []struct {
		name      string
		level     slog.Level
		verbosity int
		wantLog   bool
	}{
		{name: "info keeps V(0)", level: slog.LevelInfo, verbosity: 0, wantLog: true},
		{name: "info drops V(1)", level: slog.LevelInfo, verbosity: 1, wantLog: false},
		{name: "debug keeps V(1)", level: slog.LevelDebug, verbosity: 1, wantLog: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var buf strings.Builder
			log := newControllerRuntimeLogger(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: tt.level}))

			log.V(tt.verbosity).Info(probe)

			if got := strings.Contains(buf.String(), probe); got != tt.wantLog {
				t.Errorf("logged = %v, want %v (output %q)", got, tt.wantLog, buf.String())
			}
		})
	}
}

// The e2e read-back asserts atecontroller reaches the collector, which holds only
// because the bridged registry already has series before any manager starts: the
// controller_runtime_* vectors are empty until controllers register, so the Go and
// process collectors are what make the first push non-empty.
func TestBridgedRegistryProducesBeforeManagerStart(t *testing.T) {
	t.Parallel()

	produced, err := prombridge.NewMetricProducer(prombridge.WithGatherer(ctrlmetrics.Registry)).
		Produce(context.Background())
	if err != nil {
		t.Fatalf("Produce: %v", err)
	}

	var names []string
	for _, sm := range produced {
		for _, m := range sm.Metrics {
			names = append(names, m.Name)
		}
	}
	if len(names) == 0 {
		t.Fatal("bridging controller-runtime's registry produced no metrics")
	}
	if !slices.ContainsFunc(names, func(n string) bool { return strings.HasPrefix(n, "go_") }) {
		t.Errorf("no go_* family in %v", names)
	}
}

// Metrics leave over OTLP, so the manager must not open its own listener.
func TestManagerOptionsServeNoMetrics(t *testing.T) {
	t.Parallel()

	opts := managerOptions(types.NamespacedName{Namespace: "ate-system", Name: "pool"})

	srv, err := metricsserver.NewServer(opts.Metrics, nil, nil)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	if srv != nil {
		t.Error("the manager would serve a metrics listener")
	}
}

func TestManagerOptionsScopeSecretCache(t *testing.T) {
	t.Parallel()

	ref := types.NamespacedName{Namespace: "ate-system", Name: "pool"}
	opts := managerOptions(ref)

	var byObject cache.ByObject
	found := false
	for obj, by := range opts.Cache.ByObject {
		if _, isSecret := obj.(*corev1.Secret); isSecret {
			byObject, found = by, true
		}
	}
	if !found {
		t.Fatal("no cache scope for Secrets")
	}
	cfg, ok := byObject.Namespaces[ref.Namespace]
	if !ok || len(byObject.Namespaces) != 1 {
		t.Fatalf("Secret cache namespaces = %v, want only %q", byObject.Namespaces, ref.Namespace)
	}
	if got := cfg.FieldSelector.String(); got != "metadata.name="+ref.Name {
		t.Errorf("Secret field selector = %q, want %q", got, "metadata.name="+ref.Name)
	}
}
