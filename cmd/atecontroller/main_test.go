// Copyright 2026 The Agent Substrate Authors
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

	"github.com/spf13/pflag"
	prombridge "go.opentelemetry.io/contrib/bridges/prometheus"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

func TestDNSEgressPolicyFlagDefault(t *testing.T) {
	t.Parallel()
	f := pflag.Lookup("dns-egress-policy")
	if f == nil {
		t.Fatal("no --dns-egress-policy flag registered")
	}
	if f.DefValue != "" {
		t.Errorf("--dns-egress-policy default = %q, want empty", f.DefValue)
	}
}

func TestLoadFlagsFromEnvWorkerPprof(t *testing.T) {
	tests := []struct {
		name      string
		env       map[string]string
		setFlag   map[string]string
		wantPprof bool
		wantMut   int
		wantBlk   int
		wantErr   string
	}{
		{
			name: "unset leaves defaults",
		},
		{
			name: "ATE_WORKER_PPROF true enables worker pprof",
			env: map[string]string{
				"ATE_WORKER_PPROF": "true",
			},
			wantPprof: true,
		},
		{
			name: "ATE_WORKER_PPROF 1 enables worker pprof",
			env: map[string]string{
				"ATE_WORKER_PPROF": "1",
			},
			wantPprof: true,
		},
		{
			name: "invalid ATE_WORKER_PPROF fails startup",
			env: map[string]string{
				"ATE_WORKER_PPROF": "invalid",
			},
			wantErr: "ATE_WORKER_PPROF must be a boolean",
		},
		{
			name: "explicit worker-pprof flag overrides invalid ATE_WORKER_PPROF",
			env: map[string]string{
				"ATE_WORKER_PPROF": "invalid",
			},
			setFlag: map[string]string{
				"worker-pprof": "false",
			},
			wantPprof: false,
		},
		{
			name: "valid env vars populate flags",
			env: map[string]string{
				"ATE_WORKER_PPROF":                "true",
				"ATE_WORKER_PPROF_MUTEX_FRACTION": "10",
				"ATE_WORKER_PPROF_BLOCK_RATE":     "1000000",
			},
			wantPprof: true,
			wantMut:   10,
			wantBlk:   1000000,
		},
		{
			name: "explicit flag overrides env var",
			env: map[string]string{
				"ATE_WORKER_PPROF_MUTEX_FRACTION": "invalid",
				"ATE_WORKER_PPROF_BLOCK_RATE":     "1000000",
			},
			setFlag: map[string]string{
				"worker-pprof-mutex-fraction": "5",
			},
			wantMut: 5,
			wantBlk: 1000000,
		},
		{
			name: "invalid mutex fraction fails startup",
			env: map[string]string{
				"ATE_WORKER_PPROF_MUTEX_FRACTION": "invalid",
			},
			wantErr: "ATE_WORKER_PPROF_MUTEX_FRACTION must be a positive integer",
		},
		{
			name: "zero block rate fails startup",
			env: map[string]string{
				"ATE_WORKER_PPROF_BLOCK_RATE": "0",
			},
			wantErr: "ATE_WORKER_PPROF_BLOCK_RATE must be a positive integer",
		},
		{
			name: "negative mutex fraction flag fails startup",
			setFlag: map[string]string{
				"worker-pprof-mutex-fraction": "-1",
			},
			wantErr: "--worker-pprof-mutex-fraction must not be negative",
		},
		{
			name: "negative block rate flag fails startup",
			setFlag: map[string]string{
				"worker-pprof-block-rate": "-1",
			},
			wantErr: "--worker-pprof-block-rate must not be negative",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			oldPprof, oldMut, oldBlk := *workerPprof, *workerPprofMutexFraction, *workerPprofBlockRate
			t.Cleanup(func() {
				*workerPprof, *workerPprofMutexFraction, *workerPprofBlockRate = oldPprof, oldMut, oldBlk
			})
			*workerPprof, *workerPprofMutexFraction, *workerPprofBlockRate = false, 0, 0

			for name, val := range tt.setFlag {
				f := pflag.CommandLine.Lookup(name)
				oldChanged := f.Changed
				t.Cleanup(func() { f.Changed = oldChanged })
				if err := pflag.CommandLine.Set(name, val); err != nil {
					t.Fatalf("Set(%q, %q): %v", name, val, err)
				}
			}

			for _, k := range []string{"ATE_WORKER_PPROF", "ATE_WORKER_PPROF_MUTEX_FRACTION", "ATE_WORKER_PPROF_BLOCK_RATE"} {
				t.Setenv(k, tt.env[k])
			}

			err := loadFlagsFromEnv()
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("loadFlagsFromEnv() error = %v, want substring %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("loadFlagsFromEnv() unexpected error: %v", err)
			}
			if *workerPprof != tt.wantPprof || *workerPprofMutexFraction != tt.wantMut || *workerPprofBlockRate != tt.wantBlk {
				t.Errorf("got (%v, %d, %d), want (%v, %d, %d)",
					*workerPprof, *workerPprofMutexFraction, *workerPprofBlockRate,
					tt.wantPprof, tt.wantMut, tt.wantBlk)
			}
		})
	}
}

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

// The manager opens its metrics listener only when OTEL_METRICS_EXPORTER leaves
// the scrape endpoint on.
func TestManagerOptionsMetricsListener(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		servePull  bool
		wantAddr   string
		wantServed bool
	}{
		{"serving", true, metricsPullAddr, true},
		{"disabled", false, metricsOffAddr, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			opts := managerOptions(types.NamespacedName{Namespace: "ate-system", Name: "pool"}, tt.servePull)

			if opts.Metrics.BindAddress != tt.wantAddr {
				t.Errorf("metrics address = %q, want %q", opts.Metrics.BindAddress, tt.wantAddr)
			}
			srv, err := metricsserver.NewServer(opts.Metrics, nil, nil)
			if err != nil {
				t.Fatalf("NewServer: %v", err)
			}
			if served := srv != nil; served != tt.wantServed {
				t.Errorf("serves a metrics listener = %t, want %t", served, tt.wantServed)
			}
		})
	}
}

func TestManagerOptionsScopeSecretCache(t *testing.T) {
	t.Parallel()

	ref := types.NamespacedName{Namespace: "ate-system", Name: "pool"}
	opts := managerOptions(ref, true)

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
