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
	"os"

	"github.com/agent-substrate/substrate/cmd/atecontroller/internal/controllers"
	"github.com/agent-substrate/substrate/internal/ateapiauth"
	"github.com/agent-substrate/substrate/internal/serverboot"
	clientv1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/go-logr/logr"
	"github.com/spf13/pflag"
	prombridge "go.opentelemetry.io/contrib/bridges/prometheus"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	// Import all Kubernetes client auth plugins (e.g. Azure, GCP, OIDC, etc.)
	_ "k8s.io/client-go/plugin/pkg/client/auth"
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")

	ateAPIConnSpec = pflag.String("ateapi-conn-spec", "dns:///api.ate-system.svc:443", "")

	logLevelFlag = pflag.String("log-level", "info", "Minimum log level: debug, info, warn, or error.")

	otelEndpoint = pflag.String("otel-exporter-otlp-endpoint", os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"),
		"OTLP endpoint set on ateom worker pods so they push telemetry. Defaults to the controller's own OTEL_EXPORTER_OTLP_ENDPOINT.")

	otelMetricExportInterval = pflag.String("otel-metric-export-interval", os.Getenv("OTEL_METRIC_EXPORT_INTERVAL"),
		"Metric export interval in milliseconds set on ateom worker pods. Empty keeps the OTel SDK's 60s default. Defaults to the controller's own OTEL_METRIC_EXPORT_INTERVAL.")

	otelMetricExportTimeout = pflag.String("otel-metric-export-timeout", os.Getenv("OTEL_METRIC_EXPORT_TIMEOUT"),
		"Per-export timeout in milliseconds set on ateom worker pods. Empty keeps the OTel SDK's 30s default. Defaults to the controller's own OTEL_METRIC_EXPORT_TIMEOUT.")

	otelTracesSampler = pflag.String("otel-traces-sampler", os.Getenv("OTEL_TRACES_SAMPLER"),
		"Trace sampler set on ateom worker pods. Empty keeps the ateom binary's default. Defaults to the controller's own OTEL_TRACES_SAMPLER.")

	otelTracesSamplerArg = pflag.String("otel-traces-sampler-arg", os.Getenv("OTEL_TRACES_SAMPLER_ARG"),
		"Trace sampler argument set on ateom worker pods, ignored unless --otel-traces-sampler is set. Defaults to the controller's own OTEL_TRACES_SAMPLER_ARG.")

	ateapiCAFile     = pflag.String("ateapi-ca-file", ateapiauth.DefaultServiceAccountCAFile, "PEM file with CAs trusted to verify the ateapi server cert.")
	ateapiServerName = pflag.String("ateapi-server-name", "", "SNI / hostname expected on the ateapi server cert. Optional.")
	ateapiTokenAuth  = pflag.Bool("ateapi-use-token-auth", false, "Authenticate to ateapi with the Bearer token from --ateapi-token-file instead of the client certificate from --ateapi-client-cert.")
	ateapiTokenFile  = pflag.String("ateapi-token-file", "", "Projected SA token file used as Bearer credential. Required with --ateapi-use-token-auth, ignored otherwise.")
	ateapiClientCert = pflag.String("ateapi-client-cert", "", "Credential bundle presented as the client certificate when dialing ateapi. Required unless --ateapi-use-token-auth is set, ignored otherwise.")
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(clientv1alpha1.AddToScheme(scheme)) // Register our CRD
}

const serviceName = "atecontroller"

// logr verbosity V(n) maps to slog level -n, so V(1) stays below Info until
// --log-level=debug. logr carries no context, so these records have no trace IDs.
func newControllerRuntimeLogger(h slog.Handler) logr.Logger {
	return logr.FromSlogHandler(h)
}

func main() {
	pflag.Parse()

	ctx := context.Background()
	serverboot.InitLogger()
	if err := serverboot.SetLogLevel(*logLevelFlag); err != nil {
		serverboot.Fatal(ctx, "Invalid --log-level", err)
	}
	ctrl.SetLogger(newControllerRuntimeLogger(slog.Default().Handler()))

	// Both providers must be registered before the ateapi client below:
	// otelgrpc.NewClientHandler captures the global tracer and meter providers at
	// construction, so a later init leaves it bound to the no-op ones.
	tp, err := serverboot.InitTracing(ctx, serverboot.TracingOptions{
		ServiceName: serviceName,
		Sampling:    serverboot.ResolveTraceSampling(ctx, serverboot.ParentRatioSampling(serverboot.ControlPlaneTraceRatio)),
	})
	if err != nil {
		serverboot.Fatal(ctx, "Failed to initialize tracing", err)
	}
	defer serverboot.ShutdownProvider("TracerProvider", tp.Shutdown)

	// controller-runtime records reconcile, workqueue, and runtime metrics into its
	// own Prometheus registry, which the manager serves on a port nothing scrapes.
	// Bridging it as a Producer puts them on the OTLP path instead.
	mp, err := serverboot.InitMetricsPushOnly(ctx, serviceName,
		prombridge.NewMetricProducer(prombridge.WithGatherer(ctrlmetrics.Registry)))
	if err != nil {
		serverboot.Fatal(ctx, "Failed to initialize metrics", err)
	}
	defer serverboot.ShutdownProvider("MeterProvider", mp.Shutdown)

	dialOpts, err := ateapiauth.DialOptions(ateapiauth.ClientConfig{
		UseTokenAuth:     *ateapiTokenAuth,
		CAFile:           *ateapiCAFile,
		ServerName:       *ateapiServerName,
		TokenFile:        *ateapiTokenFile,
		ClientCredBundle: *ateapiClientCert,
	})
	if err != nil {
		setupLog.Error(err, "building ateapi dial options")
		os.Exit(1)
	}
	dialOpts = append(dialOpts, grpc.WithStatsHandler(otelgrpc.NewClientHandler()))

	ateapiConn, err := grpc.NewClient(*ateAPIConnSpec, dialOpts...)
	if err != nil {
		setupLog.Error(err, "Error creating grpc connection to ate api")
		os.Exit(1)
	}

	ateapiClient := ateapipb.NewControlClient(ateapiConn)

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme: scheme,
	})
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		os.Exit(1)
	}

	if err = (&controllers.WorkerPoolReconciler{
		Client:                   mgr.GetClient(),
		Scheme:                   mgr.GetScheme(),
		OTelEndpoint:             *otelEndpoint,
		OTelMetricExportInterval: *otelMetricExportInterval,
		OTelMetricExportTimeout:  *otelMetricExportTimeout,
		OTelTracesSampler:        *otelTracesSampler,
		OTelTracesSamplerArg:     *otelTracesSamplerArg,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "WorkerPool")
		os.Exit(1)
	}

	if err = (&controllers.NetworkPolicyReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "NetPolicy")
		os.Exit(1)
	}

	if err = (&controllers.ActorTemplateReconciler{
		Client:    mgr.GetClient(),
		Scheme:    mgr.GetScheme(),
		AteClient: ateapiClient,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "ActorTemplate")
		os.Exit(1)
	}

	//+kubebuilder:scaffold:builder

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up ready check")
		os.Exit(1)
	}

	setupLog.Info("starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "problem running manager")
		os.Exit(1)
	}
}
