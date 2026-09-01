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
	"github.com/agent-substrate/substrate/cmd/atecontroller/internal/workersync"
	"github.com/agent-substrate/substrate/internal/ateapiauth"
	"github.com/agent-substrate/substrate/internal/serverboot"
	clientv1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/client/clientset/versioned"
	"github.com/agent-substrate/substrate/pkg/client/informers/externalversions"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/go-logr/logr"
	"github.com/spf13/pflag"
	prombridge "go.opentelemetry.io/contrib/bridges/prometheus"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	// Import all Kubernetes client auth plugins (e.g. Azure, GCP, OIDC, etc.)
	_ "k8s.io/client-go/plugin/pkg/client/auth"
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")

	ateAPIConnSpec = pflag.String("ateapi-conn-spec", "k8s:///api.ate-system.svc:443", "")

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
	ateapiClientCert = pflag.String("ateapi-client-cert", "", "Credential bundle presented as the client certificate when dialing ateapi. Required.")
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

	k8sConfig := ctrl.GetConfigOrDie()
	k8sClient, err := kubernetes.NewForConfig(k8sConfig)
	if err != nil {
		setupLog.Error(err, "creating kubernetes client for ateapi dialer")
		os.Exit(1)
	}
	ateClient, err := versioned.NewForConfig(k8sConfig)
	if err != nil {
		setupLog.Error(err, "creating ate clientset for the worker syncer")
		os.Exit(1)
	}

	dialOpts, err := ateapiauth.DialOptions(ateapiauth.ClientConfig{
		K8sClient:        k8sClient,
		CAFile:           *ateapiCAFile,
		ServerName:       *ateapiServerName,
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

	// EgressMITMTrustReconciler watches the Secret `egress-mitm-ca-pool`.
	egressMITMCAPool := controllers.EgressMITMCAPoolRef()
	mgr, err := ctrl.NewManager(k8sConfig, ctrl.Options{
		Scheme: scheme,
		Cache: cache.Options{
			ByObject: map[client.Object]cache.ByObject{
				&corev1.Secret{}: {
					Namespaces: map[string]cache.Config{
						egressMITMCAPool.Namespace: {
							FieldSelector: fields.OneTermEqualSelector("metadata.name", egressMITMCAPool.Name),
						},
					},
				},
			},
		},
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

	if err = (&controllers.EgressMITMTrustReconciler{
		Client: mgr.GetClient(),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "EgressMITMTrust")
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

	runCtx := ctrl.SetupSignalHandler()

	// The worker syncer runs on informers of its own rather than the manager's
	// shared cache. It needs a resync to sweep for registry records that drifted
	// without a pod event, and an informer's resync period is a property of the
	// informer, so asking the shared cache for one would impose it on every other
	// controller here too.
	ateFactory := externalversions.NewSharedInformerFactory(ateClient, 0)
	workerPoolLister := ateFactory.Api().V1alpha1().WorkerPools().Lister()
	workerPodInformerFactory, workerPodInformer := workersync.WorkerPodInformer(k8sClient)

	// Start registers the informer event handlers, so it has to run before the
	// factory does: the initial list then synthesizes an Add for every pod that
	// already exists, and no explicit startup re-list is needed.
	workersync.NewWorkerPoolSyncer(ateapiClient, workerPodInformer, workerPoolLister).Start(runCtx)

	workerPodInformerFactory.Start(runCtx.Done())
	ateFactory.Start(runCtx.Done())
	workerPodInformerFactory.WaitForCacheSync(runCtx.Done())
	ateFactory.WaitForCacheSync(runCtx.Done())

	setupLog.Info("starting manager")
	if err := mgr.Start(runCtx); err != nil {
		setupLog.Error(err, "problem running manager")
		os.Exit(1)
	}
}
