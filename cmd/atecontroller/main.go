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
	"os"
	"time"

	"github.com/agent-substrate/substrate/cmd/atecontroller/internal/controllers"
	"github.com/agent-substrate/substrate/internal/ateapiauth"
	"github.com/agent-substrate/substrate/internal/autoscaler"
	clientv1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/spf13/pflag"
	"google.golang.org/grpc"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	// Import all Kubernetes client auth plugins (e.g. Azure, GCP, OIDC, etc.)
	_ "k8s.io/client-go/plugin/pkg/client/auth"
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")

	ateAPIConnSpec = pflag.String("ateapi-conn-spec", "dns:///api.ate-system.svc:443", "")

	ateapiAuthMode   = pflag.String("ateapi-auth", "mtls", "Client auth to ateapi: mtls|jwt. 'mtls' (default) dials with insecure TLS and relies on pod-projected mTLS credentials for identity. 'jwt' verifies the server cert and sends a Bearer SA token.")
	ateapiCAFile     = pflag.String("ateapi-ca-file", ateapiauth.DefaultServiceAccountCAFile, "PEM file with CAs trusted to verify the ateapi server cert. Required for jwt.")
	ateapiServerName = pflag.String("ateapi-server-name", "", "SNI / hostname expected on the ateapi server cert. Optional.")
	ateapiTokenFile  = pflag.String("ateapi-token-file", ateapiauth.DefaultServiceAccountTokenFile, "Projected SA token file used as Bearer credential. Required for jwt.")
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(clientv1alpha1.AddToScheme(scheme)) // Register our CRD
}

func main() {
	pflag.Parse()
	ctrl.SetLogger(zap.New(zap.UseDevMode(true)))

	mode, err := ateapiauth.ParseMode(*ateapiAuthMode)
	if err != nil {
		setupLog.Error(err, "invalid --ateapi-auth")
		os.Exit(1)
	}

	dialOpts, err := ateapiauth.DialOptions(ateapiauth.ClientConfig{
		Mode:       mode,
		CAFile:     *ateapiCAFile,
		ServerName: *ateapiServerName,
		TokenFile:  *ateapiTokenFile,
	})
	if err != nil {
		setupLog.Error(err, "building ateapi dial options")
		os.Exit(1)
	}

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
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "WorkerPool")
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

	if err = (&controllers.WorkerPoolAutoscaler{
		Client:    mgr.GetClient(),
		AteClient: ateapiClient,
		Config: autoscaler.Config{
			// Cluster-wide defaults; a pool overrides them per-field via
			// spec.autoscaling (scaleDownStabilization, maxScaleUpStep).
			ScaleDownStabilization: 60 * time.Second,
			MaxScaleUpStep:         0, // unlimited
		},
		Interval: 10 * time.Second,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "WorkerPoolAutoscaler")
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
