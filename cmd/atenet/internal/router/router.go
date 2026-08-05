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

package router

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/config"

	"github.com/agent-substrate/substrate/internal/ateapiauth"
	"github.com/agent-substrate/substrate/internal/serverboot"
	v1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// dataPlaneTraceRatio is the default root sampling fraction for parentless
// data plane requests; OTEL_TRACES_SAMPLER / OTEL_TRACES_SAMPLER_ARG override it.
const dataPlaneTraceRatio = 0.01

var (
	scheme = runtime.NewScheme()
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(v1alpha1.AddToScheme(scheme))
}

// RouterServer instantiates and coordinates runtime threads executing system modules.
type RouterServer struct {
	cfg routerConfig

	Cmd        *cobra.Command
	k8sClient  client.Client
	clientset  kubernetes.Interface
	apiClient  ateapipb.ControlClient
	extprocSrv *ExtProcServer
	health     *routerHealth
	atStore    atStore
}

func NewRouterServer(cfg routerConfig) (*RouterServer, error) {
	var k8sClient client.Client
	var clientset kubernetes.Interface

	if cfg.TemplatesFile == "" {
		k8sCfg, err := config.GetConfig()
		if err != nil {
			if cfg.Kubeconfig != "" {
				k8sCfg, err = clientcmd.BuildConfigFromFlags("", cfg.Kubeconfig)
				if err != nil {
					return nil, fmt.Errorf("failed to read config from path %s: %w", cfg.Kubeconfig, err)
				}
			} else {
				return nil, fmt.Errorf("unable to establish Kubernetes configuration parameters: %w", err)
			}
		}
		slog.Info("Connecting to Kubernetes API server", slog.String("host", k8sCfg.Host))

		k8sClient, err = client.New(k8sCfg, client.Options{
			Scheme: scheme,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to initialize cluster client: %w", err)
		}

		clientset, err = kubernetes.NewForConfig(k8sCfg)
		if err != nil {
			return nil, fmt.Errorf("failed to initialize core client: %w", err)
		}
	}

	var store atStore
	if cfg.TemplatesFile != "" {
		store = newFileATStore(cfg.TemplatesFile)
	} else {
		store = newk8sATStore(k8sClient)
	}

	return &RouterServer{
		cfg:       cfg,
		k8sClient: k8sClient,
		clientset: clientset,
		atStore:   store,
	}, nil
}

func (s *RouterServer) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		cancel()
	}()

	// Validate the configuration before doing any other work, so a bad flag
	// combination fails fast — no tracing, metrics, or connections are set up
	// for a router that is about to refuse to start. The parking config is
	// resolved once here so every consumer — the resumer's retry loop and the
	// Envoy ext_proc timeout — sees the same effective values.
	if err := s.cfg.validate(); err != nil {
		return fmt.Errorf("invalid router configuration: %w", err)
	}
	parkCfg := s.cfg.ParkedRequest.normalized()

	serverboot.InitLogger()
	if err := serverboot.SetLogLevel(s.cfg.LogLevel); err != nil {
		return err
	}

	// Tracing must be initialized before constructing the ateapi gRPC client
	// below, because otelgrpc.NewClientHandler captures the global
	// TracerProvider at construction time. Resolved once so the router's SDK
	// sampler and Envoy's RandomSampling percent cannot drift.
	sampling := serverboot.ResolveTraceSampling(ctx, serverboot.ParentRatioSampling(dataPlaneTraceRatio))
	tp, err := serverboot.InitTracing(ctx, serverboot.TracingOptions{
		ServiceName: routerServiceName,
		Sampling:    sampling,
	})
	if err != nil {
		return fmt.Errorf("failed to initialize tracing: %w", err)
	}
	defer serverboot.ShutdownProvider("TracerProvider", tp.Shutdown)

	mp, err := serverboot.InitMetrics(ctx, routerServiceName)
	if err != nil {
		return fmt.Errorf("failed to initialize metrics: %w", err)
	}
	defer serverboot.ShutdownProvider("MeterProvider", mp.Shutdown)

	go serverboot.StartMetricsServer(ctx, serverboot.MetricsServerOptions{Addr: s.cfg.MetricsAddr})

	dialOpts, err := ateapiauth.DialOptions(ateapiauth.ClientConfig{
		UseTokenAuth:     s.cfg.Auth.AteapiUseTokenAuth,
		CAFile:           s.cfg.Auth.AteapiCAFile,
		ServerName:       s.cfg.Auth.AteapiServerName,
		TokenFile:        s.cfg.Auth.AteapiTokenFile,
		ClientCredBundle: s.cfg.Auth.AteapiClientCertPath,
	})
	if err != nil {
		return fmt.Errorf("building ateapi dial options: %w", err)
	}
	dialOpts = append(dialOpts, grpc.WithStatsHandler(otelgrpc.NewClientHandler()))
	conn, err := grpc.NewClient(
		s.cfg.AteapiAddr,
		dialOpts...,
	)
	if err != nil {
		return fmt.Errorf("failed to establish grpc channel to ateapi client: %w", err)
	}
	slog.InfoContext(ctx, "Connecting to ateapi", slog.String("address", s.cfg.AteapiAddr), slog.Bool("use-api-token-auth", s.cfg.Auth.AteapiUseTokenAuth))
	s.apiClient = ateapipb.NewControlClient(conn)

	slog.InfoContext(ctx, "Starting substrate router subsystem",
		slog.Bool("standalone", s.cfg.Standalone),
		slog.String("atenet_router", string(s.cfg.atenetRouter())))

	g, ctx := errgroup.WithContext(ctx)

	if s.extprocSrv == nil {
		routeDuration, err := newRouteDurationHistogram()
		if err != nil {
			return fmt.Errorf("failed to create route-duration histogram: %w", err)
		}
		parkMetrics, err := newParkingMetrics()
		if err != nil {
			return fmt.Errorf("failed to create parking metrics: %w", err)
		}
		s.extprocSrv = NewExtProcServer(s.cfg.ExtprocPort, s.apiClient, routeDuration, parkCfg, parkMetrics, s.cfg.atenetRouter().routeViaAuthority())
	}
	s.health = newRouterHealth(s.cfg.HealthInterval, s.clientset, s.apiClient, s.cfg)

	if err := s.startDataplane(ctx, g, parkCfg, sampling.RootSamplingPercent()); err != nil {
		return err
	}

	// Start periodic service checking logic
	g.Go(func() error {
		slog.InfoContext(ctx, "Starting periodic health checker", slog.Duration("interval", s.cfg.HealthInterval))
		s.health.Start(ctx)
		return nil
	})

	// Start ExtProc Server
	g.Go(func() error {
		slog.InfoContext(ctx, "Starting ExtProc Server", slog.Int("port", s.cfg.ExtprocPort))
		lis, err := net.Listen("tcp", fmt.Sprintf(":%d", s.cfg.ExtprocPort))
		if err != nil {
			return fmt.Errorf("failed to listen on extproc port %d: %w", s.cfg.ExtprocPort, err)
		}
		defer lis.Close()

		return s.extprocSrv.Serve(ctx, lis)
	})

	// Start HTTP status endpoint
	if s.cfg.StatusPort > 0 {
		listener, err := net.Listen("tcp", fmt.Sprintf(":%d", s.cfg.StatusPort))
		if err != nil {
			return fmt.Errorf("failed binding Router HTTP status server port: %w", err)
		}

		mux := http.NewServeMux()
		mux.HandleFunc("/statusz", s.handleStatusz)

		httpServer := &http.Server{
			Handler: otelhttp.NewHandler(mux, "/"),
		}

		g.Go(func() error {
			go func() {
				if err := httpServer.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
					slog.ErrorContext(ctx, "status HTTP server exited unexpectedly", slog.Any("err", err))
				}
			}()
			<-ctx.Done()
			return httpServer.Close()
		})
	}

	return g.Wait()
}

// setOtlpCollector points Envoy's tracer at the configured collector, and
// gives up on Envoy-side tracing if the address is one Envoy cannot use.
//
// It never fails the router. The address defaults to
// OTEL_EXPORTER_OTLP_ENDPOINT, which the router's own exporter reads too and
// which legitimately carries forms Envoy's plaintext tracer cluster cannot
// reach — an https collector, most of all. Refusing to start would take the
// xDS control plane for every Envoy in the mesh down over a tracing endpoint
// that works fine for its other reader. Losing Envoy's spans is the smaller
// failure, so take it and say so loudly.
func setOtlpCollector(ctx context.Context, xdsSrv *XdsServer, addr string) {
	if err := xdsSrv.SetOtlpCollector(addr); err != nil {
		slog.WarnContext(ctx, "Envoy-side tracing disabled: the OTLP collector address is not one Envoy can use. The router's own spans are unaffected; set --otlp-collector-address to point Envoy at a plaintext collector",
			slog.String("address", addr), slog.Any("err", err))
		xdsSrv.DisableOtlpCollector()
	}
}
