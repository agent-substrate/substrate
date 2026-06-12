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
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/config"

	"github.com/agent-substrate/substrate/internal/serverboot"
	v1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

var (
	scheme = runtime.NewScheme()
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(v1alpha1.AddToScheme(scheme))
}

// RouterConfig holds deployment setup and endpoint options for the router node instance.
type RouterConfig struct {
	Standalone     bool
	Namespace      string
	Kubeconfig     string
	AteapiAddr     string
	HttpPort       int
	XdsPort        int
	ExtprocPort    int
	ExtprocAddr    string
	EnvoyImage     string
	TemplatesFile  string
	StatusPort     int
	HealthInterval time.Duration
	HttpsPort      int
	EnvoyCertPath  string
	LogLevel       string
	MetricsAddr    string
}

// RouterServer instantiates and coordinates runtime threads executing system modules.
type RouterServer struct {
	cfg RouterConfig

	Cmd        *cobra.Command
	k8sClient  client.Client
	clientset  kubernetes.Interface
	apiClient  ateapipb.ControlClient
	extprocSrv *ExtProcServer
	health     *routerHealth
	atStore    atStore
}

func NewRouterServer(cfg RouterConfig) (*RouterServer, error) {
	var k8sClient client.Client
	var clientset kubernetes.Interface
	var err error

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

	conn, err := grpc.NewClient(cfg.AteapiAddr, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{InsecureSkipVerify: true})))
	if err != nil {
		return nil, fmt.Errorf("failed to establish grpc channel to ateapi client: %w", err)
	}
	slog.Info("Connecting to ateapi", slog.String("address", cfg.AteapiAddr))

	apiClient := ateapipb.NewControlClient(conn)

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
		apiClient: apiClient,
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

	var level slog.Level
	switch strings.ToLower(s.cfg.LogLevel) {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level})))

	mp, err := serverboot.InitMetrics(ctx, routerServiceName)
	if err != nil {
		return fmt.Errorf("failed to initialize metrics: %w", err)
	}
	defer serverboot.ShutdownProvider("MeterProvider", mp.Shutdown)

	go serverboot.StartMetricsServer(ctx, serverboot.MetricsServerOptions{Addr: s.cfg.MetricsAddr})

	slog.InfoContext(ctx, "Starting substrate router subsystem", slog.Bool("standalone", s.cfg.Standalone))

	g, ctx := errgroup.WithContext(ctx)

	xdsSrv := NewXdsServer(s.cfg.XdsPort)
	xdsSrv.SetConfig(s.cfg.HttpPort, s.cfg.ExtprocPort, s.cfg.ExtprocAddr)

	var certContent, keyContent string
	if s.cfg.EnvoyCertPath == "" {
		slog.InfoContext(ctx, "No Envoy certificate path provided, generating self-signed certificate for testing")
		var err error
		certContent, keyContent, err = generateSelfSignedCert()
		if err != nil {
			return fmt.Errorf("failed to generate self-signed cert: %w", err)
		}
	}

	xdsSrv.SetTlsConfig(s.cfg.HttpsPort, s.cfg.EnvoyCertPath, certContent, keyContent)
	if s.extprocSrv == nil {
		routeDuration, err := newRouteDurationHistogram()
		if err != nil {
			return fmt.Errorf("failed to create route-duration histogram: %w", err)
		}
		s.extprocSrv = NewExtProcServer(s.cfg.ExtprocPort, s.apiClient, routeDuration)
	}
	ctrl := NewController(s.k8sClient, s.clientset, s.cfg, xdsSrv, s.extprocSrv)

	s.health = newRouterHealth(s.cfg.HealthInterval, s.clientset, s.apiClient, s.cfg)

	// Start Controller / Watcher
	g.Go(func() error {
		slog.InfoContext(ctx, "Starting ActorTemplate controller")
		return ctrl.Start(ctx)
	})

	// Start periodic service checking logic
	g.Go(func() error {
		slog.InfoContext(ctx, "Starting periodic health checker", slog.Duration("interval", s.cfg.HealthInterval))
		s.health.Start(ctx)
		return nil
	})

	// Start xDS Server
	g.Go(func() error {
		slog.InfoContext(ctx, "Starting Envoy xDS Server", slog.Int("port", s.cfg.XdsPort))
		lis, err := net.Listen("tcp", fmt.Sprintf(":%d", s.cfg.XdsPort))
		if err != nil {
			return fmt.Errorf("failed to listen on port %d: %w", s.cfg.XdsPort, err)
		}
		defer lis.Close()

		return xdsSrv.Serve(ctx, lis)
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
			Handler: mux,
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

func generateSelfSignedCert() (string, string, error) {
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return "", "", err
	}

	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization: []string{"Substrate Local Test"},
		},
		NotBefore: time.Now(),
		NotAfter:  time.Now().Add(time.Hour * 24 * 365),

		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		DNSNames:              []string{"localhost"},
	}

	derBytes, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	if err != nil {
		return "", "", err
	}

	certPem := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: derBytes})

	privBytes, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		return "", "", err
	}
	keyPem := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: privBytes})

	return string(certPem), string(keyPem), nil
}
