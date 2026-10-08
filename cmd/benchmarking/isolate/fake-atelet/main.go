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

// Command fake-atelet stands in for atelet in control-plane benchmarks. It
// runs as atelet's DaemonSet would, on the benchmark nodes only, so
// ate-api-server dials it as the node's atelet. It answers every Atelet
// call with success after a delay, without running or saving any workload.
//
// Actors it "runs" do not exist. Never run it on a cluster that serves real
// actors.
package main

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/agent-substrate/substrate/internal/atelet"
	"github.com/agent-substrate/substrate/internal/credbundle"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/internal/serverboot"
	"github.com/agent-substrate/substrate/internal/version"
	"github.com/spf13/pflag"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

var (
	port             = pflag.Int("port", atelet.DefaultPort, "Port to serve Atelet on. ate-api-server dials atelet.DefaultPort.")
	healthListenAddr = pflag.String("health-listen-addr", ":9090", "Address to serve /healthz and /readyz on.")

	grpcServerCredBundle = pflag.String("grpc-server-cred-bundle", "/run/podidentity.podcert.ate.dev/credential-bundle.pem", "Credential bundle presented as the serving certificate.")
	clientCACerts        = pflag.String("client-ca-certs", "/run/podidentity.podcert.ate.dev/trust-bundle.pem", "CA bundle used to verify client certificates.")

	delay                       = pflag.Duration("delay", 0, "How long each Atelet call takes before it succeeds.")
	delayRun                    = pflag.Duration("delay-run", -1, "Override --delay for Run. Negative uses --delay.")
	delayRestore                = pflag.Duration("delay-restore", -1, "Override --delay for Restore. Negative uses --delay.")
	delayCheckpoint             = pflag.Duration("delay-checkpoint", -1, "Override --delay for Checkpoint. Negative uses --delay.")
	delayUploadPausedCheckpoint = pflag.Duration("delay-upload-paused-checkpoint", -1, "Override --delay for UploadPausedCheckpoint. Negative uses --delay.")
	delayTerminate              = pflag.Duration("delay-terminate", -1, "Override --delay for Terminate. Negative uses --delay.")

	showVersion  = pflag.Bool("version", false, "Print version and exit.")
	logLevelFlag = pflag.String("log-level", "info", "Minimum log level: debug, info, warn, or error.")
)

func main() {
	pflag.Parse()
	if *showVersion {
		fmt.Println(version.String())
		return
	}
	serverboot.InitLogger()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := serverboot.SetLogLevel(*logLevelFlag); err != nil {
		serverboot.Fatal(ctx, "Invalid --log-level", err)
	}
	callDelays, err := resolveDelays(*delay, *delayRun, *delayRestore, *delayCheckpoint, *delayUploadPausedCheckpoint, *delayTerminate)
	if err != nil {
		serverboot.Fatal(ctx, "Invalid --delay", err)
	}

	herderTLS, err := serverTLSConfig(*grpcServerCredBundle, *clientCACerts)
	if err != nil {
		serverboot.Fatal(ctx, "Failed to build server TLS config", err)
	}
	lis, err := net.Listen("tcp", ":"+strconv.Itoa(*port))
	if err != nil {
		serverboot.Fatal(ctx, "Failed to listen", err)
	}
	h := &herder{delays: callDelays}
	srv := grpc.NewServer(grpc.Creds(credentials.NewTLS(herderTLS)))
	ateletpb.RegisterAteletServer(srv, h)
	health := &http.Server{Addr: *healthListenAddr, Handler: healthHandler(), ReadHeaderTimeout: 10 * time.Second}

	slog.WarnContext(ctx, "Serving the fake atelet: actor lifecycle calls succeed without running any workload",
		slog.String("delays", fmt.Sprintf("%+v", callDelays)))

	go func() {
		if err := srv.Serve(lis); err != nil {
			serverboot.Fatal(ctx, "Failed to serve Atelet", err)
		}
	}()
	go func() {
		if err := health.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverboot.Fatal(ctx, "Failed to serve health endpoints", err)
		}
	}()

	<-ctx.Done()
	slog.InfoContext(context.Background(), "Shutting down")
	srv.GracefulStop()
	_ = health.Shutdown(context.Background())
}

// healthHandler serves /healthz and /readyz, both 200 while the process
// runs: fake-atelet holds no state that has to be ready first.
func healthHandler() http.Handler {
	mux := http.NewServeMux()
	ok := func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }
	mux.HandleFunc("/healthz", ok)
	mux.HandleFunc("/readyz", ok)
	return mux
}

// serverTLSConfig mirrors atelet's: the pod-identity certificate as the
// serving certificate, and client certificates required and verified against
// the CA bundle as it is at each handshake, so a CA rotation is picked up
// without a restart.
func serverTLSConfig(servingBundlePath, clientCAPath string) (*tls.Config, error) {
	loadClientCAs := credbundle.PoolLoader(clientCAPath)
	if _, err := loadClientCAs(); err != nil {
		return nil, fmt.Errorf("load CA bundle %s: %w", clientCAPath, err)
	}
	serverCert := credbundle.Loader(servingBundlePath)
	return &tls.Config{
		MinVersion: tls.VersionTLS13,
		GetConfigForClient: func(*tls.ClientHelloInfo) (*tls.Config, error) {
			clientCAs, err := loadClientCAs()
			if err != nil {
				return nil, err
			}
			return &tls.Config{
				MinVersion:     tls.VersionTLS13,
				GetCertificate: serverCert,
				ClientAuth:     tls.RequireAndVerifyClientCert,
				ClientCAs:      clientCAs,
			}, nil
		},
	}, nil
}
