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

// Command macletd exposes the native macOS VM supervisor over HostRuntime gRPC.
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"syscall"
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/hostruntimepb"
	"github.com/spf13/pflag"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

var digestImage = regexp.MustCompile(`^.+@sha256:[0-9a-f]{64}$`)

type options struct {
	listen, maclet, stateDir, image, sourceBundle string
	advertiseHost, proxyListen                    string
	cert, key, clientCA                           string
	drain                                         time.Duration
}

func main() {
	var o options
	pflag.StringVar(&o.listen, "listen-address", "", "gRPC listen address (required)")
	pflag.StringVar(&o.maclet, "maclet-executable", "", "path to the signed maclet executable (required)")
	pflag.StringVar(&o.stateDir, "state-directory", "", "private Actor bundle directory (required)")
	pflag.StringVar(&o.image, "image", "", "exact digest-pinned image reference accepted by this host (required)")
	pflag.StringVar(&o.sourceBundle, "source-bundle", "", "powered-off source Lume bundle (required)")
	pflag.StringVar(&o.advertiseHost, "advertise-host", "", "Mac hostname or IP reachable by the Actor dataplane (required)")
	pflag.StringVar(&o.proxyListen, "proxy-listen-address", ":0", "listen address for per-Actor workload proxies; port must be zero")
	pflag.StringVar(&o.cert, "tls-cert-file", "", "TLS server certificate PEM (required)")
	pflag.StringVar(&o.key, "tls-key-file", "", "TLS server private key PEM (required)")
	pflag.StringVar(&o.clientCA, "client-ca-file", "", "CA bundle used to verify client certificates (required)")
	pflag.DurationVar(&o.drain, "drain-grace", 5*time.Second, "graceful gRPC shutdown deadline")
	pflag.Parse()
	if err := run(o); err != nil {
		log.Fatal(err)
	}
}

func run(o options) error {
	if o.listen == "" || o.maclet == "" || o.stateDir == "" || o.sourceBundle == "" || o.advertiseHost == "" || o.cert == "" || o.key == "" || o.clientCA == "" {
		return errors.New("listen, maclet, state, image/source, and TLS flags are required")
	}
	if host, port, err := net.SplitHostPort(o.proxyListen); err != nil || port != "0" || host == "" && o.proxyListen != ":0" {
		return errors.New("--proxy-listen-address must be a host with port 0, or :0")
	}
	if !digestImage.MatchString(o.image) {
		return errors.New("--image must be an exact sha256 digest-pinned reference")
	}
	if o.drain <= 0 {
		return errors.New("--drain-grace must be positive")
	}
	if !filepath.IsAbs(o.maclet) || !filepath.IsAbs(o.stateDir) || !filepath.IsAbs(o.sourceBundle) {
		return errors.New("--maclet-executable, --state-directory, and --source-bundle must be absolute paths")
	}
	macletInfo, err := os.Stat(o.maclet)
	if err != nil {
		return fmt.Errorf("--maclet-executable must be an executable regular file: %w", err)
	}
	if !macletInfo.Mode().IsRegular() || macletInfo.Mode().Perm()&0o111 == 0 {
		return errors.New("--maclet-executable must be an executable regular file")
	}
	if exists, err := safeDirectory(o.sourceBundle); err != nil || !exists {
		return fmt.Errorf("--source-bundle must be a real directory: %v", err)
	}
	if err := os.MkdirAll(o.stateDir, 0o700); err != nil {
		return fmt.Errorf("create state directory: %w", err)
	}
	if exists, err := safeDirectory(o.stateDir); err != nil || !exists {
		return fmt.Errorf("--state-directory must be a real directory: %v", err)
	}
	if err := os.Chmod(o.stateDir, 0o700); err != nil {
		return fmt.Errorf("make state directory private: %w", err)
	}
	cert, err := tls.LoadX509KeyPair(o.cert, o.key)
	if err != nil {
		return fmt.Errorf("load server certificate: %w", err)
	}
	caPEM, err := os.ReadFile(o.clientCA)
	if err != nil {
		return fmt.Errorf("read client CA: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return errors.New("client CA contains no certificates")
	}
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{cert}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: pool}
	lis, err := net.Listen("tcp", o.listen)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	g := grpc.NewServer(grpc.Creds(credentials.NewTLS(tlsConfig)))
	hostruntimepb.RegisterHostRuntimeServer(g, newServer(o.maclet, o.stateDir, o.image, o.sourceBundle, o.advertiseHost, o.proxyListen))
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	go func() {
		<-ctx.Done()
		done := make(chan struct{})
		go func() { g.GracefulStop(); close(done) }()
		select {
		case <-done:
		case <-time.After(o.drain):
			g.Stop()
		}
	}()
	if err := g.Serve(lis); err != nil && err != grpc.ErrServerStopped {
		return fmt.Errorf("serve: %w", err)
	}
	return nil
}
