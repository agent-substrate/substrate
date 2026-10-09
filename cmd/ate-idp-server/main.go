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

// Command ate-idp-server serves the OpenID Connect discovery document and JWK
// set that relying parties use to verify actor JWTs. It publishes the public
// keys of the authority pool in --actor-id-jwt-pool, which it rereads at most
// once a minute.
package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"net/http"
	"os/signal"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/pflag"

	"github.com/agent-substrate/substrate/cmd/ate-idp-server/internal/server"
	"github.com/agent-substrate/substrate/internal/credbundle"
	"github.com/agent-substrate/substrate/internal/localjwtauthority"
	"github.com/agent-substrate/substrate/internal/serverboot"
	"github.com/agent-substrate/substrate/internal/version"
)

var (
	issuer       = pflag.String("issuer", "", "Actor JWT issuer URL; must match ate-api-server's --actor-jwt-issuer (required).")
	poolFile     = pflag.String("actor-id-jwt-pool", "", "File holding the actor JWT authority pool that ate-api-server signs with (required).")
	listenAddr   = pflag.String("listen-address", ":8443", "HTTPS listen address.")
	serverBundle = pflag.String("server-cred-bundle", "", "Credential bundle (PEM key and chain) presented for serving TLS (required).")
	logLevel     = pflag.String("log-level", "info", "One of debug, info, warn, error.")
	drainGrace   = pflag.Duration("drain-grace", 5*time.Second, "How long to wait for in-flight requests on shutdown.")
)

func main() {
	pflag.Parse()

	ctx := context.Background()
	serverboot.InitLogger()
	if err := serverboot.SetLogLevel(*logLevel); err != nil {
		serverboot.Fatal(ctx, "invalid --log-level", err)
	}
	slog.InfoContext(ctx, "starting ate-idp-server", slog.String("version", version.String()))

	if err := run(ctx); err != nil {
		serverboot.Fatal(ctx, "ate-idp-server exited with error", err)
	}
}

func run(ctx context.Context) error {
	if missing := missingFlags(map[string]string{
		"--issuer":             *issuer,
		"--actor-id-jwt-pool":  *poolFile,
		"--server-cred-bundle": *serverBundle,
	}); len(missing) > 0 {
		return fmt.Errorf("required flags not set: %s", strings.Join(missing, ", "))
	}
	pool, err := localjwtauthority.NewRefreshingPool(*poolFile)
	if err != nil {
		return fmt.Errorf("loading --actor-id-jwt-pool: %w", err)
	}
	srv, err := server.New(*issuer, pool)
	if err != nil {
		return err
	}
	mux := http.NewServeMux()
	srv.Register(mux)
	slog.InfoContext(ctx, "serving issuer", slog.String("issuer", *issuer))

	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	httpSrv := &http.Server{
		Addr:              *listenAddr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		TLSConfig: &tls.Config{
			MinVersion:     tls.VersionTLS12,
			GetCertificate: credbundle.Loader(*serverBundle),
		},
	}
	serveErr := make(chan error, 1)
	go func() { serveErr <- httpSrv.ListenAndServeTLS("", "") }()

	select {
	case err := <-serveErr:
		return fmt.Errorf("serving on %s: %w", *listenAddr, err)
	case <-ctx.Done():
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), *drainGrace)
	defer cancel()
	return httpSrv.Shutdown(shutdownCtx)
}

// missingFlags returns the sorted names of the flags whose values are empty.
func missingFlags(flags map[string]string) []string {
	var missing []string
	for name, value := range flags {
		if value == "" {
			missing = append(missing, name)
		}
	}
	slices.Sort(missing)
	return missing
}
