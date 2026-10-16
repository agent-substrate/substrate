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

//go:build linux

package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/pprof"
	"os"
	"time"

	"github.com/spf13/cobra"

	"go.ate.dev/substrate/benchmarking/atunnel/internal/procstats"
	"go.ate.dev/substrate/benchmarking/atunnel/internal/workerhost"
	"go.ate.dev/substrate/internal/ateworker/tunnel"
)

// tunnelCommand is process B of the benchmark: atunnel started and hosting
// actors with the worker's own code (see workerhost), and nothing else, so
// this process's CPU and memory are atunnel's. It takes the worker's atunnel
// flags, serves its counters on /stats and Go's profiles on /debug/pprof, and
// needs root, for the actors' network namespaces. The worker starts it.
func tunnelCommand() *cobra.Command {
	var actors int
	var gateway, adminListen, logLevel string
	var cfg *tunnel.Config
	cmd := &cobra.Command{
		Use:   "tunnel",
		Short: "Run atunnel, hosting actors with the worker's own code",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			var level slog.Level
			if err := level.UnmarshalText([]byte(logLevel)); err != nil {
				return fmt.Errorf("--log-level: %w", err)
			}
			// atunnel logs a warning per failed dial, which floods the
			// output under a deliberately failing load.
			slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level})))
			return runTunnel(cmd.Context(), *cfg, actors, gateway, adminListen)
		},
	}
	cfg = tunnel.RegisterFlags(cmd.Flags())
	cmd.Flags().IntVar(&actors, "actors", 10, "actors to host")
	cmd.Flags().StringVar(&gateway, "gateway", "atunnel-sink:9443", "egress gateway every actor tunnels to, host:port")
	cmd.Flags().StringVar(&adminListen, "admin", ":9090", "address for /stats, /healthz, and /debug/pprof")
	cmd.Flags().StringVar(&logLevel, "log-level", "error", "log level: debug, info, warn, or error")
	return cmd
}

func runTunnel(ctx context.Context, cfg tunnel.Config, actors int, gateway, adminListen string) error {
	host, err := workerhost.Start(ctx, cfg, actors, gateway)
	if err != nil {
		return err
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = host.Close(closeCtx)
	}()

	mux := http.NewServeMux()
	mux.Handle("/stats", procstats.Handler(func() map[string]any {
		return map[string]any{"tunnel": host.Info()}
	}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) })
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	adminServer := &http.Server{Addr: adminListen, Handler: mux}
	errc := make(chan error, 1)
	go func() { errc <- adminServer.ListenAndServe() }()
	context.AfterFunc(ctx, func() { _ = adminServer.Close() })

	// Not through slog, which is quiet by default: the supervisor waits for
	// /healthz, a person reading the log for this.
	fmt.Fprintf(os.Stderr, "tunnel serving: ingress %s, connect %s, %d actors, admin %s\n",
		cfg.ListenAddress, cfg.ConnectListenAddress, actors, adminListen)
	select {
	case <-ctx.Done():
		return nil
	case err := <-errc:
		if ctx.Err() != nil {
			return nil
		}
		return err
	}
}
