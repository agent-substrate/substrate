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

// Command actor is the workload of the egress scale test. Once told to start,
// it opens every endpoint, one per interval, then on one goroutine per
// endpoint sends it an HTTP GET once per period of C intervals, and counts
// what happened under one lock.
//
// The loop starts on POST /start rather than at boot: every actor restores
// from a golden snapshot taken after the template's warmup, so a loop started
// at boot would run in the golden actor, which has no egress policy, and be
// frozen into every actor's snapshot, failed requests included.
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	listen := flag.String("listen", ":80", "Address to serve the start, stats and stop routes on.")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	srv := &http.Server{
		Addr:              *listen,
		Handler:           newServer().handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	slog.Info("egress-tunnels actor listening", "addr", *listen)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("serving", "err", err)
		os.Exit(1)
	}
}
