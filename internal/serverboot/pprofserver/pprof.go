// Copyright 2026 The Agent Substrate Authors
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

// Package pprofserver serves Go runtime pprof profiles on an explicit HTTP mux.
package pprofserver

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/pprof"
	"os"
	"runtime"
	"strconv"
	"time"

	"github.com/spf13/pflag"
)

const httpServerReadHeaderTimeout = 10 * time.Second

// Config holds pprof server settings populated via RegisterFlags.
type Config struct {
	ListenAddr    string
	MutexFraction int
	BlockRate     int

	fs *pflag.FlagSet
}

// RegisterFlags registers --pprof-listen-addr, --pprof-mutex-fraction, and
// --pprof-block-rate on fs and returns the Config they fill once fs is parsed.
func RegisterFlags(fs *pflag.FlagSet) *Config {
	cfg := &Config{fs: fs}
	fs.StringVar(&cfg.ListenAddr, "pprof-listen-addr", "",
		"Listen address for net/http/pprof (e.g. :6060; disabled when empty). "+
			"Defaults to ATE_PPROF_LISTEN_ADDR. "+
			"For benchmarking only: never enable pprof on a cluster that serves real actors.")
	fs.IntVar(&cfg.MutexFraction, "pprof-mutex-fraction", 0,
		"Mutex contention sampling rate for runtime.SetMutexProfileFraction (0 = disabled). Defaults to ATE_PPROF_MUTEX_FRACTION.")
	fs.IntVar(&cfg.BlockRate, "pprof-block-rate", 0,
		"Blocking profile rate in nanoseconds for runtime.SetBlockProfileRate (0 = disabled). Defaults to ATE_PPROF_BLOCK_RATE.")
	return cfg
}

// IntFromEnv parses a positive integer from environment variable key,
// returning 0 when unset or empty.
func IntFromEnv(key string) (int, error) {
	raw := os.Getenv(key)
	if raw == "" {
		return 0, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer", key)
	}
	return n, nil
}

// Validate applies ATE_PPROF_* environment defaults for flags not explicitly
// set on the command line and rejects negative rates.
func (c *Config) Validate() error {
	if c == nil {
		return nil
	}
	if c.fs != nil {
		if !c.fs.Changed("pprof-listen-addr") {
			c.ListenAddr = os.Getenv("ATE_PPROF_LISTEN_ADDR")
		}
		if !c.fs.Changed("pprof-mutex-fraction") {
			n, err := IntFromEnv("ATE_PPROF_MUTEX_FRACTION")
			if err != nil {
				return err
			}
			c.MutexFraction = n
		}
		if !c.fs.Changed("pprof-block-rate") {
			n, err := IntFromEnv("ATE_PPROF_BLOCK_RATE")
			if err != nil {
				return err
			}
			c.BlockRate = n
		}
	}
	if c.MutexFraction < 0 {
		return fmt.Errorf("--pprof-mutex-fraction must not be negative, got %d", c.MutexFraction)
	}
	if c.BlockRate < 0 {
		return fmt.Errorf("--pprof-block-rate must not be negative, got %d", c.BlockRate)
	}
	return nil
}

func newMux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	return mux
}

func newHTTPServer(addr string) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           newMux(),
		ReadHeaderTimeout: httpServerReadHeaderTimeout,
		// WriteTimeout is intentionally 0 so ?seconds=N CPU/allocs/trace requests are not cut off.
	}
}

// Start runs a dedicated pprof HTTP server on cfg.ListenAddr. Blocks until the
// server stops; designed to be go-launched. If cfg is nil or cfg.ListenAddr is
// empty, Start is a no-op.
func Start(ctx context.Context, cfg *Config) {
	if cfg == nil || cfg.ListenAddr == "" {
		return
	}
	if cfg.MutexFraction > 0 {
		runtime.SetMutexProfileFraction(cfg.MutexFraction)
	}
	if cfg.BlockRate > 0 {
		runtime.SetBlockProfileRate(cfg.BlockRate)
	}

	slog.InfoContext(ctx, "Starting pprof HTTP server", slog.String("addr", cfg.ListenAddr))
	if err := newHTTPServer(cfg.ListenAddr).ListenAndServe(); err != nil {
		slog.ErrorContext(ctx, "Pprof HTTP server failed", slog.Any("err", err))
		os.Exit(1)
	}
}
