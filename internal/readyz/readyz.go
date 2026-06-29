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

// Package readyz polls a container's HTTP readiness endpoint from inside an
// ateom. The intent is to detect the moment a container's HTTP server
// starts accepting connections with single-millisecond latency: while the
// server is still booting the kernel returns RST in microseconds, so a
// sub-millisecond poll loop spends almost no time blocked, and once the
// listen socket is up the next iteration completes the GET on veth-local
// latency.
package readyz

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	"golang.org/x/sync/errgroup"
)

// Tuning knobs. Sized for actor cold-start where the HTTP server may take
// a few seconds to bind; HTTPClient below is a var so tests can substitute
// a transport that targets a test server's loopback address.
const (
	OverallTimeout   = 30 * time.Second
	RequestTimeout   = 250 * time.Millisecond
	PollInterval     = 1 * time.Millisecond
	DefaultPath      = "/readyz"
	maxIdleConnsHost = 1
)

// HTTPClient builds a keep-alive HTTP client tuned for fast, repeated
// probing of a single endpoint. Exposed as a var so tests can substitute a
// transport that targets a test server's loopback address.
var HTTPClient = func() *http.Client {
	tr := &http.Transport{
		DisableCompression:    true,
		MaxIdleConnsPerHost:   maxIdleConnsHost,
		DialContext:           (&net.Dialer{Timeout: RequestTimeout}).DialContext,
		ResponseHeaderTimeout: RequestTimeout,
	}
	return &http.Client{Transport: tr, Timeout: RequestTimeout}
}

// WaitAll blocks until every container with a readyz probe set reports 200,
// or returns the first error. Containers without a probe are skipped (their
// absence means "no readiness gate").
func WaitAll(ctx context.Context, containers []*ateompb.Container, actorIP string) error {
	g, gctx := errgroup.WithContext(ctx)
	for _, ac := range containers {
		if ac.GetReadyz() == nil {
			continue
		}
		ac := ac
		g.Go(func() error {
			return Wait(gctx, ac.GetName(), ac.GetReadyz(), actorIP)
		})
	}
	return g.Wait()
}

// Wait polls the configured HTTP endpoint until it returns 200, the context
// is cancelled, or the overall deadline is exceeded.
func Wait(ctx context.Context, containerName string, probe *ateompb.Readyz, actorIP string) error {
	url, err := URL(probe, actorIP)
	if err != nil {
		return fmt.Errorf("invalid readyz config for %q: %w", containerName, err)
	}

	client := HTTPClient()
	defer client.CloseIdleConnections()

	start := time.Now()
	deadline := start.Add(OverallTimeout)
	attempts := 0
	var lastErr error
	for {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("readyz cancelled for %q after %s (%d attempts, last error: %v): %w",
				containerName, time.Since(start), attempts, lastErr, err)
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("readyz for %q never returned 200 within %s (%d attempts, last error: %v)",
				containerName, OverallTimeout, attempts, lastErr)
		}

		attempts++
		ok, err := tryOnce(ctx, client, url)
		if err != nil {
			lastErr = err
		}
		if ok {
			slog.InfoContext(ctx, "Readyz reached 200",
				slog.String("container", containerName),
				slog.String("url", url),
				slog.Duration("elapsed", time.Since(start)),
				slog.Int("attempts", attempts))
			return nil
		}

		// Sleep instead of busy-loop. The interval bounds the worst-case
		// detection delay; pre-readiness, each attempt also blocks for
		// tens of µs in the kernel waiting for the RST, so the actual
		// per-iteration period is somewhat longer than the sleep alone.
		select {
		case <-ctx.Done():
		case <-time.After(PollInterval):
		}
	}
}

func tryOnce(ctx context.Context, client *http.Client, url string) (bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return false, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return false, err
	}
	// Drain so the connection can be reused by the keep-alive pool.
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return false, fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	return true, nil
}

// URL builds the probe endpoint URL. Exported so callers and tests can
// validate a probe spec before kicking off a Wait.
func URL(probe *ateompb.Readyz, actorIP string) (string, error) {
	hg := probe.GetHttpGet()
	if hg == nil {
		return "", fmt.Errorf("httpGet is required")
	}
	port := hg.GetPort()
	if port < 1 || port > 65535 {
		return "", fmt.Errorf("invalid port %d", port)
	}
	path := hg.GetPath()
	if path == "" {
		path = DefaultPath
	} else if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	return fmt.Sprintf("http://%s:%d%s", actorIP, port, path), nil
}
