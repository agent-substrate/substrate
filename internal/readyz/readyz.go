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

// Package readyz polls a container's HTTP or TCP readiness endpoint from inside an
// ateom. The intent is to detect the moment a container's server
// starts accepting connections with single-millisecond latency: while the
// server is still booting the kernel returns RST in microseconds, so a
// millisecond poll loop spends almost no time blocked, and once the
// listen socket is up the next iteration completes the probe on veth-local
// latency.
package readyz

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/agent-substrate/substrate/internal/ateerrors"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
)

// Tuning knobs. Sized for actor cold-start where the HTTP server may take
// a few seconds to bind; HTTPClient below is a var so tests can substitute
// a transport that targets a test server's loopback address.
const (
	// DefaultOverallTimeout applies to probes that do not set
	// timeout_seconds. A workload that needs longer says so on its
	// ActorTemplate rather than having every actor wait as long.
	DefaultOverallTimeout = 30 * time.Second
	RequestTimeout        = 250 * time.Millisecond
	PollInterval          = 1 * time.Millisecond
	DefaultPath           = "/readyz"
	maxIdleConnsHost      = 1
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

// WaitAll blocks until every container with a readyz probe set is ready,
// or returns the first error. Containers without a probe are skipped (their
// absence means "no readiness gate").
//
// Every caller is an ateom RPC handler, so a %w-wrapped Reason dies here:
// errors.As cannot cross a process, and the interceptor would flatten it to a
// bare codes.Internal, leaving atelet reading UNKNOWN. The ErrorInfo detail is
// what carries it. Internal and no crash directive both match today's behavior.
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
	err := g.Wait()
	if err != nil && errors.Is(err, ateerrors.ReasonWorkloadNotReady) {
		return ateerrors.NewGRPCError(ctx, codes.Internal, ateerrors.ReasonWorkloadNotReady, nil, err)
	}
	return err
}

// Wait polls until the HTTP endpoint returns 200 or the TCP connection succeeds,
// the context is cancelled, or the overall deadline is exceeded.
func Wait(ctx context.Context, containerName string, probe *ateompb.Readyz, actorIP string) error {
	if (probe.GetHttpGet() == nil) == (probe.GetTcpSocket() == nil) {
		return fmt.Errorf("invalid readyz config for %q: exactly one of httpGet or tcpSocket is required", containerName)
	}

	var endpoint string
	var attempt func(context.Context) (bool, error)
	if tcp := probe.GetTcpSocket(); tcp != nil {
		port := tcp.GetPort()
		if port < 1 || port > 65535 {
			return fmt.Errorf("invalid readyz config for %q: invalid port %d", containerName, port)
		}
		address := net.JoinHostPort(actorIP, strconv.Itoa(int(port)))
		endpoint = "tcp://" + address
		attempt = func(ctx context.Context) (bool, error) {
			return tryTCP(ctx, address)
		}
	} else {
		url, err := URL(probe, actorIP)
		if err != nil {
			return fmt.Errorf("invalid readyz config for %q: %w", containerName, err)
		}
		endpoint = url
		client := HTTPClient()
		defer client.CloseIdleConnections()
		attempt = func(ctx context.Context) (bool, error) {
			return tryOnce(ctx, client, url)
		}
	}

	timeout := overallTimeout(probe)
	start := time.Now()
	deadline := start.Add(timeout)
	probeCtx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	attempts := 0
	var lastErr error
	for {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("readyz cancelled for %q after %s (%d attempts, last error: %v): %w",
				containerName, time.Since(start), attempts, lastErr, err)
		}
		if time.Now().After(deadline) {
			// Tagged only here: the cancellation above is ateom draining, not the actor failing.
			return fmt.Errorf("%w: readyz for %q did not succeed within %s (%d attempts, last error: %v)",
				ateerrors.ReasonWorkloadNotReady, containerName, timeout, attempts, lastErr)
		}

		attempts++
		ok, err := attempt(probeCtx)
		if err != nil {
			lastErr = err
		}
		if ok && probeCtx.Err() == nil {
			slog.InfoContext(ctx, "Readyz succeeded",
				slog.String("container", containerName),
				slog.String("endpoint", endpoint),
				slog.Duration("elapsed", time.Since(start)),
				slog.Int("attempts", attempts))
			return nil
		}

		// Sleep instead of busy-loop. The interval bounds the worst-case
		// detection delay; pre-readiness, each attempt also blocks for
		// tens of µs in the kernel waiting for the RST, so the actual
		// per-iteration period is somewhat longer than the sleep alone.
		select {
		case <-probeCtx.Done():
		case <-time.After(PollInterval):
		}
	}
}

func tryTCP(ctx context.Context, address string) (bool, error) {
	dialer := net.Dialer{Timeout: RequestTimeout}
	conn, err := dialer.DialContext(ctx, "tcp", address)
	if err != nil {
		return false, err
	}
	conn.Close()
	return true, nil
}

// overallTimeout resolves how long Wait polls before giving up. A
// non-positive timeout_seconds falls back to the default: unlike a warmup
// delay, a zero deadline is never a meaningful request, so it means "unset"
// rather than "fail immediately".
func overallTimeout(probe *ateompb.Readyz) time.Duration {
	if s := probe.GetTimeoutSeconds(); s > 0 {
		return time.Duration(s) * time.Second
	}
	return DefaultOverallTimeout
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

// URL builds an HTTP probe endpoint URL.
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
