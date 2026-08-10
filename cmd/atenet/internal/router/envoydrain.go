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
	"bufio"
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// envoyDrainPollInterval is how often the drainer re-reads Envoy's active
// downstream connection count while waiting for the drain to complete.
const envoyDrainPollInterval = 250 * time.Millisecond

// envoyDrainer drives the Envoy sidecar's graceful drain over its admin
// interface (same-pod loopback). The drain sequence in router.go invokes it
// after the readiness flip has propagated (no new connections arrive) and
// before the ext_proc server stops (Envoy still needs ext_proc for any
// request it accepts while draining — the filter is failClosed).
//
// Every error path degrades instead of wedging: an unreachable admin
// interface means Envoy is already gone (its own SIGTERM raced us), which is
// itself a completed drain.
type envoyDrainer struct {
	adminAddr    string
	client       *http.Client
	pollInterval time.Duration
}

func newEnvoyDrainer(adminAddr string) *envoyDrainer {
	return &envoyDrainer{
		adminAddr:    adminAddr,
		client:       &http.Client{Timeout: 2 * time.Second},
		pollInterval: envoyDrainPollInterval,
	}
}

// Drain triggers Envoy's graceful listener drain via the admin interface and
// polls active downstream connections until zero or ctx expires. Returns nil
// when drained or if Envoy has already exited.
func (d *envoyDrainer) Drain(ctx context.Context) error {
	if !d.post(ctx, "/healthcheck/fail") {
		return d.adminGone(ctx) // unreachable: Envoy already exited (or the window expired mid-call)
	}
	if !d.post(ctx, "/drain_listeners?graceful&skip_exit") {
		return d.adminGone(ctx)
	}

	ticker := time.NewTicker(d.pollInterval)
	defer ticker.Stop()
	for {
		active, ok := d.activeConnections(ctx)
		if !ok {
			return d.adminGone(ctx)
		}
		if active == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("%d downstream connections still active at the drain deadline: %w", active, ctx.Err())
		case <-ticker.C:
		}
	}
}

// adminGone disambiguates an admin-call failure: a request failing because
// the drain window expired mid-call is a deadline, not evidence that Envoy
// exited — misreporting it as "drained" would hide an incomplete drain from
// the shutdown logs.
func (d *envoyDrainer) adminGone(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("drain window expired while querying the Envoy admin interface: %w", err)
	}
	return nil // genuinely unreachable: Envoy already exited, drain moot
}

// post issues an admin POST and reports whether Envoy answered at all.
// Non-2xx answers are logged and treated as answered — the drain continues.
func (d *envoyDrainer) post(ctx context.Context, path string) bool {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+d.adminAddr+path, nil)
	if err != nil {
		slog.WarnContext(ctx, "Building Envoy admin request failed", slog.String("path", path), slog.Any("err", err))
		return true
	}
	resp, err := d.client.Do(req)
	if err != nil {
		slog.InfoContext(ctx, "Envoy admin unreachable; treating the sidecar as already stopped", slog.String("path", path), slog.Any("err", err))
		return false
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		slog.WarnContext(ctx, "Envoy admin call failed", slog.String("path", path), slog.Int("status", resp.StatusCode))
	}
	return true
}

// activeConnections sums Envoy's non-admin downstream_cx_active gauges. The
// admin listener's own connections (including this poll) are excluded, else
// the count could never reach zero.
func (d *envoyDrainer) activeConnections(ctx context.Context) (int, bool) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+d.adminAddr+"/stats?filter=downstream_cx_active", nil)
	if err != nil {
		return 0, true
	}
	resp, err := d.client.Do(req)
	if err != nil {
		return 0, false
	}
	defer resp.Body.Close()

	total := 0
	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		name, value, found := strings.Cut(scanner.Text(), ":")
		if !found || strings.Contains(name, "admin") {
			continue
		}
		if !strings.HasSuffix(strings.TrimSpace(name), "downstream_cx_active") {
			continue
		}
		n, err := strconv.Atoi(strings.TrimSpace(value))
		if err != nil {
			continue
		}
		total += n
	}
	return total, true
}
