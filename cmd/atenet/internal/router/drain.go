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
	"errors"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/agent-substrate/substrate/internal/serverboot"
)

// defaultDrainCompleteFile is where the drain sequence leaves its completion
// marker. The manifest mounts an emptyDir here in both containers; the Envoy
// container's preStop hook polls the same path.
const defaultDrainCompleteFile = "/var/run/atenet/drain-complete"

// removeStaleDrainMarker deletes a leftover drain-complete marker at startup.
// The emptyDir the marker lives on survives container restarts within the
// pod, and a stale marker would let the Envoy preStop hook exit the instant a
// later drain begins — before any connection has drained.
func removeStaleDrainMarker(ctx context.Context, path string) {
	if path == "" {
		return
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
		slog.WarnContext(ctx, "Failed to remove stale drain-complete marker", slog.String("path", path), slog.Any("err", err))
	}
}

// writeDrainMarker creates the drain-complete marker, releasing the Envoy
// preStop hook. Failure is logged, never fatal: the kubelet still bounds the
// hook at terminationGracePeriodSeconds, so a missing marker degrades to a
// slower exit, not a wedge.
func writeDrainMarker(ctx context.Context, path string) {
	if path == "" {
		return
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		slog.WarnContext(ctx, "Failed to create drain-complete marker directory", slog.String("path", path), slog.Any("err", err))
		return
	}
	if err := os.WriteFile(path, []byte(time.Now().UTC().Format(time.RFC3339)+"\n"), 0o644); err != nil {
		slog.WarnContext(ctx, "Failed to write drain-complete marker", slog.String("path", path), slog.Any("err", err))
		return
	}
	slog.InfoContext(ctx, "Drain-complete marker written", slog.String("path", path))
}

// grpcStopper is the subset of *grpc.Server the drain sequence drives.
type grpcStopper interface {
	// GracefulStop stops accepting new connections and RPCs and blocks until
	// all in-flight RPCs (parked ext_proc streams, most of all) finish.
	GracefulStop()
	// Stop cancels in-flight RPCs and closes all connections immediately.
	Stop()
}

// drainParams wires the shutdown sequence. The order is forced by the
// dataplane: ext_proc is failClosed for Envoy, so the ext_proc server must
// outlive Envoy's drain — any request Envoy still accepts during its drain
// window needs ext_proc answering.
type drainParams struct {
	readiness *serverboot.Readiness
	// delay is the route-drain window: after the readiness flip, how long to
	// keep serving while the Service endpoints drop this pod.
	delay time.Duration
	// drainEnvoy gracefully drains the dataplane sidecar; nil when there is
	// none to drive (agentgateway mode). It is handed a context bounded by
	// envoyWindow and must return when it expires.
	drainEnvoy  func(context.Context) error
	envoyWindow time.Duration
	// extproc is the ext_proc gRPC server; timeout bounds its graceful drain
	// (sized >= the parking budget so parked requests finish normally).
	extproc grpcStopper
	timeout time.Duration
	// stopRest cancels the work context, stopping the remaining subsystems
	// (xDS, controller, health checker, statusz) once no traffic depends on
	// them.
	stopRest func()
}

// drainOnShutdown drives graceful shutdown when ctx is cancelled (SIGTERM or
// interrupt): flip readiness (Service stops sending new connections), wait
// out the propagation delay, drain the Envoy sidecar (established
// connections finish), then drain ext_proc so parked requests complete —
// force-stopping past the timeout — and finally stop everything else. The
// returned channel closes once the sequence completes, so Run can block on
// it before letting the deferred tracer/meter flushes run.
func drainOnShutdown(ctx context.Context, p drainParams) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		<-ctx.Done()
		slog.InfoContext(ctx, "Shutdown signal received; draining")
		p.readiness.MarkNotReady()
		time.Sleep(p.delay)

		if p.drainEnvoy != nil {
			slog.InfoContext(ctx, "Draining Envoy", slog.Duration("window", p.envoyWindow))
			envoyCtx, cancel := context.WithTimeout(context.Background(), p.envoyWindow)
			if err := p.drainEnvoy(envoyCtx); err != nil {
				slog.WarnContext(ctx, "Envoy drain incomplete; continuing shutdown", slog.Any("err", err))
			} else {
				slog.InfoContext(ctx, "Envoy drained")
			}
			cancel()
		}

		slog.InfoContext(ctx, "Starting ext_proc drain")
		drainComplete := make(chan struct{})
		go func() {
			p.extproc.GracefulStop()
			close(drainComplete)
		}()
		select {
		case <-drainComplete:
			slog.InfoContext(ctx, "ext_proc drain completed within deadline")
		case <-time.After(p.timeout):
			slog.WarnContext(ctx, "ext_proc drain deadline exceeded; forcing stop")
			p.extproc.Stop()
		}

		p.stopRest()
	}()
	return done
}
