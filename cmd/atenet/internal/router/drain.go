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
// marker. The manifest mounts an emptyDir here in both containers; the
// dataplane container's preStop hook polls the same path.
const defaultDrainCompleteFile = "/var/run/atenet/drain-complete"

// removeStaleDrainMarker deletes a leftover drain-complete marker at startup.
// The emptyDir the marker lives on survives container restarts within the
// pod, and a stale marker would let the dataplane container's preStop hook
// exit the instant a later drain begins — before any connection has drained.
func removeStaleDrainMarker(ctx context.Context, path string) {
	if path == "" {
		return
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
		slog.WarnContext(ctx, "Failed to remove stale drain-complete marker", slog.String("path", path), slog.Any("err", err))
	}
}

// writeDrainMarker creates the drain-complete marker, releasing the dataplane
// container's preStop hook. Failure is logged, never fatal: the kubelet still bounds the
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

// dataplaneDrainer actively drains the dataplane proxy sidecar: established
// connections finish and their in-flight requests complete before the
// implementation returns. envoyDrainer is the Envoy implementation (driving
// the admin API); the orchestrator below stays agnostic of which proxy is
// deployed.
type dataplaneDrainer interface {
	// Drain blocks until the dataplane has quiesced or ctx expires; an error
	// reports an incomplete drain and the shutdown sequence continues.
	Drain(ctx context.Context) error
}

// drainParams wires the shutdown sequence. The order is forced by the
// ext_proc filter being failClosed in the dataplane: the ext_proc server must
// outlive the dataplane's drain, because any request the dataplane still
// accepts during its drain window needs ext_proc answering.
type drainParams struct {
	readiness *serverboot.Readiness
	// delay is the route-drain window: after the readiness flip, how long to
	// keep serving while the Service endpoints drop this pod.
	delay time.Duration
	// dataplane, when non-nil, is drained after the delay and before the
	// ext_proc server stops, bounded by dataplaneWindow. nil means the
	// deployed dataplane offers the router no drain hook and manages its own
	// termination; the sequence then proceeds directly to the ext_proc drain.
	dataplane       dataplaneDrainer
	dataplaneWindow time.Duration
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
// out the propagation delay, drain the dataplane (established connections
// finish), then drain ext_proc so parked requests complete — force-stopping
// past the timeout — and finally stop everything else. The returned channel
// closes once the sequence completes, so Run can block on it before letting
// the deferred tracer/meter flushes run.
func drainOnShutdown(ctx context.Context, p drainParams) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		<-ctx.Done()
		slog.InfoContext(ctx, "Shutdown signal received; draining")
		p.readiness.MarkNotReady()
		time.Sleep(p.delay)

		if p.dataplane != nil {
			slog.InfoContext(ctx, "Draining dataplane", slog.Duration("window", p.dataplaneWindow))
			dpCtx, cancel := context.WithTimeout(context.Background(), p.dataplaneWindow)
			if err := p.dataplane.Drain(dpCtx); err != nil {
				// TODO: Add a shutdown-outcome metric (clean vs
				// dataplane-drain-incomplete vs ext_proc force-stopped) so
				// unclean shutdowns are visible in dashboards, not only logs.
				slog.WarnContext(ctx, "Dataplane drain incomplete; continuing shutdown", slog.Any("err", err))
			} else {
				slog.InfoContext(ctx, "Dataplane drained")
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
			// TODO: Count this in the shutdown-outcome metric above: a
			// force-stop here means in-flight ext_proc streams (parked
			// requests included) were cancelled — the unclean-shutdown signal
			// operators most need to see.
			slog.WarnContext(ctx, "ext_proc drain deadline exceeded; forcing stop")
			p.extproc.Stop()
		}

		p.stopRest()
	}()
	return done
}
