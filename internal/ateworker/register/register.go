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

// Package register registers a worker with the control plane through the
// node-local atelet, reporting its compute capacity and the sandbox runtimes
// of its pool, and provides file-watching helpers shared by worker binaries.
package register

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"go.ate.dev/substrate/internal/ateletdial"
	"go.ate.dev/substrate/internal/hardware"
	"go.ate.dev/substrate/internal/proto/ateletpb"
	"go.ate.dev/substrate/internal/sandboxruntimes"
	"go.ate.dev/substrate/internal/workercapacity"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const (
	reportTimeout        = 10 * time.Second
	initialReportBackoff = 500 * time.Millisecond
	maxReportBackoff     = 30 * time.Second
	resyncInterval       = 5 * time.Minute
)

// FromFiles combines the downward API compute limits with the worker's own
// actor limit.
//
// A limit that is missing or unparseable is reported as zero, which the control
// plane reads as none: better to place nothing on a worker that cannot say what
// it has than to invent a number for it.
//
// TODO: Watch the projected files and report changes. For now we do not support
// in-place Pod vertical scaling (IPPR); capacity is read once at startup.
// NOTE: Please do not implement this yet. IPPR needs more general consideration.
func FromFiles(actors int) *ateletpb.WorkerResources {
	return fromDir(workercapacity.MountPath, actors)
}

func fromDir(dir string, actors int) *ateletpb.WorkerResources {
	return &ateletpb.WorkerResources{
		CpuMillicores: readLimit(filepath.Join(dir, workercapacity.CPULimitFile)),
		MemoryBytes:   readLimit(filepath.Join(dir, workercapacity.MemoryLimitFile)),
		Actors:        int32(actors),
	}
}

// buildCompat converts the compatibility attributes atelet wrote for one
// SandboxConfig version into a VersionedSandboxCompat.
func buildCompat(attrs sandboxruntimes.Attributes) *ateletpb.VersionedSandboxCompat {
	entries := make([]*ateletpb.AttributeEntry, 0, len(attrs))
	for _, k := range slices.Sorted(maps.Keys(attrs)) {
		entries = append(entries, &ateletpb.AttributeEntry{
			Key:   k,
			Value: attrs[k],
		})
	}
	return &ateletpb.VersionedSandboxCompat{
		SchemaVersion: hardware.SchemaVersionV1,
		Attributes:    entries,
	}
}

// buildRuntime converts one runtime of the sandbox runtimes file into a
// WorkerSandboxRuntime.
func buildRuntime(rt sandboxruntimes.Runtime) *ateletpb.WorkerSandboxRuntime {
	return &ateletpb.WorkerSandboxRuntime{Version: buildCompat(rt.Version)}
}

// buildRequest builds the RegisterWorkerRequest for capacity and one class
// of the sandbox runtimes file: the one this worker implements.
func buildRequest(capacity *ateletpb.WorkerResources, class *sandboxruntimes.Class) *ateletpb.RegisterWorkerRequest {
	c := &ateletpb.WorkerSandboxClass{
		SandboxClass:   class.SandboxClass,
		DefaultRuntime: buildRuntime(class.DefaultRuntime),
	}
	for _, rt := range class.RestorableRuntimes {
		c.RestorableRuntimes = append(c.RestorableRuntimes, buildRuntime(rt))
	}
	return &ateletpb.RegisterWorkerRequest{
		Capacity:       capacity,
		SandboxClasses: []*ateletpb.WorkerSandboxClass{c},
	}
}

// classOf returns f's entry for class, or nil when the file has none.
func classOf(f *sandboxruntimes.File, class string) *sandboxruntimes.Class {
	for i := range f.SandboxClasses {
		if f.SandboxClasses[i].SandboxClass == class {
			return &f.SandboxClasses[i]
		}
	}
	return nil
}

func readLimit(path string) int64 {
	contents, err := os.ReadFile(path)
	if err != nil {
		slog.Warn("Ignoring unreadable capacity limit", slog.String("path", path), slog.Any("err", err))
		return 0
	}
	raw := strings.TrimSpace(string(contents))
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || value < 0 {
		slog.Warn("Ignoring unusable capacity limit", slog.String("path", path), slog.String("value", raw))
		return 0
	}
	return value
}

// ReportConfig configures how a worker reaches the node-local atelet and what
// it registers with.
type ReportConfig struct {
	SocketPath           string
	CredentialBundlePath string
	TrustBundlePath      string
	// AteletSPIFFEID is the identity the node-local atelet must present. It
	// names atelet's namespace, not this worker's, so it is configured rather
	// than derived from the downward API.
	AteletSPIFFEID string
	// Actors is how many actors this worker will host at once.
	Actors int
	// SandboxClass is the class this worker implements ("gvisor", "microvm").
	SandboxClass string
	// RuntimesFile is the sandbox runtimes file atelet keeps for this worker
	// (nodepath.WorkerSandboxRuntimesPath).
	RuntimesFile string
}

// Report keeps the worker registered with the node-local atelet until ctx
// ends: its capacity, and the runtimes in its sandbox runtimes file, which it
// reads at startup and again whenever atelet rewrites it. It returns early
// only on a configuration error, which no retry would fix.
//
// Retrying is what makes a report durable: atelet only accepts once the
// control plane has recorded it, and the Worker record may not exist yet when
// a worker first comes up. Nothing else reports this, so giving up would leave
// the Worker holding no capacity and hosting nothing.
func Report(ctx context.Context, cfg ReportConfig) error {
	if cfg.SandboxClass == "" {
		// The control plane refuses a runtime without a class.
		return errors.New("register worker: SandboxClass is required")
	}
	if cfg.RuntimesFile == "" {
		return errors.New("register worker: RuntimesFile is required")
	}
	tlsConfig, err := ateletdial.TLSConfig(cfg.CredentialBundlePath, cfg.TrustBundlePath, cfg.AteletSPIFFEID)
	if err != nil {
		return fmt.Errorf("register worker: %w", err)
	}
	changes := WatchFilePath(ctx, cfg.RuntimesFile)
	r := newReconciler(
		cfg,
		FromFiles(cfg.Actors),
		func() (*sandboxruntimes.File, error) { return sandboxruntimes.Read(cfg.RuntimesFile) },
		func(ctx context.Context, req *ateletpb.RegisterWorkerRequest) error {
			return reportOnce(ctx, cfg.SocketPath, tlsConfig, req)
		},
	)
	return r.run(ctx, changes)
}

// reconciler registers the worker with the runtimes in its sandbox runtimes
// file whenever they differ from the ones it last registered.
type reconciler struct {
	cfg            ReportConfig
	capacity       *ateletpb.WorkerResources
	read           func() (*sandboxruntimes.File, error)
	register       func(context.Context, *ateletpb.RegisterWorkerRequest) error
	initialBackoff time.Duration
	maxBackoff     time.Duration
	resync         time.Duration
	after          func(time.Duration) <-chan time.Time
	// registered is the last request accepted by atelet, nil until the first
	// registration succeeds.
	registered *ateletpb.RegisterWorkerRequest
}

func newReconciler(
	cfg ReportConfig,
	capacity *ateletpb.WorkerResources,
	read func() (*sandboxruntimes.File, error),
	register func(context.Context, *ateletpb.RegisterWorkerRequest) error,
) *reconciler {
	return &reconciler{
		cfg:            cfg,
		capacity:       capacity,
		read:           read,
		register:       register,
		initialBackoff: initialReportBackoff,
		maxBackoff:     maxReportBackoff,
		resync:         resyncInterval,
		after:          time.After,
	}
}

// run syncs at startup, on every change to the file, with backoff after a
// transient failure, and periodically at resyncInterval, until ctx ends.
// Change signals are ignored while backing off so periodic polling or rapid
// writes cannot bypass the backoff delay when atelet or ateapi is down.
func (r *reconciler) run(ctx context.Context, changes <-chan struct{}) error {
	backoff := r.initialBackoff
	for {
		wait := r.resync
		onChange := changes
		if r.sync(ctx) {
			wait = backoff
			backoff = min(backoff*2, r.maxBackoff)
			onChange = nil
		} else {
			backoff = r.initialBackoff
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case _, ok := <-onChange:
			if !ok {
				changes = nil
			}
		case <-r.after(wait):
		}
	}
}

// sync reads the file and registers if needed. It reports whether the caller
// should retry with backoff.
func (r *reconciler) sync(ctx context.Context) (retry bool) {
	desired, err := r.read()
	if errors.Is(err, fs.ErrNotExist) {
		if r.registered == nil {
			slog.InfoContext(ctx, "Waiting for the sandbox runtimes file, which atelet writes; see its logs if this persists",
				slog.String("path", r.cfg.RuntimesFile))
		}
		return false
	}
	if err != nil {
		slog.WarnContext(ctx, "Ignoring an unreadable sandbox runtimes file", slog.String("path", r.cfg.RuntimesFile), slog.Any("err", err))
		return false
	}
	class := classOf(desired, r.cfg.SandboxClass)
	if class == nil {
		slog.WarnContext(ctx, "Ignoring a sandbox runtimes file without this worker's sandbox class",
			slog.String("path", r.cfg.RuntimesFile), slog.String("sandbox_class", r.cfg.SandboxClass))
		return false
	}
	if len(class.DefaultRuntime.Version) == 0 {
		slog.WarnContext(ctx, "Ignoring a sandbox runtimes file with no default runtime", slog.String("path", r.cfg.RuntimesFile))
		return false
	}
	req := buildRequest(r.capacity, class)
	if proto.Equal(req, r.registered) {
		return false
	}
	if err := r.register(ctx, req); err != nil {
		if status.Code(err) == codes.InvalidArgument {
			slog.ErrorContext(ctx, "Worker registration rejected as invalid; waiting for sandbox runtimes file to change", slog.Any("err", err))
			return false
		}
		slog.WarnContext(ctx, "Could not register worker; will retry", slog.Any("err", err))
		return true
	}
	r.registered = req
	slog.InfoContext(ctx, "Registered worker capacity and sandbox classes",
		slog.Any("capacity", req.GetCapacity()),
		slog.Any("sandbox_classes", req.GetSandboxClasses()))
	return false
}

// reportOnce dials atelet and sends one RegisterWorker.
func reportOnce(ctx context.Context, socketPath string, tlsConfig *tls.Config, req *ateletpb.RegisterWorkerRequest) error {
	conn, err := ateletdial.Dial(socketPath, tlsConfig)
	if err != nil {
		return err
	}
	defer conn.Close()
	callCtx, cancel := context.WithTimeout(ctx, reportTimeout)
	defer cancel()
	_, err = ateletpb.NewWorkerSupportClient(conn).RegisterWorker(callCtx, req)
	return err
}
