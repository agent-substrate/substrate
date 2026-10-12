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

// Package ateom registers an ateom worker with the control plane through the
// node-local atelet, reporting its compute capacity and the sandbox runtimes
// it found by running the sandbox versions of its pool.
package ateom

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/fsnotify/fsnotify"
	"google.golang.org/protobuf/proto"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/agent-substrate/substrate/internal/ateletdial"
	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/internal/sandboxversions"
)

// Files the atecontroller projects into the ateom container from the downward
// API, in milli-cores and bytes.
const (
	CapacityMountPath = "/run/ateom-capacity"
	CPULimitFile      = "cpu_milli"
	MemoryLimitFile   = "memory_bytes"
)

const (
	reportTimeout        = 10 * time.Second
	initialReportBackoff = 500 * time.Millisecond
	maxReportBackoff     = 30 * time.Second
	// resyncInterval bounds how long a missed change to the versions file
	// goes unnoticed.
	resyncInterval = time.Minute
)

// FromFiles combines the downward API compute limits with the ateom's own
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
	return fromDir(CapacityMountPath, actors)
}

func fromDir(dir string, actors int) *ateletpb.WorkerResources {
	return &ateletpb.WorkerResources{
		Actors: int32(actors),
		Resources: cpuMemory(
			readLimit(filepath.Join(dir, CPULimitFile)),
			readLimit(filepath.Join(dir, MemoryLimitFile)),
		),
	}
}

// cpuMemory is the Resources for those two dimensions. A zero dimension is
// left out, which the control plane reads as none of it; nil when both are.
func cpuMemory(cpuMilli, memoryBytes int64) *ateletpb.Resources {
	var limits []*ateletpb.Limits
	if cpuMilli != 0 {
		limits = append(limits, &ateletpb.Limits{Name: resources.ResourceCPU, Quantity: resource.NewMilliQuantity(cpuMilli, resource.DecimalSI).String()})
	}
	if memoryBytes != 0 {
		limits = append(limits, &ateletpb.Limits{Name: resources.ResourceMemory, Quantity: resource.NewQuantity(memoryBytes, resource.BinarySI).String()})
	}
	if len(limits) == 0 {
		return nil
	}
	return &ateletpb.Resources{Limits: limits}
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

// ReportConfig configures how an ateom reaches the node-local atelet and what
// it registers with.
type ReportConfig struct {
	SocketPath           string
	CredentialBundlePath string
	TrustBundlePath      string
	// AteletSPIFFEID is the identity the node-local atelet must present. It
	// names atelet's namespace, not this worker's, so it is configured rather
	// than derived from the downward API.
	AteletSPIFFEID string
	// Actors is how many actors this ateom will host at once.
	Actors int
	// SandboxClass is the class this ateom implements ("gvisor", "microvm").
	SandboxClass string
	// VersionsFile is the sandbox versions file atelet keeps for this worker
	// (nodepath.AteomSandboxVersionsFile).
	VersionsFile string
	// Prober runs each sandbox version's assets to find the class-specific
	// attributes of its runtime. Nil reports only the host's attributes and
	// the identity of each version's assets.
	Prober RuntimeProber
}

// Report keeps the worker registered with the node-local atelet until ctx
// ends: its capacity, and one runtime per sandbox version in its versions
// file, which it reads at startup and again whenever atelet rewrites it. It
// returns early only on a configuration error, which no retry would fix.
//
// Retrying is what makes a report durable: atelet only accepts once the
// control plane has recorded it, and the Worker record may not exist yet when
// an ateom first comes up. Nothing else reports this, so giving up would leave
// the Worker holding no capacity and hosting nothing.
func Report(ctx context.Context, cfg ReportConfig) error {
	if cfg.SandboxClass == "" {
		// The control plane refuses a runtime without a class.
		return errors.New("capacity report: SandboxClass is required")
	}
	if cfg.VersionsFile == "" {
		return errors.New("capacity report: VersionsFile is required")
	}
	tlsConfig, err := ateletdial.TLSConfig(cfg.CredentialBundlePath, cfg.TrustBundlePath, cfg.AteletSPIFFEID)
	if err != nil {
		return fmt.Errorf("capacity report: %w", err)
	}
	r := &reconciler{
		cfg:      cfg,
		capacity: FromFiles(cfg.Actors),
		read:     func() (*ateletpb.WorkerSandboxVersions, error) { return sandboxversions.Read(cfg.VersionsFile) },
		register: func(ctx context.Context, req *ateletpb.RegisterWorkerRequest) error {
			return reportOnce(ctx, cfg.SocketPath, tlsConfig, req)
		},
		runtimes: newRuntimeCache(),
	}
	return r.run(ctx, watchFile(ctx, cfg.VersionsFile))
}

// reconciler registers the worker with the versions in its sandbox versions
// file whenever they differ from the versions it last registered.
type reconciler struct {
	cfg      ReportConfig
	capacity *ateletpb.WorkerResources
	read     func() (*ateletpb.WorkerSandboxVersions, error)
	register func(context.Context, *ateletpb.RegisterWorkerRequest) error
	runtimes *runtimeCache
	// registered is nil until the first success.
	registered *ateletpb.WorkerSandboxVersions
}

// run syncs on every change to the file, every resyncInterval in case a
// change went unnoticed, and with backoff after a failure, until ctx ends.
func (r *reconciler) run(ctx context.Context, changes <-chan struct{}) error {
	resync := time.NewTicker(resyncInterval)
	defer resync.Stop()
	backoff := initialReportBackoff
	for {
		var retry <-chan time.Time
		if r.sync(ctx) {
			retry = time.After(backoff)
			backoff = min(backoff*2, maxReportBackoff)
		} else {
			backoff = initialReportBackoff
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changes:
		case <-resync.C:
		case <-retry:
		}
	}
}

// sync reads the file and registers if needed. It reports whether the caller
// should retry with backoff.
func (r *reconciler) sync(ctx context.Context) (retry bool) {
	desired, err := r.read()
	if errors.Is(err, fs.ErrNotExist) {
		if r.registered == nil {
			slog.InfoContext(ctx, "Waiting for the sandbox versions file, which atelet writes; see its logs if this persists",
				slog.String("path", r.cfg.VersionsFile))
		}
		return false
	}
	if err != nil {
		slog.WarnContext(ctx, "Ignoring an unreadable sandbox versions file", slog.Any("err", err))
		return false
	}
	if desired.GetSandboxClass() != r.cfg.SandboxClass {
		slog.WarnContext(ctx, "Ignoring a sandbox versions file for another sandbox class",
			slog.String("path", r.cfg.VersionsFile), slog.String("sandbox_class", desired.GetSandboxClass()))
		return false
	}
	if proto.Equal(desired, r.registered) {
		return false
	}
	slog.InfoContext(ctx, "Probing sandbox versions",
		slog.String("sandbox_config", desired.GetSandboxConfig()),
		slog.Int("versions", len(desired.GetVersions())))
	def, restorable, err := sandboxRuntimes(ctx, r.cfg.SandboxClass, r.cfg.Prober, r.runtimes, desired.GetVersions())
	if err != nil {
		slog.WarnContext(ctx, "Could not build sandbox runtimes; will retry", slog.Any("err", err))
		return true
	}
	req := &ateletpb.RegisterWorkerRequest{
		Capacity:           r.capacity,
		DefaultRuntime:     def,
		RestorableRuntimes: restorable,
	}
	if err := r.register(ctx, req); err != nil {
		slog.WarnContext(ctx, "Could not register worker; will retry", slog.Any("err", err))
		return true
	}
	r.registered = desired
	slog.InfoContext(ctx, "Registered worker capacity and sandbox runtimes",
		slog.String("sandbox_config", desired.GetSandboxConfig()),
		slog.Any("capacity", req.GetCapacity()),
		slog.Any("default_runtime", req.GetDefaultRuntime()),
		slog.Any("restorable_runtimes", req.GetRestorableRuntimes()))
	return false
}

// watchFile signals on the returned channel whenever the file at path is
// created, written, or replaced. It watches the directory, as atelet
// replaces the file by rename. Without a watcher the channel never fires and
// the caller is left with its periodic resync.
func watchFile(ctx context.Context, path string) <-chan struct{} {
	changes := make(chan struct{}, 1)
	w, err := fsnotify.NewWatcher()
	if err == nil {
		err = w.Add(filepath.Dir(path))
	}
	if err != nil {
		slog.WarnContext(ctx, "Cannot watch the sandbox versions file; changes are picked up on resync only",
			slog.String("path", path), slog.Duration("resync", resyncInterval), slog.Any("err", err))
		if w != nil {
			w.Close()
		}
		return changes
	}
	name := filepath.Base(path)
	go func() {
		defer w.Close()
		for {
			select {
			case <-ctx.Done():
				return
			case ev, ok := <-w.Events:
				if !ok {
					return
				}
				if sandboxversions.IsTemp(ev.Name) || filepath.Base(ev.Name) != name {
					continue
				}
				select {
				case changes <- struct{}{}:
				default: // a signal is already pending
				}
			case err, ok := <-w.Errors:
				if !ok {
					return
				}
				slog.WarnContext(ctx, "Sandbox versions file watch error", slog.Any("err", err))
			}
		}
	}()
	return changes
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
	_, err = ateletpb.NewAteomSupportClient(conn).RegisterWorker(callCtx, req)
	return err
}
