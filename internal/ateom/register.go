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
// node-local atelet, reporting its compute capacity and the sandbox runtime
// its actors start under.
package ateom

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/agent-substrate/substrate/internal/ateletdial"
	"github.com/agent-substrate/substrate/internal/hardware"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
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

// defaultRuntime is the runtime actors hosted by this ateom start under: its
// sandbox class on this host's hardware. The name is left empty: it names a
// SandboxConfig version, which the ateom does not know.
//
// TODO: Report the pool's SandboxConfig so atelet can complete the identity
// with the digests of the default version's assets and list its other
// enabled versions as restorable_runtimes.
func defaultRuntime(sandboxClass string) *ateletpb.SandboxRuntime {
	return &ateletpb.SandboxRuntime{
		SandboxClass:  sandboxClass,
		CompatVersion: toCompat(hardware.ProbeHost()),
	}
}

// toCompat converts the probe's VersionedSandboxCompat to atelet's, which it
// mirrors field for field.
func toCompat(in *ateapipb.VersionedSandboxCompat) *ateletpb.VersionedSandboxCompat {
	out := &ateletpb.VersionedSandboxCompat{SchemaVersion: in.GetSchemaVersion()}
	for _, a := range in.GetAttributes() {
		out.Attributes = append(out.Attributes, &ateletpb.AttributeEntry{Key: a.GetKey(), Value: a.GetValue()})
	}
	return out
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
// capacity and sandbox runtime class it registers with.
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
}

// Report tells the node-local atelet what this ateom can supply and the
// runtime its actors start under, retrying until it is accepted or ctx ends.
//
// Retrying is what makes a single report durable: atelet only accepts once the
// control plane has recorded it, and the Worker record may not exist yet when
// an ateom first comes up. Nothing else reports this, so giving up would leave
// the Worker holding no capacity and hosting nothing.
func Report(ctx context.Context, cfg ReportConfig) error {
	if cfg.SandboxClass == "" {
		// The control plane refuses a runtime without a class, and retrying
		// would not change that.
		return errors.New("capacity report: SandboxClass is required")
	}
	tlsConfig, err := ateletdial.TLSConfig(cfg.CredentialBundlePath, cfg.TrustBundlePath, cfg.AteletSPIFFEID)
	if err != nil {
		return fmt.Errorf("capacity report: %w", err)
	}
	req := &ateletpb.RegisterWorkerRequest{
		Capacity:       FromFiles(cfg.Actors),
		DefaultRuntime: defaultRuntime(cfg.SandboxClass),
	}
	err = retryReport(ctx, func() error {
		return reportOnce(ctx, cfg.SocketPath, tlsConfig, req)
	}, initialReportBackoff)
	if err != nil {
		return err
	}
	slog.InfoContext(ctx, "Registered worker capacity and sandbox runtime",
		slog.Any("capacity", req.GetCapacity()),
		slog.Any("default_runtime", req.GetDefaultRuntime()))
	return nil
}

// retryReport calls send until it succeeds or ctx ends, backing off between
// attempts. send is a parameter so the loop can be exercised without a socket
// or certificates.
func retryReport(ctx context.Context, send func() error, backoff time.Duration) error {
	for {
		err := send()
		if err == nil {
			return nil
		}
		slog.WarnContext(ctx, "Retrying worker capacity report", slog.Duration("in", backoff), slog.Any("err", err))
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, maxReportBackoff)
	}
}

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
