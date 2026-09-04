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

// Package ateomcapacity reports what an ateom can supply to the actors it
// hosts. Both ateoms answer GetCapacity from here so they answer it alike.
package ateomcapacity

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/agent-substrate/substrate/internal/ateletdial"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"

	"github.com/agent-substrate/substrate/internal/proto/ateletpb"
)

// Files the atecontroller projects into the ateom container from the downward
// API, in milli-cores and bytes.
const (
	CapacityMountPath = "/run/ateom-capacity"
	CPULimitFile      = "cpu_milli"
	MemoryLimitFile   = "memory_bytes"
)

// actorsPerAteom is how many actors an ateom hosts at once. One, today.
const actorsPerAteom = 1

const (
	reportTimeout        = 10 * time.Second
	initialReportBackoff = 500 * time.Millisecond
	maxReportBackoff     = 30 * time.Second
)

// FromFiles reads the ateom's compute limits from its downward API volume, as
// the report it sends to the node-local atelet.
//
// A limit that is missing or unparseable is reported as zero, which the control
// plane reads as none: better to place nothing on a worker that cannot say what
// it has than to invent a number for it.
//
// TODO: Watch the projected files and report changes. For now we do not support
// in-place Pod vertical scaling (IPPR); capacity is read once at startup.
// NOTE: Please do not implement this yet. IPPR needs more general consideration.
func FromFiles() *ateletpb.SetWorkerCapacityRequest {
	return fromDir(CapacityMountPath)
}

func fromDir(dir string) *ateletpb.SetWorkerCapacityRequest {
	return &ateletpb.SetWorkerCapacityRequest{
		Capacity: &ateapipb.WorkerResources{
			Actors: actorsPerAteom,
			// A limit read as zero is left out, which the control plane reads
			// as none of that dimension.
			Resources: resources.CPUMemory(
				readLimit(filepath.Join(dir, CPULimitFile)),
				readLimit(filepath.Join(dir, MemoryLimitFile)),
			),
		},
	}
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

// ReportConfig is what an ateom needs to reach the atelet on its node.
type ReportConfig struct {
	SocketPath           string
	CredentialBundlePath string
	TrustBundlePath      string
}

// Report tells the node-local atelet what this ateom can supply, retrying
// until it is accepted or ctx ends.
//
// Retrying is what makes a single report durable: atelet only accepts once the
// control plane has recorded it, and the Worker record may not exist yet when
// an ateom first comes up. Nothing else reports this, so giving up would leave
// the Worker holding no capacity and hosting nothing.
func Report(ctx context.Context, cfg ReportConfig) error {
	tlsConfig, err := ateletdial.TLSConfig(cfg.CredentialBundlePath, cfg.TrustBundlePath)
	if err != nil {
		return fmt.Errorf("capacity report: %w", err)
	}
	capacity := FromFiles()
	err = retryReport(ctx, func() error {
		return reportOnce(ctx, cfg.SocketPath, tlsConfig, capacity)
	}, initialReportBackoff)
	if err != nil {
		return err
	}
	slog.InfoContext(ctx, "Reported worker capacity", slog.Any("capacity", capacity.GetCapacity()))
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

func reportOnce(ctx context.Context, socketPath string, tlsConfig *tls.Config, capacity *ateletpb.SetWorkerCapacityRequest) error {
	conn, err := ateletdial.Dial(socketPath, tlsConfig)
	if err != nil {
		return err
	}
	defer conn.Close()
	callCtx, cancel := context.WithTimeout(ctx, reportTimeout)
	defer cancel()
	_, err = ateletpb.NewWorkerCapacityClient(conn).SetWorkerCapacity(callCtx, capacity)
	return err
}
