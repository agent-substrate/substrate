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

package overcommit

import (
	"context"
	"crypto/tls"
	"fmt"
	"time"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/internal/ateletdial"
	"go.ate.dev/substrate/internal/proto/ateletpb"
)

// adjustTimeout bounds a single allocation RPC to the atelet.
const adjustTimeout = 10 * time.Second

// Adjuster changes the calling Worker's allocated resources. Every quantity in
// delta is non-negative; the method names the direction.
type Adjuster interface {
	AddWorkerAllocated(ctx context.Context, delta *ateapi.Resources) error
	SubtractWorkerAllocated(ctx context.Context, delta *ateapi.Resources) error
}

// DialConfig is what an ateworker needs to reach the atelet on its node.
type DialConfig struct {
	SocketPath           string
	CredentialBundlePath string
	TrustBundlePath      string
	// AteletSPIFFEID is the identity the node-local atelet must present.
	AteletSPIFFEID string
}

// grpcAdjuster sends allocation changes to the node-local atelet, which names
// the Worker from this pod's certificate.
type grpcAdjuster struct {
	socketPath string
	tlsConfig  *tls.Config
}

// NewGRPCAdjuster returns an Adjuster that calls the node-local atelet.
func NewGRPCAdjuster(cfg DialConfig) (Adjuster, error) {
	tlsConfig, err := ateletdial.TLSConfig(cfg.CredentialBundlePath, cfg.TrustBundlePath, cfg.AteletSPIFFEID)
	if err != nil {
		return nil, fmt.Errorf("overcommit adjuster: %w", err)
	}
	return &grpcAdjuster{socketPath: cfg.SocketPath, tlsConfig: tlsConfig}, nil
}

func (a *grpcAdjuster) AddWorkerAllocated(ctx context.Context, delta *ateapi.Resources) error {
	return a.call(ctx, func(ctx context.Context, client ateletpb.WorkerSupportClient) error {
		_, err := client.AddWorkerAllocated(ctx, &ateletpb.AddWorkerAllocatedRequest{ResourceDelta: toAteletResources(delta)})
		return err
	})
}

func (a *grpcAdjuster) SubtractWorkerAllocated(ctx context.Context, delta *ateapi.Resources) error {
	return a.call(ctx, func(ctx context.Context, client ateletpb.WorkerSupportClient) error {
		_, err := client.SubtractWorkerAllocated(ctx, &ateletpb.SubtractWorkerAllocatedRequest{ResourceDelta: toAteletResources(delta)})
		return err
	})
}

// call dials afresh for each call, so rotated worker credentials are picked
// up and atelet is re-verified.
func (a *grpcAdjuster) call(ctx context.Context, fn func(context.Context, ateletpb.WorkerSupportClient) error) error {
	conn, err := ateletdial.Dial(a.socketPath, a.tlsConfig)
	if err != nil {
		return err
	}
	defer conn.Close()
	callCtx, cancel := context.WithTimeout(ctx, adjustTimeout)
	defer cancel()
	return fn(callCtx, ateletpb.NewWorkerSupportClient(conn))
}

// toAteletResources converts the control plane's Resources to atelet's, which
// mirrors it field for field.
func toAteletResources(in *ateapi.Resources) *ateletpb.Resources {
	if in == nil {
		return nil
	}
	return &ateletpb.Resources{
		CpuMillicores: in.GetCpuMillicores(),
		MemoryBytes:   in.GetMemoryBytes(),
	}
}
