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

package main

import (
	"context"
	"crypto/tls"
	"log/slog"

	"github.com/agent-substrate/substrate/cmd/benchmarking/isolate/internal/fakeworker"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc"
)

// capacityClient is the part of ateapipb.WorkerServiceClient the relay uses.
type capacityClient interface {
	RegisterWorker(ctx context.Context, in *ateapipb.RegisterWorkerRequest, opts ...grpc.CallOption) (*ateapipb.RegisterWorkerResponse, error)
}

// relay forwards fake-workersync's capacity reports to ate-api-server under
// this pod's atelet identity. It stands where atelet's AteomSupport stands in
// production: ateom knows its capacity and atelet relays it, because
// ate-api-server accepts a report only from the atelet on the Worker's node.
// The relay decides nothing; fake-workersync names the Worker and its
// capacity, and ate-api-server checks the Worker is on this node.
//
// It serves WorkerService so the request and ate-api-server's status pass
// through unchanged; every other WorkerService call is Unimplemented. Unlike
// the herder it keeps UnimplementedWorkerServiceServer on purpose: refusing
// every call but RegisterWorker is what keeps the relay from lending this
// pod's atelet identity to anything else.
type relay struct {
	ateapipb.UnimplementedWorkerServiceServer

	client capacityClient
}

func (r *relay) RegisterWorker(ctx context.Context, in *ateapipb.RegisterWorkerRequest) (*ateapipb.RegisterWorkerResponse, error) {
	resp, err := r.client.RegisterWorker(ctx, in)
	if err != nil {
		slog.WarnContext(ctx, "Capacity report rejected",
			slog.String("worker", in.GetWorker().GetName()), slog.Any("err", err))
		return nil, err
	}
	slog.DebugContext(ctx, "Relayed capacity report",
		slog.String("worker", in.GetWorker().GetName()), slog.Any("capacity", in.GetCapacity()))
	return resp, nil
}

// relayTLSConfig serves the pod-identity certificate and admits only a client
// whose certificate chains to the pod-identity trust bundle and carries the
// SPIFFE ID allowed: anything else could set any Worker's capacity. Like the
// herder's, it reads the trust bundle at each handshake, so a CA rotation is
// picked up without a restart.
func relayTLSConfig(servingBundlePath, clientCAPath, allowed string) (*tls.Config, error) {
	cfg, err := serverTLSConfig(servingBundlePath, clientCAPath)
	if err != nil {
		return nil, err
	}
	perClient := cfg.GetConfigForClient
	cfg.GetConfigForClient = func(hello *tls.ClientHelloInfo) (*tls.Config, error) {
		c, err := perClient(hello)
		if err != nil {
			return nil, err
		}
		c.VerifyConnection = func(cs tls.ConnectionState) error {
			return fakeworker.VerifyPeerID(cs, allowed)
		}
		return c, nil
	}
	return cfg, nil
}
