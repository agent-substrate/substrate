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
	"crypto/x509"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/cmd/benchmarking/isolate/internal/fakeworker"
	"go.ate.dev/substrate/internal/credbundle"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// reportTimeout bounds one capacity report, relay hop included.
const reportTimeout = 10 * time.Second

// grpcRelay sends capacity reports to the fake-atelet relay on a Worker's
// node, as worker sends them to the atelet on its own node. A rejected report
// carries ate-api-server's status unchanged.
type grpcRelay struct {
	creds credentials.TransportCredentials

	mu sync.Mutex
	// conns holds one connection per relay address. There is one relay per
	// fake node, so the map stays small; a connection to a relay that is gone
	// goes idle and holds nothing open.
	conns map[string]*grpc.ClientConn
}

// newGRPCRelay presents this pod's certificate and accepts only a server
// whose certificate chains to the pod-identity trust bundle and carries
// atelet's SPIFFE ID. The server is dialed by pod IP and its certificate has
// no DNS or IP SAN, so the chain is verified here rather than by hostname.
// The trust bundle is read at each handshake, so a CA rotation is picked up
// without a restart.
func newGRPCRelay(clientBundlePath, trustBundlePath, ateletID string) (*grpcRelay, error) {
	loadRoots := credbundle.PoolLoader(trustBundlePath)
	if _, err := loadRoots(); err != nil {
		return nil, fmt.Errorf("load trust bundle %s: %w", trustBundlePath, err)
	}
	cfg := &tls.Config{
		MinVersion:           tls.VersionTLS13,
		GetClientCertificate: credbundle.ClientLoader(clientBundlePath),
		InsecureSkipVerify:   true,
		VerifyConnection: func(cs tls.ConnectionState) error {
			if len(cs.PeerCertificates) == 0 {
				return errors.New("relay presented no certificate")
			}
			roots, err := loadRoots()
			if err != nil {
				return err
			}
			intermediates := x509.NewCertPool()
			for _, c := range cs.PeerCertificates[1:] {
				intermediates.AddCert(c)
			}
			if _, err := cs.PeerCertificates[0].Verify(x509.VerifyOptions{
				Roots:         roots,
				Intermediates: intermediates,
				KeyUsages:     []x509.ExtKeyUsage{x509.ExtKeyUsageAny},
			}); err != nil {
				return fmt.Errorf("verifying relay certificate: %w", err)
			}
			return fakeworker.VerifyPeerID(cs, ateletID)
		},
	}
	return &grpcRelay{creds: credentials.NewTLS(cfg), conns: map[string]*grpc.ClientConn{}}, nil
}

// Report registers report's Worker through the relay at addr (host:port), as a
// worker process does at startup.
func (g *grpcRelay) Report(ctx context.Context, addr string, report workerReport) error {
	conn, err := g.conn(addr)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, reportTimeout)
	defer cancel()
	client := ateapi.NewWorkerServiceClient(conn)
	ref := &ateapi.ObjectRef{Name: report.Name}
	epoch := report.Epoch
	if epoch == 0 {
		resp, err := client.AcquireWorkerEpoch(ctx, &ateapi.AcquireWorkerEpochRequest{Worker: ref})
		if err != nil {
			return err
		}
		epoch = resp.GetEpoch()
	}
	if _, err := client.RegisterWorker(ctx, &ateapi.RegisterWorkerRequest{
		Worker:   ref,
		Epoch:    epoch,
		Ips:      report.IPs,
		Capacity: report.Capacity,
	}); err != nil {
		return err
	}
	_, err = client.SetWorkerSandboxRuntimes(ctx, &ateapi.SetWorkerSandboxRuntimesRequest{
		Worker:         ref,
		Epoch:          epoch,
		SandboxClasses: report.SandboxClasses,
	})
	return err
}

func (g *grpcRelay) conn(addr string) (*grpc.ClientConn, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if c, ok := g.conns[addr]; ok {
		return c, nil
	}
	c, err := grpc.NewClient("passthrough:///"+addr, grpc.WithTransportCredentials(g.creds))
	if err != nil {
		return nil, fmt.Errorf("dialing relay %s: %w", addr, err)
	}
	g.conns[addr] = c
	return c, nil
}

// Prune closes the connections to relays not in live, the relay addresses by
// node from the latest pod list, so a fake-atelet that restarted under a new
// IP leaves no connection behind.
func (g *grpcRelay) Prune(live map[string]string) {
	keep := map[string]bool{}
	for _, addr := range live {
		keep[addr] = true
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	for addr, c := range g.conns {
		if !keep[addr] {
			_ = c.Close()
			delete(g.conns, addr)
		}
	}
}

// Close closes every relay connection.
func (g *grpcRelay) Close() {
	g.mu.Lock()
	defer g.mu.Unlock()
	for addr, c := range g.conns {
		_ = c.Close()
		delete(g.conns, addr)
	}
}
