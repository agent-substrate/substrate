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

package fakeatelet

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"path/filepath"
	"testing"
	"time"

	"go.ate.dev/substrate/benchmarking/atunnel/internal/pki"
	"go.ate.dev/substrate/internal/atunnel/egress"
	"go.ate.dev/substrate/internal/credbundle"
	"go.ate.dev/substrate/internal/installdefaults"
	"go.ate.dev/substrate/internal/substratex509"
)

// TestMintThroughAtunnelsClient mints an actor certificate the way atunnel
// does, with its own broker client, over the fake atelet's socket.
func TestMintThroughAtunnelsClient(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	dir := t.TempDir()
	if err := pki.Generate(dir, pki.Options{WorkerHosts: []string{"atunnel-worker"}, GatewayHosts: []string{"atunnel-sink"}}); err != nil {
		t.Fatal(err)
	}
	server, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	// Unix socket paths are limited to about 100 bytes; TempDir can be long.
	socket := filepath.Join("/tmp", "fakeatelet-"+time.Now().Format("150405.000000")+".sock")
	errc := make(chan error, 1)
	go func() { errc <- server.Serve(ctx, dir, socket) }()

	source, err := egress.NewBrokerCertificateSource(egress.BrokerConfig{
		SocketPath:           socket,
		CredentialBundlePath: filepath.Join(dir, pki.WorkerBundle),
		TrustBundlePath:      filepath.Join(dir, pki.CAFile),
		ActorAtespace:        "bench",
		ActorName:            "actor-0",
		ActorUID:             "00000000-0000-4000-8000-000000000000",
		AteletSPIFFEID:       installdefaults.AteletSPIFFEID(installdefaults.SystemNamespace),
	})
	if err != nil {
		t.Fatal(err)
	}
	var expiry time.Time
	for range 50 {
		if expiry, err = source.MintWorkerCertificate(ctx); err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("minting: %v", err)
	}
	if time.Until(expiry) < time.Hour {
		t.Errorf("certificate expires at %v, too soon", expiry)
	}
	cert, err := source.GetClientCertificate(&tls.CertificateRequestInfo{})
	if err != nil {
		t.Fatal(err)
	}
	roots, err := credbundle.ParsePool(filepath.Join(dir, pki.CAFile))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cert.Leaf.Verify(x509.VerifyOptions{Roots: roots, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}); err != nil {
		t.Errorf("minted certificate does not verify: %v", err)
	}
	id, err := substratex509.ActorIdentityFromCertificate(cert.Leaf)
	if err != nil || id == nil || id.ActorName != "actor-0" {
		t.Errorf("minted certificate actor identity = %+v, %v", id, err)
	}
}
