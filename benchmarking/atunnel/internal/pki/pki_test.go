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

package pki

import (
	"crypto/x509"
	"path/filepath"
	"testing"

	"go.ate.dev/substrate/internal/ateletdial"
	"go.ate.dev/substrate/internal/credbundle"
	"go.ate.dev/substrate/internal/installdefaults"
	"go.ate.dev/substrate/internal/substratex509"
)

func TestGenerate(t *testing.T) {
	dir := t.TempDir()
	if err := Generate(dir, Options{WorkerHosts: []string{"atunnel-worker"}, GatewayHosts: []string{"atunnel-sink"}}); err != nil {
		t.Fatal(err)
	}
	roots, err := credbundle.ParsePool(filepath.Join(dir, CAFile))
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		file  string
		usage x509.ExtKeyUsage
		name  string
		uri   string
	}{
		{WorkerBundle, x509.ExtKeyUsageServerAuth, "atunnel-worker", ""},
		{WorkerBundle, x509.ExtKeyUsageClientAuth, "", ""},
		{AteletBundle, x509.ExtKeyUsageServerAuth, "", installdefaults.AteletSPIFFEID(installdefaults.SystemNamespace)},
		{RouterBundle, x509.ExtKeyUsageClientAuth, "", installdefaults.RouterSPIFFEID(installdefaults.SystemNamespace)},
		{GatewayBundle, x509.ExtKeyUsageServerAuth, "atunnel-sink", ""},
	} {
		cert, err := credbundle.Parse(filepath.Join(dir, tc.file))
		if err != nil {
			t.Fatalf("%s: %v", tc.file, err)
		}
		if _, err := cert.Leaf.Verify(x509.VerifyOptions{Roots: roots, DNSName: tc.name, KeyUsages: []x509.ExtKeyUsage{tc.usage}}); err != nil {
			t.Errorf("%s does not verify for %v: %v", tc.file, tc.usage, err)
		}
		if tc.uri != "" && (len(cert.Leaf.URIs) != 1 || cert.Leaf.URIs[0].String() != tc.uri) {
			t.Errorf("%s URIs = %v, want %s", tc.file, cert.Leaf.URIs, tc.uri)
		}
	}

	// atunnel's own atelet client accepts the pair: the worker certificate
	// carries a Pod identity, and atelet's names the same node.
	if _, err := ateletdial.TLSConfig(filepath.Join(dir, WorkerBundle), filepath.Join(dir, CAFile), installdefaults.AteletSPIFFEID(installdefaults.SystemNamespace)); err != nil {
		t.Fatalf("ateletdial.TLSConfig rejects the worker certificate: %v", err)
	}
	worker, _ := credbundle.Parse(filepath.Join(dir, WorkerBundle))
	atelet, _ := credbundle.Parse(filepath.Join(dir, AteletBundle))
	w, _ := substratex509.PodIdentityFromCertificate(worker.Leaf)
	a, _ := substratex509.PodIdentityFromCertificate(atelet.Leaf)
	if w == nil || a == nil || w.NodeName != a.NodeName || w.NodeUID != a.NodeUID {
		t.Fatalf("worker and atelet Pod identities do not share a node: %+v, %+v", w, a)
	}

	if _, _, err := LoadCA(dir); err != nil {
		t.Fatal(err)
	}
}
