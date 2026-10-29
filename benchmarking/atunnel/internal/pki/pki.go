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

// Package pki writes the throwaway certificates the benchmark processes share,
// standing in for the pod-identity and service certificates a cluster
// projects. One CA signs everything; the fake atelet keeps its key to sign
// actor certificates.
package pki

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"go.ate.dev/substrate/internal/installdefaults"
	"go.ate.dev/substrate/internal/substratex509"
)

// File names written by Generate, relative to its directory.
const (
	// CAFile is the CA certificate. Every peer verifies against it.
	CAFile = "ca.pem"
	// CAKeyFile is the CA's private key, for the fake atelet.
	CAKeyFile = "ca-key.pem"
	// WorkerBundle is the worker Pod's credential bundle: atunnel serves
	// ingress with it and authenticates to atelet with it.
	WorkerBundle = "worker.pem"
	// AteletBundle is the fake atelet's serving certificate.
	AteletBundle = "atelet.pem"
	// RouterBundle is the client certificate the load generator presents to
	// atunnel's ingress, carrying the router's SPIFFE ID.
	RouterBundle = "router.pem"
	// GatewayBundle is the fake egress gateway's serving certificate.
	GatewayBundle = "gateway.pem"
)

// The node the worker and atelet share. atunnel checks that atelet's
// certificate names the same node as its own.
var node = substratex509.PodIdentity{NodeName: "bench-node", NodeUID: "00000000-0000-4000-8000-000000000001"}

// Options shape the certificates.
type Options struct {
	// WorkerHosts are the names and IPs atunnel's serving certificate is
	// valid for.
	WorkerHosts []string
	// GatewayHosts are the names and IPs the gateway's certificate is valid for.
	GatewayHosts []string
	// Validity is how long every certificate lasts.
	Validity time.Duration
}

// Generate writes the CA and the bundles into dir, creating it if needed.
func Generate(dir string, opts Options) error {
	if len(opts.WorkerHosts) == 0 || len(opts.GatewayHosts) == 0 {
		return fmt.Errorf("pki: worker and gateway hosts are required")
	}
	if opts.Validity <= 0 {
		opts.Validity = 30 * 24 * time.Hour
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	notBefore := time.Now().Add(-time.Minute)
	notAfter := notBefore.Add(opts.Validity)

	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return err
	}
	caTemplate := &x509.Certificate{
		SerialNumber:          serial(),
		Subject:               pkix.Name{CommonName: "atunnel-bench CA"},
		NotBefore:             notBefore,
		NotAfter:              notAfter,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		return err
	}
	ca, err := x509.ParseCertificate(caDER)
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, CAFile), PEMCertificate(caDER), 0o644); err != nil {
		return err
	}
	caKeyDER, err := x509.MarshalPKCS8PrivateKey(caKey)
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, CAKeyFile), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: caKeyDER}), 0o600); err != nil {
		return err
	}

	worker := node
	worker.Namespace, worker.ServiceAccountName, worker.ServiceAccountUID = "bench", "worker", "00000000-0000-4000-8000-000000000002"
	worker.PodName, worker.PodUID = "worker-0", "00000000-0000-4000-8000-000000000003"
	atelet := node
	atelet.Namespace, atelet.ServiceAccountName, atelet.ServiceAccountUID = installdefaults.SystemNamespace, installdefaults.AteletServiceAccount, "00000000-0000-4000-8000-000000000004"
	atelet.PodName, atelet.PodUID = "atelet-0", "00000000-0000-4000-8000-000000000005"

	leaves := []struct {
		file  string
		name  string
		usage []x509.ExtKeyUsage
		hosts []string
		uri   string
		pod   *substratex509.PodIdentity
	}{
		{WorkerBundle, "worker", []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}, opts.WorkerHosts, "", &worker},
		{AteletBundle, "atelet", []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, nil, installdefaults.AteletSPIFFEID(installdefaults.SystemNamespace), &atelet},
		{RouterBundle, "router", []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}, nil, installdefaults.RouterSPIFFEID(installdefaults.SystemNamespace), nil},
		{GatewayBundle, "egress-gateway", []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, opts.GatewayHosts, "", nil},
	}
	for _, l := range leaves {
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			return err
		}
		template := &x509.Certificate{
			SerialNumber: serial(),
			Subject:      pkix.Name{CommonName: l.name},
			NotBefore:    notBefore,
			NotAfter:     notAfter,
			KeyUsage:     x509.KeyUsageDigitalSignature,
			ExtKeyUsage:  l.usage,
		}
		for _, h := range l.hosts {
			if ip := net.ParseIP(h); ip != nil {
				template.IPAddresses = append(template.IPAddresses, ip)
			} else {
				template.DNSNames = append(template.DNSNames, h)
			}
		}
		if l.uri != "" {
			u, err := url.Parse(l.uri)
			if err != nil {
				return err
			}
			template.URIs = []*url.URL{u}
		}
		if l.pod != nil {
			if err := substratex509.AddPodIdentityToCertificate(l.pod, template); err != nil {
				return err
			}
		}
		der, err := x509.CreateCertificate(rand.Reader, template, ca, &key.PublicKey, caKey)
		if err != nil {
			return err
		}
		keyDER, err := x509.MarshalPKCS8PrivateKey(key)
		if err != nil {
			return err
		}
		// Leaf, then CA, then key: the layout of a Kubernetes Pod Certificate
		// credential bundle.
		bundle := append(PEMCertificate(der), PEMCertificate(caDER)...)
		bundle = append(bundle, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})...)
		if err := os.WriteFile(filepath.Join(dir, l.file), bundle, 0o644); err != nil {
			return err
		}
	}
	return nil
}

// LoadCA reads the CA certificate and key Generate wrote.
func LoadCA(dir string) (*x509.Certificate, crypto.Signer, error) {
	certPEM, err := os.ReadFile(filepath.Join(dir, CAFile))
	if err != nil {
		return nil, nil, err
	}
	block, _ := pem.Decode(certPEM)
	if block == nil {
		return nil, nil, fmt.Errorf("pki: %s holds no certificate", CAFile)
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return nil, nil, err
	}
	keyPEM, err := os.ReadFile(filepath.Join(dir, CAKeyFile))
	if err != nil {
		return nil, nil, err
	}
	block, _ = pem.Decode(keyPEM)
	if block == nil {
		return nil, nil, fmt.Errorf("pki: %s holds no key", CAKeyFile)
	}
	key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, nil, err
	}
	signer, ok := key.(crypto.Signer)
	if !ok {
		return nil, nil, fmt.Errorf("pki: CA key cannot sign")
	}
	return cert, signer, nil
}

// PEMCertificate encodes a DER certificate as PEM.
func PEMCertificate(der []byte) []byte {
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

// Serial returns a random certificate serial number.
func Serial() *big.Int { return serial() }

func serial() *big.Int {
	n, _ := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 62))
	return n
}
