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

package harness

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"net/url"
	"time"

	"go.ate.dev/substrate/internal/installdefaults"
	"go.ate.dev/substrate/internal/localca"
	"go.ate.dev/substrate/internal/substratex509"
)

// serverName is the name ate-api's serving certificate is issued for.
const serverName = "ateapi.harness.local"

// pki is the pod-identity CA of one test. It issues ate-api's serving
// certificate and the certificates atelets present to WorkerService.
type pki struct {
	ca   *localca.ConcretePool
	pool *x509.CertPool
}

func newPKI() (*pki, error) {
	ca, err := localca.GenerateCA("harness", localca.KeyTypeECDSAP256, 24*time.Hour)
	if err != nil {
		return nil, fmt.Errorf("generating the pod-identity CA: %w", err)
	}
	p := &pki{ca: &localca.ConcretePool{CAs: []*localca.CA{ca}, ActiveForSigning: ca.ID}, pool: x509.NewCertPool()}
	p.pool.AddCert(ca.RootCertificate)
	return p, nil
}

// issue signs a leaf from template with a fresh key.
func (p *pki) issue(template *x509.Certificate) (tls.Certificate, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("generating a leaf key: %w", err)
	}
	template.NotBefore = time.Now()
	template.NotAfter = time.Now().Add(24 * time.Hour)
	template.KeyUsage = x509.KeyUsageDigitalSignature
	chain, err := p.ca.CreateCertificate(template, &key.PublicKey)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("creating a leaf certificate: %w", err)
	}
	return tls.Certificate{Certificate: chain, PrivateKey: key}, nil
}

// servingCert is a serving certificate for name.
func (p *pki) servingCert(name string) (tls.Certificate, error) {
	return p.issue(&x509.Certificate{
		Subject:     pkix.Name{CommonName: name},
		DNSNames:    []string{name},
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	})
}

// serverTLS is ate-api's transport config. As in production, client
// certificates are optional: Control callers present none, atelets present
// their pod identity.
func (p *pki) serverTLS() (*tls.Config, error) {
	cert, err := p.servingCert(serverName)
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tls.VerifyClientCertIfGiven,
		ClientCAs:    p.pool,
	}, nil
}

// clientTLS is the transport config of a client of ate-api, presenting certs.
func (p *pki) clientTLS(certs ...tls.Certificate) *tls.Config {
	return &tls.Config{
		RootCAs:      p.pool,
		ServerName:   serverName,
		Certificates: certs,
	}
}

// clientCert is the certificate of a Control client authenticating as id, a
// SPIFFE ID.
func (p *pki) clientCert(id string) (tls.Certificate, error) {
	uri, err := url.Parse(id)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("parsing the client ID %q: %w", id, err)
	}
	return p.issue(&x509.Certificate{
		Subject:     pkix.Name{CommonName: uri.Path},
		URIs:        []*url.URL{uri},
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	})
}

// ateletCert is the pod-identity certificate of the atelet pod podName on
// nodeName, which WorkerService authenticates.
func (p *pki) ateletCert(podName, podUID, nodeName string) (tls.Certificate, error) {
	spiffeID, err := url.Parse(installdefaults.AteletSPIFFEID(installdefaults.SystemNamespace))
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("parsing the atelet SPIFFE ID: %w", err)
	}
	template := &x509.Certificate{
		Subject:     pkix.Name{CommonName: podName},
		URIs:        []*url.URL{spiffeID},
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	if err := substratex509.AddPodIdentityToCertificate(&substratex509.PodIdentity{
		Namespace:          installdefaults.SystemNamespace,
		ServiceAccountName: installdefaults.AteletServiceAccount,
		ServiceAccountUID:  "harness-atelet-sa",
		PodName:            podName,
		PodUID:             podUID,
		NodeName:           nodeName,
		NodeUID:            "harness-node-" + nodeName,
	}, template); err != nil {
		return tls.Certificate{}, err
	}
	return p.issue(template)
}
