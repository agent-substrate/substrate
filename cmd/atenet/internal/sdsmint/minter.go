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

package sdsmint

import (
	"bytes"
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"time"

	"github.com/agent-substrate/substrate/internal/localca"
)

// errHostNotAllowed is returned when a requested hostname will not be minted.
var errHostNotAllowed = errors.New("host not allowed")

// minter returns a leaf certificate for a hostname.
type minter struct {
	ttl time.Duration

	pool localca.Pool

	leafKey    crypto.Signer
	leafKeyPEM []byte
}

// minterOptions configures newMinter.
type minterOptions struct {
	// TTL is the leaf lifetime.
	TTL time.Duration
}

// defaultTTL for leaf cert lifetime.
const defaultTTL = 15 * time.Minute

// newMinter builds a minter over signer.
func newMinter(pool localca.Pool, opts minterOptions) (*minter, error) {
	if opts.TTL <= 0 {
		opts.TTL = defaultTTL
	}

	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generating leaf key: %w", err)
	}
	leafKeyDER, err := x509.MarshalPKCS8PrivateKey(leafKey)
	if err != nil {
		return nil, fmt.Errorf("marshalling leaf key: %w", err)
	}
	leafKeyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: leafKeyDER})

	return &minter{
		ttl:        opts.TTL,
		pool:       pool,
		leafKey:    leafKey,
		leafKeyPEM: leafKeyPEM,
	}, nil
}

// MintedCert is a freshly issued leaf plus its private key, in the PEM form
// Envoy's Secret proto expects.
type MintedCert struct {
	CertChainPEM  []byte // leaf, followed by any intermediates in leaf-to-root order.
	PrivateKeyPEM []byte // leaf private key
	NotAfter      time.Time
	Serial        string // hex, for the issuance audit log
}

// certificate mints a leaf for host. It returns an error wrapping
// errHostNotAllowed if host is not a name this will mint for.
func (m *minter) certificate(ctx context.Context, host string) (*MintedCert, error) {
	if err := checkHostSyntax(host); err != nil {
		slog.WarnContext(ctx, "certificate request denied",
			slog.String("host", host),
			slog.String("reason", err.Error()),
		)
		// checkHostSyntax already quotes the host, so this does not repeat it.
		return nil, fmt.Errorf("%w: %w", errHostNotAllowed, err)
	}

	now := time.Now()

	tmpl := &x509.Certificate{
		NotBefore:             now.Add(-5 * time.Minute),
		NotAfter:              now.Add(m.ttl),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  false,
	}

	// A literal IP in the SNI position has to land in IPAddresses, not
	// DNSNames, or clients will reject the leaf.
	// Envoy will hand us whatever was in the SNI, and although SNI is
	// not supposed to carry IP literals, some clients send them anyway.
	if ip := net.ParseIP(host); ip != nil {
		tmpl.IPAddresses = append(tmpl.IPAddresses, ip)
	} else {
		tmpl.DNSNames = []string{host}
	}

	derChain, err := m.pool.CreateCertificate(tmpl, m.leafKey.Public())
	if err != nil {
		return nil, fmt.Errorf("while signing certificate: %w", err)
	}

	var pemChain bytes.Buffer
	for _, der := range derChain {
		pemChain.Write(pem.EncodeToMemory(&pem.Block{
			Type:  "CERTIFICATE",
			Bytes: der,
		}))
	}

	leafCert, err := x509.ParseCertificate(derChain[0])
	if err != nil {
		return nil, fmt.Errorf("while parsing issued leaf: %w", err)
	}

	slog.InfoContext(ctx, "certificate issued",
		slog.String("host", host),
		slog.String("serial", leafCert.SerialNumber.Text(16)),
		slog.Time("not_after", leafCert.NotAfter),
	)

	return &MintedCert{
		CertChainPEM:  pemChain.Bytes(),
		PrivateKeyPEM: m.leafKeyPEM,
		NotAfter:      leafCert.NotAfter,
		Serial:        leafCert.SerialNumber.Text(16),
	}, nil
}

// checkHostSyntax checks whether host is a valid DNS name or IP address.
func checkHostSyntax(host string) error {
	if isValidDNSName(host) || isValidIPAddress(host) {
		return nil
	}
	return fmt.Errorf("invalid host name %q", host)
}

// isValidDNSName reports whether host is a syntactically valid DNS name.
func isValidDNSName(host string) bool {
	// A trailing dot names the root explicitly. It is legal in a DNS name and
	// not in SNI, but Envoy passes on whatever it was given, so it is dropped
	// here rather than making the final label look empty.
	host = strings.TrimSuffix(host, ".")
	if host == "" || len(host) > 253 {
		return false
	}
	for _, label := range strings.Split(host, ".") {
		if !isValidDNSLabel(label) {
			return false
		}
	}
	return true
}

// isValidDNSLabel reports whether one dot-separated component is a legal label:
// letters, digits, and interior hyphens, up to 63 bytes.
// https://datatracker.ietf.org/doc/html/rfc1035
func isValidDNSLabel(label string) bool {
	if label == "" || len(label) > 63 {
		return false
	}
	for i := 0; i < len(label); i++ {
		switch c := label[i]; {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		// A hyphen may not open or close a label.
		case c == '-' && i > 0 && i < len(label)-1:
		// Not legal in a hostname, but real names carry it -- service records
		// and a fair number of internal names -- and it smuggles nothing into a
		// certificate. Refusing it would fail handshakes for no gain.
		case c == '_':
		default:
			return false
		}
	}
	return true
}

// isValidIPAddress reports whether host is an IP literal, v4 or v6. SNI is not
// supposed to carry an IP addresss, but some clients send it anyway.
func isValidIPAddress(host string) bool {
	return net.ParseIP(host) != nil
}
