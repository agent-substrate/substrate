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
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"time"

	"github.com/agent-substrate/substrate/cmd/atenet/internal/sdsmint/certauth"
)

// errHostNotAllowed is returned when a requested hostname will not be minted.
var errHostNotAllowed = errors.New("host not allowed")

// minter returns a leaf certificate for a hostname.
type minter struct {
	signer *certauth.Signer
	ttl    time.Duration
	log    *slog.Logger
}

// minterOptions configures newMinter.
type minterOptions struct {
	// TTL is the leaf lifetime.
	TTL    time.Duration
	Logger *slog.Logger
}

// defaultTTL for leaf cert lifetime.
const defaultTTL = 15 * time.Minute

// newMinter builds a minter over signer.
func newMinter(signer *certauth.Signer, opts minterOptions) (*minter, error) {
	if signer == nil {
		return nil, errors.New("nil signer")
	}
	if opts.TTL <= 0 {
		opts.TTL = defaultTTL
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	return &minter{
		signer: signer,
		ttl:    opts.TTL,
		log:    opts.Logger,
	}, nil
}

// certificate mints a leaf for host. It returns an error wrapping
// errHostNotAllowed if host is not a name this will mint for.
func (m *minter) certificate(ctx context.Context, host string) (*certauth.MintedCert, error) {
	if err := checkHostSyntax(host); err != nil {
		m.log.WarnContext(ctx, "certificate request denied",
			slog.String("host", host),
			slog.String("reason", err.Error()),
		)
		// checkHostSyntax already quotes the host, so this does not repeat it.
		return nil, fmt.Errorf("%w: %w", errHostNotAllowed, err)
	}

	cert, err := m.signer.Sign(host, m.ttl)
	if err != nil {
		return nil, err
	}

	if m.log.Enabled(ctx, slog.LevelInfo) {
		m.log.InfoContext(ctx, "certificate issued",
			slog.String("host", host),
			slog.String("serial", cert.Serial),
			slog.Time("not_after", cert.NotAfter),
		)
	}
	return cert, nil
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
