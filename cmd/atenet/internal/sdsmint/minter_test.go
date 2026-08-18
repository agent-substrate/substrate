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
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/cmd/atenet/internal/sdsmint/certauth"
	"github.com/agent-substrate/substrate/internal/localca"
)

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// testSigner builds a signer over a throwaway CA.
func testSigner(t *testing.T) *certauth.Signer {
	t.Helper()
	ca, err := localca.GenerateCA(localca.GenerateOptions{
		ID:         "mitm",
		CommonName: "sdsmint test CA",
		KeyType:    localca.KeyTypeECDSAP256,
		Lifetime:   time.Hour,
	})
	if err != nil {
		t.Fatalf("generating test CA: %v", err)
	}
	signer, err := certauth.New(&localca.Pool{CAs: []*localca.CA{ca}}, "")
	if err != nil {
		t.Fatalf("certauth.New: %v", err)
	}
	return signer
}

func testMinter(t *testing.T, opts minterOptions) *minter {
	t.Helper()
	if opts.Logger == nil {
		opts.Logger = quietLogger()
	}
	m, err := newMinter(testSigner(t), opts)
	if err != nil {
		t.Fatalf("newMinter: %v", err)
	}
	return m
}

// TestMinterMintsPerCall pins that the minter holds nothing between calls. It
// used to cache by host, and the SDS layer is written on the assumption that
// it no longer does: pack reads cert.Serial as the resource version, so two
// mints of one name have to look like two versions to Envoy.
func TestMinterMintsPerCall(t *testing.T) {
	m := testMinter(t, minterOptions{TTL: time.Minute})
	ctx := context.Background()

	first, err := m.certificate(ctx, "a.example")
	if err != nil {
		t.Fatalf("first certificate: %v", err)
	}
	second, err := m.certificate(ctx, "a.example")
	if err != nil {
		t.Fatalf("second certificate: %v", err)
	}
	if first.Serial == second.Serial {
		t.Errorf("both calls returned serial %s; the minter is holding onto leaves", first.Serial)
	}

	other, err := m.certificate(ctx, "b.example")
	if err != nil {
		t.Fatalf("certificate for a different host: %v", err)
	}
	if other.Serial == first.Serial {
		t.Error("different hosts were served the same certificate")
	}
}

// TestMinterRefusesNonHostnames pins the one thing that can still make the
// minter say no. There is no destination allowlist any more, so the SDS
// server's withdraw path is only ever reached through checkHostSyntax, and it
// is reached through a wrapped errHostNotAllowed.
func TestMinterRefusesNonHostnames(t *testing.T) {
	m := testMinter(t, minterOptions{TTL: time.Minute})
	ctx := context.Background()

	// Nothing about this name is on a list; it just is a name.
	if _, err := m.certificate(ctx, "anything.at.all.test"); err != nil {
		t.Fatalf("ordinary hostname was rejected: %v", err)
	}

	_, err := m.certificate(ctx, "*.evil.test")
	if err == nil {
		t.Fatal("a wildcard SNI was minted")
	}
	if !errors.Is(err, errHostNotAllowed) {
		t.Errorf("error = %v, want it to wrap errHostNotAllowed", err)
	}
}

func TestMinterIsConcurrencySafe(t *testing.T) {
	m := testMinter(t, minterOptions{TTL: time.Minute})
	ctx := context.Background()

	var wg sync.WaitGroup
	errs := make(chan error, 64)
	for i := range 64 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := m.certificate(ctx, fmt.Sprintf("h%d.example", i%16)); err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		t.Fatalf("concurrent GetCertificate: %v", err)
	}
}

func TestCheckHostSyntax(t *testing.T) {
	validate := checkHostSyntax

	allowed := []string{
		"example.com",
		"a.example.com",
		"a.b.c.d.example.com", // any depth, unlike a "*" label
		"EXAMPLE.COM",         // case is not a hostname's business
		"anything.test",       // any TLD
		"localhost",           // a single label is still a name
		"xn--80ak6aa92e.com",  // punycode
		"a.example.com.",      // trailing dot
		"my-host.example.com", // interior hyphen
		"_dmarc.example.com",  // underscore; not a hostname, but a real name

		strings.Repeat("a", 63) + ".example.com", // a label at the 63-byte limit

		// IP literals. SNI is not supposed to carry one, but clients send them,
		// and Sign puts them in IPAddresses rather than DNSNames.
		"192.0.2.1",
		"127.0.0.1",
		"2001:db8::1",
		"::1",
		"::ffff:192.0.2.1", // v4-mapped v6
	}
	for _, host := range allowed {
		if err := validate(host); err != nil {
			t.Errorf("validate(%q) = %v, want nil", host, err)
		}
	}

	denied := []string{
		"",                   // empty
		".",                  // the root alone is not a name to mint for
		"*.example.com",      // a wildcard in the SNI itself
		"a.example.com/../x", // path separator smuggling
		"..example.com",      // empty label
		".example.com",       // leading dot
		"a.example.com\nfoo", // embedded newline
		"-example.com",       // label opens with a hyphen
		"example-.com",       // label closes with a hyphen
		"[::1]",              // bracketed; Sign would read it as a DNS name
		"fe80::1%eth0",       // a zone is not part of an address in a SAN
		"192.0.2.1:8443",     // a port is not part of a host here

		strings.Repeat("a", 64) + ".example.com",  // label over 63 bytes
		strings.Repeat("a.", 200) + "example.com", // over 253 bytes
	}
	for _, host := range denied {
		if err := validate(host); err == nil {
			t.Errorf("validate(%q) = nil, want an error", host)
		}
	}
}
