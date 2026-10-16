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
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestHealthzAndResolvConf(t *testing.T) {
	tmp := t.TempDir()
	resolvPath := filepath.Join(tmp, "resolv.conf")
	const rawResolv = "nameserver 169.254.17.1\nsearch default.svc.cluster.local corp.example.internal\noptions ndots:5\n"
	const wantResolv = "nameserver 169.254.17.1\nsearch default.svc.cluster.local\noptions ndots:2 timeout:1 attempts:2\n"
	if err := os.WriteFile(resolvPath, []byte(rawResolv), 0o644); err != nil {
		t.Fatal(err)
	}

	srv := newServer(tmp, resolvPath)
	ts := httptest.NewServer(srv.routes())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("/healthz status = %d, want 200", resp.StatusCode)
	}

	resp, err = http.Get(ts.URL + "/resolvconf")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var got map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if got["content"] != wantResolv {
		t.Errorf("/resolvconf content = %q, want %q", got["content"], wantResolv)
	}
}

func TestHandleProbeExecutesScriptAndRestoresResolvConf(t *testing.T) {
	tmp := t.TempDir()
	resolvPath := filepath.Join(tmp, "resolv.conf")
	const origResolv = "nameserver 169.254.17.1\noptions ndots:2 timeout:1 attempts:2\n"
	if err := os.WriteFile(resolvPath, []byte(origResolv), 0o644); err != nil {
		t.Fatal(err)
	}

	probeScript := filepath.Join(tmp, "fake-runtime")
	scriptBody := `#!/bin/sh
echo '{"attempts":[{"success":true,"addresses":["127.0.0.1"],"durationMs":2}]}'
`
	if err := os.WriteFile(probeScript, []byte(scriptBody), 0o755); err != nil {
		t.Fatal(err)
	}

	srv := newServer(tmp, resolvPath)
	ts := httptest.NewServer(srv.routes())
	defer ts.Close()

	reqBody, err := json.Marshal(ProbeRequest{
		Runtime: "fake-runtime",
		Op:      "resolve",
		Host:    "example.com",
	})
	if err != nil {
		t.Fatal(err)
	}

	resp, err := http.Post(ts.URL+"/probe", "application/json", bytes.NewReader(reqBody))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var got ProbeResponse
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if got.Error != "" {
		t.Fatalf("unexpected probe error: %s", got.Error)
	}
	if got.Runtime != "fake-runtime" || len(got.Attempts) != 1 || !got.Attempts[0].Success {
		t.Errorf("unexpected ProbeResponse: %+v", got)
	}

	afterResolv, err := os.ReadFile(resolvPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(afterResolv) != origResolv {
		t.Errorf("resolv.conf after probe = %q, want %q", string(afterResolv), origResolv)
	}
}

func TestMockDNSHarnessUDPAndTCP(t *testing.T) {
	// Bind ephemeral ports on 127.0.0.1 to test without CAP_NET_BIND_SERVICE.
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr1 := pc.LocalAddr().String()
	pc.Close()

	pc2, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr2 := pc2.LocalAddr().String()
	pc2.Close()

	harness, err := startMockDNSHarness([]MockServerSpec{
		{Addr: addr1, RCode: "REFUSED"},
		{Addr: addr2, RCode: "NOERROR", AnswerIP: "127.0.0.42"},
	})
	if err != nil {
		t.Skipf("unable to bind matching TCP/UDP port in test environment: %v", err)
	}
	defer harness.Close()

	// Resolve against server 1 (expect REFUSED / server misbehaving).
	r1 := &net.Resolver{
		PreferGo: true,
		Dial: func(ctx context.Context, network, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, network, addr1)
		},
	}
	if _, err := r1.LookupHost(context.Background(), "blocked.test.local"); err == nil {
		t.Errorf("LookupHost against REFUSED server succeeded, want error")
	} else if !strings.Contains(err.Error(), "server misbehaving") {
		t.Errorf("LookupHost error = %v, want 'server misbehaving'", err)
	}

	// Resolve against server 2 over TCP (expect 127.0.0.42).
	r2 := &net.Resolver{
		PreferGo: true,
		Dial: func(ctx context.Context, _, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "tcp", addr2)
		},
	}
	ips, err := r2.LookupIPAddr(context.Background(), "allowed.test.local")
	if err != nil {
		t.Fatalf("LookupIPAddr against NOERROR server failed: %v", err)
	}
	if len(ips) == 0 || ips[0].IP.String() != "127.0.0.42" {
		t.Errorf("LookupIPAddr = %v, want [127.0.0.42]", ips)
	}

	queries := harness.Queries()
	if len(queries) < 2 {
		t.Fatalf("recorded %d queries, want at least 2", len(queries))
	}
}
