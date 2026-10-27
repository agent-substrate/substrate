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

package sink

import (
	"bufio"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"go.ate.dev/substrate/benchmarking/atunnel/internal/pki"
	"go.ate.dev/substrate/internal/atunnel/egress"
	"go.ate.dev/substrate/internal/credbundle"
)

func TestIngressActor(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	actor := NewIngressActor(5)
	lis := listen(t)
	go func() { _ = actor.Serve(ctx, lis) }()

	resp, err := http.Get("http://" + lis.Addr().String() + "/")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if string(body) != "abcde" {
		t.Errorf("body = %q, want abcde", body)
	}
	if st := actor.Stats(); st.Requests != 1 || st.ConnectionsSeen != 1 {
		t.Errorf("Stats = %+v, want 1 request on 1 connection", st)
	}
}

// TestGatewayThroughAtunnelsClient opens tunnels the way atunnel does, with
// its own egress client, which prefers HTTP/2.
func TestGatewayThroughAtunnelsClient(t *testing.T) {
	gateway, dir := startGateway(t)
	client := egressClient(t, gateway, dir)

	conn, err := client.DialContext(context.Background(), net.JoinHostPort("192.0.2.10", AnswerPort))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	for range 2 {
		if _, err := io.WriteString(conn, "GET / HTTP/1.1\r\nHost: x\r\n\r\n"); err != nil {
			t.Fatal(err)
		}
		reader := bufio.NewReader(conn)
		resp, err := http.ReadResponse(reader, nil)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != http.StatusOK || len(body) != 16 {
			t.Fatalf("answer = %d %q, want 200 and a 16-byte body", resp.StatusCode, body)
		}
	}

	discard, err := client.DialContext(context.Background(), net.JoinHostPort("192.0.2.10", DiscardPort))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := discard.Write(make([]byte, 1000)); err != nil {
		t.Fatal(err)
	}
	_ = discard.Close()

	waitFor(t, func() bool {
		st := gateway.g.Stats()
		return st.Requests == 2 && st.BytesIn >= 1000 && st.TunnelsOpen <= 1
	})
	if st := gateway.g.Stats(); st.Tunnels != 2 || st.TunnelsH2 != 2 {
		t.Errorf("Stats = %+v, want 2 tunnels, both HTTP/2", st)
	}
}

func TestGatewayOverHTTP1(t *testing.T) {
	gateway, dir := startGateway(t)
	conn, err := tls.Dial("tcp", gateway.addr, clientTLS(t, dir, "http/1.1"))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	fmt.Fprintf(conn, "CONNECT 192.0.2.10:%s HTTP/1.1\r\nHost: 192.0.2.10:%s\r\n\r\n", AnswerPort, AnswerPort)
	reader := bufio.NewReader(conn)
	resp, err := http.ReadResponse(reader, &http.Request{Method: http.MethodConnect})
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("CONNECT = %v, %v; want 200", resp, err)
	}
	if _, err := io.WriteString(conn, "GET / HTTP/1.1\r\nHost: x\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	inner, err := http.ReadResponse(reader, nil)
	if err != nil || inner.StatusCode != http.StatusOK {
		t.Fatalf("request in tunnel = %v, %v; want 200", inner, err)
	}
	if st := gateway.g.Stats(); st.Tunnels != 1 || st.TunnelsH2 != 0 {
		t.Errorf("Stats = %+v, want 1 HTTP/1.1 tunnel", st)
	}
}

func TestGatewayRejectsAnUnknownPort(t *testing.T) {
	gateway, dir := startGateway(t)
	client := egressClient(t, gateway, dir)
	if _, err := client.DialContext(context.Background(), "192.0.2.10:443"); err == nil {
		t.Fatal("tunnel to an unknown port opened")
	}
	if st := gateway.g.Stats(); st.Rejected != 1 {
		t.Errorf("Rejected = %d, want 1", st.Rejected)
	}
}

type runningGateway struct {
	g    *Gateway
	addr string
}

func startGateway(t *testing.T) (runningGateway, string) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	dir := t.TempDir()
	if err := pki.Generate(dir, pki.Options{WorkerHosts: []string{"127.0.0.1"}, GatewayHosts: []string{"127.0.0.1"}}); err != nil {
		t.Fatal(err)
	}
	g, err := NewGateway(GatewayConfig{
		CredentialBundle: filepath.Join(dir, pki.GatewayBundle),
		ClientCA:         filepath.Join(dir, pki.CAFile),
		BodyBytes:        16,
	})
	if err != nil {
		t.Fatal(err)
	}
	lis := listen(t)
	go func() { _ = g.Serve(ctx, lis) }()
	return runningGateway{g: g, addr: lis.Addr().String()}, dir
}

// egressClient is atunnel's egress client, presenting the router certificate,
// which the gateway, checking only the chain, accepts as an actor's.
func egressClient(t *testing.T, gateway runningGateway, dir string) *egress.Client {
	t.Helper()
	cert, err := credbundle.Parse(filepath.Join(dir, pki.RouterBundle))
	if err != nil {
		t.Fatal(err)
	}
	client, err := egress.NewClient(egress.ClientConfig{
		GatewayAddress:       gateway.addr,
		ServerName:           "127.0.0.1",
		GetClientCertificate: func(*tls.CertificateRequestInfo) (*tls.Certificate, error) { return cert, nil },
		TrustBundlePath:      filepath.Join(dir, pki.CAFile),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func clientTLS(t *testing.T, dir string, proto string) *tls.Config {
	t.Helper()
	cert, err := credbundle.Parse(filepath.Join(dir, pki.RouterBundle))
	if err != nil {
		t.Fatal(err)
	}
	roots, err := credbundle.ParsePool(filepath.Join(dir, pki.CAFile))
	if err != nil {
		t.Fatal(err)
	}
	return &tls.Config{Certificates: []tls.Certificate{*cert}, RootCAs: roots, ServerName: "127.0.0.1", NextProtos: []string{proto}}
}

func listen(t *testing.T) net.Listener {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lis.Close() })
	return lis
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met within 5s")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
