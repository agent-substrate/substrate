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

//go:build linux

package workerhost

import (
	"context"
	"crypto/tls"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"go.ate.dev/substrate/benchmarking/atunnel/internal/fakeatelet"
	"go.ate.dev/substrate/benchmarking/atunnel/internal/pki"
	"go.ate.dev/substrate/benchmarking/atunnel/internal/sink"
	"go.ate.dev/substrate/internal/atenet"
	"go.ate.dev/substrate/internal/atunnel"
	"go.ate.dev/substrate/internal/credbundle"
	"go.ate.dev/substrate/internal/egresspolicy"
	"go.ate.dev/substrate/internal/installdefaults"
	"go.ate.dev/substrate/internal/netns"
	"go.ate.dev/substrate/internal/nodepath"
	"go.ate.dev/substrate/internal/roottest"
)

// TestHostsActorsWithTheWorkersCode starts atunnel with two actors, then sends
// ingress to an ingress actor in a sandbox and egress out of it, through the
// sandbox's real network and redirect.
func TestHostsActorsWithTheWorkersCode(t *testing.T) {
	roottest.Require(t, "creates network namespaces and nftables rules")
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	dir := t.TempDir()
	if err := pki.Generate(dir, pki.Options{WorkerHosts: []string{"127.0.0.1"}, GatewayHosts: []string{"127.0.0.1"}}); err != nil {
		t.Fatal(err)
	}

	socket := filepath.Join(dir, "worker-support.sock")
	saved := nodepath.WorkerSupportSocket
	nodepath.WorkerSupportSocket = socket
	t.Cleanup(func() { nodepath.WorkerSupportSocket = saved })
	atelet, err := fakeatelet.New(dir)
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = atelet.Serve(ctx, dir, socket) }()

	gateway, err := sink.NewGateway(sink.GatewayConfig{
		CredentialBundle: filepath.Join(dir, pki.GatewayBundle),
		ClientCA:         filepath.Join(dir, pki.CAFile),
		BodyBytes:        16,
	})
	if err != nil {
		t.Fatal(err)
	}
	gatewayLis := listen(t)
	go func() { _ = gateway.Serve(ctx, gatewayLis) }()

	ingressAddr := freeAddress(t)
	host, err := Start(ctx, atunnel.Config{
		ListenAddress:        ingressAddr,
		ConnectListenAddress: freeAddress(t),
		CredentialBundle:     filepath.Join(dir, pki.WorkerBundle),
		TrustBundle:          filepath.Join(dir, pki.CAFile),
		ClientIdentity:       installdefaults.RouterSPIFFEID(installdefaults.SystemNamespace),
		BrokerIdentity:       installdefaults.AteletSPIFFEID(installdefaults.SystemNamespace),
		EgressListenAddress:  "0.0.0.0:15001",
		EgressTrustBundle:    filepath.Join(dir, pki.CAFile),
		DNSEgressPolicy:      egresspolicy.DNSEgressPolicyEnforceInAtunnel,
	}, 2, gatewayLis.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := host.Close(closeCtx); err != nil {
			t.Errorf("closing: %v", err)
		}
	})
	info := host.Info()
	if len(info.Sandboxes) != 2 || info.ActorPort != ActorPort {
		t.Fatalf("Info = %+v, want 2 sandboxes on port %d", info, ActorPort)
	}

	// The ingress actor: a server inside the first actor's sandbox.
	sandbox, err := netns.GetFromName(nodepath.ActorNetNSName(info.Sandboxes[0].UID))
	if err != nil {
		t.Fatal(err)
	}
	ingressActorLis, err := netns.Listen(ctx, sandbox, []uint16{ActorPort})
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		_ = http.Serve(ingressActorLis[0], http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte("ingress-actor"))
		}))
	}()

	// Ingress, as the router sends it.
	router, err := credbundle.Parse(filepath.Join(dir, pki.RouterBundle))
	if err != nil {
		t.Fatal(err)
	}
	roots, err := credbundle.ParsePool(filepath.Join(dir, pki.CAFile))
	if err != nil {
		t.Fatal(err)
	}
	ingress := &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{
		Certificates: []tls.Certificate{*router},
		RootCAs:      roots,
		ServerName:   "127.0.0.1",
	}}}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://"+ingressAddr+"/", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set(atenet.TargetActorHeader, info.Sandboxes[0].Actor)
	if got := get(t, ingress, req); got != "ingress-actor" {
		t.Errorf("ingress reached %q, want the ingress actor", got)
	}

	// Egress from inside the sandbox, redirected to atunnel and tunneled to
	// the gateway, which answers on port 80.
	egress := &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{DialContext: netns.Dialer(sandbox)}}
	req, err = http.NewRequestWithContext(ctx, http.MethodGet, "http://192.0.2.10:80/", nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := get(t, egress, req); len(got) != 16 {
		t.Errorf("egress got %q, want the gateway's 16-byte body", got)
	}
	if st := gateway.Stats(); st.Tunnels == 0 {
		t.Errorf("gateway saw no tunnels: %+v", st)
	}
}

func get(t *testing.T, client *http.Client, req *http.Request) string {
	t.Helper()
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("%s: HTTP %d: %s", req.URL, resp.StatusCode, body)
	}
	return string(body)
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

// freeAddress returns a loopback address with a port free a moment ago, for
// listeners atunnel.Start binds itself.
func freeAddress(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := lis.Addr().String()
	_ = lis.Close()
	return addr
}
