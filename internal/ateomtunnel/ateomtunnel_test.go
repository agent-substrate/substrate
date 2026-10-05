//go:build linux

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

package ateomtunnel

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"flag"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/spf13/pflag"
	"go.ate.dev/substrate/internal/proto/ateompb"
	"go.ate.dev/substrate/internal/resources"
)

func TestRegisterFlags(t *testing.T) {
	args := []string{
		"--atunnel-listen-address=:1",
		"--atunnel-connect-listen-address=:2",
		"--atunnel-credential-bundle=cred",
		"--atunnel-trust-bundle=trust",
		"--atunnel-client-identity=client",
		"--atunnel-broker-identity=broker",
		"--atunnel-egress-listen-address=0.0.0.0:3",
		"--atunnel-egress-trust-bundle=egress-trust",
	}
	want := Config{
		ListenAddress:        ":1",
		ConnectListenAddress: ":2",
		CredentialBundle:     "cred",
		TrustBundle:          "trust",
		ClientIdentity:       "client",
		BrokerIdentity:       "broker",
		EgressListenAddress:  "0.0.0.0:3",
		EgressTrustBundle:    "egress-trust",
	}

	std := flag.NewFlagSet("std", flag.ContinueOnError)
	stdCfg := RegisterFlags(std)
	pf := pflag.NewFlagSet("pflag", pflag.ContinueOnError)
	pfCfg := RegisterFlags(pf)

	for name, tc := range map[string]struct {
		cfg   *Config
		parse func([]string) error
	}{
		"flag":  {stdCfg, std.Parse},
		"pflag": {pfCfg, pf.Parse},
	} {
		t.Run(name, func(t *testing.T) {
			if tc.cfg.ListenAddress != ":443" || tc.cfg.ConnectListenAddress != ":8443" || tc.cfg.EgressListenAddress != "0.0.0.0:15001" {
				t.Errorf("unexpected listen defaults: %+v", *tc.cfg)
			}
			if tc.cfg.CredentialBundle == "" || tc.cfg.TrustBundle == "" || tc.cfg.ClientIdentity == "" || tc.cfg.BrokerIdentity == "" || tc.cfg.EgressTrustBundle == "" {
				t.Errorf("unset default: %+v", *tc.cfg)
			}
			if err := tc.parse(args); err != nil {
				t.Fatal(err)
			}
			if *tc.cfg != want {
				t.Errorf("parsed config = %+v, want %+v", *tc.cfg, want)
			}
		})
	}
}

// testConfig returns a Config over fresh credentials and free loopback ports.
func testConfig(t *testing.T) Config {
	t.Helper()
	dir := t.TempDir()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test"},
		NotBefore:             now.Add(-time.Minute),
		NotAfter:              now.Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	bundle := filepath.Join(dir, "bundle.pem")
	trust := filepath.Join(dir, "trust.pem")
	if err := os.WriteFile(bundle, append(certPEM, keyPEM...), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(trust, certPEM, 0o600); err != nil {
		t.Fatal(err)
	}

	resolvConf := filepath.Join(dir, "resolv.conf")
	if err := os.WriteFile(resolvConf, []byte("nameserver 127.0.0.1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	old := resolvConfPath
	resolvConfPath = resolvConf
	t.Cleanup(func() { resolvConfPath = old })

	return Config{
		ListenAddress:        freeAddress(t),
		ConnectListenAddress: freeAddress(t),
		CredentialBundle:     bundle,
		TrustBundle:          trust,
		ClientIdentity:       "spiffe://cluster.local/ns/ate-system/sa/atenet-router",
		BrokerIdentity:       "spiffe://cluster.local/ns/ate-system/sa/atelet",
		EgressListenAddress:  "0.0.0.0:15001",
		EgressTrustBundle:    trust,
	}
}

// freeAddress returns a loopback address nothing is listening on.
func freeAddress(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer lis.Close()
	return lis.Addr().String()
}

func startTunnel(t *testing.T, cfg Config) *Tunnel {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	tunnel, err := Start(ctx, cfg, "http://169.254.17.2:80")
	if err != nil {
		t.Fatal(err)
	}
	return tunnel
}

func TestStart(t *testing.T) {
	cfg := testConfig(t)
	tunnel := startTunnel(t, cfg)

	if tunnel.EgressPort != 15001 {
		t.Errorf("EgressPort = %d, want 15001", tunnel.EgressPort)
	}
	if tunnel.Ingress == nil || tunnel.Egress == nil || tunnel.DNSRelay == nil {
		t.Errorf("tunnel is missing a component: %+v", tunnel)
	}
	for _, address := range []string{cfg.ListenAddress, cfg.ConnectListenAddress} {
		conn, err := net.DialTimeout("tcp", address, 5*time.Second)
		if err != nil {
			t.Fatalf("nothing is serving %s: %v", address, err)
		}
		conn.Close()
	}
}

func TestStartErrors(t *testing.T) {
	inUse, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer inUse.Close()

	for name, tc := range map[string]struct {
		edit    func(t *testing.T, cfg *Config)
		wantErr bool
	}{
		"no resolvers": {
			edit: func(t *testing.T, _ *Config) {
				empty := filepath.Join(t.TempDir(), "resolv.conf")
				if err := os.WriteFile(empty, nil, 0o600); err != nil {
					t.Fatal(err)
				}
				resolvConfPath = empty
			},
			wantErr: true,
		},
		"missing credentials": {
			edit:    func(t *testing.T, cfg *Config) { cfg.CredentialBundle = filepath.Join(t.TempDir(), "absent.pem") },
			wantErr: true,
		},
		"ingress address in use": {
			edit:    func(_ *testing.T, cfg *Config) { cfg.ListenAddress = inUse.Addr().String() },
			wantErr: true,
		},
		"CONNECT address in use": {
			edit:    func(_ *testing.T, cfg *Config) { cfg.ConnectListenAddress = inUse.Addr().String() },
			wantErr: true,
		},
		"egress address without a port": {
			edit:    func(_ *testing.T, cfg *Config) { cfg.EgressListenAddress = "0.0.0.0" },
			wantErr: true,
		},
	} {
		t.Run(name, func(t *testing.T) {
			cfg := testConfig(t)
			tc.edit(t, &cfg)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			_, err := Start(ctx, cfg, "http://169.254.17.2:80")
			if (err != nil) != tc.wantErr {
				t.Fatalf("Start error = %v, wantErr = %v", err, tc.wantErr)
			}
		})
	}

	t.Run("bad upstream", func(t *testing.T) {
		_, err := Start(context.Background(), testConfig(t), "http://[::1")
		if err == nil {
			t.Fatalf("Start error = %v, want an error", err)
		}
	})
}

func TestPrepareEgress(t *testing.T) {
	tunnel := &Tunnel{}
	actor := resources.ActorAttribution{Ref: resources.ActorRef{Atespace: "space", Name: "actor"}, UID: "uid"}

	got, err := tunnel.PrepareEgress(context.Background(), actor, nil)
	if err != nil || got != nil {
		t.Errorf("PrepareEgress(nil gateway) = %v, %v; want nil, nil", got, err)
	}
	for name, address := range map[string]string{
		"empty address":   "",
		"missing a port":  "gateway.example",
		"unbalanced port": "[::1",
	} {
		t.Run(name, func(t *testing.T) {
			got, err := tunnel.PrepareEgress(context.Background(), actor, &ateompb.EgressGateway{Address: address})
			if err == nil || got != nil {
				t.Errorf("PrepareEgress(%q) = %v, %v; want an error", address, got, err)
			}
		})
	}
}

func TestActivateAndDeactivate(t *testing.T) {
	tunnel := startTunnel(t, testConfig(t))
	ctx := context.Background()
	actor := resources.ActorAttribution{Ref: resources.ActorRef{Atespace: "space", Name: "actor"}, UID: "uid"}
	dial := func(context.Context, string, string) (net.Conn, error) { return nil, net.ErrClosed }

	if err := tunnel.Activate(actor, dial, nil); err != nil {
		t.Fatalf("Activate without egress: %v", err)
	}
	if err := tunnel.Deactivate(ctx, actor); err != nil {
		t.Fatalf("Deactivate: %v", err)
	}
	// Deactivating an actor that is not active is not an error.
	if err := tunnel.Deactivate(ctx, actor); err != nil {
		t.Fatalf("second Deactivate: %v", err)
	}

	err := tunnel.Activate(resources.ActorAttribution{UID: "uid"}, dial, nil)
	if err == nil || !strings.Contains(err.Error(), "while activating actor ingress") {
		t.Fatalf("Activate with no actor reference = %v, want an ingress error", err)
	}
}
