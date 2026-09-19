// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"os"
	"testing"
	"time"
)

func TestEgressProbeConnect(t *testing.T) {
	caKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test CA"}, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	caDER, err := x509.CreateCertificate(rand.Reader, ca, ca, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	ca, err = x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatal(err)
	}
	serverTLS, _ := testTLSCredential(t, ca, caKey, false)
	clientTLS, clientCert := testTLSCredential(t, ca, caKey, true)
	rootsPath := writePEMFile(t, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}))
	bundlePath := writeBundleFile(t, clientTLS, clientCert)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	tlsListener := tls.NewListener(listener, &tls.Config{Certificates: []tls.Certificate{serverTLS}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: certPool(ca)})
	defer tlsListener.Close()
	received := make(chan *connectRequest, 4)
	heldReady, heldRelease := make(chan struct{}), make(chan struct{})
	stallReady, stallRelease := make(chan struct{}), make(chan struct{})
	go func() {
		for {
			conn, err := tlsListener.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				tlsConn := conn.(*tls.Conn)
				if err := tlsConn.Handshake(); err != nil {
					return
				}
				req, err := http.ReadRequest(bufio.NewReader(tlsConn))
				if err != nil {
					return
				}
				received <- &connectRequest{method: req.Method, host: req.Host, xfcc: req.Header.Get("X-Forwarded-Client-Cert"), serial: tlsConn.ConnectionState().PeerCertificates[0].SerialNumber.String()}
				status := 200
				if req.Header.Get("X-Forwarded-Client-Cert") != "" {
					status = 403
				}
				if req.Host == "192.0.2.45:443" {
					close(stallReady)
					<-stallRelease
					return
				}
				if req.Host == "192.0.2.46:443" {
					fmt.Fprintf(tlsConn, "HTTP/1.1 200\r\n\r\n")
					close(heldReady)
					<-heldRelease
					return
				}
				fmt.Fprintf(tlsConn, "HTTP/1.1 %d\r\nContent-Length: 0\r\n\r\n", status)
			}()
		}
	}()
	cfg := probeConfig{gatewayAddress: listener.Addr().String(), trustBundlePath: rootsPath, handshakeTimeout: 5 * time.Second}
	for _, tc := range []struct {
		name, xfcc string
		wantStatus int
		wantStage  string
	}{{"success", "", 200, ""}, {"forbidden", `Chain="hostile"`, 403, stageConnect}} {
		t.Run(tc.name, func(t *testing.T) {
			status, stage, err := connect(t.Context(), "192.0.2.44:443", bundlePath, tc.xfcc, cfg)
			if err != nil || stage != tc.wantStage || status != tc.wantStatus {
				t.Fatalf("connect() = (%d, %q, %v), want (%d, %q, nil)", status, stage, err, tc.wantStatus, tc.wantStage)
			}
			select {
			case got := <-received:
				if got.method != "CONNECT" || got.host != "192.0.2.44:443" || got.xfcc != tc.xfcc {
					t.Fatalf("request = %+v", got)
				}
				if got.serial != clientCert.SerialNumber.String() {
					t.Fatalf("client serial = %s, want %s", got.serial, clientCert.SerialNumber)
				}
			case <-time.After(time.Second):
				t.Fatal("server did not receive CONNECT")
			}
		})
	}
	t.Run("stalled response is bounded", func(t *testing.T) {
		defer close(stallRelease)
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
		defer cancel()
		result := make(chan struct {
			status int
			stage  string
			err    error
		}, 1)
		go func() {
			status, stage, err := connect(ctx, "192.0.2.45:443", bundlePath, "", cfg)
			result <- struct {
				status int
				stage  string
				err    error
			}{status, stage, err}
		}()
		select {
		case <-stallReady:
		case <-time.After(time.Second):
			t.Fatal("server did not receive stalled CONNECT")
		}
		var out struct {
			status int
			stage  string
			err    error
		}
		select {
		case out = <-result:
		case <-time.After(3 * time.Second):
			t.Fatal("stalled CONNECT did not respect deadline")
		}
		status, stage, err := out.status, out.stage, out.err
		var netErr net.Error
		if err == nil || !errors.As(err, &netErr) || !netErr.Timeout() || status != 0 || stage != stageTunnel {
			t.Fatalf("connect() = (%d, %q, %v), want bounded tunnel failure", status, stage, err)
		}
	})
	t.Run("held-open success returns before release", func(t *testing.T) {
		defer close(heldRelease)
		result := make(chan struct {
			status int
			stage  string
			err    error
		}, 1)
		go func() {
			status, stage, err := connect(t.Context(), "192.0.2.46:443", bundlePath, "", cfg)
			result <- struct {
				status int
				stage  string
				err    error
			}{status, stage, err}
		}()
		select {
		case <-heldReady:
		case <-time.After(time.Second):
			t.Fatal("server did not receive held CONNECT")
		}
		select {
		case out := <-result:
			if out.err != nil || out.status != 200 || out.stage != "" {
				t.Fatalf("held CONNECT = %+v", out)
			}
		case <-time.After(time.Second):
			t.Fatal("CONNECT waited for held tunnel release")
		}
	})
}

type connectRequest struct{ method, host, xfcc, serial string }

func testTLSCredential(t *testing.T, ca *x509.Certificate, caKey *ecdsa.PrivateKey, client bool) (tls.Certificate, *x509.Certificate) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	ext := x509.ExtKeyUsageServerAuth
	if client {
		ext = x509.ExtKeyUsageClientAuth
	}
	cert := &x509.Certificate{SerialNumber: newSerial(), Subject: pkix.Name{CommonName: "fixture"}, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{ext}}
	if !client {
		cert.DNSNames = []string{"localhost"}
		cert.IPAddresses = []net.IP{net.ParseIP("127.0.0.1")}
	}
	der, err := x509.CreateCertificate(rand.Reader, cert, ca, &key.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}, parsed
}

func certPool(ca *x509.Certificate) *x509.CertPool { p := x509.NewCertPool(); p.AddCert(ca); return p }
func writePEMFile(t *testing.T, data []byte) string {
	t.Helper()
	p := t.TempDir() + "/trust.pem"
	if err := os.WriteFile(p, data, 0600); err != nil {
		t.Fatal(err)
	}
	return p
}
func writeBundleFile(t *testing.T, cert tls.Certificate, parsed *x509.Certificate) string {
	t.Helper()
	keyDER, _ := x509.MarshalPKCS8PrivateKey(cert.PrivateKey)
	data := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	data = append(data, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: parsed.Raw})...)
	p := t.TempDir() + "/bundle.pem"
	if err := os.WriteFile(p, data, 0600); err != nil {
		t.Fatal(err)
	}
	return p
}

func newSerial() *big.Int { return big.NewInt(time.Now().UnixNano()) }
