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
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestHTTPHandlerDefaultsToEmptyHealthz(t *testing.T) {
	server := startOriginServer(t, newHTTPHandler(""), "", "")

	client := &http.Client{Timeout: 2 * time.Second}
	response, err := client.Get(server.httpURL + "/healthz")
	if err != nil {
		t.Fatalf("GET /healthz: %v", err)
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		t.Fatalf("GET /healthz status = %d, want %d", response.StatusCode, http.StatusOK)
	}
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("reading /healthz: %v", err)
	}
	if len(body) != 0 {
		t.Errorf("GET /healthz body = %q, want empty", body)
	}
}

func TestHTTPSTestResponseUsesHTTP11AndVerifiedIdentity(t *testing.T) {
	cert := writeOriginCertificate(t)
	server := startOriginServer(t, newHTTPHandler("fixture response"), cert.certFile, cert.keyFile)

	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(cert.caPEM) {
		t.Fatal("adding fixture CA to trust pool")
	}
	client := &http.Client{
		Timeout: 2 * time.Second,
		Transport: &http.Transport{TLSClientConfig: &tls.Config{
			RootCAs:    roots,
			ServerName: "127.0.0.1",
		}},
	}
	response, err := client.Get(server.httpsURL + "/healthz")
	if err != nil {
		t.Fatalf("GET /healthz over verified TLS: %v", err)
	}
	defer response.Body.Close()

	if response.Proto != "HTTP/1.1" {
		t.Errorf("HTTPS response protocol = %q, want HTTP/1.1", response.Proto)
	}
	if response.StatusCode != http.StatusOK {
		t.Errorf("HTTPS response status = %d, want %d", response.StatusCode, http.StatusOK)
	}
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("reading HTTPS response: %v", err)
	}
	if string(body) != "fixture response" {
		t.Errorf("HTTPS response body = %q, want %q", body, "fixture response")
	}
}

func TestHTTPSOriginStaysHTTP11WhenClientOffersHTTP2(t *testing.T) {
	cert := writeOriginCertificate(t)
	server := startOriginServer(t, newHTTPHandler("fixture response"), cert.certFile, cert.keyFile)

	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(cert.caPEM) {
		t.Fatal("adding fixture CA to trust pool")
	}
	protocols := new(http.Protocols)
	protocols.SetHTTP1(true)
	protocols.SetHTTP2(true)
	client := &http.Client{
		Timeout: 2 * time.Second,
		Transport: &http.Transport{
			Protocols: protocols,
			TLSClientConfig: &tls.Config{
				RootCAs:    roots,
				ServerName: "127.0.0.1",
			},
		},
	}
	response, err := client.Get(server.httpsURL + "/healthz")
	if err != nil {
		t.Fatalf("GET /healthz with an HTTP/2-capable client: %v", err)
	}
	defer response.Body.Close()

	if response.Proto != "HTTP/1.1" {
		t.Errorf("HTTPS response protocol with HTTP/2 offered = %q, want HTTP/1.1", response.Proto)
	}
}

func TestTLSConfigurationRejectsIncompleteOrUnreadableCredentials(t *testing.T) {
	for name, paths := range map[string][2]string{
		"missing key":     {"cert.pem", ""},
		"missing cert":    {"", "key.pem"},
		"unreadable pair": {"missing-cert.pem", "missing-key.pem"},
	} {
		t.Run(name, func(t *testing.T) {
			server := &http.Server{}
			if err := configureTLSServer(server, paths[0], paths[1]); err == nil {
				t.Fatal("configureTLSServer succeeded, want error")
			}
		})
	}
}

func TestWebsocketDefaultRespondsToPingWithPong(t *testing.T) {
	server := startOriginServer(t, newWebsocketHandler(false), "", "")
	dialer := websocket.Dialer{HandshakeTimeout: 2 * time.Second}
	conn, response, err := dialer.Dial(server.wsURL+"/ws", nil)
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	defer conn.Close()
	if response.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("websocket handshake status = %d, want %d", response.StatusCode, http.StatusSwitchingProtocols)
	}
	if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("set websocket read deadline: %v", err)
	}
	if err := conn.SetWriteDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("set websocket write deadline: %v", err)
	}

	if err := conn.WriteMessage(websocket.TextMessage, []byte("PING")); err != nil {
		t.Fatalf("write PING: %v", err)
	}
	messageType, message, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read PONG: %v", err)
	}
	if messageType != websocket.TextMessage || string(message) != "PONG" {
		t.Errorf("PING response = type %d, %q; want text PONG", messageType, message)
	}
}

func TestWebsocketEchoPreservesMessageTypeAndPayload(t *testing.T) {
	cert := writeOriginCertificate(t)
	server := startOriginServer(t, newWebsocketHandler(true), cert.certFile, cert.keyFile)
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(cert.caPEM) {
		t.Fatal("adding fixture CA to trust pool")
	}
	dialer := websocket.Dialer{
		HandshakeTimeout: 2 * time.Second,
		TLSClientConfig: &tls.Config{
			RootCAs:    roots,
			ServerName: "127.0.0.1",
		},
	}
	conn, response, err := dialer.Dial(server.wssURL+"/ws", nil)
	if err != nil {
		t.Fatalf("dial secure websocket: %v", err)
	}
	defer conn.Close()
	if response.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("secure websocket handshake status = %d, want %d", response.StatusCode, http.StatusSwitchingProtocols)
	}
	if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("set secure websocket read deadline: %v", err)
	}
	if err := conn.SetWriteDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("set secure websocket write deadline: %v", err)
	}

	messages := []struct {
		messageType int
		payload     []byte
	}{
		{websocket.TextMessage, []byte("first message")},
		{websocket.BinaryMessage, []byte{0, 1, 2, 255}},
		{websocket.TextMessage, []byte("PING")},
	}
	for i, want := range messages {
		if err := conn.WriteMessage(want.messageType, want.payload); err != nil {
			t.Fatalf("write message %d: %v", i, err)
		}
		gotType, gotPayload, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("read echo %d: %v", i, err)
		}
		if gotType != want.messageType {
			t.Errorf("echo %d message type = %d, want %d", i, gotType, want.messageType)
		}
		if string(gotPayload) != string(want.payload) {
			t.Errorf("echo %d payload = %q, want %q", i, gotPayload, want.payload)
		}
	}
}

type originCertificate struct {
	caPEM    []byte
	certFile string
	keyFile  string
}

func writeOriginCertificate(t *testing.T) originCertificate {
	t.Helper()

	caKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate CA key: %v", err)
	}
	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "testserver CA"},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create CA certificate: %v", err)
	}
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})

	serverKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate server key: %v", err)
	}
	serverTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "testserver origin"},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	serverDER, err := x509.CreateCertificate(rand.Reader, serverTemplate, caTemplate, &serverKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create server certificate: %v", err)
	}

	certFile := filepath.Join(t.TempDir(), "server.crt")
	keyFile := filepath.Join(t.TempDir(), "server.key")
	if err := os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: serverDER}), 0o600); err != nil {
		t.Fatalf("write server certificate: %v", err)
	}
	if err := os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(serverKey)}), 0o600); err != nil {
		t.Fatalf("write server key: %v", err)
	}
	return originCertificate{caPEM: caPEM, certFile: certFile, keyFile: keyFile}
}

type originServer struct {
	httpURL  string
	httpsURL string
	wsURL    string
	wssURL   string
}

func startOriginServer(t *testing.T, handler http.Handler, certFile, keyFile string) originServer {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen on loopback: %v", err)
	}
	server := &http.Server{Handler: handler}
	if certFile != "" || keyFile != "" {
		if err := configureTLSServer(server, certFile, keyFile); err != nil {
			t.Fatalf("configure TLS: %v", err)
		}
	}
	serveErr := make(chan error, 1)
	go func() {
		if certFile == "" && keyFile == "" {
			serveErr <- server.Serve(listener)
			return
		}
		serveErr <- server.ServeTLS(listener, "", "")
	}()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := server.Shutdown(ctx); err != nil {
			t.Errorf("shutdown origin: %v", err)
		}
		if err := <-serveErr; err != nil && !errors.Is(err, http.ErrServerClosed) {
			t.Errorf("serve origin: %v", err)
		}
	})

	address := listener.Addr().String()
	return originServer{
		httpURL:  "http://" + address,
		httpsURL: "https://" + address,
		wsURL:    "ws://" + address,
		wssURL:   "wss://" + address,
	}
}
