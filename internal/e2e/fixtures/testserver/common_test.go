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
	"errors"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"go.ate.dev/substrate/internal/testcert"
)

type originCertificate struct {
	caPEM    []byte
	certFile string
	keyFile  string
}

type originServer struct {
	httpURL  string
	httpsURL string
	wsURL    string
	wssURL   string
}

func writeOriginCertificate(t *testing.T) originCertificate {
	t.Helper()
	material := testcert.NewServerTLS(t, net.ParseIP("127.0.0.1"))

	certFile := filepath.Join(t.TempDir(), "server.crt")
	keyFile := filepath.Join(t.TempDir(), "server.key")
	if err := os.WriteFile(certFile, material.Certificate, 0o600); err != nil {
		t.Fatalf("write server certificate: %v", err)
	}
	if err := os.WriteFile(keyFile, material.PrivateKey, 0o600); err != nil {
		t.Fatalf("write server key: %v", err)
	}
	return originCertificate{caPEM: material.RootCA, certFile: certFile, keyFile: keyFile}
}

// startOriginServer serves handler the way serveHTTP does: in cleartext on one
// loopback listener and, given a certificate, over TLS on a second. The URLs of
// a scheme the server does not serve are empty.
func startOriginServer(t *testing.T, handler http.Handler, certFile, keyFile string) originServer {
	t.Helper()
	server := &http.Server{Handler: handler}
	if err := configureTLSServer(server, certFile, keyFile); err != nil {
		t.Fatalf("configure TLS: %v", err)
	}
	cleartext := listenLoopback(t)
	var encrypted net.Listener
	if server.TLSConfig != nil {
		encrypted = listenLoopback(t)
	}
	serveErr := make(chan error, 1)
	go func() { serveErr <- serveListeners(server, cleartext, encrypted) }()
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

	origin := originServer{
		httpURL: "http://" + cleartext.Addr().String(),
		wsURL:   "ws://" + cleartext.Addr().String(),
	}
	if encrypted != nil {
		origin.httpsURL = "https://" + encrypted.Addr().String()
		origin.wssURL = "wss://" + encrypted.Addr().String()
	}
	return origin
}

func listenLoopback(t *testing.T) net.Listener {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen on loopback: %v", err)
	}
	return listener
}
