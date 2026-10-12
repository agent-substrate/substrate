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

	"github.com/agent-substrate/substrate/internal/testcert"
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
