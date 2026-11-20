// Copyright 2026 The Agent Substrate Authors
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

// Package actorauthztest provides a fake actor JWT issuer for tests of the
// actor checks. It is separate from objectstoreplugintest so that the atelet
// and ate-api-server tests that import that package don't pull in
// localjwtauthority.
package actorauthztest

import (
	"encoding/json"
	"encoding/pem"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"go.ate.dev/substrate/internal/localjwtauthority"
)

// ServeIssuer starts a TLS server that serves OIDC discovery and pool's JWKS.
// connState, if set, is the server's http.Server.ConnState.
func ServeIssuer(t *testing.T, pool *localjwtauthority.ConcretePool, connState func(net.Conn, http.ConnState)) *httptest.Server {
	t.Helper()
	keys, err := pool.VerificationKeys()
	if err != nil {
		t.Fatal(err)
	}
	jwks, err := localjwtauthority.JWKS(keys)
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	var srv *httptest.Server
	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{"issuer": srv.URL, "jwks_uri": srv.URL + "/jwks"})
	})
	mux.HandleFunc("/jwks", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write(jwks) })
	srv = httptest.NewUnstartedServer(mux)
	srv.Config.ConnState = connState
	srv.StartTLS()
	t.Cleanup(srv.Close)
	return srv
}

// WriteServerCA writes srv's certificate to a PEM file and returns its path.
func WriteServerCA(t *testing.T, srv *httptest.Server) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "issuer-ca.pem")
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw}), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// ConnCounter counts the connections a server opens and closes.
type ConnCounter struct {
	opened, closed atomic.Int32
	closedCh       chan struct{}
}

// NewConnCounter returns a ConnCounter with no connections counted.
func NewConnCounter() *ConnCounter {
	return &ConnCounter{closedCh: make(chan struct{}, 1)}
}

// ConnState is an http.Server.ConnState that counts connections.
func (c *ConnCounter) ConnState(_ net.Conn, s http.ConnState) {
	switch s {
	case http.StateNew:
		c.opened.Add(1)
	case http.StateClosed, http.StateHijacked:
		c.closed.Add(1)
		select {
		case c.closedCh <- struct{}{}:
		default:
		}
	}
}

// WaitAllClosed waits until at least one connection was opened and every
// opened one is closed.
func (c *ConnCounter) WaitAllClosed(t *testing.T) {
	t.Helper()
	timeout := time.After(10 * time.Second)
	for c.opened.Load() == 0 || c.closed.Load() < c.opened.Load() {
		select {
		case <-c.closedCh:
		case <-timeout:
			t.Fatalf("%d of %d issuer connections closed", c.closed.Load(), c.opened.Load())
		}
	}
}
