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

package target

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"go.ate.dev/substrate/cmd/benchmarking/egress/internal/contract"
	"go.ate.dev/substrate/cmd/benchmarking/egress/internal/targetcert"
)

func TestHandler(t *testing.T) {
	t.Parallel()
	h := newHandler(32)
	for _, tc := range []struct {
		path     string
		wantBody int
	}{
		{"/", 32},
		{"/anything", 32},
		{"/healthz", len("ok\n")},
	} {
		t.Run(tc.path, func(t *testing.T) {
			t.Parallel()
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, tc.path, nil))
			if rec.Code != http.StatusOK || rec.Body.Len() != tc.wantBody {
				t.Errorf("GET %s = %d with %d bytes, want 200 with %d", tc.path, rec.Code, rec.Body.Len(), tc.wantBody)
			}
		})
	}
}

func TestHandlerOverTLS(t *testing.T) {
	t.Parallel()
	bundle, err := targetcert.Generate(time.Now())
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	if err := bundle.Write(dir); err != nil {
		t.Fatal(err)
	}
	cfg, err := newTLSConfig(filepath.Join(dir, "tls.crt"), filepath.Join(dir, "tls.key"))
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewUnstartedServer(newHandler(32))
	srv.TLS = cfg
	srv.StartTLS()
	t.Cleanup(srv.Close)

	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(bundle.CA) {
		t.Fatal("bundle CA does not parse")
	}
	// Dial the test server whatever the URL names, so the name can be an
	// endpoint's while the TLS check runs for real.
	client := &http.Client{Transport: &http.Transport{
		TLSClientConfig: &tls.Config{RootCAs: roots},
		DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, network, srv.Listener.Addr().String())
		},
	}}

	resp, err := client.Get(contract.EndpointURL(42, contract.SchemeHTTPS))
	if err != nil {
		t.Fatalf("GET over TLS as an endpoint name: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || len(body) != 32 {
		t.Errorf("GET over TLS = %d with %d bytes, want 200 with 32", resp.StatusCode, len(body))
	}

	_, err = client.Get("https://example.com/")
	var hostErr x509.HostnameError
	if !errors.As(err, &hostErr) {
		t.Errorf("GET over TLS as another name = %v, want a hostname mismatch", err)
	}
}

func TestPortRange(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		in      string
		want    []int
		wantErr bool
	}{
		{"", nil, false},
		{"8080", []int{8080}, false},
		{"10000-10002", []int{10000, 10001, 10002}, false},
		{"10002-10000", nil, true},
		{"0-3", nil, true},
		{"65535-65536", nil, true},
		{"a-b", nil, true},
		{"10000-", nil, true},
	} {
		t.Run(tc.in, func(t *testing.T) {
			t.Parallel()
			got, err := portRange(tc.in)
			if (err != nil) != tc.wantErr || !slices.Equal(got, tc.want) {
				t.Errorf("portRange(%q) = %v, %v; want %v, error %v", tc.in, got, err, tc.want, tc.wantErr)
			}
		})
	}
}

func TestListenAddrs(t *testing.T) {
	t.Parallel()
	got, err := listenAddrs("127.0.0.1:0", "10000-10002")
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"127.0.0.1:0", ":10000", ":10001", ":10002"}; !slices.Equal(got, want) {
		t.Errorf("listenAddrs() = %v, want %v", got, want)
	}
	if _, err := listenAddrs("", "a-b"); err == nil {
		t.Error("listenAddrs accepted a bad port range")
	}
}

func TestListenAllClosesWhatItOpenedOnFailure(t *testing.T) {
	t.Parallel()
	held, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { held.Close() })
	if lns, err := listenAll([]string{"127.0.0.1:0", held.Addr().String()}); err == nil {
		for _, ln := range lns {
			ln.Close()
		}
		t.Fatal("listenAll bound a port that was taken")
	}
}
