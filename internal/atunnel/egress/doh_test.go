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

package egress

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"go.ate.dev/substrate/internal/atunnel/dns/protocol"
)

var (
	testDNSQuery    = []byte{0x00, 0x00, 0x01, 0x00, 0xfb, 0xff, 0x3e}
	testDNSResponse = []byte{0x00, 0x00, 0x81, 0x80, 0xff, 0x00}
)

// dnsGatewayHandler answers DNS GETs with testDNSResponse and tunnels CONNECT.
func dnsGatewayHandler(t *testing.T) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodConnect {
			w.WriteHeader(http.StatusOK)
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			_, _ = io.Copy(w, r.Body)
			return
		}
		if r.Method != http.MethodGet || r.URL.Path != protocol.DoHPath {
			t.Errorf("request = %s %s, want GET %s", r.Method, r.URL.Path, protocol.DoHPath)
		}
		if got := r.Header.Get("Accept"); got != protocol.DoHMediaType {
			t.Errorf("Accept = %q, want %s", got, protocol.DoHMediaType)
		}
		query, err := base64.RawURLEncoding.DecodeString(r.URL.Query().Get(protocol.DoHQueryParam))
		if err != nil || !bytes.Equal(query, testDNSQuery) {
			t.Errorf("dns parameter decodes to %x (err %v), want %x", query, err, testDNSQuery)
		}
		w.Header().Set("Content-Type", protocol.DoHMediaType)
		_, _ = w.Write(testDNSResponse)
	})
}

func TestClientExchangeDNS(t *testing.T) {
	ca := newTestCA(t)
	gatewayAddress, _ := serveTestH2ConnectGateway(t, ca, dnsGatewayHandler(t))
	client := newTestClient(t, ca, withDialer(dialFixedAddress(gatewayAddress)))

	got, err := client.ExchangeDNS(context.Background(), testDNSQuery)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, testDNSResponse) {
		t.Errorf("ExchangeDNS() = %x, want %x", got, testDNSResponse)
	}
}

func TestClientExchangeDNSRejectsBadResponses(t *testing.T) {
	tests := []struct {
		name    string
		handler http.HandlerFunc
		wantErr string
	}{
		{
			name: "status is not 200",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				http.Error(w, "egress denied", http.StatusForbidden)
			},
			wantErr: "403 Forbidden: egress denied",
		},
		{
			name: "content type is not a DNS message",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "text/plain")
				_, _ = w.Write(testDNSResponse)
			},
			wantErr: "content type",
		},
		{
			name: "body is larger than a DNS message",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", protocol.DoHMediaType)
				_, _ = w.Write(make([]byte, maxDNSMessageSize+1))
			},
			wantErr: "larger than",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ca := newTestCA(t)
			gatewayAddress, _ := serveTestH2ConnectGateway(t, ca, tc.handler)
			client := newTestClient(t, ca, withDialer(dialFixedAddress(gatewayAddress)))

			_, err := client.ExchangeDNS(context.Background(), testDNSQuery)
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("ExchangeDNS() error = %v, want it to contain %q", err, tc.wantErr)
			}
		})
	}
}

func TestClientExchangeDNSHonorsContext(t *testing.T) {
	ca := newTestCA(t)
	gatewayAddress, _ := serveTestH2ConnectGateway(t, ca, http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	client := newTestClient(t, ca, withDialer(dialFixedAddress(gatewayAddress)))

	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() {
		_, err := client.ExchangeDNS(ctx, testDNSQuery)
		errc <- err
	}()
	cancel()
	if err := <-errc; !errors.Is(err, context.Canceled) {
		t.Errorf("ExchangeDNS() error = %v, want context.Canceled", err)
	}
}

// A DNS query shares the pooled connection with CONNECT tunnels and dials
// again once the gateway closes it.
func TestClientExchangeDNSSharesAndReplacesConnection(t *testing.T) {
	ca := newTestCA(t)
	gatewayAddress, closeActiveConns := serveTestH2ConnectGateway(t, ca, dnsGatewayHandler(t))
	var tcpDials atomic.Int32
	client := newTestClient(t, ca, withDialer(func(ctx context.Context, network, _ string) (net.Conn, error) {
		tcpDials.Add(1)
		return (&net.Dialer{}).DialContext(ctx, network, gatewayAddress)
	}))

	tunnel, err := client.DialContext(context.Background(), "192.0.2.10:443")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.ExchangeDNS(context.Background(), testDNSQuery); err != nil {
		t.Fatalf("ExchangeDNS() beside an open tunnel: %v", err)
	}
	if dials := tcpDials.Load(); dials != 1 {
		t.Errorf("tcpDials = %d with a tunnel open, want 1", dials)
	}
	_ = tunnel.Close()

	closeActiveConns()

	if _, err := client.ExchangeDNS(context.Background(), testDNSQuery); err != nil {
		t.Fatalf("ExchangeDNS() after the gateway closed the connection: %v", err)
	}
	if dials := tcpDials.Load(); dials != 2 {
		t.Errorf("tcpDials = %d after reconnect, want 2", dials)
	}
}

func TestClientExchangeDNSHTTP1(t *testing.T) {
	ca := newTestCA(t)
	gatewayAddress := serveTestConnectGateway(t, ca, func(conn net.Conn, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != protocol.DoHPath {
			t.Errorf("request = %s %s, want GET %s", r.Method, r.URL.Path, protocol.DoHPath)
		}
		resp := &http.Response{
			StatusCode:    http.StatusOK,
			ProtoMajor:    1,
			ProtoMinor:    1,
			Header:        http.Header{"Content-Type": {protocol.DoHMediaType}},
			Body:          io.NopCloser(bytes.NewReader(testDNSResponse)),
			ContentLength: int64(len(testDNSResponse)),
		}
		if err := resp.Write(conn); err != nil {
			t.Errorf("writing DNS response: %v", err)
		}
	})
	client := newTestClient(t, ca, withDialer(dialFixedAddress(gatewayAddress)))

	got, err := client.ExchangeDNS(context.Background(), testDNSQuery)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, testDNSResponse) {
		t.Errorf("ExchangeDNS() = %x, want %x", got, testDNSResponse)
	}
}

// dnsDialer is a tunnelDialer that only exchanges DNS.
type dnsDialer struct {
	tunnelDialerFunc
	exchange func(context.Context, []byte) ([]byte, error)
}

func (d dnsDialer) ExchangeDNS(ctx context.Context, query []byte) ([]byte, error) {
	return d.exchange(ctx, query)
}

func TestProxyExchangeDNS(t *testing.T) {
	echo := dnsDialer{exchange: func(_ context.Context, query []byte) ([]byte, error) { return query, nil }}
	tests := []struct {
		name    string
		prepare func(*testing.T, *Proxy)
		wantErr error
	}{
		{
			name: "active actor",
			prepare: func(t *testing.T, e *Proxy) {
				if err := e.Activate(testActorUID, echo, fakeActorCertificateSource{}, time.Now().Add(time.Hour)); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name:    "actor was never activated",
			prepare: func(*testing.T, *Proxy) {},
			wantErr: ErrEgressNotActive,
		},
		{
			name: "listener is bound but egress is not activated",
			prepare: func(t *testing.T, e *Proxy) {
				if _, err := e.Bind(testActorUID); err != nil {
					t.Fatal(err)
				}
			},
			wantErr: ErrEgressNotActive,
		},
		{
			name: "certificate expired",
			prepare: func(t *testing.T, e *Proxy) {
				if err := e.Activate(testActorUID, echo, fakeActorCertificateSource{}, time.Now().Add(time.Hour)); err != nil {
					t.Fatal(err)
				}
				e.mu.Lock()
				e.active[testActorUID].expiresAt = time.Now().Add(-time.Second)
				e.mu.Unlock()
			},
			wantErr: ErrEgressNotActive,
		},
		{
			name: "actor was deactivated",
			prepare: func(t *testing.T, e *Proxy) {
				if err := e.Activate(testActorUID, echo, fakeActorCertificateSource{}, time.Now().Add(time.Hour)); err != nil {
					t.Fatal(err)
				}
				if err := e.Deactivate(context.Background(), testActorUID); err != nil {
					t.Fatal(err)
				}
			},
			wantErr: ErrEgressNotActive,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			e, err := New(func(net.Conn) (string, error) { return "192.0.2.10:443", nil })
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = e.Deactivate(context.Background(), testActorUID) })
			tc.prepare(t, e)

			got, err := e.ExchangeDNS(context.Background(), testActorUID, testDNSQuery)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("ExchangeDNS() error = %v, want %v", err, tc.wantErr)
			}
			if tc.wantErr == nil && !bytes.Equal(got, testDNSQuery) {
				t.Errorf("ExchangeDNS() = %x, want %x", got, testDNSQuery)
			}
		})
	}
}

func TestProxyExchangeDNSStopsOnDeactivate(t *testing.T) {
	started := make(chan struct{})
	blocked := dnsDialer{exchange: func(ctx context.Context, _ []byte) ([]byte, error) {
		close(started)
		<-ctx.Done()
		return nil, ctx.Err()
	}}
	e, err := New(func(net.Conn) (string, error) { return "192.0.2.10:443", nil })
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Activate(testActorUID, blocked, fakeActorCertificateSource{}, time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}

	errc := make(chan error, 1)
	go func() {
		_, err := e.ExchangeDNS(context.Background(), testActorUID, testDNSQuery)
		errc <- err
	}()
	<-started
	if err := e.Deactivate(context.Background(), testActorUID); err != nil {
		t.Fatal(err)
	}
	if err := <-errc; !errors.Is(err, context.Canceled) {
		t.Errorf("ExchangeDNS() error = %v, want context.Canceled", err)
	}
}
