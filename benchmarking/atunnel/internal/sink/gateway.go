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
	"io"
	"net"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"go.ate.dev/substrate/benchmarking/atunnel/internal/procstats"
	"go.ate.dev/substrate/internal/credbundle"
)

// Destination ports the gateway gives meaning to. atunnel names the original
// destination in each CONNECT; the gateway never dials it.
const (
	// AnswerPort tunnels carry HTTP/1.1 requests, which the gateway answers
	// with its fixed body, as a web server behind a real gateway would.
	AnswerPort = "80"
	// DiscardPort tunnels carry raw bytes, which the gateway reads and drops,
	// for throughput.
	DiscardPort = "9"
)

// GatewayStats are the fake gateway's counters.
type GatewayStats struct {
	// Tunnels counts CONNECTs accepted; TunnelsH2 those that arrived as
	// HTTP/2 streams, which is how atunnel prefers to send them.
	Tunnels     uint64 `json:"tunnels"`
	TunnelsH2   uint64 `json:"tunnels_h2"`
	TunnelsOpen int64  `json:"tunnels_open"`
	// Connections counts TLS connections from atunnel. Under HTTP/2 many
	// tunnels share one.
	Connections uint64 `json:"connections"`
	Rejected    uint64 `json:"rejected"`
	// Requests are HTTP requests answered inside AnswerPort tunnels.
	Requests uint64 `json:"requests"`
	BytesIn  uint64 `json:"bytes_in"`
}

// GatewayConfig configures a Gateway.
type GatewayConfig struct {
	// CredentialBundle is the gateway's serving certificate and key.
	CredentialBundle string
	// ClientCA verifies the actor certificates atunnel presents.
	ClientCA  string
	BodyBytes int
}

// Gateway stands in for the remote egress gateway: an mTLS server accepting
// CONNECT over HTTP/2 or HTTP/1.1 and handling the bytes inside each tunnel by
// destination port.
type Gateway struct {
	tlsConfig *tls.Config
	response  []byte

	tunnels     atomic.Uint64
	tunnelsH2   atomic.Uint64
	tunnelsOpen atomic.Int64
	connections atomic.Uint64
	rejected    atomic.Uint64
	requests    atomic.Uint64
	bytesIn     atomic.Uint64
}

// NewGateway loads the gateway's TLS material.
func NewGateway(cfg GatewayConfig) (*Gateway, error) {
	cert, err := credbundle.Parse(cfg.CredentialBundle)
	if err != nil {
		return nil, err
	}
	clientCAs, err := credbundle.ParsePool(cfg.ClientCA)
	if err != nil {
		return nil, err
	}
	body := fixedBody(cfg.BodyBytes)
	response := "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: " + strconv.Itoa(len(body)) + "\r\n\r\n"
	return &Gateway{
		tlsConfig: &tls.Config{
			MinVersion:   tls.VersionTLS12,
			Certificates: []tls.Certificate{*cert},
			ClientAuth:   tls.RequireAndVerifyClientCert,
			ClientCAs:    clientCAs,
			NextProtos:   []string{"h2", "http/1.1"},
		},
		response: append([]byte(response), body...),
	}, nil
}

// Stats snapshots the counters.
func (g *Gateway) Stats() GatewayStats {
	return GatewayStats{
		Tunnels:     g.tunnels.Load(),
		TunnelsH2:   g.tunnelsH2.Load(),
		TunnelsOpen: g.tunnelsOpen.Load(),
		Connections: g.connections.Load(),
		Rejected:    g.rejected.Load(),
		Requests:    g.requests.Load(),
		BytesIn:     g.bytesIn.Load(),
	}
}

// Serve serves TLS on lis until ctx ends.
func (g *Gateway) Serve(ctx context.Context, lis net.Listener) error {
	server := &http.Server{
		Handler:           http.HandlerFunc(g.serveHTTP),
		TLSConfig:         g.tlsConfig,
		ReadHeaderTimeout: 10 * time.Second,
		// The HTTP/2 settings of the real gateway's Envoy listener
		// (manifests/ate-install/atenet-egress.yaml). They shape atunnel's
		// side: it opens a new connection per 100 tunnels, and sizes each
		// tunnel's send buffer from the advertised frame size, Envoy's
		// default 16 KiB where Go's server would advertise 1 MiB.
		HTTP2: &http.HTTP2Config{
			MaxConcurrentStreams:          100,
			MaxReadFrameSize:              16 << 10,
			MaxReceiveBufferPerStream:     1 << 20,
			MaxReceiveBufferPerConnection: 1 << 20,
		},
		ConnState: func(_ net.Conn, state http.ConnState) {
			if state == http.StateNew {
				g.connections.Add(1)
			}
		},
	}
	return serveUntil(ctx, server, func() error { return server.ServeTLS(lis, "", "") })
}

func (g *Gateway) serveHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodConnect {
		g.rejected.Add(1)
		http.Error(w, "CONNECT required", http.StatusMethodNotAllowed)
		return
	}
	_, port, err := net.SplitHostPort(r.Host)
	if err != nil || (port != AnswerPort && port != DiscardPort) {
		g.rejected.Add(1)
		http.Error(w, "unknown destination port", http.StatusBadGateway)
		return
	}
	g.tunnels.Add(1)
	g.tunnelsOpen.Add(1)
	defer g.tunnelsOpen.Add(-1)

	var in io.Reader
	var out io.Writer
	if r.ProtoMajor == 2 {
		// An HTTP/2 tunnel is the stream itself: the request body carries
		// atunnel's bytes and the response body carries ours.
		g.tunnelsH2.Add(1)
		w.WriteHeader(http.StatusOK)
		rc := http.NewResponseController(w)
		if err := rc.Flush(); err != nil {
			return
		}
		in, out = r.Body, flushWriter{w: w, rc: rc}
	} else {
		// An HTTP/1.1 tunnel takes over the connection.
		conn, rw, err := http.NewResponseController(w).Hijack()
		if err != nil {
			return
		}
		defer conn.Close()
		if _, err := rw.WriteString("HTTP/1.1 200 Connection Established\r\n\r\n"); err != nil {
			return
		}
		if err := rw.Flush(); err != nil {
			return
		}
		in, out = rw.Reader, conn
	}

	counted := countingReader{r: in, n: &g.bytesIn}
	if port == DiscardPort {
		_, _ = io.Copy(io.Discard, counted)
		return
	}
	g.answer(counted, out)
}

// answer reads HTTP/1.1 requests from the tunnel and writes the fixed response
// to each, until the tunnel closes.
func (g *Gateway) answer(in io.Reader, out io.Writer) {
	reader := bufio.NewReader(in)
	for {
		req, err := http.ReadRequest(reader)
		if err != nil {
			return
		}
		_, _ = io.Copy(io.Discard, req.Body)
		_ = req.Body.Close()
		g.requests.Add(1)
		if _, err := out.Write(g.response); err != nil {
			return
		}
	}
}

type countingReader struct {
	r io.Reader
	n *atomic.Uint64
}

func (c countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n.Add(uint64(n))
	return n, err
}

// flushWriter pushes every write onto the HTTP/2 stream at once, as a tunnel
// must.
type flushWriter struct {
	w  io.Writer
	rc *http.ResponseController
}

func (f flushWriter) Write(p []byte) (int, error) {
	n, err := f.w.Write(p)
	if err != nil {
		return n, err
	}
	return n, f.rc.Flush()
}

// AdminHandler serves the gateway's counters on /stats, with the process's,
// and /healthz.
func AdminHandler(gateway *Gateway) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/stats", procstats.Handler(func() map[string]any {
		return map[string]any{"gateway": gateway.Stats()}
	}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) })
	return mux
}
