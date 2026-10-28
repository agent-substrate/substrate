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
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

// echoedHeaders is the /echoheaders response body.
type echoedHeaders struct {
	// Headers holds the request headers the origin received, keyed by
	// canonical name, with repeated values joined by ",".
	Headers map[string]string `json:"headers"`
}

// dialResult is the /dial response body.
type dialResult struct {
	Address string `json:"address"`
	// OK reports whether the TCP connection was established.
	OK    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
}

// defaultDialTimeout is the /dial timeout when the request sets none.
const defaultDialTimeout = 3 * time.Second

// newHTTPHandler is the HTTP/1.1 origin an Actor's egress lands on. It exists so
// a test can assert the destination port is recovered from SO_ORIGINAL_DST
// rather than defaulted from the URL scheme: the actor fetches its /healthz on a
// non-standard port, and the gateway's access log is expected to carry that
// port. The optional body makes the same fixture useful for assertions about a
// response inside the egress tunnel. /echoheaders returns the request headers as
// they arrived, which is how a suite sees what the gateway put on the wire.
// The /fetch handler for e2e tests that need test requests with bodies.
// /dial reports whether this pod can open a TCP connection to an address; see
// dial.
func newHTTPHandler(body string) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		if body != "" {
			_, _ = io.WriteString(w, body)
		}
	})
	mux.HandleFunc("/echoheaders", func(w http.ResponseWriter, r *http.Request) {
		echoed := echoedHeaders{Headers: make(map[string]string, len(r.Header))}
		for name, values := range r.Header {
			echoed.Headers[name] = strings.Join(values, ",")
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(echoed); err != nil {
			log.Printf("testserver http: encoding /echoheaders response: %v", err)
		}
	})
	mux.HandleFunc("POST /fetch", func(w http.ResponseWriter, r *http.Request) {
		if _, err := io.Copy(io.Discard, r.Body); err != nil {
			http.Error(w, fmt.Sprintf("reading request body: %v", err), http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/dial", dial)
	return mux
}

// acceptingConnect answers every CONNECT with 200, as a forward proxy does
// when it opens the tunnel, and hands anything else to next. Nothing is
// relayed: the server stands in for a proxy only so that a client whose
// CONNECT is refused can tell the refusal did not come from here. A ServeMux
// alone would answer a CONNECT 404, which is harder to tell from one.
func acceptingConnect(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodConnect {
			next.ServeHTTP(w, r)
			return
		}
		// A 2xx to a CONNECT has no body; say so, or the server frames one.
		w.Header().Set("Content-Length", "0")
		w.WriteHeader(http.StatusOK)
	})
}

// dial opens a TCP connection to ?address=<host:port> within ?timeout=
// (default 3s) and closes it again. A suite uses it to check what this pod
// can reach, such as whether a NetworkPolicy lets the pod's traffic through.
// A malformed request gets a 400 rather than ok=false, so a mistake in the
// caller is not mistaken for an unreachable address.
func dial(w http.ResponseWriter, r *http.Request) {
	address := r.URL.Query().Get("address")
	if _, port, err := net.SplitHostPort(address); err != nil || port == "" {
		http.Error(w, fmt.Sprintf("address parameter %q is not <host>:<port>", address), http.StatusBadRequest)
		return
	}
	timeout := defaultDialTimeout
	if raw := r.URL.Query().Get("timeout"); raw != "" {
		parsed, err := time.ParseDuration(raw)
		if err != nil || parsed <= 0 {
			http.Error(w, fmt.Sprintf("timeout parameter %q is not a positive duration", raw), http.StatusBadRequest)
			return
		}
		timeout = parsed
	}

	result := dialResult{Address: address}
	dialer := net.Dialer{Timeout: timeout}
	if conn, err := dialer.DialContext(r.Context(), "tcp", address); err != nil {
		result.Error = err.Error()
	} else {
		result.OK = true
		_ = conn.Close()
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(result); err != nil {
		log.Printf("testserver http: encoding /dial response: %v", err)
	}
}

func newHTTPCmd() *cobra.Command {
	var (
		listenAddress string
		certFile      string
		keyFile       string
		body          string
		acceptConnect bool
	)
	cmd := &cobra.Command{
		Use:   "http",
		Short: "Serve an HTTP/1.1 origin answering /healthz, /echoheaders, POST /fetch and /dial, and with --accept-connect any CONNECT.",
		Args:  cobra.NoArgs,
		RunE: func(_ *cobra.Command, _ []string) error {
			handler := newHTTPHandler(body)
			if acceptConnect {
				handler = acceptingConnect(handler)
			}
			server := &http.Server{
				Addr:              listenAddress,
				Handler:           handler,
				ReadHeaderTimeout: 10 * time.Second,
				WriteTimeout:      2 * time.Minute,
			}
			log.Printf("testserver http: listening on %s", listenAddress)
			return serveHTTP(server, certFile, keyFile)
		},
	}
	cmd.Flags().StringVar(&listenAddress, "listen", ":8080", "Address the origin serves cleartext HTTP on.")
	cmd.Flags().StringVar(&certFile, "tls-cert", "", "PEM certificate file; with --tls-key, also serves HTTPS on :8443.")
	cmd.Flags().StringVar(&keyFile, "tls-key", "", "PEM private key file; with --tls-cert, also serves HTTPS on :8443.")
	cmd.Flags().StringVar(&body, "body", "", "Response body for /healthz.")
	cmd.Flags().BoolVar(&acceptConnect, "accept-connect", false, "Answer CONNECT requests 200, as a forward proxy would, without relaying anything.")
	return cmd
}
