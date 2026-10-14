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

// newHTTPHandler is the HTTP/1.1 origin an Actor's egress lands on. It exists so
// a test can assert the destination port is recovered from SO_ORIGINAL_DST
// rather than defaulted from the URL scheme: the actor fetches its /healthz on a
// non-standard port, and the gateway's access log is expected to carry that
// port. The optional body makes the same fixture useful for assertions about a
// response inside the egress tunnel. /echoheaders returns the request headers as
// they arrived, which is how a suite sees what the gateway put on the wire.
// The /fetch handler for e2e tests that need test requests with bodies.
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
	return mux
}

func newHTTPCmd() *cobra.Command {
	var (
		listenAddress string
		certFile      string
		keyFile       string
		body          string
	)
	cmd := &cobra.Command{
		Use:   "http",
		Short: "Serve an HTTP/1.1 origin answering /healthz, /echoheaders and POST /fetch.",
		Args:  cobra.NoArgs,
		RunE: func(_ *cobra.Command, _ []string) error {
			server := &http.Server{
				Addr:              listenAddress,
				Handler:           newHTTPHandler(body),
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
	return cmd
}
