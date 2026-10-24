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

// Command echodest is an in-cluster destination for the egress suite. It
// reports the local address each request arrived on, so a test can assert
// which family actually carried the request instead of inferring it from a
// 200. Egress tests that reach the public internet cannot: whether a
// destination offers a AAAA is decided outside the cluster and changes under
// them.
package main

import (
	"encoding/json"
	"flag"
	"log/slog"
	"net"
	"net/http"
	"os"
	"time"
)

var listenAddress = flag.String("listen", ":8080", "Address the destination's HTTP API listens on.")

// reply is what every endpoint returns. Family is the family the request
// arrived over, taken from the connection rather than from anything the
// client claims.
type reply struct {
	Family    string `json:"family"`
	LocalAddr string `json:"localAddr"`
}

func handle(w http.ResponseWriter, r *http.Request) {
	local, _ := r.Context().Value(http.LocalAddrContextKey).(net.Addr)
	body := reply{Family: "unknown"}
	if local != nil {
		body.LocalAddr = local.String()
		if host, _, err := net.SplitHostPort(local.String()); err == nil {
			if ip := net.ParseIP(host); ip != nil {
				// To4 is non-nil for a v4-mapped v6 address too, which is what a
				// dual-stack listener reports for a v4 connection -- exactly the
				// answer wanted here.
				if ip.To4() != nil {
					body.Family = "ipv4"
				} else {
					body.Family = "ipv6"
				}
			}
		}
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(body); err != nil {
		slog.Error("encoding reply", "err", err)
	}
}

func main() {
	flag.Parse()

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", handle)

	server := &http.Server{
		Addr:              *listenAddress,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}
	slog.Info("echodest listening", "addr", *listenAddress)
	if err := server.ListenAndServe(); err != nil {
		slog.Error("serving", "err", err)
		os.Exit(1)
	}
}
