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

// Command egress is a small HTTP service for demonstrating per-Actor egress
// policy. It accepts a URL, fetches it, and returns the upstream response.
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"time"
)

const (
	listenAddress   = ":80"
	maxRequestBody  = 64 << 10
	maxResponseBody = 1 << 20
	requestTimeout  = 15 * time.Second
)

type fetchRequest struct {
	URL string `json:"url"`
}

type fetchResponse struct {
	StatusCode int    `json:"statusCode,omitempty"`
	Body       string `json:"body,omitempty"`
	Error      string `json:"error,omitempty"`
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	client := &http.Client{Timeout: requestTimeout}
	slog.Info("starting egress demo", "address", listenAddress)
	if err := http.ListenAndServe(listenAddress, newHandler(client)); err != nil {
		slog.Error("egress demo stopped", "error", err)
		os.Exit(1)
	}
}

func newHandler(client *http.Client) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "ok\n")
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			writeJSON(w, http.StatusMethodNotAllowed, fetchResponse{Error: "method must be POST"})
			return
		}

		var input fetchRequest
		decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxRequestBody))
		if err := decoder.Decode(&input); err != nil {
			writeJSON(w, http.StatusBadRequest, fetchResponse{Error: fmt.Sprintf("invalid JSON payload: %v", err)})
			return
		}
		if err := validateURL(input.URL); err != nil {
			writeJSON(w, http.StatusBadRequest, fetchResponse{Error: err.Error()})
			return
		}

		outbound, err := http.NewRequestWithContext(r.Context(), http.MethodGet, input.URL, nil)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, fetchResponse{Error: fmt.Sprintf("invalid URL: %v", err)})
			return
		}
		if traceparent := r.Header.Get("traceparent"); traceparent != "" {
			outbound.Header.Set("traceparent", traceparent)
		}
		response, err := client.Do(outbound)
		if err != nil {
			writeJSON(w, http.StatusBadGateway, fetchResponse{Error: fmt.Sprintf("request failed: %v", err)})
			return
		}
		defer response.Body.Close()

		body, err := io.ReadAll(io.LimitReader(response.Body, maxResponseBody))
		if err != nil {
			writeJSON(w, http.StatusBadGateway, fetchResponse{Error: fmt.Sprintf("reading response: %v", err)})
			return
		}
		writeJSON(w, response.StatusCode, fetchResponse{StatusCode: response.StatusCode, Body: string(body)})
	})
	return mux
}

func validateURL(raw string) error {
	parsed, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("invalid URL: %w", err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return fmt.Errorf("URL scheme must be http or https")
	}
	if parsed.Hostname() == "" {
		return fmt.Errorf("URL must include a hostname")
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, response fetchResponse) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(response)
}
