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

// Command egress is a small HTTP workload for exercising captured actor egress.
package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"time"
)

const defaultEgressURL = "https://httpbin.org/get"

func main() {
	ctx := context.Background()
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	mux := http.NewServeMux()
	mux.HandleFunc("/", handleEgress)
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok\n"))
	})

	slog.InfoContext(ctx, "Starting egress demo server on port 80")
	if err := http.ListenAndServe(":80", mux); err != nil {
		slog.ErrorContext(ctx, "Error starting server", slog.Any("err", err))
		os.Exit(1)
	}
}

func handleEgress(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	targetURL, err := egressTargetURL(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, targetURL, nil)
	if err != nil {
		http.Error(w, fmt.Sprintf("invalid egress target %q: %v", targetURL, err), http.StatusBadRequest)
		return
	}

	start := time.Now()
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		slog.ErrorContext(ctx, "Egress request failed", slog.String("target", targetURL), slog.Any("err", err))
		http.Error(w, fmt.Sprintf("egress request to %s failed: %v\n", targetURL, err), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1024))
	if err != nil {
		slog.ErrorContext(ctx, "Failed reading egress response", slog.String("target", targetURL), slog.Any("err", err))
		http.Error(w, fmt.Sprintf("reading egress response from %s failed: %v\n", targetURL, err), http.StatusBadGateway)
		return
	}

	slog.InfoContext(ctx, "Egress request completed",
		slog.String("target", targetURL),
		slog.Int("upstream_status", resp.StatusCode),
		slog.Duration("duration", time.Since(start)))

	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "egress target: %s\n", targetURL)
	fmt.Fprintf(w, "upstream status: %s\n", resp.Status)
	fmt.Fprintf(w, "body bytes read: %d\n", len(body))
	fmt.Fprintf(w, "body:\n%s\n", body)
}

func egressTargetURL(r *http.Request) (string, error) {
	targetURL := r.URL.Query().Get("url")
	if targetURL == "" {
		targetURL = defaultEgressURL
	}

	parsed, err := url.Parse(targetURL)
	if err != nil {
		return "", fmt.Errorf("invalid egress target %q: %w", targetURL, err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return "", fmt.Errorf("invalid egress target %q: scheme must be http or https", targetURL)
	}
	if parsed.Host == "" {
		return "", fmt.Errorf("invalid egress target %q: host is required", targetURL)
	}
	return targetURL, nil
}
