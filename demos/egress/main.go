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
// policy. It accepts a URL, fetches it, and returns the upstream response, and
// on /tcp it opens a raw TCP connection so that egress can be exercised with
// something other than HTTP.
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
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
	mux.HandleFunc("/tcp", handleTCPProbe)
	return mux
}

// tcpProbeRequest asks for one raw TCP exchange.
type tcpProbeRequest struct {
	Address   string `json:"address"`
	Send      string `json:"send,omitempty"`
	ReadBytes int    `json:"readBytes,omitempty"`
	Timeout   string `json:"timeout,omitempty"`
}

type tcpProbeResponse struct {
	// Banner is whatever the peer sent before being spoken to.
	Banner   string `json:"banner,omitempty"`
	Received string `json:"received,omitempty"`
	Error    string `json:"error,omitempty"`
}

// handleTCPProbe opens a TCP connection and reads before it writes.
func handleTCPProbe(w http.ResponseWriter, r *http.Request) {
	const defaultProbeTimeout = 5 * time.Second
	// Enough for an SSH identification string or a test banner.
	const defaultProbeReadBytes = 512
	const maxProbeReadBytes = 8 << 10 // 8 KiB

	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeTCPProbeJSON(w, http.StatusMethodNotAllowed, tcpProbeResponse{Error: "method must be POST"})
		return
	}

	var input tcpProbeRequest
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxRequestBody))
	if err := decoder.Decode(&input); err != nil {
		writeTCPProbeJSON(w, http.StatusBadRequest, tcpProbeResponse{Error: fmt.Sprintf("invalid JSON payload: %v", err)})
		return
	}
	if _, _, err := net.SplitHostPort(input.Address); err != nil {
		writeTCPProbeJSON(w, http.StatusBadRequest, tcpProbeResponse{Error: fmt.Sprintf("address must be host:port: %v", err)})
		return
	}

	timeout := defaultProbeTimeout
	if input.Timeout != "" {
		parsed, err := time.ParseDuration(input.Timeout)
		if err != nil {
			writeTCPProbeJSON(w, http.StatusBadRequest, tcpProbeResponse{Error: fmt.Sprintf("invalid timeout: %v", err)})
			return
		}
		timeout = parsed
	}
	readBytes := input.ReadBytes
	if readBytes <= 0 {
		readBytes = defaultProbeReadBytes
	}
	readBytes = min(readBytes, maxProbeReadBytes)

	dialer := net.Dialer{Timeout: timeout}
	connection, err := dialer.DialContext(r.Context(), "tcp", input.Address)
	if err != nil {
		writeTCPProbeJSON(w, http.StatusBadGateway, tcpProbeResponse{Error: fmt.Sprintf("dialing %s: %v", input.Address, err)})
		return
	}
	defer connection.Close()

	// Read before writing anything at all, so an empty banner really does mean
	// the peer stayed silent.
	banner, err := readWithTimeout(connection, readBytes, timeout)
	if err != nil {
		writeTCPProbeJSON(w, http.StatusBadGateway, tcpProbeResponse{Error: fmt.Sprintf("reading banner from %s: %v", input.Address, err)})
		return
	}

	response := tcpProbeResponse{Banner: string(banner)}
	if input.Send == "" {
		writeTCPProbeJSON(w, http.StatusOK, response)
		return
	}

	if err := connection.SetWriteDeadline(time.Now().Add(timeout)); err != nil {
		writeTCPProbeJSON(w, http.StatusBadGateway, tcpProbeResponse{Error: fmt.Sprintf("setting write deadline: %v", err)})
		return
	}
	if _, err := io.WriteString(connection, input.Send); err != nil {
		writeTCPProbeJSON(w, http.StatusBadGateway, tcpProbeResponse{Error: fmt.Sprintf("writing to %s: %v", input.Address, err)})
		return
	}
	received, err := readWithTimeout(connection, readBytes, timeout)
	if err != nil {
		writeTCPProbeJSON(w, http.StatusBadGateway, tcpProbeResponse{Error: fmt.Sprintf("reading reply from %s: %v", input.Address, err)})
		return
	}
	response.Received = string(received)
	writeTCPProbeJSON(w, http.StatusOK, response)
}

// readWithTimeout returns the bytes of a single read, capped at limit. A peer
// that says nothing within timeout yields no bytes and no error, since silence
// is a legitimate answer to "does this peer speak first?".
func readWithTimeout(connection net.Conn, limit int, timeout time.Duration) ([]byte, error) {
	if err := connection.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		return nil, fmt.Errorf("setting read deadline: %w", err)
	}
	buffer := make([]byte, limit)
	n, err := connection.Read(buffer)
	if err != nil && !errors.Is(err, os.ErrDeadlineExceeded) && !errors.Is(err, io.EOF) {
		return nil, err
	}
	return buffer[:n], nil
}

func writeTCPProbeJSON(w http.ResponseWriter, status int, response tcpProbeResponse) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(response)
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
