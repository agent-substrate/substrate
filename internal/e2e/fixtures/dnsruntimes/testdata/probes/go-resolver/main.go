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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"time"
)

type probeRequest struct {
	Op         string `json:"op"`
	Host       string `json:"host"`
	URL        string `json:"url"`
	Attempts   int    `json:"attempts"`
	IntervalMs int    `json:"intervalMs"`
}

type probeAttempt struct {
	Success    bool     `json:"success"`
	Addresses  []string `json:"addresses,omitempty"`
	StatusCode int      `json:"statusCode,omitempty"`
	ErrorCode  string   `json:"errorCode,omitempty"`
	Error      string   `json:"error,omitempty"`
	DurationMs int64    `json:"durationMs"`
}

type probeResponse struct {
	Attempts []probeAttempt `json:"attempts"`
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: go-probe <json-request>")
		os.Exit(1)
	}
	var req probeRequest
	if err := json.Unmarshal([]byte(os.Args[1]), &req); err != nil {
		fmt.Fprintf(os.Stderr, "unmarshal: %v\n", err)
		os.Exit(1)
	}
	if req.Attempts <= 0 {
		req.Attempts = 1
	}

	var out probeResponse
	for i := 0; i < req.Attempts; i++ {
		if i > 0 && req.IntervalMs > 0 {
			time.Sleep(time.Duration(req.IntervalMs) * time.Millisecond)
		}
		start := time.Now()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)

		switch req.Op {
		case "http_get":
			client := &http.Client{Timeout: 10 * time.Second}
			resp, err := client.Get(req.URL)
			dur := time.Since(start).Milliseconds()
			if err != nil {
				out.Attempts = append(out.Attempts, classifyGoError(err, dur))
			} else {
				_ = resp.Body.Close()
				out.Attempts = append(out.Attempts, probeAttempt{
					Success:    resp.StatusCode >= 200 && resp.StatusCode < 400,
					StatusCode: resp.StatusCode,
					DurationMs: dur,
				})
			}
		default:
			addrs, err := net.DefaultResolver.LookupHost(ctx, req.Host)
			dur := time.Since(start).Milliseconds()
			if err != nil {
				out.Attempts = append(out.Attempts, classifyGoError(err, dur))
			} else {
				out.Attempts = append(out.Attempts, probeAttempt{
					Success:    true,
					Addresses:  addrs,
					DurationMs: dur,
				})
			}
		}
		cancel()
	}

	_ = json.NewEncoder(os.Stdout).Encode(out)
}

func classifyGoError(err error, dur int64) probeAttempt {
	code := "DNSError"
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		if dnsErr.IsTemporary {
			code = "DNSError:Temporary"
		} else if dnsErr.IsNotFound {
			code = "DNSError:NotFound"
		}
	}
	return probeAttempt{
		Success:    false,
		ErrorCode:  code,
		Error:      err.Error(),
		DurationMs: dur,
	}
}
