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

package ch

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
)

// apiBase is a placeholder host; the real transport always dials the unix
// api-socket, so the host portion of the URL is ignored.
const apiBase = "http://localhost"

// apiClient speaks the cloud-hypervisor REST API over its unix api-socket.
//
// cloud-hypervisor serves an HTTP/1.1 REST API on the api-socket, and we drive snapshot/restore
// through it (vm.pause, vm.snapshot, vm.resume, vmm.ping, ...).
type apiClient struct {
	http *http.Client
}

func newAPIClient(socketPath string) *apiClient {
	return &apiClient{
		http: &http.Client{
			Transport: &http.Transport{
				// CH's API server closes idle connections (and can get heavily
				// swapped out during reclaim). Reusing a kept-alive connection
				// then blocks forever on the next request (observed
				// empirically: vm.resume hangs on a reused connection while
				// a fresh one works instantly). Force a fresh connection per
				// request.
				DisableKeepAlives: true,
				DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
					var d net.Dialer
					return d.DialContext(ctx, "unix", socketPath)
				},
			},
		},
	}
}

// get issues a GET and checks for a 2xx status.
func (c *apiClient) get(ctx context.Context, path string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, apiBase+path, nil)
	if err != nil {
		return err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode >= 300 {
		return fmt.Errorf("GET %s: status %d", path, resp.StatusCode)
	}
	return nil
}

// getJSON issues a GET and decodes the 2xx JSON response into out.
func (c *apiClient) getJSON(ctx context.Context, path string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, apiBase+path, nil)
	if err != nil {
		return err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode >= 300 {
		return fmt.Errorf("GET %s: status %d: %s", path, resp.StatusCode, bytes.TrimSpace(b))
	}
	return json.Unmarshal(b, out)
}

// put issues a PUT with an optional JSON body and checks for a 2xx status.
func (c *apiClient) put(ctx context.Context, path string, body any) error {
	var rdr io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, apiBase+path, rdr)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	msg, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return fmt.Errorf("PUT %s: status %d: %s", path, resp.StatusCode, bytes.TrimSpace(msg))
	}
	return nil
}

// snapshotConfig is the body of /api/v1/vm.snapshot.
type snapshotConfig struct {
	DestinationURL string `json:"destination_url"`
}
