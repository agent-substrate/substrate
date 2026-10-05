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

package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// routerStatusPort is the atenet-router Service port for the router's
// --status-port (the "status" container port).
const routerStatusPort = 4040

// StatuszClient reads the atenet router's /statusz page over a
// port-forward, for suites that assert on router-internal state (e.g. the
// request-parking gauge) without going through the metrics pipeline.
type StatuszClient struct {
	baseURL string
	http    *http.Client
	stop    func()
}

// NewStatuszClient establishes a port-forward to the router's status port.
// Call Close to tear it down.
func NewStatuszClient(ctx context.Context) (*StatuszClient, error) {
	localPort, stop, err := ServicePortForward(ctx, SystemNamespace(), ResourceName("atenet-router"), routerStatusPort)
	if err != nil {
		return nil, err
	}

	return &StatuszClient{
		baseURL: fmt.Sprintf("http://127.0.0.1:%d", localPort),
		http:    &http.Client{Timeout: 10 * time.Second},
		stop:    stop,
	}, nil
}

// ParkingStatusz mirrors the "parking" section of /statusz?format=json.
type ParkingStatusz struct {
	Enabled   bool   `json:"enabled"`
	Active    int    `json:"active"`
	MaxParked int    `json:"max_parked"`
	MaxWait   string `json:"max_wait"`
}

// Parking fetches the current request-parking snapshot.
func (c *StatuszClient) Parking(ctx context.Context) (*ParkingStatusz, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/statusz?format=json", nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("statusz returned %d", resp.StatusCode)
	}
	var dashboard struct {
		Parking ParkingStatusz `json:"parking"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&dashboard); err != nil {
		return nil, fmt.Errorf("decoding statusz JSON: %w", err)
	}
	return &dashboard.Parking, nil
}

// WaitForCount polls the request-parking gauge until cond holds.
func (c *StatuszClient) WaitForCount(ctx context.Context, cond func(int) bool) (int, error) {
	return pollParkingCount(ctx, func(ctx context.Context) (int, error) {
		parking, err := c.Parking(ctx)
		if err != nil {
			return 0, err
		}
		return parking.Active, nil
	}, cond, 4*time.Second, 150*time.Millisecond)
}

// pollParkingCount reads the parking gauge until cond holds, and returns the
// last value it read either way. A failed read counts as not there yet.
func pollParkingCount(ctx context.Context, read func(context.Context) (int, error), cond func(int) bool, timeout, interval time.Duration) (int, error) {
	deadline := time.Now().Add(timeout)
	var last int
	for time.Now().Before(deadline) {
		active, err := read(ctx)
		if err == nil {
			last = active
			if cond(active) {
				return active, nil
			}
		}
		time.Sleep(interval)
	}
	return last, fmt.Errorf("timed out waiting for the parking gauge to satisfy the condition")
}

// Close tears down the port-forward.
func (c *StatuszClient) Close() {
	if c.stop != nil {
		c.stop()
	}
}
