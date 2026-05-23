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

package router

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

func TestStatuszEndpoint(t *testing.T) {
	dnsAddr, err := net.ResolveTCPAddr("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed determining dynamic port: %v", err)
	}
	l1, err := net.ListenTCP("tcp", dnsAddr)
	if err != nil {
		t.Fatalf("Failed creating listener: %v", err)
	}
	httpPort := l1.Addr().(*net.TCPAddr).Port
	l1.Close()

	// Pre-configure local yaml mockup
	tmpFile, err := os.CreateTemp("", "templates-*.yaml")
	if err != nil {
		t.Fatalf("Unable creating temp files: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	tmpFile.Close()

	cfg := RouterConfig{
		Standalone:    true,
		Namespace:     "default",
		StatusPort:    httpPort,
		HttpPort:      8080,
		XdsPort:       18000,
		ExtprocPort:   50051,
		TemplatesFile: tmpFile.Name(),
	}

	srv, err := NewRouterServer(cfg)
	if err != nil {
		t.Fatalf("Failed generating router server: %v", err)
	}

	srv.extprocSrv = NewExtProcServer(cfg.ExtprocPort, &mockClient{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Inject recorded queries
	srv.extprocSrv.recorder.Add(RecordedQuery{
		Timestamp: time.Now(),
		Client:    "127.0.0.1",
		Host:      "example.com",
		Path:      "/v1/test",
		Method:    "GET",
		Action:    "Matched test-actor",
		Target:    "10.0.0.5",
		Duration:  time.Millisecond * 10,
	})

	statusReady := make(chan struct{})
	go func() {
		close(statusReady)
		if runErr := srv.Run(ctx); runErr != nil && !strings.Contains(runErr.Error(), "context canceled") {
			t.Logf("status server Run returned unexpected error: %v", runErr)
		}
	}()

	<-statusReady

	statuszUrl := fmt.Sprintf("http://127.0.0.1:%d/statusz", httpPort)

	var resp *http.Response
	var getErr error
	for i := 0; i < 20; i++ {
		resp, getErr = http.Get(statuszUrl)
		if getErr == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if getErr != nil {
		t.Fatalf("Failed retrieving status request output details after retries: %v", getErr)
	}
	defer resp.Body.Close()

	htmlBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Status details parsing operation error: %v", err)
	}
	content := string(htmlBytes)

	if !strings.Contains(content, "atenet Router Status") {
		t.Errorf("Status content missing expected header")
	}

	if !strings.Contains(content, "Matched test-actor") {
		t.Errorf("Recorded processed activities trace text is missing from HTML response output")
	}

	// Verify format=json serialization integration checks
	jsonUrl := fmt.Sprintf("%s?format=json", statuszUrl)
	jsonResp, err := http.Get(jsonUrl)
	if err != nil {
		t.Fatalf("JSON format endpoint call resulted in error: %v", err)
	}
	defer jsonResp.Body.Close()

	var dashboard DashboardContext
	if err := json.NewDecoder(jsonResp.Body).Decode(&dashboard); err != nil {
		t.Fatalf("JSON decoding task completed in failure: %v", err)
	}

	if len(dashboard.Queries) != 1 {
		t.Errorf("Missing operations telemetry data elements inside raw serialized response json outputs")
	}

	if dashboard.Queries[0].Target != "10.0.0.5" {
		t.Errorf("Target parameters unassigned inside context payload context properties: found %s", dashboard.Queries[0].Target)
	}
}
