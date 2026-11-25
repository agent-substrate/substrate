// Copyright 2026 The Agent Substrate Authors
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
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"

	"go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/grpc"
)

type demoWorkerPages struct {
	ateapi.ControlClient
	pages map[string]*ateapi.ListWorkersResponse
}

func (f demoWorkerPages) ListWorkers(_ context.Context, req *ateapi.ListWorkersRequest, _ ...grpc.CallOption) (*ateapi.ListWorkersResponse, error) {
	token := req.GetOptions().GetPageToken()
	if page, ok := f.pages[token]; ok {
		return page, nil
	}
	return nil, fmt.Errorf("unexpected page token %q", token)
}

func TestPodsShowDemoWorkersAcrossPages(t *testing.T) {
	previousClient := ateClient
	t.Cleanup(func() { ateClient = previousClient })
	ateClient = demoWorkerPages{pages: map[string]*ateapi.ListWorkersResponse{
		"": {
			Workers: []*ateapi.Worker{
				{WorkerNamespace: namespace + "-other", WorkerPod: "unrelated-worker"},
				{WorkerNamespace: namespace, WorkerPod: "claude-worker-2"},
			},
			NextPageToken: "empty",
		},
		"empty": {NextPageToken: "demo"},
		"demo": {Workers: []*ateapi.Worker{
			{WorkerNamespace: namespace, WorkerPod: "claude-worker-1"},
			{WorkerNamespace: namespace, WorkerPod: "claude-worker-3"},
		}},
	}}

	response := httptest.NewRecorder()
	handlePods(response, httptest.NewRequest(http.MethodGet, "/api/pods", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("GET /api/pods = %d, body %s", response.Code, response.Body)
	}
	var body struct{ Pods []podSummary }
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, pod := range body.Pods {
		names = append(names, pod.Name)
	}
	if want := []string{"claude-worker-1", "claude-worker-2", "claude-worker-3"}; !slices.Equal(names, want) {
		t.Errorf("dashboard pods = %v, want %v", names, want)
	}
	if got := countPoolWorkers(t.Context()); got != 3 {
		t.Errorf("scheduler pool size = %d, want 3", got)
	}
}

func TestCountPoolWorkersWithoutDemoWorkers(t *testing.T) {
	previousClient := ateClient
	t.Cleanup(func() { ateClient = previousClient })
	ateClient = demoWorkerPages{pages: map[string]*ateapi.ListWorkersResponse{
		"": {Workers: []*ateapi.Worker{
			{WorkerNamespace: namespace + "-other", WorkerPod: "unrelated-worker"},
		}},
	}}

	if got := countPoolWorkers(t.Context()); got != 0 {
		t.Errorf("scheduler pool size = %d, want 0", got)
	}
}
