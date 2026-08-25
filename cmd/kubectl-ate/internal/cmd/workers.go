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

package cmd

import (
	"context"
	"fmt"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc"
	"k8s.io/apimachinery/pkg/labels"
)

// WorkerLister abstracts ListWorkers RPC calls.
type WorkerLister interface {
	ListWorkers(ctx context.Context, req *ateapipb.ListWorkersRequest, opts ...grpc.CallOption) (*ateapipb.ListWorkersResponse, error)
}

// listAllWorkers pages through ListWorkers and returns all workers.
func listAllWorkers(ctx context.Context, lister WorkerLister) ([]*ateapipb.Worker, error) {
	var workers []*ateapipb.Worker
	pageToken := ""
	for {
		resp, err := lister.ListWorkers(ctx, &ateapipb.ListWorkersRequest{
			PageSize:  1000,
			PageToken: pageToken,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to list workers: %w", err)
		}
		workers = append(workers, resp.GetWorkers()...)
		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			break
		}
	}
	return workers, nil
}

// filterWorkers filters workers by Kubernetes namespace, assigned-actor
// atespace, worker pool label selector, and sandbox class. Empty values match
// everything; an atespace filter only matches workers with an assigned actor.
func filterWorkers(workers []*ateapipb.Worker, namespace, atespace, selector, sandboxClass string) ([]*ateapipb.Worker, error) {
	var labelSel labels.Selector
	if selector != "" {
		var err error
		labelSel, err = labels.Parse(selector)
		if err != nil {
			return nil, fmt.Errorf("invalid label selector %q: %w", selector, err)
		}
	}

	var filtered []*ateapipb.Worker
	for _, w := range workers {
		if namespace != "" && w.GetWorkerNamespace() != namespace {
			continue
		}
		if atespace != "" && w.GetStatus().GetAssignment().GetActor().GetAtespace() != atespace {
			continue
		}
		if labelSel != nil && !labelSel.Matches(labels.Set(w.GetLabels())) {
			continue
		}
		if sandboxClass != "" && w.GetSandboxClass() != sandboxClass {
			continue
		}
		filtered = append(filtered, w)
	}
	return filtered, nil
}
