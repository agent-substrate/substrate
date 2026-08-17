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
	"bytes"
	"context"
	"testing"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/go-cmp/cmp"
)

func TestGetWorkersRunner_Filters(t *testing.T) {
	workers := []*ateapipb.Worker{
		{
			WorkerNamespace: "ns-1",
			WorkerPool:      "counter",
			WorkerPod:       "pod-1",
			SandboxClass:    "microvm",
			Assignment: &ateapipb.Assignment{
				ActorTemplate: &ateapipb.KubeNamespacedObjectRef{Namespace: "ns-1", Name: "counter"},
				Actor:         &ateapipb.ObjectRef{Atespace: "space-a", Name: "actor-a"},
			},
			Labels: map[string]string{"ate.dev/worker-pool": "counter"},
		},
		{
			WorkerNamespace: "ns-1",
			WorkerPool:      "other",
			WorkerPod:       "pod-2",
			SandboxClass:    "gvisor",
			Labels:          map[string]string{"ate.dev/worker-pool": "other"},
		},
		{
			WorkerNamespace: "ns-2",
			WorkerPool:      "counter",
			WorkerPod:       "pod-3",
			SandboxClass:    "gvisor",
			Assignment: &ateapipb.Assignment{
				ActorTemplate: &ateapipb.KubeNamespacedObjectRef{Namespace: "ns-2", Name: "counter"},
				Actor:         &ateapipb.ObjectRef{Atespace: "space-b", Name: "actor-b"},
			},
			Labels: map[string]string{"ate.dev/worker-pool": "counter"},
		},
	}

	header := "NAMESPACE   POOL      CLASS     POD     STATUS     ASSIGNED ACTOR\n"
	row1 := "ns-1        counter   microvm   pod-1   ASSIGNED   ns-1/counter/space-a/actor-a\n"
	row2 := "ns-1        other     gvisor    pod-2   FREE       <none>\n"
	row3 := "ns-2        counter   gvisor    pod-3   ASSIGNED   ns-2/counter/space-b/actor-b\n"

	tests := []struct {
		name         string
		namespace    string
		atespace     string
		selector     string
		sandboxClass string
		expected     string
	}{
		{name: "no filter", expected: header + row1 + row2 + row3},
		{name: "namespace", namespace: "ns-1", expected: header + row1 + row2},
		{name: "atespace", atespace: "space-a", expected: header + row1},
		// With no matching rows the tabwriter sizes columns to the header alone.
		{name: "atespace excludes free workers", atespace: "no-such-space", expected: "NAMESPACE   POOL   CLASS   POD   STATUS   ASSIGNED ACTOR\n"},
		{name: "selector", selector: "ate.dev/worker-pool=counter", expected: header + row1 + row3},
		{name: "sandbox class", sandboxClass: "microvm", expected: header + row1},
		// gvisor-only rows shrink the CLASS column to the widest survivor.
		{name: "sandbox class gvisor", sandboxClass: "gvisor", expected: "NAMESPACE   POOL      CLASS    POD     STATUS     ASSIGNED ACTOR\n" +
			"ns-1        other     gvisor   pod-2   FREE       <none>\n" +
			"ns-2        counter   gvisor   pod-3   ASSIGNED   ns-2/counter/space-b/actor-b\n"},
		{name: "combined", namespace: "ns-1", selector: "ate.dev/worker-pool=counter", expected: header + row1},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var buf bytes.Buffer
			runner := &GetWorkersRunner{
				workerLister: &mockWorkerLister{workers: workers},
				namespace:    test.namespace,
				atespace:     test.atespace,
				selector:     test.selector,
				sandboxClass: test.sandboxClass,
				outputFmt:    "table",
				out:          &buf,
			}
			if err := runner.Run(context.Background()); err != nil {
				t.Fatalf("Run() unexpected error: %v", err)
			}
			if diff := cmp.Diff(test.expected, buf.String()); diff != "" {
				t.Errorf("output mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestGetWorkersRunner_InvalidSelector(t *testing.T) {
	runner := &GetWorkersRunner{
		workerLister: &mockWorkerLister{workers: nil},
		selector:     "invalid==selector==",
	}

	if err := runner.Run(context.Background()); err == nil {
		t.Errorf("expected error for invalid label selector, got nil")
	}
}
