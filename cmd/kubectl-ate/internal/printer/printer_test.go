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

package printer

import (
	"bytes"
	"testing"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/go-cmp/cmp"
)

func TestPrintActorsTo_Table(t *testing.T) {
	var buf bytes.Buffer
	actors := []*ateapipb.Actor{
		{
			ActorId:                "id-1",
			ActorTemplateNamespace: "default",
			ActorTemplateName:      "template-1",
			Status:                 ateapipb.Actor_STATUS_RUNNING,
			Version:                2,
			AteomPodNamespace:      "worker-ns",
			AteomPodName:           "pod-1",
			AteomPodIp:             "1.2.3.4",
		},
	}

	if err := PrintActorsTo(&buf, actors, "table"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	output := buf.String()

	expected := `NAMESPACE   TEMPLATE     ID     STATUS           ATEOM POD         ATEOM IP   VERSION
default     template-1   id-1   STATUS_RUNNING   worker-ns/pod-1   1.2.3.4    2
`
	if diff := cmp.Diff(expected, output); diff != "" {
		t.Errorf("output mismatch (-want +got):\n%s", diff)
	}
}

func TestPrintActorsTo_JSON(t *testing.T) {
	var buf bytes.Buffer
	actors := []*ateapipb.Actor{
		{
			ActorId: "id-1",
			Version: 2,
		},
	}

	if err := PrintActorsTo(&buf, actors, "json"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	output := buf.String()

	expected := `{
  "actors": [
    {
      "actorId": "id-1",
      "version": "2"
    }
  ]
}
`
	if diff := cmp.Diff(expected, output); diff != "" {
		t.Errorf("output mismatch (-want +got):\n%s", diff)
	}
}

func TestPrintActorsTo_YAML(t *testing.T) {
	var buf bytes.Buffer
	actors := []*ateapipb.Actor{
		{
			ActorId: "id-1",
			Version: 2,
		},
	}

	if err := PrintActorsTo(&buf, actors, "yaml"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	output := buf.String()

	expected := `actors:
- actorId: id-1
  version: "2"
`
	if diff := cmp.Diff(expected, output); diff != "" {
		t.Errorf("output mismatch (-want +got):\n%s", diff)
	}
}

func TestPrintActorsTo_Table_Sorted(t *testing.T) {
	var buf bytes.Buffer
	actors := []*ateapipb.Actor{
		{
			ActorId:                "zebra",
			ActorTemplateNamespace: "default",
			ActorTemplateName:      "template-1",
			Status:                 ateapipb.Actor_STATUS_SUSPENDED,
		},
		{
			ActorId:                "alpha",
			ActorTemplateNamespace: "default",
			ActorTemplateName:      "template-1",
			Status:                 ateapipb.Actor_STATUS_RUNNING,
		},
		{
			ActorId:                "beta",
			ActorTemplateNamespace: "other",
			ActorTemplateName:      "template-2",
			Status:                 ateapipb.Actor_STATUS_SUSPENDED,
		},
	}

	if err := PrintActorsTo(&buf, actors, "table"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expected := `NAMESPACE   TEMPLATE     ID      STATUS             ATEOM POD   ATEOM IP   VERSION
default     template-1   alpha   STATUS_RUNNING     <none>                 0
default     template-1   zebra   STATUS_SUSPENDED   <none>                 0
other       template-2   beta    STATUS_SUSPENDED   <none>                 0
`
	if diff := cmp.Diff(expected, buf.String()); diff != "" {
		t.Errorf("output mismatch (-want +got):\n%s", diff)
	}
}

func TestPrintActorsTo_Invalid(t *testing.T) {
	var buf bytes.Buffer
	err := PrintActorsTo(&buf, nil, "xml")
	if err == nil {
		t.Errorf("expected error for invalid format, got nil")
	}
}

func TestPrintWorkersTo_Table(t *testing.T) {
	var buf bytes.Buffer
	workers := []*ateapipb.Worker{
		{
			WorkerNamespace: "default",
			WorkerPool:      "pool-1",
			WorkerPod:       "pod-1",
			ActorNamespace:  "default",
			ActorTemplate:   "template-1",
			ActorId:         "id-1",
		},
	}

	if err := PrintWorkersTo(&buf, workers, "table"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	output := buf.String()

	expected := `NAMESPACE   POOL     POD     STATUS     ASSIGNED ACTOR
default     pool-1   pod-1   ASSIGNED   default/template-1/id-1
`
	if diff := cmp.Diff(expected, output); diff != "" {
		t.Errorf("output mismatch (-want +got):\n%s", diff)
	}
}

func TestPrintWorkersTo_Table_Free(t *testing.T) {
	var buf bytes.Buffer
	workers := []*ateapipb.Worker{
		{
			WorkerNamespace: "default",
			WorkerPool:      "pool-1",
			WorkerPod:       "pod-1",
		},
	}

	if err := PrintWorkersTo(&buf, workers, "table"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	output := buf.String()

	expected := `NAMESPACE   POOL     POD     STATUS   ASSIGNED ACTOR
default     pool-1   pod-1   FREE     <none>
`
	if diff := cmp.Diff(expected, output); diff != "" {
		t.Errorf("output mismatch (-want +got):\n%s", diff)
	}
}

func TestPrintWorkersTo_Table_Sorted(t *testing.T) {
	var buf bytes.Buffer
	workers := []*ateapipb.Worker{
		{
			WorkerNamespace: "default",
			WorkerPool:      "pool-1",
			WorkerPod:       "pod-z",
		},
		{
			WorkerNamespace: "default",
			WorkerPool:      "pool-1",
			WorkerPod:       "pod-a",
		},
		{
			WorkerNamespace: "other",
			WorkerPool:      "pool-2",
			WorkerPod:       "pod-1",
		},
	}

	if err := PrintWorkersTo(&buf, workers, "table"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expected := `NAMESPACE   POOL     POD     STATUS   ASSIGNED ACTOR
default     pool-1   pod-a   FREE     <none>
default     pool-1   pod-z   FREE     <none>
other       pool-2   pod-1   FREE     <none>
`
	if diff := cmp.Diff(expected, buf.String()); diff != "" {
		t.Errorf("output mismatch (-want +got):\n%s", diff)
	}
}

func TestPrintWorkersTo_Invalid(t *testing.T) {
	var buf bytes.Buffer
	err := PrintWorkersTo(&buf, nil, "xml")
	if err == nil {
		t.Errorf("expected error for invalid format, got nil")
	}
}
