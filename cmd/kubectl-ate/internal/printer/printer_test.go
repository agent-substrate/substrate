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
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// pinNow overrides the printer's clock for the duration of a test so that
// age rendering is deterministic, restoring it on cleanup.
func pinNow(t *testing.T, now time.Time) {
	t.Helper()
	prev := timeNow
	timeNow = func() time.Time { return now }
	t.Cleanup(func() { timeNow = prev })
}

func TestFormatAge(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	pinNow(t, now)

	cases := []struct {
		ago  time.Duration
		want string
	}{
		{5 * time.Minute, "5m"},
		{5 * time.Hour, "5h"},
		{72 * time.Hour, "3d"},
	}
	for _, c := range cases {
		ts := timestamppb.New(now.Add(-c.ago))
		if got := formatAge(ts); got != c.want {
			t.Errorf("formatAge(%s ago) = %q, want %q", c.ago, got, c.want)
		}
	}
}

func TestPrintActorsTo_Table(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	pinNow(t, now)

	var buf bytes.Buffer
	actors := []*ateapipb.Actor{
		{
			Metadata: &ateapipb.ResourceMetadata{
				Name:       "id-1",
				Atespace:   "team-a",
				Version:    2,
				CreateTime: timestamppb.New(now.Add(-5 * time.Minute)),
			},
			ActorTemplateNamespace: "default",
			ActorTemplateName:      "template-1",
			Status:                 ateapipb.Actor_STATUS_RUNNING,
			AteomPodNamespace:      "worker-ns",
			AteomPodName:           "pod-1",
			AteomPodIp:             "1.2.3.4",
		},
	}

	if err := PrintActorsTo(&buf, actors, "table"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	output := buf.String()

	expected := `ATESPACE   NAME   TEMPLATE             STATUS           ATEOM POD         ATEOM IP   VERSION   AGE
team-a     id-1   default/template-1   STATUS_RUNNING   worker-ns/pod-1   1.2.3.4    2         5m
`
	if diff := cmp.Diff(expected, output); diff != "" {
		t.Errorf("output mismatch (-want +got):\n%s", diff)
	}
}

func TestPrintActorsTo_JSON(t *testing.T) {
	var buf bytes.Buffer
	actors := []*ateapipb.Actor{
		{
			Metadata: &ateapipb.ResourceMetadata{Name: "id-1", Version: 2},
		},
	}

	if err := PrintActorsTo(&buf, actors, "json"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	output := buf.String()

	expected := `{
  "actors": [
    {
      "metadata": {
        "name": "id-1",
        "version": "2"
      }
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
			Metadata: &ateapipb.ResourceMetadata{Name: "id-1", Version: 2},
		},
	}

	if err := PrintActorsTo(&buf, actors, "yaml"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	output := buf.String()

	expected := `actors:
- metadata:
    name: id-1
    version: "2"
`
	if diff := cmp.Diff(expected, output); diff != "" {
		t.Errorf("output mismatch (-want +got):\n%s", diff)
	}
}

func TestPrintActorsTo_Table_Sorted(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	pinNow(t, now)

	var buf bytes.Buffer
	actors := []*ateapipb.Actor{
		{
			Metadata: &ateapipb.ResourceMetadata{
				Name:       "zebra",
				Atespace:   "team-b",
				CreateTime: timestamppb.New(now.Add(-72 * time.Hour)),
			},
			ActorTemplateNamespace: "default",
			ActorTemplateName:      "template-1",
			Status:                 ateapipb.Actor_STATUS_SUSPENDED,
		},
		{
			Metadata: &ateapipb.ResourceMetadata{
				Name:       "alpha",
				Atespace:   "team-a",
				CreateTime: timestamppb.New(now.Add(-5 * time.Minute)),
			},
			ActorTemplateNamespace: "default",
			ActorTemplateName:      "template-1",
			Status:                 ateapipb.Actor_STATUS_RUNNING,
		},
		{
			Metadata: &ateapipb.ResourceMetadata{
				Name:       "beta",
				Atespace:   "team-a",
				CreateTime: timestamppb.New(now.Add(-5 * time.Hour)),
			},
			ActorTemplateNamespace: "other",
			ActorTemplateName:      "template-2",
			Status:                 ateapipb.Actor_STATUS_SUSPENDED,
		},
	}

	if err := PrintActorsTo(&buf, actors, "table"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Sorted by atespace first, then template namespace, template name, name.
	expected := `ATESPACE   NAME    TEMPLATE             STATUS             ATEOM POD   ATEOM IP   VERSION   AGE
team-a     alpha   default/template-1   STATUS_RUNNING     <none>                 0         5m
team-a     beta    other/template-2     STATUS_SUSPENDED   <none>                 0         5h
team-b     zebra   default/template-1   STATUS_SUSPENDED   <none>                 0         3d
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
			Assignment: &ateapipb.Assignment{
				ActorTemplate: &ateapipb.KubeNamespacedObjectRef{
					Namespace: "default",
					Name:      "template-1",
				},
				Actor: &ateapipb.ObjectRef{
					Name: "id-1",
				},
			},
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

func TestPrintAtespacesTo_Table(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	pinNow(t, now)

	var buf bytes.Buffer
	atespaces := []*ateapipb.Atespace{
		{Metadata: &ateapipb.ResourceMetadata{
			Name:       "team-a",
			CreateTime: timestamppb.New(now.Add(-5 * time.Minute)),
		}},
	}

	if err := PrintAtespacesTo(&buf, atespaces, "table"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expected := `NAME     AGE
team-a   5m
`
	if diff := cmp.Diff(expected, buf.String()); diff != "" {
		t.Errorf("output mismatch (-want +got):\n%s", diff)
	}
}

func TestPrintAtespacesTo_JSON(t *testing.T) {
	var buf bytes.Buffer
	atespaces := []*ateapipb.Atespace{
		{Metadata: &ateapipb.ResourceMetadata{Name: "team-a"}},
	}

	if err := PrintAtespacesTo(&buf, atespaces, "json"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expected := `{
  "atespaces": [
    {
      "metadata": {
        "name": "team-a"
      }
    }
  ]
}
`
	if diff := cmp.Diff(expected, buf.String()); diff != "" {
		t.Errorf("output mismatch (-want +got):\n%s", diff)
	}
}

func TestPrintAtespacesTo_YAML(t *testing.T) {
	var buf bytes.Buffer
	atespaces := []*ateapipb.Atespace{
		{Metadata: &ateapipb.ResourceMetadata{Name: "team-a"}},
	}

	if err := PrintAtespacesTo(&buf, atespaces, "yaml"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expected := `atespaces:
- metadata:
    name: team-a
`
	if diff := cmp.Diff(expected, buf.String()); diff != "" {
		t.Errorf("output mismatch (-want +got):\n%s", diff)
	}
}

func TestPrintAtespacesTo_Table_Sorted(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	pinNow(t, now)

	var buf bytes.Buffer
	atespaces := []*ateapipb.Atespace{
		{Metadata: &ateapipb.ResourceMetadata{Name: "team-c", CreateTime: timestamppb.New(now.Add(-72 * time.Hour))}},
		{Metadata: &ateapipb.ResourceMetadata{Name: "team-a", CreateTime: timestamppb.New(now.Add(-5 * time.Minute))}},
		{Metadata: &ateapipb.ResourceMetadata{Name: "team-b", CreateTime: timestamppb.New(now.Add(-5 * time.Hour))}},
	}

	if err := PrintAtespacesTo(&buf, atespaces, "table"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Sorted by name.
	expected := `NAME     AGE
team-a   5m
team-b   5h
team-c   3d
`
	if diff := cmp.Diff(expected, buf.String()); diff != "" {
		t.Errorf("output mismatch (-want +got):\n%s", diff)
	}
}

func TestPrintAtespacesTo_Invalid(t *testing.T) {
	var buf bytes.Buffer
	if err := PrintAtespacesTo(&buf, nil, "xml"); err == nil {
		t.Errorf("expected error for invalid format, got nil")
	}
}
