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
	"cmp"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"slices"
	"text/tabwriter"
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"k8s.io/apimachinery/pkg/util/duration"
	"sigs.k8s.io/yaml"
)

// timeNow returns the current time. It is a package variable so tests can pin
// it and make age rendering deterministic.
var timeNow = time.Now

// formatAge renders a resource's age from its creation timestamp, kubectl-style
// (e.g. "5m", "3h", "2d").
func formatAge(ts *timestamppb.Timestamp) string {
	return duration.HumanDuration(timeNow().Sub(ts.AsTime()))
}

// PrintActors prints a slice of actors to stdout in the requested format.
func PrintActors(actors []*ateapipb.Actor, format string) error {
	return PrintActorsTo(os.Stdout, actors, format)
}

func sortActors(actors []*ateapipb.Actor) {
	slices.SortFunc(actors, func(a, b *ateapipb.Actor) int {
		if c := cmp.Compare(a.GetMetadata().GetAtespace(), b.GetMetadata().GetAtespace()); c != 0 {
			return c
		}
		if c := cmp.Compare(a.GetActorTemplateNamespace(), b.GetActorTemplateNamespace()); c != 0 {
			return c
		}
		if c := cmp.Compare(a.GetActorTemplateName(), b.GetActorTemplateName()); c != 0 {
			return c
		}
		return cmp.Compare(a.GetMetadata().GetName(), b.GetMetadata().GetName())
	})
}

// PrintActorsTo prints a slice of actors to the provided writer.
func PrintActorsTo(out io.Writer, actors []*ateapipb.Actor, format string) error {
	sortActors(actors)
	switch format {
	case "json", "yaml":
		return printProto(out, &ateapipb.ListActorsResponse{Actors: actors}, format)
	case "table":
		w := tabwriter.NewWriter(out, 0, 0, 3, ' ', 0)
		fmt.Fprintln(w, "ATESPACE\tNAME\tTEMPLATE\tSTATUS\tATEOM POD\tATEOM IP\tVERSION\tAGE")
		for _, actor := range actors {
			atespace := actor.GetMetadata().GetAtespace()
			name := actor.GetMetadata().GetName()
			template := actor.GetActorTemplateNamespace() + "/" + actor.GetActorTemplateName()
			status := actor.GetStatus().String()

			assignment := actor.GetWorkerAssignment()
			worker := "<none>"
			if assignment != nil {
				worker = assignment.GetWorkerNamespace() + "/" + assignment.GetWorkerPod()
			}

			version := actor.GetMetadata().GetVersion()
			age := formatAge(actor.GetMetadata().GetCreateTime())
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\t%d\t%s\n", atespace, name, template, status, worker, assignment.GetWorkerPodIp(), version, age)
		}
		return w.Flush()
	default:
		return fmt.Errorf("unsupported format %q", format)
	}
}

// PrintWorkers prints a slice of workers to stdout in the requested format.
func PrintWorkers(workers []*ateapipb.Worker, format string) error {
	return PrintWorkersTo(os.Stdout, workers, format)
}

func sortWorkers(workers []*ateapipb.Worker) {
	slices.SortFunc(workers, func(a, b *ateapipb.Worker) int {
		if c := cmp.Compare(a.GetWorkerNamespace(), b.GetWorkerNamespace()); c != 0 {
			return c
		}
		if c := cmp.Compare(a.GetWorkerPool(), b.GetWorkerPool()); c != 0 {
			return c
		}
		return cmp.Compare(a.GetWorkerPod(), b.GetWorkerPod())
	})
}

// PrintWorkersTo prints a slice of workers to the provided writer.
func PrintWorkersTo(out io.Writer, workers []*ateapipb.Worker, format string) error {
	sortWorkers(workers)
	switch format {
	case "json", "yaml":
		return printProto(out, &ateapipb.ListWorkersResponse{Workers: workers}, format)
	case "table":
		w := tabwriter.NewWriter(out, 0, 0, 3, ' ', 0)
		fmt.Fprintln(w, "NAMESPACE\tPOOL\tCLASS\tPOD\tSTATUS\tASSIGNED ACTOR")
		for _, worker := range workers {
			ns := worker.GetWorkerNamespace()
			pool := worker.GetWorkerPool()
			class := worker.GetSandboxClass()
			pod := worker.GetWorkerPod()

			status := "FREE"
			assignedActor := "<none>"
			if wass := worker.Assignment; wass != nil {
				status = "ASSIGNED"
				assignedActor = fmt.Sprintf("%s/%s/%s/%s",
					wass.ActorTemplate.Namespace, wass.ActorTemplate.Name, wass.Actor.Atespace, wass.Actor.Name)
			}

			fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\n", ns, pool, class, pod, status, assignedActor)
		}
		return w.Flush()
	default:
		return fmt.Errorf("unsupported format %q", format)
	}
}

// WorkerTopItem represents real-time hardware resource utilization for a worker pod.
type WorkerTopItem struct {
	Pod           string `json:"pod" yaml:"pod"`
	Pool          string `json:"pool" yaml:"pool"`
	Class         string `json:"class,omitempty" yaml:"class,omitempty"`
	Status        string `json:"status" yaml:"status"`
	AssignedActor string `json:"assignedActor" yaml:"assignedActor"`
	CPU           string `json:"cpu" yaml:"cpu"`
	Memory        string `json:"memory" yaml:"memory"`
	Namespace     string `json:"namespace,omitempty" yaml:"namespace,omitempty"`
}

// WorkerTopList wraps worker top items for JSON/YAML output.
type WorkerTopList struct {
	Workers []*WorkerTopItem `json:"workers" yaml:"workers"`
}

func sortWorkerTopItems(items []*WorkerTopItem) {
	slices.SortFunc(items, func(a, b *WorkerTopItem) int {
		if c := cmp.Compare(a.Namespace, b.Namespace); c != 0 {
			return c
		}
		if c := cmp.Compare(a.Pool, b.Pool); c != 0 {
			return c
		}
		return cmp.Compare(a.Pod, b.Pod)
	})
}

// PrintWorkerTop prints a slice of worker top items to stdout in the requested format.
func PrintWorkerTop(items []*WorkerTopItem, format string) error {
	return PrintWorkerTopTo(os.Stdout, items, format)
}

// PrintWorkerTopTo prints a slice of worker top items to the provided writer.
func PrintWorkerTopTo(out io.Writer, items []*WorkerTopItem, format string) error {
	sortWorkerTopItems(items)
	switch format {
	case "json":
		return PrintWorkerTopJSON(out, items)
	case "yaml":
		return PrintWorkerTopYAML(out, items)
	case "table":
		return PrintWorkerTopTable(out, items)
	default:
		return fmt.Errorf("unsupported format %q", format)
	}
}

// PrintWorkerTopTable prints worker top items as a formatted table.
func PrintWorkerTopTable(out io.Writer, items []*WorkerTopItem) error {
	w := tabwriter.NewWriter(out, 0, 0, 3, ' ', 0)
	fmt.Fprintln(w, "NAME\tPOOL\tCLASS\tSTATUS\tASSIGNED ACTOR\tCPU(CORES)\tMEMORY(bytes)")
	for _, item := range items {
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			item.Pod, item.Pool, item.Class, item.Status, item.AssignedActor, item.CPU, item.Memory)
	}
	return w.Flush()
}

// PrintWorkerTopJSON prints worker top items as JSON.
func PrintWorkerTopJSON(out io.Writer, items []*WorkerTopItem) error {
	list := &WorkerTopList{Workers: items}
	b, err := json.MarshalIndent(list, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal json: %w", err)
	}
	if _, err := out.Write(b); err != nil {
		return err
	}
	_, err = out.Write([]byte{'\n'})
	return err
}

// PrintWorkerTopYAML prints worker top items as YAML.
func PrintWorkerTopYAML(out io.Writer, items []*WorkerTopItem) error {
	list := &WorkerTopList{Workers: items}
	b, err := json.Marshal(list)
	if err != nil {
		return fmt.Errorf("failed to marshal json for yaml: %w", err)
	}
	yb, err := yaml.JSONToYAML(b)
	if err != nil {
		return err
	}
	_, err = out.Write(yb)
	return err
}

// PrintActor prints a single actor in the requested format.
func PrintActor(actor *ateapipb.Actor, format string) error {
	return PrintActors([]*ateapipb.Actor{actor}, format)
}

// PrintActorSnapshots prints actor snapshots to stdout in the requested format.
func PrintActorSnapshots(snapshots []*ateapipb.ActorSnapshot, format string) error {
	if format == "json" || format == "yaml" {
		return printProto(os.Stdout, &ateapipb.ListActorSnapshotsResponse{Snapshots: snapshots}, format)
	}
	if format != "table" {
		return fmt.Errorf("unsupported format %q", format)
	}
	slices.SortFunc(snapshots, func(a, b *ateapipb.ActorSnapshot) int {
		if c := cmp.Compare(a.GetMetadata().GetAtespace(), b.GetMetadata().GetAtespace()); c != 0 {
			return c
		}
		return cmp.Compare(a.GetMetadata().GetName(), b.GetMetadata().GetName())
	})
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
	fmt.Fprintln(w, "ATESPACE\tNAME\tSOURCE ACTOR\tSOURCE VERSION\tSCOPE\tAGE")
	for _, snapshot := range snapshots {
		fmt.Fprintf(w, "%s\t%s\t%s/%s\t%d\t%s\t%s\n",
			snapshot.GetMetadata().GetAtespace(), snapshot.GetMetadata().GetName(),
			snapshot.GetSourceActor().GetAtespace(), snapshot.GetSourceActor().GetName(),
			snapshot.GetSourceActorVersion(), snapshot.GetContentScope(), formatAge(snapshot.GetMetadata().GetCreateTime()))
	}
	return w.Flush()
}

// PrintActorSnapshotTag prints an actor snapshot tag to stdout.
func PrintActorSnapshotTag(tag *ateapipb.ActorSnapshotTag, format string) error {
	if format == "json" || format == "yaml" {
		return printProto(os.Stdout, tag, format)
	}
	if format != "table" {
		return fmt.Errorf("unsupported format %q", format)
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
	fmt.Fprintln(w, "ATESPACE\tNAME\tSNAPSHOT\tSCOPE\tAGE")
	fmt.Fprintf(w, "%s\t%s\t%s/%s\t%s\t%s\n",
		tag.GetMetadata().GetAtespace(), tag.GetMetadata().GetName(),
		tag.GetSnapshot().GetAtespace(), tag.GetSnapshot().GetName(),
		tag.GetScope(), formatAge(tag.GetMetadata().GetCreateTime()))
	return w.Flush()
}

// PrintAtespaces prints a slice of atespaces to stdout in the requested format.
func PrintAtespaces(atespaces []*ateapipb.Atespace, format string) error {
	return PrintAtespacesTo(os.Stdout, atespaces, format)
}

func sortAtespaces(atespaces []*ateapipb.Atespace) {
	slices.SortFunc(atespaces, func(a, b *ateapipb.Atespace) int {
		return cmp.Compare(a.GetMetadata().GetName(), b.GetMetadata().GetName())
	})
}

// PrintAtespacesTo prints a slice of atespaces to the provided writer.
func PrintAtespacesTo(out io.Writer, atespaces []*ateapipb.Atespace, format string) error {
	sortAtespaces(atespaces)
	switch format {
	case "json", "yaml":
		return printProto(out, &ateapipb.ListAtespacesResponse{Atespaces: atespaces}, format)
	case "table":
		w := tabwriter.NewWriter(out, 0, 0, 3, ' ', 0)
		fmt.Fprintln(w, "NAME\tAGE")
		for _, a := range atespaces {
			fmt.Fprintf(w, "%s\t%s\n", a.GetMetadata().GetName(), formatAge(a.GetMetadata().GetCreateTime()))
		}
		return w.Flush()
	default:
		return fmt.Errorf("unsupported format %q", format)
	}
}

// PrintAtespace prints a single atespace in the requested format.
func PrintAtespace(atespace *ateapipb.Atespace, format string) error {
	return PrintAtespaces([]*ateapipb.Atespace{atespace}, format)
}

func printProto(out io.Writer, msg proto.Message, format string) error {
	m := protojson.MarshalOptions{}
	b, err := m.Marshal(msg)
	if err != nil {
		return err
	}

	// Normalize JSON output to ensure consistency across environments.
	// This works around non-deterministic spacing in protojson.
	// See: https://github.com/golang/protobuf/issues/1121
	var obj any
	if err := json.Unmarshal(b, &obj); err != nil {
		return fmt.Errorf("failed to unmarshal protojson: %w", err)
	}
	b, err = json.MarshalIndent(obj, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal indent: %w", err)
	}

	if format == "yaml" {
		yb, err := yaml.JSONToYAML(b)
		if err != nil {
			return err
		}
		// The YAML encoder natively appends a trailing newline to the document block.
		_, err = out.Write(yb)
		return err
	}

	// We manually append a trailing newline here so the CLI output doesn't smash
	// into the user's terminal prompt.
	if _, err = out.Write(b); err != nil {
		return err
	}
	_, err = out.Write([]byte{'\n'})
	return err
}
