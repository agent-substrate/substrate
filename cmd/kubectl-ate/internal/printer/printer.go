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

// TimeNow returns the current time. It is a package variable, exported so
// that tests in this package and in cmd can both pin it and make age
// rendering deterministic.
var TimeNow = time.Now

// formatAge renders a resource's age from its creation timestamp, kubectl-style
// (e.g. "5m", "3h", "2d").
func formatAge(ts *timestamppb.Timestamp) string {
	return duration.HumanDuration(TimeNow().Sub(ts.AsTime()))
}

func sortActors(actors []*ateapipb.Actor) {
	slices.SortFunc(actors, func(a, b *ateapipb.Actor) int {
		if c := cmp.Compare(a.GetMetadata().GetAtespace(), b.GetMetadata().GetAtespace()); c != 0 {
			return c
		}
		if c := cmp.Compare(actorTemplateDisplay(a), actorTemplateDisplay(b)); c != 0 {
			return c
		}
		return cmp.Compare(a.GetMetadata().GetName(), b.GetMetadata().GetName())
	})
}

// actorTemplateDisplay renders the template an actor was created from, in
// "<atespace>/<name>" form.
func actorTemplateDisplay(a *ateapipb.Actor) string {
	ref := a.GetActorTemplate()
	return ref.GetAtespace() + "/" + ref.GetName()
}

// PrintActorsTo prints a slice of actors to the provided writer.
func PrintActorsTo(out io.Writer, actors []*ateapipb.Actor, format string) error {
	sortActors(actors)
	switch format {
	case "json", "yaml":
		return printProto(out, &ateapipb.ListActorsResponse{Actors: actors}, format)
	case "table":
		w := tabwriter.NewWriter(out, 0, 0, 3, ' ', 0)
		fmt.Fprintln(w, "ATESPACE\tNAME\tTEMPLATE\tSTATE\tWORKER POD\tWORKER IP\tVERSION\tAGE")
		for _, actor := range actors {
			atespace := actor.GetMetadata().GetAtespace()
			name := actor.GetMetadata().GetName()
			template := actorTemplateDisplay(actor)
			state := actor.GetStatus().GetState().String()

			assignment := actor.GetStatus().GetWorkerAssignment()
			worker := "<none>"
			if assignment != nil {
				worker = assignment.GetWorkerNamespace() + "/" + assignment.GetWorkerPod()
			}

			version := actor.GetMetadata().GetVersion()
			age := formatAge(actor.GetMetadata().GetCreateTime())
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\t%d\t%s\n", atespace, name, template, state, worker, assignment.GetWorkerPodIp(), version, age)
		}
		return w.Flush()
	default:
		return fmt.Errorf("unsupported format %q", format)
	}
}

// WorkerOccupancy is how full a Worker is, as a count against its limit. A
// count rather than the Actors themselves: a listing does not carry them, and
// naming them all would be unreadable long before a Worker is full.
func WorkerOccupancy(worker *ateapipb.Worker) string {
	hosted := worker.GetStatus().GetAllocated().GetActors()
	if hosted == 0 {
		return "FREE"
	}
	return fmt.Sprintf("ASSIGNED(%d/%d)", hosted, worker.GetStatus().GetCapacity().GetActors())
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

// workerActorsColumn renders a Worker's actor count as allocated/capacity
// (e.g. "3/10").
func workerActorsColumn(worker *ateapipb.Worker) string {
	return fmt.Sprintf("%d/%d", worker.GetStatus().GetAllocated().GetActors(), worker.GetStatus().GetCapacity().GetActors())
}

// resourceLimit returns the quantity of the named limit (e.g. "cpu",
// "memory"), or "" if it isn't set.
func resourceLimit(res *ateapipb.Resources, name string) string {
	for _, limit := range res.GetLimits() {
		if limit.GetName() == name {
			return limit.GetQuantity()
		}
	}
	return ""
}

// workerResourceColumn renders a Worker's named resource limit as
// allocated/capacity (e.g. "500m/2"). "-" if the Worker has no capacity
// limit for that resource, since there is then nothing to compare against.
func workerResourceColumn(worker *ateapipb.Worker, name string) string {
	capacity := resourceLimit(worker.GetStatus().GetCapacity().GetResources(), name)
	if capacity == "" {
		return "-"
	}
	allocated := resourceLimit(worker.GetStatus().GetAllocated().GetResources(), name)
	if allocated == "" {
		allocated = "0"
	}
	return allocated + "/" + capacity
}

// PrintWorkersTo prints a slice of workers to the provided writer.
func PrintWorkersTo(out io.Writer, workers []*ateapipb.Worker, format string) error {
	sortWorkers(workers)
	switch format {
	case "json", "yaml":
		return printProto(out, &ateapipb.ListWorkersResponse{Workers: workers}, format)
	case "table":
		w := tabwriter.NewWriter(out, 0, 0, 3, ' ', 0)
		fmt.Fprintln(w, "NAME\tPOOL\tSTATE\tACTORS\tCPU\tMEMORY\tPOD\tAGE")
		for _, worker := range workers {
			name := worker.GetMetadata().GetName()
			pool := worker.GetWorkerPool()
			state := worker.GetStatus().GetState().String()
			pod := worker.GetWorkerNamespace() + "/" + worker.GetWorkerPod()
			age := formatAge(worker.GetMetadata().GetCreateTime())
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
				name, pool, state, workerActorsColumn(worker),
				workerResourceColumn(worker, "cpu"), workerResourceColumn(worker, "memory"),
				pod, age)
		}
		return w.Flush()
	default:
		return fmt.Errorf("unsupported format %q", format)
	}
}

// PrintWorkerTo prints a single worker to the provided writer.
func PrintWorkerTo(out io.Writer, worker *ateapipb.Worker, format string) error {
	if format == "json" || format == "yaml" {
		return printProto(out, worker, format)
	}
	// table has no singular/plural distinction, so reuse the list renderer.
	return PrintWorkersTo(out, []*ateapipb.Worker{worker}, format)
}

// WorkerTopItem represents real-time hardware resource utilization for a worker pod.
type WorkerTopItem struct {
	Pod       string `json:"pod" yaml:"pod"`
	Pool      string `json:"pool" yaml:"pool"`
	Class     string `json:"class,omitempty" yaml:"class,omitempty"`
	Status    string `json:"status" yaml:"status"`
	CPU       string `json:"cpu" yaml:"cpu"`
	Memory    string `json:"memory" yaml:"memory"`
	Namespace string `json:"namespace,omitempty" yaml:"namespace,omitempty"`
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
	fmt.Fprintln(w, "NAME\tPOOL\tCLASS\tSTATUS\tCPU(CORES)\tMEMORY(bytes)")
	for _, item := range items {
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\n",
			item.Pod, item.Pool, item.Class, item.Status, item.CPU, item.Memory)
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

// PrintActorTo prints a single actor to the provided writer.
func PrintActorTo(out io.Writer, actor *ateapipb.Actor, format string) error {
	if format == "json" || format == "yaml" {
		return printProto(out, actor, format)
	}
	// table has no singular/plural distinction, so reuse the list renderer.
	return PrintActorsTo(out, []*ateapipb.Actor{actor}, format)
}

func sortActorTemplates(templates []*ateapipb.ActorTemplate) {
	slices.SortFunc(templates, func(a, b *ateapipb.ActorTemplate) int {
		if c := cmp.Compare(a.GetMetadata().GetAtespace(), b.GetMetadata().GetAtespace()); c != 0 {
			return c
		}
		return cmp.Compare(a.GetMetadata().GetName(), b.GetMetadata().GetName())
	})
}

// PrintActorTemplatesTo prints a slice of actor templates to the provided writer.
func PrintActorTemplatesTo(out io.Writer, templates []*ateapipb.ActorTemplate, format string) error {
	sortActorTemplates(templates)
	switch format {
	case "json", "yaml":
		return printProto(out, &ateapipb.ListActorTemplatesResponse{ActorTemplates: templates}, format)
	case "table":
		w := tabwriter.NewWriter(out, 0, 0, 3, ' ', 0)
		fmt.Fprintln(w, "ATESPACE\tNAME\tSANDBOX CLASS\tGOLDEN SNAPSHOT\tERROR\tAGE")
		for _, t := range templates {
			gss := t.GetStatus().GetGoldenSnapshotStatus()
			// Error messages are too long for a table cell.
			errFlag := ""
			if gss.GetErrorMessage() != "" {
				errFlag = "ERROR"
			}
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\n",
				t.GetMetadata().GetAtespace(), t.GetMetadata().GetName(),
				t.GetSandboxConfig().GetSandboxClass(),
				gss.GetGoldenSnapshot().GetSnapshotUri(), errFlag,
				formatAge(t.GetMetadata().GetCreateTime()))
		}
		return w.Flush()
	default:
		return fmt.Errorf("unsupported format %q", format)
	}
}

// PrintActorTemplateTo prints a single actor template to the provided writer.
func PrintActorTemplateTo(out io.Writer, template *ateapipb.ActorTemplate, format string) error {
	if format == "json" || format == "yaml" {
		return printProto(out, template, format)
	}
	// table has no singular/plural distinction, so reuse the list renderer.
	return PrintActorTemplatesTo(out, []*ateapipb.ActorTemplate{template}, format)
}

// PrintTagsTo prints a slice of tags to the
// provided writer.
func PrintTagsTo(out io.Writer, tags []*ateapipb.Tag, format string) error {
	slices.SortFunc(tags, func(a, b *ateapipb.Tag) int {
		if c := cmp.Compare(a.GetMetadata().GetAtespace(), b.GetMetadata().GetAtespace()); c != 0 {
			return c
		}
		return cmp.Compare(a.GetMetadata().GetName(), b.GetMetadata().GetName())
	})
	switch format {
	case "json", "yaml":
		return printProto(out, &ateapipb.ListTagsResponse{Tags: tags}, format)
	case "table":
		w := tabwriter.NewWriter(out, 0, 0, 3, ' ', 0)
		fmt.Fprintln(w, "ATESPACE\tNAME\tSCOPE\tSTATE\tSNAPSHOT\tCONTENT SCOPE\tAGE")
		for _, tag := range tags {
			// A pending tag has no snapshot yet, so neither its URI nor its
			// content scope says anything.
			snapshotURI, contentScope := "<none>", "<none>"
			if snapshot := tag.GetStatus().GetSnapshot(); snapshot.GetSnapshotUri() != "" {
				snapshotURI = snapshot.GetSnapshotUri()
				contentScope = snapshot.GetContentScope().String()
			}
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
				tag.GetMetadata().GetAtespace(), tag.GetMetadata().GetName(), tag.GetScope(),
				tagState(tag), snapshotURI, contentScope,
				formatAge(tag.GetMetadata().GetCreateTime()))
		}
		return w.Flush()
	default:
		return fmt.Errorf("unsupported format %q", format)
	}
}

// tagState reports whether a tag is usable. A tag is Pending until
// the copy of its own snapshot lands; until then it names nothing an Actor can
// be created from, and deleting it collects whatever the create stranded.
func tagState(tag *ateapipb.Tag) string {
	if tag.GetStatus().GetSnapshot().GetSnapshotUri() == "" {
		return "Pending"
	}
	return "Ready"
}

// PrintTagTo prints a single tag to the provided writer.
func PrintTagTo(out io.Writer, tag *ateapipb.Tag, format string) error {
	if format == "json" || format == "yaml" {
		return printProto(out, tag, format)
	}
	// table has no singular/plural distinction, so reuse the list renderer.
	return PrintTagsTo(out, []*ateapipb.Tag{tag}, format)
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

// PrintAtespaceTo prints a single atespace to the provided writer.
func PrintAtespaceTo(out io.Writer, atespace *ateapipb.Atespace, format string) error {
	if format == "json" || format == "yaml" {
		return printProto(out, atespace, format)
	}
	// table has no singular/plural distinction, so reuse the list renderer.
	return PrintAtespacesTo(out, []*ateapipb.Atespace{atespace}, format)
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
