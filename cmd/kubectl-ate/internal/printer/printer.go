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
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"k8s.io/apimachinery/pkg/api/resource"
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

func sortActors(actors []*ateapi.Actor) {
	slices.SortFunc(actors, func(a, b *ateapi.Actor) int {
		if c := cmp.Compare(a.GetMetadata().GetAtespace(), b.GetMetadata().GetAtespace()); c != 0 {
			return c
		}
		if c := cmp.Compare(actorTemplateDisplay(a), actorTemplateDisplay(b)); c != 0 {
			return c
		}
		return cmp.Compare(a.GetMetadata().GetName(), b.GetMetadata().GetName())
	})
}

// actorTemplateDisplay renders the template an actor references, in
// "<atespace>/<name>" form.
func actorTemplateDisplay(a *ateapi.Actor) string {
	ref := a.GetActorTemplate()
	return ref.GetAtespace() + "/" + ref.GetName()
}

// PrintActorsTo prints a slice of actors to the provided writer.
func PrintActorsTo(out io.Writer, actors []*ateapi.Actor, format string) error {
	sortActors(actors)
	switch format {
	case "json", "yaml":
		return printProto(out, &ateapi.ListActorsResponse{Actors: actors}, format)
	case "table":
		w := tabwriter.NewWriter(out, 0, 0, 3, ' ', 0)
		fmt.Fprintln(w, "ATESPACE\tNAME\tTEMPLATE\tSTATE\tWORKER POD\tWORKER IPS\tVERSION\tAGE")
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
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\t%d\t%s\n", atespace, name, template, state, worker, strings.Join(assignment.GetWorkerPodIps(), ","), version, age)
		}
		return w.Flush()
	default:
		return fmt.Errorf("unsupported format %q", format)
	}
}

// WorkerOccupancy is how full a Worker is, as a count against its limit. A
// count rather than the Actors themselves: a listing does not carry them, and
// naming them all would be unreadable long before a Worker is full.
func WorkerOccupancy(worker *ateapi.Worker) string {
	hosted := worker.GetStatus().GetAllocated().GetActors()
	if hosted == 0 {
		return "FREE"
	}
	return fmt.Sprintf("ASSIGNED(%d/%d)", hosted, worker.GetStatus().GetCapacity().GetActors())
}

func sortWorkers(workers []*ateapi.Worker) {
	slices.SortFunc(workers, func(a, b *ateapi.Worker) int {
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
func workerActorsColumn(worker *ateapi.Worker) string {
	return fmt.Sprintf("%d/%d", worker.GetStatus().GetAllocated().GetActors(), worker.GetStatus().GetCapacity().GetActors())
}

// workerResourceColumn renders one resource of a Worker as allocated/capacity
// (e.g. "500/2000"). "-" if the Worker has no capacity for it, since there is
// then nothing to compare against.
func workerResourceColumn(allocated, capacity int64, format func(int64) string) string {
	if capacity == 0 {
		return "-"
	}
	return format(allocated) + "/" + format(capacity)
}

// formatBytes renders bytes with binary suffixes, e.g. 2<<30 as "2Gi"
func formatBytes(n int64) string {
	return resource.NewQuantity(n, resource.BinarySI).String()
}

func formatInt(n int64) string {
	return strconv.FormatInt(n, 10)
}

// PrintWorkersTo prints a slice of workers to the provided writer.
func PrintWorkersTo(out io.Writer, workers []*ateapi.Worker, format string) error {
	sortWorkers(workers)
	switch format {
	case "json", "yaml":
		return printProto(out, &ateapi.ListWorkersResponse{Workers: workers}, format)
	case "table":
		w := tabwriter.NewWriter(out, 0, 0, 3, ' ', 0)
		fmt.Fprintln(w, "NAME\tPOOL\tSTATE\tACTORS\tCPU(MILLICORES)\tMEMORY\tPOD\tAGE")
		for _, worker := range workers {
			name := worker.GetMetadata().GetName()
			pool := worker.GetWorkerPool()
			state := worker.GetStatus().GetState().String()
			pod := worker.GetWorkerNamespace() + "/" + worker.GetWorkerPod()
			age := formatAge(worker.GetMetadata().GetCreateTime())
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
				name, pool, state, workerActorsColumn(worker),
				workerResourceColumn(worker.GetStatus().GetAllocated().GetCpuMillicores(),
					worker.GetStatus().GetCapacity().GetCpuMillicores(), formatInt),
				workerResourceColumn(worker.GetStatus().GetAllocated().GetMemoryBytes(),
					worker.GetStatus().GetCapacity().GetMemoryBytes(), formatBytes),
				pod, age)
		}
		return w.Flush()
	default:
		return fmt.Errorf("unsupported format %q", format)
	}
}

// PrintWorkerTo prints a single worker to the provided writer.
func PrintWorkerTo(out io.Writer, worker *ateapi.Worker, format string) error {
	if format == "json" || format == "yaml" {
		return printProto(out, worker, format)
	}
	// table has no singular/plural distinction, so reuse the list renderer.
	return PrintWorkersTo(out, []*ateapi.Worker{worker}, format)
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

// PrintWhoAmITo prints the caller's principal to the provided writer.
func PrintWhoAmITo(out io.Writer, resp *ateapi.WhoAmIResponse, format string) error {
	if format == "json" || format == "yaml" {
		return printProto(out, resp, format)
	}
	kind := strings.ToLower(strings.TrimPrefix(resp.GetKind().String(), "PRINCIPAL_KIND_"))
	w := tabwriter.NewWriter(out, 0, 0, 3, ' ', 0)
	fmt.Fprintln(w, "PRINCIPAL\tMETHOD\tISSUER")
	fmt.Fprintf(w, "%s\t%s\t%s\n", resp.GetId(), kind, resp.GetIssuer())
	return w.Flush()
}

// PrintActorTo prints a single actor to the provided writer.
func PrintActorTo(out io.Writer, actor *ateapi.Actor, format string) error {
	if format == "json" || format == "yaml" {
		return printProto(out, actor, format)
	}
	// table has no singular/plural distinction, so reuse the list renderer.
	return PrintActorsTo(out, []*ateapi.Actor{actor}, format)
}

func sortActorTemplates(templates []*ateapi.ActorTemplate) {
	slices.SortFunc(templates, func(a, b *ateapi.ActorTemplate) int {
		if c := cmp.Compare(a.GetMetadata().GetAtespace(), b.GetMetadata().GetAtespace()); c != 0 {
			return c
		}
		return cmp.Compare(a.GetMetadata().GetName(), b.GetMetadata().GetName())
	})
}

// PrintActorTemplatesTo prints a slice of actor templates to the provided writer.
func PrintActorTemplatesTo(out io.Writer, templates []*ateapi.ActorTemplate, format string) error {
	sortActorTemplates(templates)
	switch format {
	case "json", "yaml":
		return printProto(out, &ateapi.ListActorTemplatesResponse{ActorTemplates: templates}, format)
	case "table":
		w := tabwriter.NewWriter(out, 0, 0, 3, ' ', 0)
		fmt.Fprintln(w, "ATESPACE\tNAME\tGOLDEN TAG\tERROR\tAGE")
		for _, t := range templates {
			gss := t.GetStatus().GetGoldenSnapshotStatus()
			goldenTag := gss.GetGoldenTag().GetName()
			if t.GetSnapshotConfig().GetGoldenSnapshotConfig().GetMode() == ateapi.GoldenSnapshotMode_GOLDEN_SNAPSHOT_MODE_DISABLED {
				goldenTag = "DISABLED"
			}
			// Error messages are too long for a table cell.
			errFlag := ""
			if gss.GetErrorMessage() != "" {
				errFlag = "ERROR"
			}
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n",
				t.GetMetadata().GetAtespace(), t.GetMetadata().GetName(),
				goldenTag, errFlag,
				formatAge(t.GetMetadata().GetCreateTime()))
		}
		return w.Flush()
	default:
		return fmt.Errorf("unsupported format %q", format)
	}
}

// PrintActorTemplateTo prints a single actor template to the provided writer.
func PrintActorTemplateTo(out io.Writer, template *ateapi.ActorTemplate, format string) error {
	if format == "json" || format == "yaml" {
		return printProto(out, template, format)
	}
	// table has no singular/plural distinction, so reuse the list renderer.
	return PrintActorTemplatesTo(out, []*ateapi.ActorTemplate{template}, format)
}

// PrintTagsTo prints a slice of tags to the
// provided writer.
func PrintTagsTo(out io.Writer, tags []*ateapi.Tag, format string) error {
	slices.SortFunc(tags, func(a, b *ateapi.Tag) int {
		if c := cmp.Compare(a.GetMetadata().GetAtespace(), b.GetMetadata().GetAtespace()); c != 0 {
			return c
		}
		return cmp.Compare(a.GetMetadata().GetName(), b.GetMetadata().GetName())
	})
	switch format {
	case "json", "yaml":
		return printProto(out, &ateapi.ListTagsResponse{Tags: tags}, format)
	case "table":
		w := tabwriter.NewWriter(out, 0, 0, 3, ' ', 0)
		fmt.Fprintln(w, "ATESPACE\tNAME\tSCOPE\tSTATE\tSNAPSHOT\tFIDELITY\tAGE")
		for _, tag := range tags {
			// A pending tag has no snapshot yet, so neither its URI nor its
			// fidelity says anything.
			snapshotURI, fidelity := "<none>", "<none>"
			snap := tag.GetStatus().GetSnapshot()
			if st := durableSnapshotStorage(snap); st != nil {
				snapshotURI = st.GetObject().GetSnapshotUri()
				fidelity = snap.GetFidelity().String()
			}
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
				tag.GetMetadata().GetAtespace(), tag.GetMetadata().GetName(), tag.GetScope(),
				tagState(tag), snapshotURI, fidelity,
				formatAge(tag.GetMetadata().GetCreateTime()))
		}
		return w.Flush()
	default:
		return fmt.Errorf("unsupported format %q", format)
	}
}

func durableSnapshotStorage(snapshot *ateapi.Snapshot) *ateapi.SnapshotStorage {
	st := snapshot.GetDurableSnapshot()
	if st.GetStatus() == ateapi.SnapshotStorageStatus_SNAPSHOT_STORAGE_STATUS_COMPLETED &&
		st.GetObject().GetSnapshotUri() != "" {
		return st
	}
	return nil
}

// tagState reports whether a tag is usable. A tag is Pending until
// the copy of its own snapshot lands; until then it names nothing an Actor can
// be created from, and deleting it collects whatever the create stranded.
func tagState(tag *ateapi.Tag) string {
	if durableSnapshotStorage(tag.GetStatus().GetSnapshot()) == nil {
		return "Pending"
	}
	return "Ready"
}

// PrintTagTo prints a single tag to the provided writer.
func PrintTagTo(out io.Writer, tag *ateapi.Tag, format string) error {
	if format == "json" || format == "yaml" {
		return printProto(out, tag, format)
	}
	// table has no singular/plural distinction, so reuse the list renderer.
	return PrintTagsTo(out, []*ateapi.Tag{tag}, format)
}

func sortAtespaces(atespaces []*ateapi.Atespace) {
	slices.SortFunc(atespaces, func(a, b *ateapi.Atespace) int {
		return cmp.Compare(a.GetMetadata().GetName(), b.GetMetadata().GetName())
	})
}

// PrintAtespacesTo prints a slice of atespaces to the provided writer.
func PrintAtespacesTo(out io.Writer, atespaces []*ateapi.Atespace, format string) error {
	sortAtespaces(atespaces)
	switch format {
	case "json", "yaml":
		return printProto(out, &ateapi.ListAtespacesResponse{Atespaces: atespaces}, format)
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
func PrintAtespaceTo(out io.Writer, atespace *ateapi.Atespace, format string) error {
	if format == "json" || format == "yaml" {
		return printProto(out, atespace, format)
	}
	// table has no singular/plural distinction, so reuse the list renderer.
	return PrintAtespacesTo(out, []*ateapi.Atespace{atespace}, format)
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
