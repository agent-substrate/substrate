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

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"sigs.k8s.io/yaml"
)

// PrintActors prints a slice of actors to stdout in the requested format.
func PrintActors(actors []*ateapipb.Actor, format string) error {
	return PrintActorsTo(os.Stdout, actors, format)
}

func sortActors(actors []*ateapipb.Actor) {
	slices.SortFunc(actors, func(a, b *ateapipb.Actor) int {
		if c := cmp.Compare(a.GetActorTemplateNamespace(), b.GetActorTemplateNamespace()); c != 0 {
			return c
		}
		if c := cmp.Compare(a.GetActorTemplateName(), b.GetActorTemplateName()); c != 0 {
			return c
		}
		return cmp.Compare(a.GetActorId(), b.GetActorId())
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
		fmt.Fprintln(w, "NAMESPACE\tTEMPLATE\tID\tSTATUS\tATEOM POD\tATEOM IP\tVERSION")
		for _, actor := range actors {
			ns := actor.GetActorTemplateNamespace()
			tmpl := actor.GetActorTemplateName()
			id := actor.GetActorId()
			status := actor.GetStatus().String()

			worker := "<none>"
			if actor.GetAteomPodNamespace() != "" {
				worker = actor.GetAteomPodNamespace() + "/" + actor.GetAteomPodName()
			}

			version := actor.GetVersion()
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\t%d\n", ns, tmpl, id, status, worker, actor.GetAteomPodIp(), version)
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
		fmt.Fprintln(w, "NAMESPACE\tPOOL\tPOD\tSTATUS\tASSIGNED ACTOR")
		for _, worker := range workers {
			ns := worker.GetWorkerNamespace()
			pool := worker.GetWorkerPool()
			pod := worker.GetWorkerPod()

			status := "FREE"
			assignedActor := "<none>"
			if worker.GetActorId() != "" {
				status = "ASSIGNED"
				assignedActor = fmt.Sprintf("%s/%s/%s", worker.GetActorNamespace(), worker.GetActorTemplate(), worker.GetActorId())
			}

			fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n", ns, pool, pod, status, assignedActor)
		}
		return w.Flush()
	default:
		return fmt.Errorf("unsupported format %q", format)
	}
}

// PrintActor prints a single actor in the requested format.
func PrintActor(actor *ateapipb.Actor, format string) error {
	return PrintActors([]*ateapipb.Actor{actor}, format)
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
