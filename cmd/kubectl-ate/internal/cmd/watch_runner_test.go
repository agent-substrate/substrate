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
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

func TestPrintOrWatchPrintsOnce(t *testing.T) {
	actor := &ateapipb.Actor{Metadata: &ateapipb.ResourceMetadata{Name: "actor-1"}}
	var fetchCalls int
	var out bytes.Buffer

	err := printOrWatch(
		context.Background(),
		&out,
		false,
		"yaml",
		func(context.Context) ([]*ateapipb.Actor, error) {
			fetchCalls++
			return []*ateapipb.Actor{actor}, nil
		},
		func(out io.Writer, actors []*ateapipb.Actor, format string) error {
			if format != "yaml" {
				t.Errorf("format = %q, want yaml", format)
			}
			fmt.Fprint(out, actors[0].GetMetadata().GetName())
			return nil
		},
		nil,
	)
	if err != nil {
		t.Fatalf("printOrWatch() error = %v", err)
	}
	if fetchCalls != 1 {
		t.Errorf("fetch calls = %d, want 1", fetchCalls)
	}
	if got, want := out.String(), "actor-1"; got != want {
		t.Errorf("output = %q, want %q", got, want)
	}
}

func TestPrintOrWatchReturnsFetchError(t *testing.T) {
	wantErr := errors.New("fetch failed")
	err := printOrWatch(
		context.Background(),
		io.Discard,
		false,
		"table",
		func(context.Context) ([]*ateapipb.Actor, error) { return nil, wantErr },
		func(io.Writer, []*ateapipb.Actor, string) error { return nil },
		nil,
	)
	if !errors.Is(err, wantErr) {
		t.Fatalf("printOrWatch() error = %v, want %v", err, wantErr)
	}
}

func TestGetWatchRunnerPrintsOnlyChanges(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	responses := [][]*ateapipb.Actor{
		{{Metadata: &ateapipb.ResourceMetadata{Name: "actor-1", Version: 1}}},
		{{Metadata: &ateapipb.ResourceMetadata{Name: "actor-1", Version: 1}}},
		{{Metadata: &ateapipb.ResourceMetadata{Name: "actor-1", Version: 2}}},
	}
	var fetchCalls int
	var out bytes.Buffer
	runner := &getWatchRunner[*ateapipb.Actor]{
		fetch: func(context.Context) ([]*ateapipb.Actor, error) {
			response := responses[fetchCalls]
			fetchCalls++
			if fetchCalls == len(responses) {
				cancel()
			}
			return response, nil
		},
		print: func(out io.Writer, actors []*ateapipb.Actor) error {
			fmt.Fprintln(out, "HEADER")
			for _, actor := range actors {
				fmt.Fprintf(out, "%s %d\n", actor.GetMetadata().GetName(), actor.GetMetadata().GetVersion())
			}
			return nil
		},
		key:          func(actor *ateapipb.Actor) string { return actor.GetMetadata().GetName() },
		out:          &out,
		format:       "table",
		pollInterval: time.Millisecond,
	}

	err := runner.Run(ctx)
	if err != context.Canceled {
		t.Fatalf("Run() error = %v, want context.Canceled", err)
	}
	if got, want := out.String(), "HEADER\nactor-1 1\nactor-1 2\n"; got != want {
		t.Errorf("output = %q, want %q", got, want)
	}
}

func TestChangedResourcesIncludesAddedChangedAndDeleted(t *testing.T) {
	previous := map[string]*ateapipb.Actor{
		"unchanged": {Metadata: &ateapipb.ResourceMetadata{Name: "unchanged", Version: 1}},
		"changed":   {Metadata: &ateapipb.ResourceMetadata{Name: "changed", Version: 1}},
		"deleted":   {Metadata: &ateapipb.ResourceMetadata{Name: "deleted", Version: 1}},
	}
	current := map[string]*ateapipb.Actor{
		"unchanged": {Metadata: &ateapipb.ResourceMetadata{Name: "unchanged", Version: 1}},
		"changed":   {Metadata: &ateapipb.ResourceMetadata{Name: "changed", Version: 2}},
		"added":     {Metadata: &ateapipb.ResourceMetadata{Name: "added", Version: 1}},
	}

	changed := changedResources(previous, current)
	var names []string
	for _, actor := range changed {
		names = append(names, actor.GetMetadata().GetName())
	}
	for _, name := range []string{"added", "changed", "deleted"} {
		if !strings.Contains(strings.Join(names, ","), name) {
			t.Errorf("changed resources %v do not include %q", names, name)
		}
	}
	if len(names) != 3 {
		t.Errorf("changed resource count = %d, want 3", len(names))
	}
}
