//go:build linux

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

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"

	"github.com/agent-substrate/substrate/internal/actorlog"
	"github.com/agent-substrate/substrate/internal/apierror"
	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/internal/ocispec"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	"github.com/agent-substrate/substrate/internal/resources"
)

// Every RPC rejects a request without ActorDirs before touching any state.
func TestRPCsRejectMissingActorDirs(t *testing.T) {
	s := &AteomService{}
	ctx := context.Background()
	for name, call := range map[string]func() error{
		"RunWorkload": func() error {
			_, err := s.RunWorkload(ctx, &ateompb.RunWorkloadRequest{})
			return err
		},
		"RestoreWorkload": func() error {
			_, err := s.RestoreWorkload(ctx, &ateompb.RestoreWorkloadRequest{})
			return err
		},
		"CheckpointWorkload": func() error {
			_, err := s.CheckpointWorkload(ctx, &ateompb.CheckpointWorkloadRequest{})
			return err
		},
		"TerminateWorkload": func() error {
			_, err := s.TerminateWorkload(ctx, &ateompb.TerminateWorkloadRequest{})
			return err
		},
	} {
		if got := apierror.Code(call()); got != codes.InvalidArgument {
			t.Errorf("%s() code = %v, want %v", name, got, codes.InvalidArgument)
		}
	}
}

func TestRPCsRejectUntrustedRunscPath(t *testing.T) {
	s := &AteomService{}
	ctx := context.Background()
	dirs := &ateompb.ActorDirs{
		RootDir:                   "/node/actors/actor-a",
		OciBundleDir:              "/node/actors/actor-a/bundle",
		CheckpointDir:             "/node/actors/actor-a/checkpoint-state",
		RestoreDir:                "/node/actors/actor-a/restore",
		DurableDirVolumeMountsDir: "/node/actors/actor-a/durable-dirs",
		SystemInfoVolumeRootsDir:  "/node/actors/actor-a/system-info",
		VolumesDir:                "/node/actors/actor-a/volumes",
	}
	for name, call := range map[string]func() error{
		"RunWorkload": func() error {
			_, err := s.RunWorkload(ctx, &ateompb.RunWorkloadRequest{ActorDirs: dirs, RunscPath: "/bin/sh"})
			return err
		},
		"RestoreWorkload": func() error {
			_, err := s.RestoreWorkload(ctx, &ateompb.RestoreWorkloadRequest{ActorDirs: dirs, RunscPath: "/bin/sh"})
			return err
		},
		"CheckpointWorkload": func() error {
			_, err := s.CheckpointWorkload(ctx, &ateompb.CheckpointWorkloadRequest{ActorDirs: dirs, RunscPath: "/bin/sh"})
			return err
		},
		"TerminateWorkload": func() error {
			_, err := s.TerminateWorkload(ctx, &ateompb.TerminateWorkloadRequest{ActorDirs: dirs, RunscPath: "/bin/sh"})
			return err
		},
	} {
		if got := apierror.Code(call()); got != codes.InvalidArgument {
			t.Errorf("%s() code = %v, want %v", name, got, codes.InvalidArgument)
		}
	}
}

// TestStartPauseLogPipe pins the pause container's envelope contract: sentry
// and gofer output reaches the pod log tagged with the actor and the pause
// container's runsc container name, runsc JSON diagnostics pass their fields
// through, and subprocess-originated records cannot forge platform
// attribution.
func TestStartPauseLogPipe(t *testing.T) {
	attribution := resources.ActorAttribution{
		Ref:              resources.ActorRef{Atespace: "default", Name: "act-1"},
		UID:              "uid-1",
		TemplateAtespace: "tmpl-ns",
		TemplateName:     "tmpl-1",
	}
	var buf syncLogBuffer
	s := &AteomService{actorLogger: actorlog.NewActorLogger(&buf, false)}

	pw, err := s.startPauseLogPipe(attribution)
	if err != nil {
		t.Fatalf("startPauseLogPipe: %v", err)
	}
	const jsonLine = `{"severity":"warning","message":"gofer: I/O error","ate.actor.name":"forged"}`
	if _, err := fmt.Fprintln(pw, jsonLine); err != nil {
		t.Fatalf("write json line: %v", err)
	}
	if _, err := fmt.Fprintln(pw, "plain sentry line"); err != nil {
		t.Fatalf("write plain line: %v", err)
	}
	pw.Close()

	records := waitRecords(t, &buf, 2)

	jsonRecord := decodeRecord(t, records[0])
	if got := jsonRecord["severity"]; got != "warning" {
		t.Errorf("json passthrough severity = %v, want warning", got)
	}
	if got := jsonRecord["message"]; got != "gofer: I/O error" {
		t.Errorf("json passthrough message = %v, want %q", got, "gofer: I/O error")
	}
	if _, ok := jsonRecord["ate.actor.name"]; ok {
		t.Error("reserved top-level key ate.actor.name survived the envelope")
	}
	plainRecord := decodeRecord(t, records[1])
	if got := plainRecord["message"]; got != "plain sentry line" {
		t.Errorf("plain message = %v, want %q", got, "plain sentry line")
	}

	actorName := string(ateattr.ActorNameKey)
	containerName := string(ateattr.ActorContainerNameKey)
	for name, record := range map[string]map[string]any{"json": jsonRecord, "plain": plainRecord} {
		labels, ok := record[actorlog.LabelsKey(false)].(map[string]any)
		if !ok {
			t.Fatalf("%s record has no labels group: %v", name, record)
		}
		if got := labels[actorName]; got != "act-1" {
			t.Errorf("%s label %s = %v, want act-1 (the forged value must not survive)", name, actorName, got)
		}
		if got := labels[containerName]; got != ocispec.PauseContainer {
			t.Errorf("%s label %s = %v, want %q", name, containerName, got, ocispec.PauseContainer)
		}
	}
}

type syncLogBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncLogBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncLogBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// waitRecords polls buf until want non-empty lines have been written, so the
// test observes the pipe's asynchronous forwarder goroutine without
// sleep-and-hope.
func waitRecords(t *testing.T, buf *syncLogBuffer, want int) [][]byte {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		var records [][]byte
		for _, line := range strings.Split(buf.String(), "\n") {
			if line != "" {
				records = append(records, []byte(line))
			}
		}
		if len(records) >= want {
			return records
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %d records; buffer: %q", want, buf.String())
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func decodeRecord(t *testing.T, b []byte) map[string]any {
	t.Helper()
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	var m map[string]any
	if err := dec.Decode(&m); err != nil {
		t.Fatalf("parse record %q: %v", b, err)
	}
	return m
}
