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
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/sys/unix"
	"google.golang.org/grpc/codes"

	"go.ate.dev/substrate/internal/actorlock"
	"go.ate.dev/substrate/internal/actorlog"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/ateattr"
	"go.ate.dev/substrate/internal/atunnel"
	"go.ate.dev/substrate/internal/atunnel/egress"
	"go.ate.dev/substrate/internal/atunnel/ingress"
	"go.ate.dev/substrate/internal/linuxsys"
	"go.ate.dev/substrate/internal/nodepath"
	"go.ate.dev/substrate/internal/ocispec"
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/internal/roottest"
)

// Every RPC with a request body rejects an empty one before touching any
// state: the generated validators run first in each handler.
func TestRPCsRejectInvalidRequests(t *testing.T) {
	s := &AteWorkerService{}
	ctx := context.Background()
	for name, call := range map[string]func() error{
		"RunWorkload": func() error {
			_, err := s.RunWorkload(ctx, &ateworkerpb.RunWorkloadRequest{})
			return err
		},
		"RestoreWorkload": func() error {
			_, err := s.RestoreWorkload(ctx, &ateworkerpb.RestoreWorkloadRequest{})
			return err
		},
		"CheckpointWorkload": func() error {
			_, err := s.CheckpointWorkload(ctx, &ateworkerpb.CheckpointWorkloadRequest{})
			return err
		},
		"TerminateWorkload": func() error {
			_, err := s.TerminateWorkload(ctx, &ateworkerpb.TerminateWorkloadRequest{})
			return err
		},
		"GetWorkloadStats": func() error {
			_, err := s.GetWorkloadStats(ctx, &ateworkerpb.GetWorkloadStatsRequest{})
			return err
		},
	} {
		if got := apierror.Code(call()); got != codes.InvalidArgument {
			t.Errorf("%s() code = %v, want %v", name, got, codes.InvalidArgument)
		}
	}
}

// A worker process starts no workload before it registers: the control plane
// could not tell which process ran it. Unavailable so the caller retries.
func TestStartupRPCsRejectedBeforeRegistration(t *testing.T) {
	s := &AteWorkerService{}
	ctx := context.Background()
	for name, call := range map[string]func() error{
		"RunWorkload": func() error {
			_, err := s.RunWorkload(ctx, validRunRequest())
			return err
		},
		"RestoreWorkload": func() error {
			_, err := s.RestoreWorkload(ctx, validRestoreRequest())
			return err
		},
	} {
		if got := apierror.Code(call()); got != codes.Unavailable {
			t.Errorf("%s() before registration code = %v, want %v", name, got, codes.Unavailable)
		}
	}
}

func TestRPCsRejectUntrustedRunscPath(t *testing.T) {
	s := &AteWorkerService{}
	s.epoch.Store(1)
	ctx := context.Background()
	for name, call := range map[string]func() error{
		"RunWorkload": func() error {
			req := validRunRequest()
			req.RunscPath = "/bin/sh"
			_, err := s.RunWorkload(ctx, req)
			return err
		},
		"RestoreWorkload": func() error {
			req := validRestoreRequest()
			req.RunscPath = "/bin/sh"
			_, err := s.RestoreWorkload(ctx, req)
			return err
		},
		"CheckpointWorkload": func() error {
			req := validCheckpointRequest()
			req.RunscPath = "/bin/sh"
			_, err := s.CheckpointWorkload(ctx, req)
			return err
		},
		"TerminateWorkload": func() error {
			req := validTerminateRequest()
			req.RunscPath = "/bin/sh"
			_, err := s.TerminateWorkload(ctx, req)
			return err
		},
	} {
		err := call()
		if apierror.Code(err) != codes.InvalidArgument || !strings.Contains(err.Error(), "runsc_path") {
			t.Errorf("%s() = %v, want InvalidArgument naming runsc_path", name, err)
		}
	}
}

func TestRestoreWorkloadRejectsUnsupportedFidelity(t *testing.T) {
	// A zero service cannot acquire locks, configure networking, or open log pipes.
	s := &AteWorkerService{}
	for _, fidelity := range []ateworkerpb.SnapshotFidelity{0, 99} {
		t.Run(fidelity.String(), func(t *testing.T) {
			// Valid but for the fidelity, so the rejection is the fidelity's;
			// the fidelity check runs before the runsc path is looked at.
			req := validRestoreRequest()
			req.Fidelity = fidelity
			resp, err := s.RestoreWorkload(context.Background(), req)
			if resp != nil || apierror.Code(err) != codes.InvalidArgument || !strings.Contains(err.Error(), "fidelity") {
				t.Errorf("RestoreWorkload() = (%v, %v), want InvalidArgument naming the fidelity", resp, err)
			}
		})
	}
}

// The requests below describe testActor and pass the generated validators, so
// a test aimed at one of the hand checks that run after them (the runsc path,
// the fidelity) sees that check's error rather than an unrelated rejection.

func testDirs() *ateworkerpb.ActorDirs {
	return &ateworkerpb.ActorDirs{
		RootDir:                   "/node/actors/actor-a",
		OciBundleDir:              "/node/actors/actor-a/bundle",
		CheckpointDir:             "/node/actors/actor-a/checkpoint-state",
		RestoreDir:                "/node/actors/actor-a/restore-state",
		DurableDirVolumeMountsDir: "/node/actors/actor-a/durable-dirs",
		SystemInfoVolumeRootsDir:  "/node/actors/actor-a/system-info",
		ExternalVolumeMountsDir:   "/node/actors/actor-a/volumes",
		SandboxAssetsDir:          nodepath.ActorSandboxAssetsDir(testActor.UID),
	}
}

func testSpec() *ateworkerpb.WorkloadSpec {
	return &ateworkerpb.WorkloadSpec{Containers: []*ateworkerpb.Container{{Name: "app"}}}
}

func validRunRequest() *ateworkerpb.RunWorkloadRequest {
	return &ateworkerpb.RunWorkloadRequest{Atespace: testActor.Ref.Atespace, ActorName: testActor.Ref.Name, ActorUid: testActor.UID, Spec: testSpec(), ActorDirs: testDirs()}
}

func validCheckpointRequest() *ateworkerpb.CheckpointWorkloadRequest {
	return &ateworkerpb.CheckpointWorkloadRequest{Atespace: testActor.Ref.Atespace, ActorName: testActor.Ref.Name, ActorUid: testActor.UID, Spec: testSpec(), ActorDirs: testDirs(), Fidelity: ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY}
}

func validRestoreRequest() *ateworkerpb.RestoreWorkloadRequest {
	return &ateworkerpb.RestoreWorkloadRequest{Atespace: testActor.Ref.Atespace, ActorName: testActor.Ref.Name, ActorUid: testActor.UID, Spec: testSpec(), ActorDirs: testDirs(), Fidelity: ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY}
}

func validTerminateRequest() *ateworkerpb.TerminateWorkloadRequest {
	return &ateworkerpb.TerminateWorkloadRequest{Atespace: testActor.Ref.Atespace, ActorName: testActor.Ref.Name, ActorUid: testActor.UID, Spec: testSpec(), ActorDirs: testDirs()}
}

// TestSandboxLogAttribution covers the actor and _sandbox labels on runsc
// CLI output, JSON field preservation, and rejection of forged attribution.
func TestSandboxLogAttribution(t *testing.T) {
	attribution := resources.ActorAttribution{
		Ref:              resources.ActorRef{Atespace: "default", Name: "act-1"},
		UID:              "uid-1",
		TemplateAtespace: "tmpl-ns",
		TemplateName:     "tmpl-1",
	}
	var buf syncLogBuffer
	logger := actorlog.NewActorLogger(&buf, false)

	pw, err := logger.StartJSONLogPipe(attribution, ocispec.SandboxID)
	if err != nil {
		t.Fatalf("StartJSONLogPipe: %v", err)
	}
	const logTime = "2026-10-08T03:40:00Z"
	const jsonMessage = "container.go:123] cannot start container"
	const jsonLine = `{"msg":"container.go:123] cannot start container","level":"warning","time":"2026-10-08T03:40:00Z","ate.actor.name":"forged"}`
	if _, err := fmt.Fprintln(pw, jsonLine); err != nil {
		t.Fatalf("write json line: %v", err)
	}
	const plainLine = "I1008 03:40:00.000000 1234 main.go:123] Args: [runsc --alsologtostderr create _sandbox]"
	if _, err := fmt.Fprintln(pw, plainLine); err != nil {
		t.Fatalf("write plain line: %v", err)
	}
	pw.Close()

	records := waitRecords(t, &buf, 2)

	jsonRecord := decodeRecord(t, records[0])
	if got := jsonRecord["level"]; got != "warning" {
		t.Errorf("json passthrough level = %v, want warning", got)
	}
	if got := jsonRecord["msg"]; got != jsonMessage {
		t.Errorf("json passthrough msg = %v, want %q", got, jsonMessage)
	}
	if got := jsonRecord["time"]; got != logTime {
		t.Errorf("json passthrough time = %v, want %q", got, logTime)
	}
	if _, ok := jsonRecord["ate.actor.name"]; ok {
		t.Error("reserved top-level key ate.actor.name survived the envelope")
	}
	plainRecord := decodeRecord(t, records[1])
	if got := plainRecord["message"]; got != plainLine {
		t.Errorf("plain message = %v, want %q", got, plainLine)
	}

	actorName := string(ateattr.ActorNameKey)
	containerName := string(ateattr.ActorContainerNameKey)
	for name, record := range map[string]map[string]any{"json": jsonRecord, "plain": plainRecord} {
		labels, ok := record["labels"].(map[string]any)
		if !ok {
			t.Fatalf("%s record has no labels group: %v", name, record)
		}
		if got := labels[actorName]; got != "act-1" {
			t.Errorf("%s label %s = %v, want act-1 (the forged value must not survive)", name, actorName, got)
		}
		if got := labels[containerName]; got != ocispec.SandboxID {
			t.Errorf("%s label %s = %v, want %q", name, containerName, got, ocispec.SandboxID)
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

// TestValidateRunscPathRequiresTheActorsOwnDir pins that the root the runsc
// path is checked against is derived from the actor UID, not taken from the
// request: a request cannot name a root of its own choosing.
func TestValidateRunscPathRequiresTheActorsOwnDir(t *testing.T) {
	orig := nodepath.ActorsDir
	nodepath.ActorsDir = t.TempDir()
	t.Cleanup(func() { nodepath.ActorsDir = orig })
	stage := func(t *testing.T, actorUID string) string {
		t.Helper()
		dir := nodepath.ActorSandboxAssetsDir(actorUID)
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
		runsc := filepath.Join(dir, "runsc")
		if err := os.WriteFile(runsc, nil, 0o555); err != nil {
			t.Fatal(err)
		}
		return runsc
	}
	own, other := stage(t, "actor-a"), stage(t, "actor-b")
	dirs := func(sandboxAssetsDir string) *ateworkerpb.ActorDirs {
		return &ateworkerpb.ActorDirs{SandboxAssetsDir: sandboxAssetsDir}
	}
	for _, tc := range []struct {
		name    string
		dir     string
		runsc   string
		wantErr bool
	}{
		{"runsc in the actor's own dir", filepath.Dir(own), own, false},
		{"runsc outside the actor's dir", filepath.Dir(own), "/bin/sh", true},
		{"another actor's dir named as the root", filepath.Dir(other), other, true},
		{"the root named as the root", "/", "/bin/sh", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := validateRunscPath("actor-a", dirs(tc.dir), tc.runsc)
			if (err != nil) != tc.wantErr {
				t.Errorf("validateRunscPath(actor-a, %q, %q) = %v, wantErr %v", tc.dir, tc.runsc, err, tc.wantErr)
			}
			if err != nil && apierror.Code(err) != codes.InvalidArgument {
				t.Errorf("code = %v, want %v", apierror.Code(err), codes.InvalidArgument)
			}
		})
	}
}

// newTestService returns an AteWorkerService that hosts no actors, with a
// tunnel no actor is active on.
func newTestService(t *testing.T) *AteWorkerService {
	t.Helper()
	egressProxy, err := egress.New(linuxsys.TCPOriginalDestination)
	if err != nil {
		t.Fatal(err)
	}
	return &AteWorkerService{
		locks:       actorlock.New(),
		actors:      map[string]*hostedActor{},
		tunnel:      &atunnel.Tunnel{Ingress: &ingress.Server{}, Egress: egressProxy},
		actorLogger: actorlog.NewActorLogger(io.Discard, false),
	}
}

// newTerminateRequest returns a TerminateWorkload request for an actor
// rooted in a temp dir. Its runsc is a script that creates the returned path.
func newTerminateRequest(t *testing.T) (req *ateworkerpb.TerminateWorkloadRequest, runscInvoked string) {
	t.Helper()
	tmp := t.TempDir()
	origActors := nodepath.ActorsDir
	nodepath.ActorsDir = filepath.Join(tmp, "actors")
	t.Cleanup(func() { nodepath.ActorsDir = origActors })
	root := filepath.Join(nodepath.ActorsDir, testActor.UID)
	actorDirs := &ateworkerpb.ActorDirs{
		RootDir:                   root,
		OciBundleDir:              filepath.Join(root, "bundles"),
		CheckpointDir:             filepath.Join(root, "checkpoint"),
		RestoreDir:                filepath.Join(root, "restore"),
		DurableDirVolumeMountsDir: filepath.Join(root, "durable"),
		SystemInfoVolumeRootsDir:  filepath.Join(root, "systeminfo"),
		ExternalVolumeMountsDir:   filepath.Join(root, "volumes"),
		SandboxAssetsDir:          nodepath.ActorSandboxAssetsDir(testActor.UID),
	}
	runscInvoked = filepath.Join(tmp, "runsc-invoked")
	if err := os.MkdirAll(actorDirs.GetSandboxAssetsDir(), 0o700); err != nil {
		t.Fatal(err)
	}
	runscPath := filepath.Join(actorDirs.GetSandboxAssetsDir(), "runsc")
	if err := os.WriteFile(runscPath, []byte("#!/bin/sh\ntouch "+runscInvoked+"\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	return &ateworkerpb.TerminateWorkloadRequest{
		Atespace:  testActor.Ref.Atespace,
		ActorName: testActor.Ref.Name,
		ActorUid:  testActor.UID,
		RunscPath: runscPath,
		ActorDirs: actorDirs,
		Spec:      testSpec(),
	}, runscInvoked
}

// An actor this worker does not host, as after a worker restart, terminates
// without runsc: the PIDs in the runsc state it left are not its sandbox's.
// Repeating the call succeeds too.
func TestTerminateWorkloadOfUnhostedActorSkipsRunsc(t *testing.T) {
	req, invoked := newTerminateRequest(t)
	actorDirs := req.GetActorDirs()
	// What a dead worker leaves behind for the actor.
	for _, f := range []string{
		filepath.Join(runscStateDir(actorDirs), "pause.state"),
		filepath.Join(pidFileDir(actorDirs), "pause.pid"),
	} {
		if err := os.MkdirAll(filepath.Dir(f), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(f, []byte("stale"), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	s := newTestService(t)
	for attempt := range 2 {
		if _, err := s.TerminateWorkload(t.Context(), req); err != nil {
			t.Fatalf("TerminateWorkload() attempt %d error = %v, want nil", attempt, err)
		}
	}

	if _, err := os.Stat(invoked); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("runsc was invoked (stat err = %v), want no runsc calls", err)
	}
	for _, dir := range []string{runscStateDir(actorDirs), pidFileDir(actorDirs)} {
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Errorf("ReadDir(%q) error = %v, want an empty dir", dir, err)
			continue
		}
		if len(entries) != 0 {
			t.Errorf("%q holds %d entries, want none", dir, len(entries))
		}
	}
}

// An earlier TerminateWorkload can unhost an actor after failing to unmount
// its bundle rootfs overlays. Retrying it unmounts them, though it skips runsc.
func TestTerminateWorkloadOfUnhostedActorUnmountsBundles(t *testing.T) {
	roottest.Require(t, "mount/unmount")
	req, _ := newTerminateRequest(t)
	target := filepath.Join(req.GetActorDirs().GetOciBundleDir(), "app", "rootfs")
	src := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "marker"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(target, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := unix.Mount(src, target, "", unix.MS_BIND, ""); err != nil {
		t.Fatalf("bind mount: %v", err)
	}
	t.Cleanup(func() { _ = unix.Unmount(target, unix.MNT_DETACH) })

	if _, err := newTestService(t).TerminateWorkload(t.Context(), req); err != nil {
		t.Fatalf("TerminateWorkload() error = %v, want nil", err)
	}

	if _, err := os.Stat(filepath.Join(target, "marker")); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("%q is still mounted (stat err = %v), want it unmounted", target, err)
	}
}
