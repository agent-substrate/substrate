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
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"

	"go.ate.dev/substrate/cmd/ateworker-microvm/internal/ch"
	"go.ate.dev/substrate/cmd/ateworker-microvm/internal/child"
	"go.ate.dev/substrate/internal/actorlock"
	"go.ate.dev/substrate/internal/actorlog"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
)

// shortSocketPath returns a unix socket path short enough for sun_path.
func shortSocketPath(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "ch-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return filepath.Join(dir, "api.sock")
}

// notReadyErr is a real WaitReady deadline error from a socket nobody serves.
func notReadyErr(t *testing.T) error {
	t.Helper()
	_, err := ch.NewClient(shortSocketPath(t)).WaitReady(t.Context(), 20*time.Millisecond)
	if !errors.Is(err, ch.ErrNotReady) {
		t.Fatalf("WaitReady on a dead socket = %v, want ch.ErrNotReady", err)
	}
	return err
}

// liveVM is a running actor whose VMM runs until the test ends.
func liveVM(t *testing.T) *runningActor {
	t.Helper()
	chProc, err := child.Start(exec.Command("sleep", "30"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(chProc.Stop)
	return &runningActor{chProc: chProc}
}

func TestCheckpointWaitReadyError(t *testing.T) {
	notReady := notReadyErr(t)
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	exitedVM := func(t *testing.T) *runningActor {
		ra := fakeVM(t)
		<-ra.chProc.Done()
		return ra
	}
	for _, tc := range []struct {
		name string
		ctx  context.Context
		ra   func(*testing.T) *runningActor
		err  error
		want codes.Code
	}{
		{"live VMM not answering", t.Context(), liveVM, notReady, codes.Unavailable},
		{"VMM exited", t.Context(), exitedVM, notReady, codes.Internal},
		{"no tracked VMM", t.Context(), func(*testing.T) *runningActor { return nil }, notReady, codes.Internal},
		{"caller gone", canceled, liveVM, context.Canceled, codes.Canceled},
		{"other error", t.Context(), liveVM, errors.New("permission denied"), codes.Internal},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := checkpointWaitReadyError(tc.ctx, tc.ra(t), tc.err)
			if got := apierror.Code(err); got != tc.want {
				t.Errorf("apierror.Code(err) = %v, want %v (err: %v)", got, tc.want, err)
			}
			if !errors.Is(err, tc.err) {
				t.Errorf("err = %v, want it to wrap %v", err, tc.err)
			}
		})
	}
}

// serveVMMPing serves a VMM api-socket that answers vmm.ping.
func serveVMMPing(t *testing.T) string {
	t.Helper()
	sock := shortSocketPath(t)
	lis, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/vmm.ping" {
			http.NotFound(w, r)
			return
		}
		_, _ = io.WriteString(w, `{"version":"53.0.0"}`)
	})}
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(func() { srv.Close() })
	return sock
}

// A checkpoint that can't reach the VMM fails before it deactivates the
// actor's networking, so the actor keeps serving. The service has no tunnel,
// so reaching the deactivation panics; with a VMM that answers, it does.
func TestCheckpointReachesVMMBeforeDeactivating(t *testing.T) {
	for _, tc := range []struct {
		name            string
		socket          func(*testing.T) string
		wantDeactivated bool
	}{
		{"VMM not answering", shortSocketPath, false},
		{"VMM answering", serveVMMPing, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &AteWorkerService{
				locks:       actorlock.New(),
				inFlight:    actorlock.NewInFlight(),
				actors:      map[string]*hostedActor{testActor.UID: {attribution: testActor, usage: testActivation()}},
				actorLogger: actorlog.NewActorLogger(io.Discard, false),
			}
			ra := liveVM(t)
			ra.apiSocket = tc.socket(t)
			s.setRunningVM(testActor.UID, ra)
			req := &ateworkerpb.CheckpointWorkloadRequest{
				Atespace:  testActor.Ref.Atespace,
				ActorName: testActor.Ref.Name,
				ActorUid:  testActor.UID,
				Spec:      testSpec(),
				ActorDirs: testDirs(),
				Fidelity:  ateworkerpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY,
			}

			ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
			defer cancel()
			var err error
			deactivated := func() (panicked bool) {
				defer func() { panicked = recover() != nil }()
				_, err = s.CheckpointWorkload(ctx, req)
				return false
			}()
			if deactivated != tc.wantDeactivated {
				t.Fatalf("reached deactivation = %v, want %v (err: %v)", deactivated, tc.wantDeactivated, err)
			}
			if !tc.wantDeactivated && (err == nil || !strings.Contains(err.Error(), "while waiting for CH api-socket")) {
				t.Errorf("CheckpointWorkload = %v, want the api-socket wait to fail", err)
			}
			if s.runningVM(testActor.UID) == nil {
				t.Error("the failed checkpoint unhosted the actor")
			}
		})
	}
}
