//go:build linux

// Copyright 2026 The Agent Substrate Authors
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
	"bufio"
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/containerd/ttrpc"
	"go.ate.dev/substrate/cmd/ateworker-microvm/internal/kata"
	"go.ate.dev/substrate/cmd/ateworker-microvm/internal/third_party/kata/agentpb"
	"go.ate.dev/substrate/internal/resources"
)

// vsockHandshakeListener answers the hybrid-vsock CONNECT handshake on every
// accepted connection, so kata.DialAgent can dial a plain unix socket serving
// ttrpc behind it — the shape a real agent connection has through the VMM.
type vsockHandshakeListener struct{ net.Listener }

func (l vsockHandshakeListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	line, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil || !strings.HasPrefix(line, "CONNECT ") {
		conn.Close()
		return nil, errors.New("bad hybrid-vsock CONNECT")
	}
	if _, err := conn.Write([]byte("OK 1\n")); err != nil {
		conn.Close()
		return nil, err
	}
	return conn, nil
}

// startFakeOOMAgent serves grpc.AgentService/GetOOMEvent behind a
// hybrid-vsock-style handshake. Each call consumes one container id from
// events and answers with it; a call with no id pending blocks until the test
// ends, so a watch sitting in a GetOOMEvent call can be released by sending an
// id or ended by closing the client.
func startFakeOOMAgent(t *testing.T, events <-chan string) *kata.AgentClient {
	t.Helper()
	sock := filepath.Join(t.TempDir(), "agent.sock")
	l, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv, err := ttrpc.NewServer()
	if err != nil {
		t.Fatalf("ttrpc.NewServer: %v", err)
	}
	srv.Register("grpc.AgentService", map[string]ttrpc.Method{
		"GetOOMEvent": func(_ context.Context, unmarshal func(interface{}) error) (interface{}, error) {
			var req agentpb.GetOOMEventRequest
			if err := unmarshal(&req); err != nil {
				return nil, err
			}
			select {
			case id := <-events:
				return &agentpb.OOMEvent{ContainerId: id}, nil
			case <-t.Context().Done():
				return nil, errors.New("test over")
			}
		},
	})
	go func() { _ = srv.Serve(context.Background(), vsockHandshakeListener{l}) }()
	t.Cleanup(func() { _ = srv.Close() })

	ac, err := kata.DialAgent(context.Background(), sock)
	if err != nil {
		t.Fatalf("dial fake agent: %v", err)
	}
	t.Cleanup(func() { _ = ac.Close() })
	return ac
}

// oomWatchTestService is the slice of AteWorkerService watchOOMEvents reads.
func oomWatchTestService() *AteWorkerService {
	return &AteWorkerService{}
}

func oomWatchActor(ac *kata.AgentClient, stopping bool) *runningActor {
	ra := &runningActor{
		guestAgent:    ac,
		workloadIDs:   []string{"containers+memhog"},
		workloadNames: map[string]string{"containers+memhog": "memhog"},
	}
	ra.stopping.Store(stopping)
	return ra
}

// lockedWriter is a bytes.Buffer safe for the watch goroutine (slog writes)
// and the test goroutine (polling reads) to share.
type lockedWriter struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (w *lockedWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Write(p)
}

func (w *lockedWriter) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.String()
}

// captureSlog redirects the default logger into a buffer for the test.
func captureSlog(t *testing.T) *lockedWriter {
	t.Helper()
	w := &lockedWriter{}
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(w, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return w
}

// waitForLog polls buf until it contains all the wants, or the deadline hits.
func waitForLog(t *testing.T, buf *lockedWriter, wants ...string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		ok := true
		for _, want := range wants {
			if !strings.Contains(buf.String(), want) {
				ok = false
				break
			}
		}
		if ok {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("log = %q, want it to contain %q", buf.String(), wants)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestWatchOOMEventsLogsKillWithActorAttrs(t *testing.T) {
	buf := captureSlog(t)
	events := make(chan string, 2)
	events <- "containers+memhog"
	ac := startFakeOOMAgent(t, events)

	s := oomWatchTestService()
	attribution := resources.ActorAttribution{Ref: resources.ActorRef{Atespace: "default", Name: "bench"}, UID: "actor-1"}
	ra := oomWatchActor(ac, false)

	done := make(chan struct{})
	go func() {
		s.watchOOMEvents(attribution, ra)
		close(done)
	}()

	// One event consumed and logged; the watch is back in a GetOOMEvent call
	// waiting for the next one.
	waitForLog(t, buf,
		"Container killed for exceeding its memory limit inside the guest",
		"ate.actor.uid=actor-1",
		"ate.actor.name=bench",
		"container_id=containers+memhog",
		"ate.actor.container.name=memhog",
	)

	// Ending the watch: the watcher is sitting in a GetOOMEvent call between
	// events, so teardown ends it by closing the agent connection — the same
	// way the stdout/stderr forwarding goroutines end.
	ac.Close()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("watch did not return when the agent connection closed")
	}
}

func TestWatchOOMEventsUnknownContainerFallsBackToID(t *testing.T) {
	buf := captureSlog(t)
	events := make(chan string, 2)
	events <- "containers+rogue"
	ac := startFakeOOMAgent(t, events)

	s := oomWatchTestService()
	ra := oomWatchActor(ac, false)

	go s.watchOOMEvents(resources.ActorAttribution{UID: "actor-2"}, ra)

	waitForLog(t, buf, "ate.actor.container.name=containers+rogue")
}

func TestWatchOOMEventsEndsWithTheConnection(t *testing.T) {
	captureSlog(t)
	events := make(chan string, 1) // nothing pending: the watch sits in a call
	ac := startFakeOOMAgent(t, events)

	s := oomWatchTestService()
	ra := oomWatchActor(ac, false)

	done := make(chan struct{})
	go func() {
		s.watchOOMEvents(resources.ActorAttribution{UID: "actor-3"}, ra)
		close(done)
	}()
	time.Sleep(50 * time.Millisecond) // let the watch reach its GetOOMEvent call

	// Teardown closes the agent client; the in-flight call fails and the
	// watch must end rather than spin.
	ac.Close()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("watch did not return when the agent connection closed")
	}
}

func TestWatchOOMEventsSkipsStoppingActor(t *testing.T) {
	captureSlog(t)
	events := make(chan string, 1)
	ac := startFakeOOMAgent(t, events)

	s := oomWatchTestService()
	ra := oomWatchActor(ac, true)

	done := make(chan struct{})
	go func() {
		s.watchOOMEvents(resources.ActorAttribution{UID: "actor-4"}, ra)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("watch did not return for a stopping actor")
	}
	// No GetOOMEvent call was ever issued: the pending event is still there.
	select {
	case id := <-events:
		t.Fatalf("event %q was consumed by a stopping actor's watch", id)
	default:
	}
}
