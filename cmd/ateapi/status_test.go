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
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateapi/internal/statusz"
	"github.com/agent-substrate/substrate/internal/serverboot"
	"github.com/spf13/pflag"
)

func TestProjectStatusFlagsRedactsByDefault(t *testing.T) {
	flags := pflag.NewFlagSet("status", pflag.ContinueOnError)
	flags.SetOutput(io.Discard)
	flags.String("postgres-connection-string", "", "")
	flags.String("postgres-schema", "public", "")
	flags.String("authentication-config", "", "")
	flags.String("actor-id-jwt-pool", "", "")
	flags.String("log-level", "info", "")
	flags.String("future-flag", "future-default", "")
	if err := flags.Parse([]string{
		"--postgres-connection-string=postgres://alice:uri-secret@db.example/app",
		"--postgres-schema=tenant_a",
		"--authentication-config=/etc/ateapi/auth.yaml",
	}); err != nil {
		t.Fatalf("Parse: %v", err)
	}

	projected := projectStatusFlags(flags, nil)
	want := map[string]statusz.Flag{
		"actor-id-jwt-pool":          {Name: "actor-id-jwt-pool", Value: "disabled", Source: "default"},
		"authentication-config":      {Name: "authentication-config", Value: "configured", Source: "command line"},
		"future-flag":                {Name: "future-flag", Value: "[redacted]", Source: "default"},
		"log-level":                  {Name: "log-level", Value: "info", Source: "default"},
		"postgres-connection-string": {Name: "postgres-connection-string", Value: "[redacted]", Source: "command line"},
		"postgres-schema":            {Name: "postgres-schema", Value: "tenant_a", Source: "command line"},
	}
	if len(projected) != len(want) {
		t.Fatalf("projected %d flags, want %d: %#v", len(projected), len(want), projected)
	}
	for _, got := range projected {
		if got != want[got.Name] {
			t.Errorf("flag %q = %#v, want %#v", got.Name, got, want[got.Name])
		}
	}
	blob, err := json.Marshal(projected)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	for _, secret := range []string{"uri-secret", "/etc/ateapi/auth.yaml", "future-default"} {
		if strings.Contains(string(blob), secret) {
			t.Errorf("projected flags retain sensitive value %q", secret)
		}
	}
}

func TestProjectStatusFlagsRedactsKeywordDSNAndEnvironmentValues(t *testing.T) {
	flags := pflag.NewFlagSet("status", pflag.ContinueOnError)
	flags.String("postgres-connection-string", "", "")
	flags.String("postgres-schema", "public", "")
	if err := flags.Parse([]string{"--postgres-connection-string=host=db.example user=alice password=keyword-secret", "--postgres-schema=@env"}); err != nil {
		t.Fatalf("Parse: %v", err)
	}
	envSources := captureStatusEnvSources(flags)
	if err := flags.Set("postgres-schema", "private_schema"); err != nil {
		t.Fatalf("resolve postgres-schema: %v", err)
	}

	projected := projectStatusFlags(flags, envSources)
	if got := projectedFlag(t, projected, "postgres-connection-string"); got.Value != "[redacted]" {
		t.Fatalf("DSN display = %q, want redacted", got.Value)
	}
	schema := projectedFlag(t, projected, "postgres-schema")
	if schema.Value != "[redacted]" || schema.Source != "environment: ATE_API_POSTGRES_SCHEMA" {
		t.Fatalf("environment schema = %#v, want redacted with environment source", schema)
	}
	blob, _ := json.Marshal(projected)
	for _, secret := range []string{"keyword-secret", "private_schema"} {
		if strings.Contains(string(blob), secret) {
			t.Errorf("projected flags retain sensitive value %q", secret)
		}
	}
}

func TestStatusPortDefaultDisabledAndBindFailure(t *testing.T) {
	if got := pflag.Lookup("status-port"); got == nil || got.DefValue != "4040" {
		t.Fatalf("status-port default = %v, want 4040", got)
	}
	var called atomic.Bool
	running, err := startStatusHTTPServer(0, http.NotFoundHandler(), func(_, _ string) (net.Listener, error) {
		called.Store(true)
		return nil, errors.New("must not listen")
	})
	if err != nil || running != nil || called.Load() {
		t.Fatalf("disabled server = (%v, %v), listener called = %t; want nil, nil, false", running, err, called.Load())
	}

	bindErr := errors.New("address already in use")
	_, err = startStatusHTTPServer(4040, http.NotFoundHandler(), func(_, _ string) (net.Listener, error) {
		return nil, bindErr
	})
	if !errors.Is(err, bindErr) || !strings.Contains(err.Error(), "status port 4040") {
		t.Fatalf("bind error = %v, want wrapped status-port error", err)
	}
}

func TestStatusHTTPRemainsReachableUntilDrainCompletes(t *testing.T) {
	readiness := &serverboot.Readiness{}
	handler := statusz.NewHandler(statusz.Config{StartedAt: time.Now()}, readiness.Ready, time.Now)
	running, err := startStatusHTTPServer(4040, handler, ephemeralListener)
	if err != nil {
		t.Fatalf("startStatusHTTPServer: %v", err)
	}
	t.Cleanup(func() { _ = running.server.Close() })
	client := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}
	url := "http://" + running.listener.Addr().String() + "/statusz?format=json"
	if got := fetchReady(t, client, url); !got {
		t.Fatal("initial readiness = false, want true")
	}
	readiness.MarkNotReady()
	if got := fetchReady(t, client, url); got {
		t.Fatal("readiness after MarkNotReady = true, want false")
	}

	drainDone := make(chan struct{})
	shutdownDone := shutdownStatusAfter(drainDone, running.server, time.Second)
	if got := fetchReady(t, client, url); got {
		t.Fatal("status became unreachable or stale before drain completion")
	}
	close(drainDone)
	result := <-shutdownDone
	if result.Err != nil || result.Forced {
		t.Fatalf("shutdown result = %#v, want graceful", result)
	}
	if _, err := client.Get(url); err == nil {
		t.Fatal("status endpoint remained reachable after drain completion")
	}
}

func TestStatusHTTPForceClosesAfterShutdownTimeout(t *testing.T) {
	requestStarted := make(chan struct{})
	handler := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		close(requestStarted)
		<-req.Context().Done()
	})
	running, err := startStatusHTTPServer(4040, handler, ephemeralListener)
	if err != nil {
		t.Fatalf("startStatusHTTPServer: %v", err)
	}
	t.Cleanup(func() { _ = running.server.Close() })
	requestDone := make(chan error, 1)
	go func() {
		resp, err := http.Get("http://" + running.listener.Addr().String() + "/statusz")
		if err == nil {
			resp.Body.Close()
		}
		requestDone <- err
	}()
	<-requestStarted

	drainDone := make(chan struct{})
	shutdownDone := shutdownStatusAfter(drainDone, running.server, 20*time.Millisecond)
	close(drainDone)
	result := <-shutdownDone
	if !result.Forced || !errors.Is(result.Err, context.DeadlineExceeded) {
		t.Fatalf("shutdown result = %#v, want forced close after deadline", result)
	}
	select {
	case <-requestDone:
	case <-time.After(time.Second):
		t.Fatal("active request was not closed after shutdown timeout")
	}
}

func projectedFlag(t *testing.T, flags []statusz.Flag, name string) statusz.Flag {
	t.Helper()
	for _, flag := range flags {
		if flag.Name == name {
			return flag
		}
	}
	t.Fatalf("projected flags do not contain %q", name)
	return statusz.Flag{}
}

func ephemeralListener(_, _ string) (net.Listener, error) {
	return net.Listen("tcp", "127.0.0.1:0")
}

func fetchReady(t *testing.T, client *http.Client, url string) bool {
	t.Helper()
	resp, err := client.Get(url)
	if err != nil {
		t.Fatalf("GET status: %v", err)
	}
	defer resp.Body.Close()
	var snapshot struct {
		Readiness struct {
			Ready bool `json:"ready"`
		} `json:"readiness"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&snapshot); err != nil {
		t.Fatalf("decode status: %v", err)
	}
	return snapshot.Readiness.Ready
}
