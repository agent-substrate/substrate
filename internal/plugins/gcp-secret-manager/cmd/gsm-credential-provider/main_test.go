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
	"io/fs"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"go.ate.dev/substrate/pkg/proto/credproviderpb"
)

// /healthz passes regardless of readiness; /readyz follows the ready flag.
func TestHealthHandler(t *testing.T) {
	var ready atomic.Bool
	h := healthHandler(&ready)

	get := func(path string) int {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		return rec.Code
	}

	if got := get("/healthz"); got != http.StatusOK {
		t.Errorf("/healthz before ready = %d, want 200", got)
	}
	if got := get("/readyz"); got != http.StatusServiceUnavailable {
		t.Errorf("/readyz before ready = %d, want 503", got)
	}

	ready.Store(true)
	if got := get("/readyz"); got != http.StatusOK {
		t.Errorf("/readyz when ready = %d, want 200", got)
	}

	ready.Store(false)
	if got := get("/readyz"); got != http.StatusServiceUnavailable {
		t.Errorf("/readyz while draining = %d, want 503", got)
	}
	if got := get("/healthz"); got != http.StatusOK {
		t.Errorf("/healthz while draining = %d, want 200", got)
	}
}

// run rejects a missing flag, an unreadable file or a non-positive fetch
// timeout before it dials anything or binds a port.
func TestRunRequiresTLSFlags(t *testing.T) {
	const caller = "spiffe://cluster.local/ns/ate-system/sa/atenet-egress"
	policy := filepath.Join(t.TempDir(), "project-policy.yaml")
	if err := os.WriteFile(policy, []byte("policies: []\n"), 0o600); err != nil {
		t.Fatalf("writing the policy: %v", err)
	}
	absent := filepath.Join(t.TempDir(), "absent")
	tests := []struct {
		name             string
		serverBundle     string
		clientCAFile     string
		injectorIdentity string
		policyFile       string
		fetchTimeout     time.Duration
		wantErr          string
		// wantIs, if set, must be in the error chain; wantErr alone matches any
		// failure of the same step.
		wantIs error
	}{
		{name: "no server bundle", clientCAFile: "/ca.pem", injectorIdentity: caller, policyFile: policy, wantErr: "--server-cred-bundle"},
		{name: "no client CA", serverBundle: "/bundle.pem", injectorIdentity: caller, policyFile: policy, wantErr: "--client-ca-file"},
		{name: "no injector identity", serverBundle: "/bundle.pem", clientCAFile: "/ca.pem", policyFile: policy, wantErr: "--injector-identity"},
		{name: "no project policy", serverBundle: "/bundle.pem", clientCAFile: "/ca.pem", injectorIdentity: caller, wantErr: "--project-policy-file"},
		{name: "project policy unreadable", serverBundle: "/bundle.pem", clientCAFile: "/ca.pem", injectorIdentity: caller, policyFile: absent, wantErr: "project policy", wantIs: fs.ErrNotExist},
		{name: "fetch timeout not positive", serverBundle: "/bundle.pem", clientCAFile: "/ca.pem", injectorIdentity: caller, policyFile: policy, fetchTimeout: -time.Second, wantErr: "--fetch-timeout"},
		{name: "client CA unreadable", serverBundle: "/bundle.pem", clientCAFile: absent, injectorIdentity: caller, policyFile: policy, wantErr: "server credentials", wantIs: fs.ErrNotExist},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			origBundle, origCA, origIdentity, origPolicy, origTimeout := *serverBundle, *clientCAFile, *injectorIdentity, *projectPolicyFile, *fetchTimeout
			t.Cleanup(func() {
				*serverBundle, *clientCAFile, *injectorIdentity, *projectPolicyFile, *fetchTimeout = origBundle, origCA, origIdentity, origPolicy, origTimeout
			})
			*serverBundle, *clientCAFile, *injectorIdentity, *projectPolicyFile = tc.serverBundle, tc.clientCAFile, tc.injectorIdentity, tc.policyFile
			if tc.fetchTimeout != 0 {
				*fetchTimeout = tc.fetchTimeout
			}

			err := run(context.Background())
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("run() error = %v, want one mentioning %q", err, tc.wantErr)
			}
			if tc.wantIs != nil && !errors.Is(err, tc.wantIs) {
				t.Errorf("run() error = %v, want one wrapping %v", err, tc.wantIs)
			}
		})
	}
}

func TestBuildVersionPrefersExplicitVersion(t *testing.T) {
	orig := version
	t.Cleanup(func() { version = orig })

	version = "v9.9.9"
	if got := buildVersion(); !strings.HasPrefix(got, "v9.9.9") {
		t.Errorf("buildVersion() = %q, want it to start with v9.9.9", got)
	}
	version = ""
	if got := buildVersion(); got == "" {
		t.Error("buildVersion() is empty without an explicit version")
	}
}

// blockingProvider blocks FetchSecret until release is closed or the call is
// canceled, closing entered once the call starts.
type blockingProvider struct {
	credproviderpb.UnimplementedCredentialProviderServer
	entered chan struct{}
	release chan struct{}
}

func (p *blockingProvider) FetchSecret(ctx context.Context, _ *credproviderpb.FetchSecretRequest) (*credproviderpb.FetchSecretResponse, error) {
	close(p.entered)
	select {
	case <-p.release:
		return &credproviderpb.FetchSecretResponse{}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// startBlockingServer serves p in memory and starts one FetchSecret against it,
// returning the server and the call's result.
func startBlockingServer(t *testing.T, p *blockingProvider) (*grpc.Server, <-chan error) {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	credproviderpb.RegisterCredentialProviderServer(srv, p)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient("passthrough:///bufconn",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	result := make(chan error, 1)
	go func() {
		_, err := credproviderpb.NewCredentialProviderClient(conn).FetchSecret(context.Background(), &credproviderpb.FetchSecretRequest{})
		result <- err
	}()
	select {
	case <-p.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("FetchSecret never reached the server")
	}
	return srv, result
}

// gracefulStop returns once an in-flight call completes within the grace period.
func TestGracefulStopDrains(t *testing.T) {
	p := &blockingProvider{entered: make(chan struct{}), release: make(chan struct{})}
	srv, result := startBlockingServer(t, p)

	stopped := make(chan struct{})
	go func() {
		gracefulStop(srv, time.Minute)
		close(stopped)
	}()
	close(p.release)
	select {
	case <-stopped:
	case <-time.After(10 * time.Second):
		t.Fatal("gracefulStop did not return after the in-flight call finished")
	}
	if err := <-result; err != nil {
		t.Errorf("in-flight FetchSecret = %v, want it to complete", err)
	}
}

// gracefulStop cuts off a call still running when the grace period ends.
func TestGracefulStopForcesAfterGrace(t *testing.T) {
	const grace = 100 * time.Millisecond
	p := &blockingProvider{entered: make(chan struct{}), release: make(chan struct{})}
	srv, result := startBlockingServer(t, p)

	start := time.Now()
	gracefulStop(srv, grace)
	if elapsed := time.Since(start); elapsed < grace || elapsed > 50*grace {
		t.Errorf("gracefulStop returned after %v, want about %v", elapsed, grace)
	}
	if err := <-result; err == nil {
		t.Error("in-flight FetchSecret succeeded, want it cut off")
	}
}
