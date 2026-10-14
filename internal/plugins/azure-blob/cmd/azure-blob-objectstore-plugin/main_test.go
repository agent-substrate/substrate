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
	"log/slog"
	"maps"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/plugins/azure-blob/internal/blobstore"
	objectstorev1 "github.com/agent-substrate/substrate/pkg/proto/objectstorepb/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

// TestMain clears the environment run reads its defaults and workload identity
// from, so a value in the developer's or CI shell cannot change a test; tests
// that need one set it with t.Setenv.
func TestMain(m *testing.M) {
	for _, name := range append([]string{
		"AZURE_STORAGE_ACCOUNT_URL",
		"AZURE_STORAGE_CONTAINERS",
		"AZURE_STORAGE_PREFIX",
		"AZURE_STORAGE_ASSET_CONTAINERS",
	}, workloadIdentityEnv...) {
		if err := os.Unsetenv(name); err != nil {
			panic(err)
		}
	}
	os.Exit(m.Run())
}

func TestRunRejectsInvalidConfiguration(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{name: "missing mode", want: "usage"},
		{name: "unknown mode", args: []string{"other"}, want: "usage"},
		{name: "missing socket", args: []string{"node", "--account-url=https://account.example", "--containers=snapshots"}, want: "--socket"},
		{name: "missing account", args: []string{"node", "--socket=/tmp/plugin.sock", "--containers=snapshots"}, want: "--account-url"},
		{name: "missing containers", args: []string{"control", "--socket=/tmp/plugin.sock", "--account-url=https://account.example"}, want: "--containers"},
		{name: "empty container", args: []string{"control", "--socket=/tmp/plugin.sock", "--account-url=https://account.example", "--containers=snapshots,,other"}, want: "invalid"},
		{name: "container with slash", args: []string{"control", "--socket=/tmp/plugin.sock", "--account-url=https://account.example", "--containers=snap/shots"}, want: "invalid"},
		{name: "uppercase container", args: []string{"control", "--socket=/tmp/plugin.sock", "--account-url=https://account.example", "--containers=snapshots,Snapshots"}, want: "invalid"},
		{name: "short container", args: []string{"control", "--socket=/tmp/plugin.sock", "--account-url=https://account.example", "--containers=ab"}, want: "invalid"},
		{name: "duplicate container", args: []string{"control", "--socket=/tmp/plugin.sock", "--account-url=https://account.example", "--containers=snapshots, snapshots"}, want: "twice"},
		{name: "credential selection", args: []string{"control", "--socket=/tmp/plugin.sock", "--account-url=https://account.example", "--containers=snapshots", "--credential=managed-identity"}, want: "flag provided but not defined: -credential"},
		{name: "bad flag", args: []string{"node", "--unknown"}, want: "flag provided but not defined"},
		{name: "asset containers in control mode", args: []string{"control", "--socket=/tmp/plugin.sock", "--account-url=https://account.example", "--containers=snapshots", "--asset-containers=assets"}, want: "only served in node mode"},
		{name: "invalid asset container", args: []string{"node", "--socket=/tmp/plugin.sock", "--account-url=https://account.example", "--containers=snapshots", "--asset-containers=Assets"}, want: "--asset-containers"},
		{name: "duplicate asset container", args: []string{"node", "--socket=/tmp/plugin.sock", "--account-url=https://account.example", "--containers=snapshots", "--asset-containers=assets,assets"}, want: "twice"},
		{name: "zero asset staging capacity", args: []string{"node", "--socket=/tmp/plugin.sock", "--account-url=https://account.example", "--containers=snapshots", "--asset-containers=assets", "--asset-staging-capacity=0"}, want: "--asset-staging-capacity"},
		{name: "invalid asset staging capacity", args: []string{"node", "--socket=/tmp/plugin.sock", "--account-url=https://account.example", "--containers=snapshots", "--asset-containers=assets", "--asset-staging-capacity=lots"}, want: "--asset-staging-capacity"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := run(context.Background(), tc.args, func(string, []string) (map[string]blobstore.Store, error) {
				t.Fatal("store factory called for invalid configuration")
				return nil, nil
			})
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("run(%v) = %v, want error containing %q", tc.args, err, tc.want)
			}
		})
	}
}

func TestRunServesSelectedModeAndHealth(t *testing.T) {
	for _, mode := range []string{"node", "control"} {
		t.Run(mode, func(t *testing.T) {
			socket := testSocket(t, "plugin.sock")
			root := t.TempDir()
			ctx, cancel := context.WithCancel(context.Background())
			errs := make(chan error, 1)
			store := &testStore{}
			go func() {
				errs <- run(ctx, []string{
					mode,
					"--socket=" + socket,
					"--account-url=https://account.example",
					"--containers=snapshots, other",
					"--root=" + root,
				}, func(accountURL string, containers []string) (map[string]blobstore.Store, error) {
					if accountURL != "https://account.example" || !slices.Equal(containers, []string{"snapshots", "other"}) {
						t.Errorf("factory called with %q, %q", accountURL, containers)
					}
					return map[string]blobstore.Store{"snapshots": store, "other": store}, nil
				})
			}()

			conn := dialPlugin(t, socket)
			checkCtx, checkCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer checkCancel()
			if _, err := healthpb.NewHealthClient(conn).Check(checkCtx, &healthpb.HealthCheckRequest{}); err != nil {
				t.Fatalf("health check: %v", err)
			}

			node := objectstorev1.NewNodeProviderClient(conn)
			control := objectstorev1.NewControlProviderClient(conn)
			_, nodeErr := node.FetchSnapshot(checkCtx, &objectstorev1.FetchSnapshotRequest{})
			_, controlErr := control.CleanupSnapshot(checkCtx, &objectstorev1.CleanupSnapshotRequest{})
			// Without --asset-containers, neither mode serves assets.
			_, assetErr := objectstorev1.NewAssetProviderClient(conn).FetchAsset(checkCtx, &objectstorev1.FetchAssetRequest{})
			if status.Code(assetErr) != codes.Unimplemented {
				t.Fatalf("asset RPC code = %v, want Unimplemented", status.Code(assetErr))
			}
			if mode == "node" {
				if status.Code(nodeErr) != codes.InvalidArgument {
					t.Fatalf("node RPC code = %v, want InvalidArgument", status.Code(nodeErr))
				}
				if status.Code(controlErr) != codes.Unimplemented {
					t.Fatalf("control RPC code = %v, want Unimplemented", status.Code(controlErr))
				}
			} else {
				if status.Code(nodeErr) != codes.Unimplemented {
					t.Fatalf("node RPC code = %v, want Unimplemented", status.Code(nodeErr))
				}
				if status.Code(controlErr) != codes.InvalidArgument {
					t.Fatalf("control RPC code = %v, want InvalidArgument", status.Code(controlErr))
				}
			}

			cancel()
			select {
			case err := <-errs:
				if err != nil {
					t.Fatalf("run after cancellation: %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("run did not stop after cancellation")
			}
		})
	}
}

// TestRunServesAssets runs node mode with asset containers, one shared with
// the snapshot containers, and fetches an asset from each through the socket.
func TestRunServesAssets(t *testing.T) {
	socket := testSocket(t, "plugin.sock")
	root, staging := t.TempDir(), filepath.Join(t.TempDir(), "asset-staging")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errs := make(chan error, 1)
	go func() {
		errs <- run(ctx, []string{
			"node",
			"--socket=" + socket,
			"--account-url=https://account.example",
			"--containers=snapshots",
			"--asset-containers=assets, snapshots",
			"--asset-staging-dir=" + staging,
			"--root=" + root,
		}, func(_ string, containers []string) (map[string]blobstore.Store, error) {
			if !slices.Equal(containers, []string{"snapshots", "assets"}) {
				t.Errorf("factory called with containers %q, want [snapshots assets]", containers)
			}
			return map[string]blobstore.Store{"snapshots": &testStore{}, "assets": &testStore{}}, nil
		})
	}()

	conn := dialPlugin(t, socket)
	client := objectstorev1.NewAssetProviderClient(conn)
	dst := filepath.Join(root, "asset")
	if err := os.WriteFile(dst, []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	const emptySHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	for _, uri := range []string{"https://account.example/assets/vmlinux", "https://account.example/snapshots/kata/vmlinux"} {
		if _, err := client.FetchAsset(ctx, &objectstorev1.FetchAssetRequest{
			AssetUri: uri, Sha256: emptySHA256, WritePath: dst, MaxBytes: 1,
		}); err != nil {
			t.Fatalf("FetchAsset(%s) = %v, want success", uri, err)
		}
	}
	if info, err := os.Stat(staging); err != nil || !info.IsDir() || info.Mode().Perm() != 0o700 {
		t.Errorf("asset staging directory: %v, %v; want a 0700 directory", info, err)
	}
	_, err := client.FetchAsset(ctx, &objectstorev1.FetchAssetRequest{
		AssetUri: "https://account.example/other/vmlinux", Sha256: emptySHA256, WritePath: dst, MaxBytes: 1,
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("FetchAsset from a container not in --asset-containers = %v, want InvalidArgument", err)
	}

	cancel()
	select {
	case err := <-errs:
		if err != nil {
			t.Fatalf("run after cancellation: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("run did not stop after cancellation")
	}
}

func TestHealthcheck(t *testing.T) {
	socket := testSocket(t, "plugin.sock")
	ctx, cancel := context.WithCancel(context.Background())
	errs := make(chan error, 1)
	go func() {
		errs <- run(ctx, []string{
			"node",
			"--socket=" + socket,
			"--account-url=https://account.example",
			"--containers=snapshots",
			"--root=" + t.TempDir(),
		}, func(string, []string) (map[string]blobstore.Store, error) {
			return map[string]blobstore.Store{"snapshots": &testStore{}}, nil
		})
	}()
	if err := run(context.Background(), []string{"healthcheck", "--socket=" + socket, "--timeout=10s"}, nil); err != nil {
		t.Fatalf("healthcheck of a serving plugin: %v", err)
	}
	cancel()
	if err := <-errs; err != nil {
		t.Fatalf("run after cancellation: %v", err)
	}

	if err := run(context.Background(), []string{"healthcheck", "--socket=" + testSocket(t, "absent.sock"), "--timeout=200ms"}, nil); err == nil {
		t.Fatal("healthcheck of an absent plugin succeeded")
	}
	if err := run(context.Background(), []string{"healthcheck"}, nil); err == nil || !strings.Contains(err.Error(), "--socket") {
		t.Fatalf("healthcheck without --socket = %v, want --socket error", err)
	}
}

func TestHealthcheckRejectsNotServing(t *testing.T) {
	socket := testSocket(t, "plugin.sock")
	listener, err := listen(socket)
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	healthServer := health.NewServer()
	healthServer.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
	healthpb.RegisterHealthServer(server, healthServer)
	go func() { _ = server.Serve(listener) }()
	defer server.Stop()

	err = run(context.Background(), []string{"healthcheck", "--socket=" + socket}, nil)
	if err == nil || !strings.Contains(err.Error(), "NOT_SERVING") {
		t.Fatalf("healthcheck of a NOT_SERVING plugin = %v, want NOT_SERVING error", err)
	}
}

func TestRunReturnsStoreFactoryError(t *testing.T) {
	want := "creating test store"
	err := run(context.Background(), []string{
		"node",
		"--socket=" + testSocket(t, "plugin.sock"),
		"--account-url=https://account.example",
		"--containers=snapshots",
	}, func(string, []string) (map[string]blobstore.Store, error) {
		return nil, io.ErrUnexpectedEOF
	})
	if err == nil || !strings.Contains(err.Error(), io.ErrUnexpectedEOF.Error()) {
		t.Fatalf("run() = %v, want %s error", err, want)
	}
}

func TestNewAzureStoresRejectsInvalidConfiguration(t *testing.T) {
	t.Setenv("AZURE_CLIENT_ID", "client-id")
	t.Setenv("AZURE_TENANT_ID", "tenant-id")
	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte("token"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("AZURE_FEDERATED_TOKEN_FILE", tokenFile)
	if _, err := newAzureStores("not-a-url", []string{"snapshots"}); err == nil || !strings.Contains(err.Error(), "account URL") {
		t.Fatal("newAzureStores accepted an invalid account URL")
	}
	stores, err := newAzureStores("https://account.example", []string{"snapshots", "other"})
	if err != nil {
		t.Fatalf("newAzureStores: %v", err)
	}
	if got := slices.Sorted(maps.Keys(stores)); !slices.Equal(got, []string{"other", "snapshots"}) {
		t.Fatalf("newAzureStores containers = %v, want [other snapshots]", got)
	}
}

func TestNewAzureStoresRequiresReadableTokenFile(t *testing.T) {
	t.Setenv("AZURE_CLIENT_ID", "client-id")
	t.Setenv("AZURE_TENANT_ID", "tenant-id")
	t.Setenv("AZURE_FEDERATED_TOKEN_FILE", filepath.Join(t.TempDir(), "missing"))
	if _, err := newAzureStores("https://account.example", []string{"snapshots"}); err == nil || !strings.Contains(err.Error(), "token file") {
		t.Fatalf("newAzureStores() = %v, want a token file error", err)
	}
}

// credentialEnv lists the variables any azidentity credential reads, so each
// case starts from an environment where only the variables it names are set.
var credentialEnv = []string{
	"AZURE_CLIENT_ID", "AZURE_TENANT_ID", "AZURE_FEDERATED_TOKEN_FILE",
	"AZURE_CLIENT_SECRET", "AZURE_CLIENT_CERTIFICATE_PATH", "AZURE_USERNAME", "AZURE_PASSWORD",
	"IDENTITY_ENDPOINT", "IDENTITY_HEADER", "MSI_ENDPOINT", "MSI_SECRET",
}

func setCredentialEnv(t *testing.T, env map[string]string) {
	t.Helper()
	for _, name := range credentialEnv {
		t.Setenv(name, env[name])
	}
}

func TestNewWorkloadIdentityCredential(t *testing.T) {
	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte("token"), 0o600); err != nil {
		t.Fatal(err)
	}
	workloadIdentity := map[string]string{"AZURE_CLIENT_ID": "client-id", "AZURE_TENANT_ID": "tenant-id", "AZURE_FEDERATED_TOKEN_FILE": tokenFile}
	without := func(name string) map[string]string {
		env := maps.Clone(workloadIdentity)
		delete(env, name)
		return env
	}
	for _, tc := range []struct {
		name    string
		env     map[string]string
		wantErr []string
	}{
		{name: "workload identity", env: workloadIdentity},
		{name: "no environment", wantErr: []string{"AZURE_CLIENT_ID, AZURE_TENANT_ID, AZURE_FEDERATED_TOKEN_FILE must be set"}},
		{name: "missing client ID", env: without("AZURE_CLIENT_ID"), wantErr: []string{"not configured: AZURE_CLIENT_ID must be set"}},
		{name: "missing tenant ID", env: without("AZURE_TENANT_ID"), wantErr: []string{"not configured: AZURE_TENANT_ID must be set"}},
		{name: "missing token file", env: without("AZURE_FEDERATED_TOKEN_FILE"), wantErr: []string{"not configured: AZURE_FEDERATED_TOKEN_FILE must be set"}},
		{
			name: "unreadable token file",
			env: map[string]string{
				"AZURE_CLIENT_ID":            "client-id",
				"AZURE_TENANT_ID":            "tenant-id",
				"AZURE_FEDERATED_TOKEN_FILE": filepath.Join(t.TempDir(), "missing"),
			},
			wantErr: []string{"token file"},
		},
		{
			// A client secret does not stand in for workload identity.
			name:    "client secret",
			env:     map[string]string{"AZURE_CLIENT_ID": "client-id", "AZURE_TENANT_ID": "tenant-id", "AZURE_CLIENT_SECRET": "secret"},
			wantErr: []string{"AZURE_FEDERATED_TOKEN_FILE must be set"},
		},
		{
			// Neither does the host's managed identity endpoint.
			name:    "managed identity endpoint",
			env:     map[string]string{"AZURE_CLIENT_ID": "client-id", "IDENTITY_ENDPOINT": "http://169.254.169.254", "IDENTITY_HEADER": "header", "MSI_ENDPOINT": "http://169.254.169.254"},
			wantErr: []string{"AZURE_TENANT_ID, AZURE_FEDERATED_TOKEN_FILE must be set"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			setCredentialEnv(t, tc.env)
			credential, err := newWorkloadIdentityCredential()
			if len(tc.wantErr) == 0 {
				if err != nil || credential == nil {
					t.Fatalf("newWorkloadIdentityCredential() = %v, %v", credential, err)
				}
				return
			}
			if err == nil {
				t.Fatalf("newWorkloadIdentityCredential() succeeded, want error containing %q", tc.wantErr)
			}
			for _, want := range tc.wantErr {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("newWorkloadIdentityCredential() = %v, want error containing %q", err, want)
				}
			}
		})
	}
}

// TestRunFailsClosedWithoutWorkloadIdentity runs the production store factory:
// startup must stop before serving when workload identity is not configured.
// TestRunDoesNotReportAccountURLCredentials starts the plugin, with workload
// identity configured, from a "Blob service SAS URL" an operator copied from
// the portal. main logs the error run returns, so it must not hold the SAS.
func TestRunDoesNotReportAccountURLCredentials(t *testing.T) {
	const sig = "c2VjcmV0LXNpZ25hdHVyZQ"
	t.Setenv("AZURE_CLIENT_ID", "client-id")
	t.Setenv("AZURE_TENANT_ID", "tenant-id")
	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte("token"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("AZURE_FEDERATED_TOKEN_FILE", tokenFile)
	for _, accountURL := range []string{
		"https://acct.blob.core.windows.net/?sv=2022-11-02&ss=b&sig=" + sig,
		"https://user:" + sig + "@acct.blob.core.windows.net",
	} {
		err := run(t.Context(), []string{
			"node",
			"--socket=" + testSocket(t, "plugin.sock"),
			"--account-url=" + accountURL,
			"--containers=snapshots",
			"--root=" + t.TempDir(),
		}, newAzureStores)
		if err == nil || !strings.Contains(err.Error(), "account URL") || strings.Contains(err.Error(), sig) {
			t.Fatalf("run() = %v, want an account URL error without the credential", err)
		}
	}
}

func TestRunFailsClosedWithoutWorkloadIdentity(t *testing.T) {
	setCredentialEnv(t, map[string]string{"AZURE_CLIENT_ID": "client-id", "AZURE_CLIENT_SECRET": "secret", "IDENTITY_ENDPOINT": "http://169.254.169.254"})
	socket := testSocket(t, "plugin.sock")
	for _, mode := range []string{"node", "control"} {
		err := run(t.Context(), []string{
			mode,
			"--socket=" + socket,
			"--account-url=https://account.example",
			"--containers=snapshots",
			"--root=" + t.TempDir(),
		}, newAzureStores)
		if err == nil || !strings.Contains(err.Error(), "workload identity is not configured: AZURE_TENANT_ID, AZURE_FEDERATED_TOKEN_FILE must be set") {
			t.Fatalf("%s: run() = %v, want a workload identity configuration error", mode, err)
		}
		if _, err := os.Stat(socket); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("%s: socket stat = %v, want the plugin to fail before listening", mode, err)
		}
	}
}

func TestLogRPCReturnsHandlerResult(t *testing.T) {
	wantResp := struct{}{}
	wantErr := errors.New("handler failed")
	gotResp, gotErr := logRPC(t.Context(), nil, &grpc.UnaryServerInfo{FullMethod: "/test.Service/Call"}, func(context.Context, any) (any, error) {
		return wantResp, wantErr
	})
	if !errors.Is(gotErr, wantErr) {
		t.Fatalf("logRPC() error = %v, want %v", gotErr, wantErr)
	}
	if gotResp != wantResp {
		t.Errorf("logRPC() response = %v, want %v", gotResp, wantResp)
	}
}

// TestLogRPCLevel checks that a FetchAsset failing with Internal or
// Unavailable is logged at WARN, and that every other result, including the
// same codes from other RPCs, stays at INFO.
func TestLogRPCLevel(t *testing.T) {
	const otherMethod = objectstorev1.NodeProvider_FetchSnapshot_FullMethodName
	for _, tc := range []struct {
		name   string
		method string
		err    error
		want   slog.Level
	}{
		{"FetchAsset Internal", objectstorev1.AssetProvider_FetchAsset_FullMethodName, status.Error(codes.Internal, "boom"), slog.LevelWarn},
		{"FetchAsset Unavailable", objectstorev1.AssetProvider_FetchAsset_FullMethodName, status.Error(codes.Unavailable, "boom"), slog.LevelWarn},
		{"FetchAsset NotFound", objectstorev1.AssetProvider_FetchAsset_FullMethodName, status.Error(codes.NotFound, "boom"), slog.LevelInfo},
		{"FetchAsset InvalidArgument", objectstorev1.AssetProvider_FetchAsset_FullMethodName, status.Error(codes.InvalidArgument, "boom"), slog.LevelInfo},
		{"FetchAsset OK", objectstorev1.AssetProvider_FetchAsset_FullMethodName, nil, slog.LevelInfo},
		{"other RPC Internal", otherMethod, status.Error(codes.Internal, "boom"), slog.LevelInfo},
		{"other RPC Unavailable", otherMethod, status.Error(codes.Unavailable, "boom"), slog.LevelInfo},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var records []slog.Record
			orig := slog.Default()
			t.Cleanup(func() { slog.SetDefault(orig) })
			slog.SetDefault(slog.New(recordHandler{&records}))

			_, err := logRPC(t.Context(), nil, &grpc.UnaryServerInfo{FullMethod: tc.method}, func(context.Context, any) (any, error) {
				return nil, tc.err
			})
			if status.Code(err) != status.Code(tc.err) {
				t.Fatalf("logRPC() error = %v, want %v", err, tc.err)
			}
			var got []slog.Level
			for _, r := range records {
				if r.Message == "handled RPC" {
					got = append(got, r.Level)
				}
			}
			if want := []slog.Level{tc.want}; !slices.Equal(got, want) {
				t.Errorf("handled RPC levels = %v, want %v", got, want)
			}
		})
	}
}

// recordHandler collects every record logged through it.
type recordHandler struct{ records *[]slog.Record }

func (recordHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h recordHandler) Handle(_ context.Context, r slog.Record) error {
	*h.records = append(*h.records, r)
	return nil
}
func (h recordHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h recordHandler) WithGroup(string) slog.Handler      { return h }

func TestRunReturnsListenError(t *testing.T) {
	parent := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(parent, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	err := run(context.Background(), []string{
		"node",
		"--socket=" + filepath.Join(parent, "plugin.sock"),
		"--account-url=https://account.example",
		"--containers=snapshots",
		"--root=" + t.TempDir(),
	}, func(string, []string) (map[string]blobstore.Store, error) {
		return map[string]blobstore.Store{"snapshots": &testStore{}}, nil
	})
	if err == nil || !strings.Contains(err.Error(), "socket directory") {
		t.Fatalf("run() = %v, want socket directory error", err)
	}
}

func TestListenReplacesStaleSocketAndRestrictsPermissions(t *testing.T) {
	socketDir, err := os.MkdirTemp("", "azsnap")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(socketDir) })
	socket := filepath.Join(socketDir, "nested", "plugin.sock")
	if err := os.MkdirAll(filepath.Dir(socket), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(socket, []byte("stale"), 0o644); err != nil {
		t.Fatal(err)
	}
	listener, err := listen(socket)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	info, err := os.Stat(socket)
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Fatalf("socket permissions = %o, want 600", got)
	}
	conn, err := net.Dial("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
}

func dialPlugin(t *testing.T, socket string) *grpc.ClientConn {
	t.Helper()
	conn, err := grpc.NewClient("unix://"+socket,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.WaitForReady(true)),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func testSocket(t *testing.T, name string) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "azsnap")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return filepath.Join(dir, name)
}

type testStore struct{}

func (s *testStore) StageBlock(context.Context, string, string, []byte, []byte) error {
	return nil
}

func (s *testStore) CommitBlockList(context.Context, string, []string, map[string]*string) error {
	return nil
}

func (s *testStore) Download(context.Context, string) (blobstore.Download, error) {
	return blobstore.Download{Body: io.NopCloser(strings.NewReader("")), Size: 0}, nil
}

func (s *testStore) List(context.Context, string) ([]string, error) {
	return nil, nil
}

func (s *testStore) Delete(context.Context, string) error {
	return nil
}

var _ blobstore.Store = (*testStore)(nil)
