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

package cmd

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestCsiDriverConfigCommandArgs(t *testing.T) {
	runCommandArgsTests(t, []commandArgsTest{
		{name: "list", command: getCsiDriverConfigsCmd},
		{name: "get", command: getCsiDriverConfigsCmd, args: []string{"csi-nfs"}},
		{name: "get multiple", command: getCsiDriverConfigsCmd, args: []string{"csi-nfs", "pd-csi"}},
		{name: "create missing -f", command: createCsiDriverConfigCmd, wantErr: true},
		{name: "create extra arg", command: createCsiDriverConfigCmd, args: []string{"csi-nfs"}, wantErr: true},
		{name: "update missing -f", command: updateCsiDriverConfigCmd, wantErr: true},
		{name: "update extra arg", command: updateCsiDriverConfigCmd, args: []string{"csi-nfs"}, wantErr: true},
		{name: "delete", command: deleteCsiDriverConfigCmd, args: []string{"csi-nfs"}},
		{name: "delete missing name", command: deleteCsiDriverConfigCmd, wantErr: true},
		{name: "delete extra name", command: deleteCsiDriverConfigCmd, args: []string{"a", "b"}, wantErr: true},
	})
}

func TestLoadCsiDriverConfigManifest(t *testing.T) {
	t.Parallel()
	wantNFS := &ateapi.CsiDriverConfig{
		Metadata:           &ateapi.ResourceMetadata{Name: "csi-nfs"},
		DriverName:         "nfs.csi.k8s.io",
		ControllerEndpoint: "tcp://csi-nfs-controller.kube-system.svc:50052",
		NodeSocketOverride: "unix:///var/lib/kubelet/plugins/csi-nfsplugin/csi.sock",
		Tls:                &ateapi.CsiDriverTlsConfig{Enabled: true, UsePodIdentity: true, ServerName: "csi-nfs-controller.kube-system.svc"},
	}
	for _, tc := range []struct {
		name, manifest, wantErr string
		want                    *ateapi.CsiDriverConfig
	}{
		{
			name:     "camelCase yaml from stdin",
			manifest: "metadata: {name: csi-nfs}\ndriverName: nfs.csi.k8s.io\ncontrollerEndpoint: tcp://csi-nfs-controller.kube-system.svc:50052\nnodeSocketOverride: unix:///var/lib/kubelet/plugins/csi-nfsplugin/csi.sock\ntls: {enabled: true, usePodIdentity: true, serverName: csi-nfs-controller.kube-system.svc}\n",
			want:     wantNFS,
		},
		{
			name:     "snake_case yaml from stdin",
			manifest: "metadata: {name: csi-nfs}\ndriver_name: nfs.csi.k8s.io\ncontroller_endpoint: tcp://csi-nfs-controller.kube-system.svc:50052\nnode_socket_override: unix:///var/lib/kubelet/plugins/csi-nfsplugin/csi.sock\ntls: {enabled: true, use_pod_identity: true, server_name: csi-nfs-controller.kube-system.svc}\n",
			want:     wantNFS,
		},
		{
			name:     "json input",
			manifest: `{"metadata":{"name":"csi-nfs"},"driverName":"nfs.csi.k8s.io","controllerEndpoint":"tcp://csi-nfs-controller.kube-system.svc:50052","nodeSocketOverride":"unix:///var/lib/kubelet/plugins/csi-nfsplugin/csi.sock","tls":{"enabled":true,"usePodIdentity":true,"serverName":"csi-nfs-controller.kube-system.svc"}}`,
			want:     wantNFS,
		},
		{name: "missing metadata.name", manifest: "driverName: nfs.csi.k8s.io\ncontrollerEndpoint: tcp://127.0.0.1:9000", wantErr: "metadata.name is required"},
		{name: "atespace set", manifest: "metadata: {atespace: team-a, name: csi-nfs}", wantErr: "metadata.atespace must be empty"},
		{name: "unknown field", manifest: "metadata: {name: csi-nfs}\nendpoint: tcp://127.0.0.1:9000", wantErr: "invalid CsiDriverConfig"},
		{name: "empty", manifest: "", wantErr: "manifest is empty"},
		{name: "multi-document yaml", manifest: "metadata: {name: a}\n---\nmetadata: {name: b}\n", wantErr: "manifest holds more than one document"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := loadCsiDriverConfigManifest(strings.NewReader(tc.manifest), "-")
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("loadCsiDriverConfigManifest() error = %v, want %q", err, tc.wantErr)
				}
				return
			}
			if err != nil || cmp.Diff(tc.want, got, protocmp.Transform()) != "" {
				t.Fatalf("loadCsiDriverConfigManifest() = (%v, %v), want %v", got, err, tc.want)
			}
		})
	}

	t.Run("from file", func(t *testing.T) {
		t.Parallel()
		path := filepath.Join(t.TempDir(), "csi-nfs.yaml")
		content := "metadata: {name: csi-nfs}\ndriverName: nfs.csi.k8s.io\ncontrollerEndpoint: tcp://csi-nfs-controller.kube-system.svc:50052\nnodeSocketOverride: unix:///var/lib/kubelet/plugins/csi-nfsplugin/csi.sock\ntls: {enabled: true, usePodIdentity: true, serverName: csi-nfs-controller.kube-system.svc}\n"
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		got, err := loadCsiDriverConfigManifest(strings.NewReader("ignored"), path)
		if err != nil || cmp.Diff(wantNFS, got, protocmp.Transform()) != "" {
			t.Fatalf("loadCsiDriverConfigManifest(file) = (%v, %v), want %v", got, err, wantNFS)
		}
	})

	t.Run("missing file", func(t *testing.T) {
		t.Parallel()
		if _, err := loadCsiDriverConfigManifest(strings.NewReader(""), filepath.Join(t.TempDir(), "missing.yaml")); err == nil {
			t.Fatal("loadCsiDriverConfigManifest(missing) = nil, want error")
		}
	})
}

func TestLoadCsiDriverConfigForUpdate(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		manifest string
		want     *ateapi.CsiDriverConfig
		wantErr  string
	}{
		{
			name:     "manifest carries uid and version",
			manifest: "metadata: {name: csi-nfs, uid: u-1, version: \"3\"}\ndriverName: nfs.csi.k8s.io\ncontrollerEndpoint: tcp://127.0.0.1:9000\n",
			want: &ateapi.CsiDriverConfig{
				Metadata:           &ateapi.ResourceMetadata{Name: "csi-nfs", Uid: "u-1", Version: 3},
				DriverName:         "nfs.csi.k8s.io",
				ControllerEndpoint: "tcp://127.0.0.1:9000",
			},
		},
		{
			name:     "missing uid in manifest",
			manifest: "metadata: {name: csi-nfs, version: \"3\"}\ndriverName: nfs.csi.k8s.io\ncontrollerEndpoint: tcp://127.0.0.1:9000\n",
			wantErr:  "lacks the metadata.uid and metadata.version preconditions",
		},
		{
			name:     "missing version in manifest",
			manifest: "metadata: {name: csi-nfs, uid: u-1}\ndriverName: nfs.csi.k8s.io\ncontrollerEndpoint: tcp://127.0.0.1:9000\n",
			wantErr:  "lacks the metadata.uid and metadata.version preconditions",
		},
		{
			name:     "negative version in manifest",
			manifest: "metadata: {name: csi-nfs, uid: u-1, version: \"-2\"}\ndriverName: nfs.csi.k8s.io\ncontrollerEndpoint: tcp://127.0.0.1:9000\n",
			wantErr:  "lacks the metadata.uid and metadata.version preconditions",
		},
		{
			name:     "atespace set",
			manifest: "metadata: {atespace: team-a, name: csi-nfs, uid: u-1, version: \"1\"}\n",
			wantErr:  "metadata.atespace must be empty",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := loadCsiDriverConfigForUpdate(strings.NewReader(tc.manifest), "-")
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("loadCsiDriverConfigForUpdate() error = %v, want %q", err, tc.wantErr)
				}
				return
			}
			if err != nil || cmp.Diff(tc.want, got, protocmp.Transform()) != "" {
				t.Fatalf("loadCsiDriverConfigForUpdate() = (%v, %v), want %v", got, err, tc.want)
			}
		})
	}
}

type fakeCsiDriverConfigClient struct {
	byName    map[string]*ateapi.CsiDriverConfig
	listPages []*ateapi.ListCsiDriverConfigsResponse
	listReqs  []*ateapi.ListCsiDriverConfigsRequest
	createReq *ateapi.CreateCsiDriverConfigRequest
	updateReq *ateapi.UpdateCsiDriverConfigRequest
	deleteReq *ateapi.DeleteCsiDriverConfigRequest
	err       error
}

func (f *fakeCsiDriverConfigClient) GetCsiDriverConfig(_ context.Context, req *ateapi.GetCsiDriverConfigRequest, _ ...grpc.CallOption) (*ateapi.CsiDriverConfig, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.byName[req.GetCsiDriverConfig().GetName()], nil
}

func (f *fakeCsiDriverConfigClient) ListCsiDriverConfigs(_ context.Context, req *ateapi.ListCsiDriverConfigsRequest, _ ...grpc.CallOption) (*ateapi.ListCsiDriverConfigsResponse, error) {
	f.listReqs = append(f.listReqs, req)
	if f.err != nil {
		return nil, f.err
	}
	if len(f.listPages) == 0 {
		return &ateapi.ListCsiDriverConfigsResponse{}, nil
	}
	page := f.listPages[0]
	f.listPages = f.listPages[1:]
	return page, nil
}

func (f *fakeCsiDriverConfigClient) CreateCsiDriverConfig(_ context.Context, req *ateapi.CreateCsiDriverConfigRequest, _ ...grpc.CallOption) (*ateapi.CsiDriverConfig, error) {
	f.createReq = req
	if f.err != nil {
		return nil, f.err
	}
	created := proto.Clone(req.GetCsiDriverConfig()).(*ateapi.CsiDriverConfig)
	created.Metadata.Uid, created.Metadata.Version = "srv-uid-1", 1
	return created, nil
}

func (f *fakeCsiDriverConfigClient) UpdateCsiDriverConfig(_ context.Context, req *ateapi.UpdateCsiDriverConfigRequest, _ ...grpc.CallOption) (*ateapi.CsiDriverConfig, error) {
	f.updateReq = req
	if f.err != nil {
		return nil, f.err
	}
	updated := proto.Clone(req.GetCsiDriverConfig()).(*ateapi.CsiDriverConfig)
	updated.Metadata.Version++
	return updated, nil
}

func (f *fakeCsiDriverConfigClient) DeleteCsiDriverConfig(_ context.Context, req *ateapi.DeleteCsiDriverConfigRequest, _ ...grpc.CallOption) (*ateapi.CsiDriverConfig, error) {
	f.deleteReq = req
	if f.err != nil {
		return nil, f.err
	}
	return &ateapi.CsiDriverConfig{Metadata: &ateapi.ResourceMetadata{Name: req.GetCsiDriverConfig().GetName()}}, nil
}

func TestCsiDriverConfigRunners(t *testing.T) {
	ctx := context.Background()
	t0 := timestamppb.New(time.Date(2026, 1, 1, 11, 55, 0, 0, time.UTC))
	csiNFS := &ateapi.CsiDriverConfig{
		Metadata:           &ateapi.ResourceMetadata{Name: "csi-nfs", Uid: "u-nfs", Version: 2, CreateTime: t0},
		DriverName:         "nfs.csi.k8s.io",
		ControllerEndpoint: "tcp://csi-nfs.kube-system.svc:50052",
		NodeSocketOverride: "unix:///var/lib/kubelet/plugins/csi-nfsplugin/csi.sock",
		Tls:                &ateapi.CsiDriverTlsConfig{Enabled: true, UsePodIdentity: true, ServerName: "csi-nfs.kube-system.svc"},
	}
	pdCSI := &ateapi.CsiDriverConfig{
		Metadata:           &ateapi.ResourceMetadata{Name: "pd-csi", Uid: "u-pd", Version: 1, CreateTime: t0},
		DriverName:         "pd.csi.storage.gke.io",
		ControllerEndpoint: "dns:///pd-csi.kube-system.svc:9000",
	}

	client := &fakeCsiDriverConfigClient{
		byName: map[string]*ateapi.CsiDriverConfig{"csi-nfs": csiNFS, "pd-csi": pdCSI},
		listPages: []*ateapi.ListCsiDriverConfigsResponse{
			{CsiDriverConfigs: []*ateapi.CsiDriverConfig{pdCSI}, NextPageToken: "page-2"},
			{CsiDriverConfigs: []*ateapi.CsiDriverConfig{csiNFS}},
		},
	}

	// Single get -o yaml, plus round-trip into both create -f - and update -f -.
	var getOut bytes.Buffer
	if err := runGetCsiDriverConfigs(ctx, client, &getOut, "yaml", []string{"csi-nfs"}); err != nil || strings.Contains(getOut.String(), "csiDriverConfigs:") {
		t.Fatalf("single get err = %v, out = %s", err, getOut.String())
	}

	createdInput, err := loadCsiDriverConfigManifest(strings.NewReader(getOut.String()), "-")
	if err != nil || cmp.Diff(csiNFS, createdInput, protocmp.Transform()) != "" {
		t.Fatalf("get -o yaml | create -f - parse = (%v, %v), want %v", createdInput, err, csiNFS)
	}
	var out bytes.Buffer
	if err := runCreateCsiDriverConfig(ctx, client, &out, "yaml", createdInput); err != nil || cmp.Diff(csiNFS, client.createReq.GetCsiDriverConfig(), protocmp.Transform()) != "" {
		t.Fatalf("create err = %v, req = %v", err, client.createReq)
	}
	for _, w := range []string{"uid: srv-uid-1", `version: "1"`, "driverName: nfs.csi.k8s.io"} {
		if !strings.Contains(out.String(), w) {
			t.Errorf("create out = %q, want server response field %q", out.String(), w)
		}
	}

	updatedInput, err := loadCsiDriverConfigForUpdate(strings.NewReader(getOut.String()), "-")
	if err != nil {
		t.Fatalf("get -o yaml | update -f - load error = %v", err)
	}
	updatedInput.ControllerEndpoint = "tcp://csi-nfs-v2.kube-system.svc:50052"
	out.Reset()
	if err := runUpdateCsiDriverConfig(ctx, client, &out, "yaml", updatedInput); err != nil || cmp.Diff(updatedInput, client.updateReq.GetCsiDriverConfig(), protocmp.Transform()) != "" {
		t.Fatalf("update err = %v, req = %v", err, client.updateReq)
	}
	for _, w := range []string{"uid: u-nfs", `version: "3"`, "controllerEndpoint: tcp://csi-nfs-v2.kube-system.svc:50052"} {
		if !strings.Contains(out.String(), w) {
			t.Errorf("update out = %q, want server response field %q", out.String(), w)
		}
	}

	// Multi-name get and paged list.
	out.Reset()
	if err := runGetCsiDriverConfigs(ctx, client, &out, "yaml", []string{"pd-csi", "csi-nfs"}); err != nil || !strings.Contains(out.String(), "csiDriverConfigs:") {
		t.Fatalf("multi get err = %v, out = %s", err, out.String())
	}
	out.Reset()
	if err := runGetCsiDriverConfigs(ctx, client, &out, "yaml", nil); err != nil || len(client.listReqs) != 2 || client.listReqs[1].GetOptions().GetPageToken() != "page-2" {
		t.Fatalf("paged list err = %v, reqs = %+v", err, client.listReqs)
	}

	// Delete without guards and with guards.
	out.Reset()
	if err := runDeleteCsiDriverConfig(ctx, client, &out, "csi-nfs", nil); err != nil || out.String() != "csi driver config \"csi-nfs\" deleted\n" {
		t.Fatalf("unguarded delete err = %v, out = %q", err, out.String())
	}
	if diff := cmp.Diff(&ateapi.DeleteCsiDriverConfigRequest{CsiDriverConfig: &ateapi.ObjectRef{Name: "csi-nfs"}}, client.deleteReq, protocmp.Transform()); diff != "" {
		t.Errorf("unguarded deleteReq mismatch (-want +got):\n%s", diff)
	}
	out.Reset()
	opts := &ateapi.DeleteOptions{Uid: "u-nfs", Version: 2}
	if err := runDeleteCsiDriverConfig(ctx, client, &out, "csi-nfs", opts); err != nil || out.String() != "csi driver config \"csi-nfs\" deleted\n" {
		t.Fatalf("guarded delete err = %v, out = %q", err, out.String())
	}
	if diff := cmp.Diff(&ateapi.DeleteCsiDriverConfigRequest{CsiDriverConfig: &ateapi.ObjectRef{Name: "csi-nfs"}, Options: opts}, client.deleteReq, protocmp.Transform()); diff != "" {
		t.Errorf("guarded deleteReq mismatch (-want +got):\n%s", diff)
	}

	// Cobra flag wiring and pre-dial checks not covered by unit helpers.
	dir := t.TempDir()
	missingKubeconfig := filepath.Join(dir, "missing-kubeconfig")
	atespaceFile := filepath.Join(dir, "atespace.yaml")
	if err := os.WriteFile(atespaceFile, []byte("metadata: {atespace: a, name: csi-nfs}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	missingPrecondFile := filepath.Join(dir, "no-precond.yaml")
	if err := os.WriteFile(missingPrecondFile, []byte("metadata: {name: csi-nfs}\ndriverName: nfs.csi.k8s.io\ncontrollerEndpoint: tcp://127.0.0.1:9000\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name      string
		args      []string
		wantErr   string
		wantUsage bool
	}{
		{
			name:    "create -f rejected before connect",
			args:    []string{"--kubeconfig", missingKubeconfig, "create", "csi-driver-config", "-f", atespaceFile},
			wantErr: "metadata.atespace must be empty",
		},
		{
			name:    "update -f missing preconditions rejected before connect",
			args:    []string{"--kubeconfig", missingKubeconfig, "update", "csi-driver-config", "-f", missingPrecondFile},
			wantErr: "lacks the metadata.uid and metadata.version preconditions",
		},
		{
			name:      "delete --version 0 is usage error",
			args:      []string{"--kubeconfig", missingKubeconfig, "delete", "csi-driver-config", "csi-nfs", "--version", "0"},
			wantErr:   "--version must be at least 1, got 0",
			wantUsage: true,
		},
		{
			name:      "delete negative --version is usage error",
			args:      []string{"--kubeconfig", missingKubeconfig, "delete", "csi-driver-config", "csi-nfs", "--version=-5"},
			wantErr:   "--version must be at least 1, got -5",
			wantUsage: true,
		},
		{
			name:      "delete empty --uid is usage error",
			args:      []string{"--kubeconfig", missingKubeconfig, "delete", "csi-driver-config", "csi-nfs", "--uid", ""},
			wantErr:   "--uid must not be empty",
			wantUsage: true,
		},
	} {
		if _, stderr, err := runRootCLI(t, tc.args...); err == nil || !strings.Contains(err.Error(), tc.wantErr) {
			t.Fatalf("%s: runRootCLI(%v) error = %v (stderr = %q), want %q", tc.name, tc.args, err, stderr, tc.wantErr)
		} else {
			var uerr *usageError
			if errors.As(err, &uerr) != tc.wantUsage {
				t.Fatalf("%s: errors.As(usageError) = %v, want %v (err = %v)", tc.name, errors.As(err, &uerr), tc.wantUsage, err)
			}
		}
	}
}

func TestCsiDriverConfigRunnersPropagateErrors(t *testing.T) {
	ctx := context.Background()
	boom := errors.New("boom")
	client := &fakeCsiDriverConfigClient{err: boom}
	config := &ateapi.CsiDriverConfig{
		Metadata:           &ateapi.ResourceMetadata{Name: "x", Uid: "u-1", Version: 1},
		DriverName:         "d.csi.example.io",
		ControllerEndpoint: "tcp://127.0.0.1:9000",
	}
	var out bytes.Buffer
	for _, tc := range []struct {
		name, want string
		run        func() error
	}{
		{"get", `failed to get csi driver config "x"`, func() error { return runGetCsiDriverConfigs(ctx, client, &out, "table", []string{"x"}) }},
		{"list", "failed to list csi driver configs", func() error { return runGetCsiDriverConfigs(ctx, client, &out, "table", nil) }},
		{"create", "failed to create csi driver config", func() error { return runCreateCsiDriverConfig(ctx, client, &out, "yaml", config) }},
		{"update", "failed to update csi driver config", func() error { return runUpdateCsiDriverConfig(ctx, client, &out, "yaml", config) }},
		{"delete", "failed to delete csi driver config", func() error { return runDeleteCsiDriverConfig(ctx, client, &out, "x", nil) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out.Reset()
			err := tc.run()
			if !errors.Is(err, boom) || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want %q wrapping boom", err, tc.want)
			}
			if out.Len() != 0 {
				t.Errorf("out = %q, want nothing printed on error", out.String())
			}
		})
	}
}
