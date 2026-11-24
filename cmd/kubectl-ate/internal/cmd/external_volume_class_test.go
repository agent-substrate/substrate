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
	"go.ate.dev/substrate/cmd/kubectl-ate/internal/printer"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestExternalVolumeClassCommandArgs(t *testing.T) {
	runCommandArgsTests(t, []commandArgsTest{
		{name: "list", command: getExternalVolumeClassesCmd},
		{name: "get", command: getExternalVolumeClassesCmd, args: []string{"fast-ssd"}},
		{name: "get multiple", command: getExternalVolumeClassesCmd, args: []string{"fast-ssd", "standard-rwo"}},
		{name: "create missing -f", command: createExternalVolumeClassCmd, wantErr: true},
		{name: "create extra arg", command: createExternalVolumeClassCmd, args: []string{"fast-ssd"}, wantErr: true},
		{name: "delete", command: deleteExternalVolumeClassCmd, args: []string{"fast-ssd"}},
		{name: "delete missing name", command: deleteExternalVolumeClassCmd, wantErr: true},
		{name: "delete extra name", command: deleteExternalVolumeClassCmd, args: []string{"a", "b"}, wantErr: true},
	})
}

func TestLoadExternalVolumeClassManifest(t *testing.T) {
	t.Parallel()
	wantFastSSD := &ateapi.ExternalVolumeClass{Metadata: &ateapi.ResourceMetadata{Name: "fast-ssd"}, DriverName: "pd.csi.storage.gke.io", Parameters: map[string]string{"type": "pd-ssd"}}
	for _, tc := range []struct {
		name, manifest, wantErr string
		want                    *ateapi.ExternalVolumeClass
	}{
		{name: "camelCase yaml", manifest: "metadata: {name: fast-ssd}\ndriverName: pd.csi.storage.gke.io\nparameters: {type: pd-ssd}\n", want: wantFastSSD},
		{name: "snake_case yaml", manifest: "metadata: {name: fast-ssd}\ndriver_name: pd.csi.storage.gke.io\nparameters: {type: pd-ssd}\n", want: wantFastSSD},
		{name: "json input", manifest: `{"metadata": {"name": "fast-ssd"}, "driverName": "pd.csi.storage.gke.io", "parameters": {"type": "pd-ssd"}}`, want: wantFastSSD},
		{name: "missing metadata.name", manifest: "driverName: pd.csi.storage.gke.io", wantErr: "metadata.name is required"},
		{name: "atespace set", manifest: "metadata: {atespace: team-a, name: fast-ssd}", wantErr: "metadata.atespace must be empty"},
		{name: "unknown field", manifest: "metadata: {name: fast-ssd}\nprovisioner: pd.csi.storage.gke.io", wantErr: "invalid ExternalVolumeClass"},
		{name: "empty", manifest: "", wantErr: "manifest is empty"},
		{name: "multi-document yaml", manifest: "metadata: {name: a}\n---\nmetadata: {name: b}\n", wantErr: "manifest holds more than one document"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := loadExternalVolumeClassManifest(strings.NewReader(tc.manifest), "-")
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("loadExternalVolumeClassManifest() error = %v, want %q", err, tc.wantErr)
				}
				return
			}
			if err != nil || cmp.Diff(tc.want, got, protocmp.Transform()) != "" {
				t.Fatalf("loadExternalVolumeClassManifest() = (%v, %v), want %v", got, err, tc.want)
			}
		})
	}

	t.Run("yaml round-trip", func(t *testing.T) {
		t.Parallel()
		serverObj := &ateapi.ExternalVolumeClass{
			Metadata:   &ateapi.ResourceMetadata{Name: "fast-ssd", Uid: "u-1", Version: 1, CreateTime: timestamppb.New(time.Date(2026, 1, 1, 11, 55, 0, 0, time.UTC))},
			DriverName: "pd.csi.storage.gke.io",
			Parameters: map[string]string{"type": "pd-ssd"},
		}
		var yamlOut bytes.Buffer
		if err := printer.PrintExternalVolumeClassTo(&yamlOut, serverObj, "yaml"); err != nil {
			t.Fatalf("PrintExternalVolumeClassTo(yaml) error = %v", err)
		}
		parsed, err := loadExternalVolumeClassManifest(&yamlOut, "-")
		if err != nil || cmp.Diff(serverObj, parsed, protocmp.Transform()) != "" {
			t.Fatalf("round-trip = (%v, %v), want %v", parsed, err, serverObj)
		}
	})
}

type fakeExternalVolumeClassClient struct {
	byName    map[string]*ateapi.ExternalVolumeClass
	listPages []*ateapi.ListExternalVolumeClassesResponse
	listReqs  []*ateapi.ListExternalVolumeClassesRequest
	createReq *ateapi.CreateExternalVolumeClassRequest
	deleteReq *ateapi.DeleteExternalVolumeClassRequest
	err       error
}

func (f *fakeExternalVolumeClassClient) GetExternalVolumeClass(_ context.Context, req *ateapi.GetExternalVolumeClassRequest, _ ...grpc.CallOption) (*ateapi.ExternalVolumeClass, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.byName[req.GetExternalVolumeClass().GetName()], nil
}
func (f *fakeExternalVolumeClassClient) ListExternalVolumeClasses(_ context.Context, req *ateapi.ListExternalVolumeClassesRequest, _ ...grpc.CallOption) (*ateapi.ListExternalVolumeClassesResponse, error) {
	f.listReqs = append(f.listReqs, req)
	if f.err != nil {
		return nil, f.err
	}
	if len(f.listPages) == 0 {
		return &ateapi.ListExternalVolumeClassesResponse{}, nil
	}
	page := f.listPages[0]
	f.listPages = f.listPages[1:]
	return page, nil
}
func (f *fakeExternalVolumeClassClient) CreateExternalVolumeClass(_ context.Context, req *ateapi.CreateExternalVolumeClassRequest, _ ...grpc.CallOption) (*ateapi.ExternalVolumeClass, error) {
	f.createReq = req
	if f.err != nil {
		return nil, f.err
	}
	created := proto.Clone(req.GetExternalVolumeClass()).(*ateapi.ExternalVolumeClass)
	created.Metadata.Uid, created.Metadata.Version = "srv-uid-1", 7
	return created, nil
}
func (f *fakeExternalVolumeClassClient) DeleteExternalVolumeClass(_ context.Context, req *ateapi.DeleteExternalVolumeClassRequest, _ ...grpc.CallOption) (*ateapi.ExternalVolumeClass, error) {
	f.deleteReq = req
	if f.err != nil {
		return nil, f.err
	}
	return &ateapi.ExternalVolumeClass{Metadata: &ateapi.ResourceMetadata{Name: req.GetExternalVolumeClass().GetName()}}, nil
}

func TestExternalVolumeClassRunners(t *testing.T) {
	ctx := context.Background()
	fastSSD := &ateapi.ExternalVolumeClass{Metadata: &ateapi.ResourceMetadata{Name: "fast-ssd", Version: 1}, DriverName: "pd.csi.storage.gke.io", Parameters: map[string]string{"type": "pd-ssd"}}
	standardRWO := &ateapi.ExternalVolumeClass{Metadata: &ateapi.ResourceMetadata{Name: "standard-rwo", Version: 1}, DriverName: "pd.csi.storage.gke.io"}

	client := &fakeExternalVolumeClassClient{
		byName: map[string]*ateapi.ExternalVolumeClass{"fast-ssd": fastSSD, "standard-rwo": standardRWO},
		listPages: []*ateapi.ListExternalVolumeClassesResponse{
			{ExternalVolumeClasses: []*ateapi.ExternalVolumeClass{standardRWO}, NextPageToken: "page-2"},
			{ExternalVolumeClasses: []*ateapi.ExternalVolumeClass{fastSSD}},
		},
	}
	var out bytes.Buffer
	if err := runGetExternalVolumeClasses(ctx, client, &out, "yaml", []string{"fast-ssd"}); err != nil || strings.Contains(out.String(), "externalVolumeClasses:") {
		t.Fatalf("single get err = %v, out = %s", err, out.String())
	}
	out.Reset()
	if err := runGetExternalVolumeClasses(ctx, client, &out, "yaml", []string{"standard-rwo", "fast-ssd"}); err != nil || !strings.Contains(out.String(), "externalVolumeClasses:") {
		t.Fatalf("multi get err = %v, out = %s", err, out.String())
	}
	out.Reset()
	if err := runGetExternalVolumeClasses(ctx, client, &out, "yaml", nil); err != nil || len(client.listReqs) != 2 || client.listReqs[1].GetOptions().GetPageToken() != "page-2" {
		t.Fatalf("paged list err = %v, reqs = %+v", err, client.listReqs)
	}
	out.Reset()
	if err := runCreateExternalVolumeClass(ctx, client, &out, "yaml", fastSSD); err != nil || cmp.Diff(fastSSD, client.createReq.GetExternalVolumeClass(), protocmp.Transform()) != "" {
		t.Fatalf("create err = %v, req = %v", err, client.createReq)
	}
	for _, w := range []string{"uid: srv-uid-1", `version: "7"`, "driverName: pd.csi.storage.gke.io"} {
		if !strings.Contains(out.String(), w) {
			t.Errorf("create out = %q, want server response field %q", out.String(), w)
		}
	}
	out.Reset()
	opts := &ateapi.DeleteOptions{Uid: "u-1", Version: 3}
	if err := runDeleteExternalVolumeClass(ctx, client, &out, "fast-ssd", opts); err != nil || out.String() != "external volume class \"fast-ssd\" deleted\n" {
		t.Fatalf("delete err = %v, out = %q", err, out.String())
	}
	if diff := cmp.Diff(&ateapi.DeleteExternalVolumeClassRequest{ExternalVolumeClass: &ateapi.ObjectRef{Name: "fast-ssd"}, Options: opts}, client.deleteReq, protocmp.Transform()); diff != "" {
		t.Errorf("deleteReq mismatch (-want +got):\n%s", diff)
	}
	bad := filepath.Join(t.TempDir(), "evc.yaml")
	if err := os.WriteFile(bad, []byte("metadata: {atespace: a, name: b}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := runRootCLI(t, "create", "external-volume-class", "-f", bad); err == nil || !strings.Contains(err.Error(), "metadata.atespace must be empty") {
		t.Fatalf("create -f bad error = %v, want metadata.atespace must be empty", err)
	}
	if _, _, err := runRootCLI(t, "delete", "external-volume-class", "fast-ssd", "--version", "0"); err == nil || !strings.Contains(err.Error(), "--version must be at least 1") {
		t.Fatalf("delete --version 0 error = %v, want --version must be at least 1", err)
	}
	if _, _, err := runRootCLI(t, "delete", "external-volume-class", "fast-ssd", "--uid", ""); err == nil || !strings.Contains(err.Error(), "--uid must not be empty") {
		t.Fatalf("delete --uid '' error = %v, want --uid must not be empty", err)
	}
}

func TestExternalVolumeClassRunnersPropagateErrors(t *testing.T) {
	ctx := context.Background()
	boom := errors.New("boom")
	client := &fakeExternalVolumeClassClient{err: boom}
	class := &ateapi.ExternalVolumeClass{Metadata: &ateapi.ResourceMetadata{Name: "x"}, DriverName: "d"}
	var out bytes.Buffer
	for _, tc := range []struct {
		name, want string
		run        func() error
	}{
		{"get", `failed to get external volume class "x"`, func() error { return runGetExternalVolumeClasses(ctx, client, &out, "table", []string{"x"}) }},
		{"list", "failed to list external volume classes", func() error { return runGetExternalVolumeClasses(ctx, client, &out, "table", nil) }},
		{"create", "failed to create external volume class", func() error { return runCreateExternalVolumeClass(ctx, client, &out, "yaml", class) }},
		{"delete", "failed to delete external volume class", func() error { return runDeleteExternalVolumeClass(ctx, client, &out, "x", nil) }},
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
