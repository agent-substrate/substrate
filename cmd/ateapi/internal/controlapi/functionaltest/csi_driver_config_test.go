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

package functionaltest

import (
	"context"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/internal/preview"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func newCsiDriverConfig(name, driverName string) *ateapi.CsiDriverConfig {
	return &ateapi.CsiDriverConfig{
		Metadata:           &ateapi.ResourceMetadata{Name: name},
		DriverName:         driverName,
		ControllerEndpoint: "dns:///csi-nfs-controller.csi-nfs.svc:12345",
		NodeSocketOverride: "unix:///var/lib/kubelet/plugins/nfs.csi.k8s.io/csi.sock",
		Tls:                &ateapi.CsiDriverTlsConfig{Enabled: true, UsePodIdentity: true, ServerName: "csi-nfs-controller.csi-nfs.svc"},
	}
}

func createCsiDriverConfig(t *testing.T, tc *testContext, name, driverName string) *ateapi.CsiDriverConfig {
	t.Helper()
	created, err := tc.client.CreateCsiDriverConfig(context.Background(), &ateapi.CreateCsiDriverConfigRequest{
		CsiDriverConfig: newCsiDriverConfig(name, driverName),
	})
	if err != nil {
		t.Fatalf("CreateCsiDriverConfig(%s) failed: %v", name, err)
	}
	return created
}

func TestCreateCsiDriverConfig(t *testing.T) {
	preview.InitForTest(t)
	ns := namespaceForTest("ns-create-cdc")
	tc := setupPreviewTest(t, ns)
	defer tc.cleanup()
	ctx := context.Background()

	// Gate disabled rejects even an opt-in client.
	_, err := tc.client.CreateCsiDriverConfig(ctx, &ateapi.CreateCsiDriverConfigRequest{CsiDriverConfig: newCsiDriverConfig("csi-nfs", "nfs.csi.k8s.io")})
	assertGrpcErrorRegex(t, err, codes.InvalidArgument, "csi_driver_config: Forbidden")

	preview.InitForTest(t, "ExternalVolumes")

	in := newCsiDriverConfig("csi-nfs", "nfs.csi.k8s.io")
	in.Metadata = &ateapi.ResourceMetadata{
		Name:       "csi-nfs",
		Uid:        "caller-supplied-uid",
		Version:    999,
		CreateTime: timestamppb.New(time.Unix(1, 0)),
		UpdateTime: timestamppb.New(time.Unix(2, 0)),
		DeleteTime: timestamppb.New(time.Unix(3, 0)),
	}
	created, err := tc.client.CreateCsiDriverConfig(ctx, &ateapi.CreateCsiDriverConfigRequest{CsiDriverConfig: in})
	if err != nil {
		t.Fatalf("CreateCsiDriverConfig failed: %v", err)
	}
	if diff := cmp.Diff(newCsiDriverConfig("csi-nfs", "nfs.csi.k8s.io"), created, protocmp.Transform(), ignoreServerMetadata, ignoreVersion); diff != "" {
		t.Errorf("CreateCsiDriverConfig mismatch (-want +got):\n%s", diff)
	}
	md := created.GetMetadata()
	if md.GetVersion() != 1 || md.GetUid() == "" || md.GetUid() == "caller-supplied-uid" ||
		md.GetCreateTime() == nil || md.GetCreateTime().AsTime().Unix() == 1 ||
		md.GetUpdateTime() == nil || md.GetUpdateTime().AsTime().Unix() == 2 ||
		md.GetDeleteTime() != nil {
		t.Errorf("metadata = %v, want server-assigned uid/timestamps and version 1", md)
	}

	got, err := tc.client.GetCsiDriverConfig(ctx, &ateapi.GetCsiDriverConfigRequest{CsiDriverConfig: &ateapi.ObjectRef{Name: "csi-nfs"}})
	if err != nil {
		t.Fatalf("GetCsiDriverConfig failed: %v", err)
	}
	if diff := cmp.Diff(created, got, protocmp.Transform()); diff != "" {
		t.Errorf("GetCsiDriverConfig mismatch (-created +got):\n%s", diff)
	}

	_, err = tc.client.CreateCsiDriverConfig(ctx, &ateapi.CreateCsiDriverConfigRequest{CsiDriverConfig: newCsiDriverConfig("csi-nfs", "other.csi.k8s.io")})
	assertGrpcErrorRegex(t, err, codes.AlreadyExists, "csi-nfs.*already exists")

	_, err = tc.client.CreateCsiDriverConfig(ctx, &ateapi.CreateCsiDriverConfigRequest{CsiDriverConfig: newCsiDriverConfig("csi-nfs-2", "nfs.csi.k8s.io")})
	assertGrpcErrorRegex(t, err, codes.AlreadyExists, "driver_name.*nfs\\.csi\\.k8s\\.io")

	_, err = tc.noOptInClient.CreateCsiDriverConfig(ctx, &ateapi.CreateCsiDriverConfigRequest{CsiDriverConfig: newCsiDriverConfig("csi-other", "other.csi.k8s.io")})
	assertGrpcErrorRegex(t, err, codes.InvalidArgument, "csi_driver_config: Forbidden")
}

func TestGetAndListCsiDriverConfigs(t *testing.T) {
	preview.InitForTest(t, "ExternalVolumes")
	ns := namespaceForTest("ns-list-cdc")
	tc := setupPreviewTest(t, ns)
	defer tc.cleanup()
	ctx := context.Background()

	_, err := tc.client.GetCsiDriverConfig(ctx, &ateapi.GetCsiDriverConfigRequest{CsiDriverConfig: &ateapi.ObjectRef{Name: "missing"}})
	assertGrpcErrorRegex(t, err, codes.NotFound, "missing.*not found")

	for _, item := range []struct{ name, driver string }{
		{"cfg-c", "c.csi.k8s.io"},
		{"cfg-a", "a.csi.k8s.io"},
		{"cfg-b", "b.csi.k8s.io"},
	} {
		createCsiDriverConfig(t, tc, item.name, item.driver)
	}

	var got []string
	for token := ""; ; {
		resp, err := tc.client.ListCsiDriverConfigs(ctx, &ateapi.ListCsiDriverConfigsRequest{Options: &ateapi.ListOptions{PageSize: 2, PageToken: token}})
		if err != nil {
			t.Fatalf("ListCsiDriverConfigs failed: %v", err)
		}
		for _, cfg := range resp.GetCsiDriverConfigs() {
			got = append(got, cfg.GetMetadata().GetName())
		}
		if token = resp.GetNextPageToken(); token == "" {
			break
		}
	}
	if diff := cmp.Diff([]string{"cfg-a", "cfg-b", "cfg-c"}, got); diff != "" {
		t.Errorf("listed configs mismatch (-want +got):\n%s", diff)
	}

	_, err = tc.client.ListCsiDriverConfigs(ctx, &ateapi.ListCsiDriverConfigsRequest{Options: &ateapi.ListOptions{PageToken: "%%%"}})
	assertGrpcError(t, err, codes.InvalidArgument, "invalid page_token")
}

func TestUpdateCsiDriverConfig(t *testing.T) {
	preview.InitForTest(t, "ExternalVolumes")
	ns := namespaceForTest("ns-update-cdc")
	tc := setupPreviewTest(t, ns)
	defer tc.cleanup()
	ctx := context.Background()

	created := createCsiDriverConfig(t, tc, "csi-nfs", "nfs.csi.k8s.io")
	uid := created.GetMetadata().GetUid()

	// Replace mutable fields while attempting to smuggle caller timestamps.
	reqCfg := proto.CloneOf(created)
	reqCfg.ControllerEndpoint = "tcp://10.0.0.2:9000"
	reqCfg.NodeSocketOverride = "unix:///run/csi/nfs.sock"
	reqCfg.Tls = &ateapi.CsiDriverTlsConfig{Enabled: true, UsePodIdentity: true, ServerName: "alt-nfs.csi-nfs.svc"}
	reqCfg.Metadata.CreateTime = timestamppb.New(time.Unix(1, 0))
	reqCfg.Metadata.UpdateTime = timestamppb.New(time.Unix(2, 0))
	reqCfg.Metadata.DeleteTime = timestamppb.New(time.Unix(3, 0))

	updated, err := tc.client.UpdateCsiDriverConfig(ctx, &ateapi.UpdateCsiDriverConfigRequest{CsiDriverConfig: reqCfg})
	if err != nil {
		t.Fatalf("UpdateCsiDriverConfig failed: %v", err)
	}
	md := updated.GetMetadata()
	if md.GetVersion() != 2 || md.GetUid() != uid ||
		!md.GetCreateTime().AsTime().Equal(created.GetMetadata().GetCreateTime().AsTime()) ||
		md.GetUpdateTime().AsTime().Before(created.GetMetadata().GetUpdateTime().AsTime()) ||
		md.GetDeleteTime() != nil {
		t.Errorf("updated metadata = %v, created = %v", md, created.GetMetadata())
	}

	// Full replacement clears optional node_socket_override and tls when omitted.
	clearedReq := proto.CloneOf(updated)
	clearedReq.NodeSocketOverride = ""
	clearedReq.Tls = nil
	cleared, err := tc.client.UpdateCsiDriverConfig(ctx, &ateapi.UpdateCsiDriverConfigRequest{CsiDriverConfig: clearedReq})
	if err != nil {
		t.Fatalf("UpdateCsiDriverConfig(clear optional) failed: %v", err)
	}
	if cleared.GetMetadata().GetVersion() != 3 || cleared.GetNodeSocketOverride() != "" || cleared.GetTls() != nil {
		t.Errorf("cleared config = %v, want version 3 with empty socket override and nil tls", cleared)
	}

	// Missing uid or version precondition -> InvalidArgument.
	noUID := proto.CloneOf(cleared)
	noUID.Metadata.Uid = ""
	_, err = tc.client.UpdateCsiDriverConfig(ctx, &ateapi.UpdateCsiDriverConfigRequest{CsiDriverConfig: noUID})
	assertGrpcErrorRegex(t, err, codes.InvalidArgument, "precondition required")

	noVer := proto.CloneOf(cleared)
	noVer.Metadata.Version = 0
	_, err = tc.client.UpdateCsiDriverConfig(ctx, &ateapi.UpdateCsiDriverConfigRequest{CsiDriverConfig: noVer})
	assertGrpcErrorRegex(t, err, codes.InvalidArgument, "precondition required")

	// Stale version or wrong uid -> Aborted.
	stale := proto.CloneOf(cleared)
	stale.Metadata.Version = 1
	_, err = tc.client.UpdateCsiDriverConfig(ctx, &ateapi.UpdateCsiDriverConfigRequest{CsiDriverConfig: stale})
	assertGrpcErrorRegex(t, err, codes.Aborted, "concurrent update conflict")

	wrongUID := proto.CloneOf(cleared)
	wrongUID.Metadata.Uid = foreignUID
	_, err = tc.client.UpdateCsiDriverConfig(ctx, &ateapi.UpdateCsiDriverConfigRequest{CsiDriverConfig: wrongUID})
	assertGrpcErrorRegex(t, err, codes.Aborted, foreignUID)

	// Non-existent config -> NotFound.
	missing := newCsiDriverConfig("does-not-exist", "other.csi.k8s.io")
	missing.Metadata.Uid = foreignUID
	missing.Metadata.Version = 1
	_, err = tc.client.UpdateCsiDriverConfig(ctx, &ateapi.UpdateCsiDriverConfigRequest{CsiDriverConfig: missing})
	assertGrpcErrorRegex(t, err, codes.NotFound, "does-not-exist.*not found")

	// Immutable driver_name, invalid controller_endpoint, invalid tls, and missing preview opt-in -> InvalidArgument.
	changedDriver := proto.CloneOf(cleared)
	changedDriver.DriverName = "other.csi.k8s.io"
	_, err = tc.client.UpdateCsiDriverConfig(ctx, &ateapi.UpdateCsiDriverConfigRequest{CsiDriverConfig: changedDriver})
	assertGrpcErrorRegex(t, err, codes.InvalidArgument, "driver_name")

	badEndpoint := proto.CloneOf(cleared)
	badEndpoint.ControllerEndpoint = "http://10.0.0.2:9000"
	_, err = tc.client.UpdateCsiDriverConfig(ctx, &ateapi.UpdateCsiDriverConfigRequest{CsiDriverConfig: badEndpoint})
	assertGrpcErrorRegex(t, err, codes.InvalidArgument, "controller_endpoint")

	badSocket := proto.CloneOf(cleared)
	badSocket.NodeSocketOverride = "/relative/csi.sock"
	_, err = tc.client.UpdateCsiDriverConfig(ctx, &ateapi.UpdateCsiDriverConfigRequest{CsiDriverConfig: badSocket})
	assertGrpcErrorRegex(t, err, codes.InvalidArgument, "node_socket_override")

	badTLS := proto.CloneOf(cleared)
	badTLS.Tls = &ateapi.CsiDriverTlsConfig{Enabled: true, UsePodIdentity: false, ServerName: "x.svc"}
	_, err = tc.client.UpdateCsiDriverConfig(ctx, &ateapi.UpdateCsiDriverConfigRequest{CsiDriverConfig: badTLS})
	assertGrpcErrorRegex(t, err, codes.InvalidArgument, "use_pod_identity")

	_, err = tc.noOptInClient.UpdateCsiDriverConfig(ctx, &ateapi.UpdateCsiDriverConfigRequest{CsiDriverConfig: proto.CloneOf(cleared)})
	assertGrpcErrorRegex(t, err, codes.InvalidArgument, "csi_driver_config: Forbidden")
}

func TestDeleteCsiDriverConfig(t *testing.T) {
	preview.InitForTest(t, "ExternalVolumes")
	ns := namespaceForTest("ns-delete-cdc")
	tc := setupPreviewTest(t, ns)
	defer tc.cleanup()
	ctx := context.Background()
	ref := &ateapi.ObjectRef{Name: "csi-nfs"}
	del := func(opts *ateapi.DeleteOptions) (*ateapi.CsiDriverConfig, error) {
		return tc.client.DeleteCsiDriverConfig(ctx, &ateapi.DeleteCsiDriverConfigRequest{CsiDriverConfig: ref, Options: opts})
	}

	created := createCsiDriverConfig(t, tc, "csi-nfs", "nfs.csi.k8s.io")
	uid, version := created.GetMetadata().GetUid(), created.GetMetadata().GetVersion()

	_, err := del(&ateapi.DeleteOptions{Uid: uid, Version: version + 1})
	assertGrpcError(t, err, codes.Aborted, "concurrent update conflict, please retry")
	_, err = del(&ateapi.DeleteOptions{Uid: foreignUID})
	assertGrpcErrorRegex(t, err, codes.Aborted, foreignUID)

	createExternalVolumeClass(t, tc, "nfs")
	_, err = del(nil)
	assertGrpcError(t, err, codes.FailedPrecondition, "CsiDriverConfig csi-nfs cannot be deleted while referenced by an ExternalVolumeClass")
	if _, err := tc.client.GetCsiDriverConfig(ctx, &ateapi.GetCsiDriverConfigRequest{CsiDriverConfig: ref}); err != nil {
		t.Fatalf("config removed despite in-use failure: %v", err)
	}

	if _, err := tc.client.DeleteExternalVolumeClass(ctx, &ateapi.DeleteExternalVolumeClassRequest{ExternalVolumeClass: &ateapi.ObjectRef{Name: "nfs"}}); err != nil {
		t.Fatalf("DeleteExternalVolumeClass failed: %v", err)
	}
	deleted, err := del(&ateapi.DeleteOptions{Uid: uid, Version: version})
	if err != nil {
		t.Fatalf("DeleteCsiDriverConfig failed: %v", err)
	}
	if deleted.GetMetadata().GetDeleteTime() == nil {
		t.Errorf("deleted config = %v, want delete_time populated", deleted.GetMetadata())
	}
	if diff := cmp.Diff(created, deleted, protocmp.Transform(), ignoreTimestamps); diff != "" {
		t.Errorf("DeleteCsiDriverConfig mismatch (-created +deleted):\n%s", diff)
	}
	_, err = tc.client.GetCsiDriverConfig(ctx, &ateapi.GetCsiDriverConfigRequest{CsiDriverConfig: ref})
	assertGrpcErrorRegex(t, err, codes.NotFound, "csi-nfs.*not found")
	_, err = del(nil)
	assertGrpcErrorRegex(t, err, codes.NotFound, "csi-nfs.*not found")
}

func TestValidation_CsiDriverConfig(t *testing.T) {
	preview.InitForTest(t, "ExternalVolumes")
	ns := namespaceForTest("ns-validation-cdc")
	tc := setupPreviewTest(t, ns)
	defer tc.cleanup()
	ctx := context.Background()

	t.Run("CreateCsiDriverConfig", func(t *testing.T) {
		for _, c := range []struct {
			name   string
			mutate func(*ateapi.CsiDriverConfig)
			want   string
		}{
			{"atespace forbidden", func(cfg *ateapi.CsiDriverConfig) { cfg.Metadata.Atespace = testAtespace }, "atespace: Forbidden"},
			{"invalid driver_name", func(cfg *ateapi.CsiDriverConfig) { cfg.DriverName = "Bad_Driver" }, "driver_name: Invalid value"},
			{"invalid controller_endpoint", func(cfg *ateapi.CsiDriverConfig) { cfg.ControllerEndpoint = "tcp://no-port" }, "controller_endpoint: Invalid value"},
			{"invalid node_socket_override", func(cfg *ateapi.CsiDriverConfig) { cfg.NodeSocketOverride = "/relative/csi.sock" }, "node_socket_override: Invalid value"},
			{"invalid tls", func(cfg *ateapi.CsiDriverConfig) {
				cfg.Tls = &ateapi.CsiDriverTlsConfig{Enabled: false, ServerName: "csi.svc"}
			}, "server_name: Forbidden"},
		} {
			t.Run(c.name, func(t *testing.T) {
				bad := newCsiDriverConfig("csi-nfs", "nfs.csi.k8s.io")
				c.mutate(bad)
				_, err := tc.client.CreateCsiDriverConfig(ctx, &ateapi.CreateCsiDriverConfigRequest{CsiDriverConfig: bad})
				assertGrpcErrorRegex(t, err, codes.InvalidArgument, c.want)
			})
		}
	})
	t.Run("GetCsiDriverConfig", func(t *testing.T) {
		_, err := tc.client.GetCsiDriverConfig(ctx, &ateapi.GetCsiDriverConfigRequest{})
		assertGrpcErrorRegex(t, err, codes.InvalidArgument, "csi_driver_config: Required value")
		_, err = tc.client.GetCsiDriverConfig(ctx, &ateapi.GetCsiDriverConfigRequest{CsiDriverConfig: &ateapi.ObjectRef{Atespace: testAtespace, Name: "csi-nfs"}})
		assertGrpcErrorRegex(t, err, codes.InvalidArgument, "atespace: Forbidden")
	})
	t.Run("ListCsiDriverConfigs", func(t *testing.T) {
		_, err := tc.client.ListCsiDriverConfigs(ctx, &ateapi.ListCsiDriverConfigsRequest{Options: &ateapi.ListOptions{PageSize: -1}})
		assertGrpcErrorRegex(t, err, codes.InvalidArgument, "page_size: Invalid value")
	})
	t.Run("UpdateCsiDriverConfig", func(t *testing.T) {
		_, err := tc.client.UpdateCsiDriverConfig(ctx, &ateapi.UpdateCsiDriverConfigRequest{})
		assertGrpcErrorRegex(t, err, codes.InvalidArgument, "csi_driver_config: Required value")
		withAtespace := newCsiDriverConfig("csi-nfs", "nfs.csi.k8s.io")
		withAtespace.Metadata.Atespace = testAtespace
		_, err = tc.client.UpdateCsiDriverConfig(ctx, &ateapi.UpdateCsiDriverConfigRequest{CsiDriverConfig: withAtespace})
		assertGrpcErrorRegex(t, err, codes.InvalidArgument, "atespace: Forbidden")
	})
	t.Run("DeleteCsiDriverConfig", func(t *testing.T) {
		_, err := tc.client.DeleteCsiDriverConfig(ctx, &ateapi.DeleteCsiDriverConfigRequest{})
		assertGrpcErrorRegex(t, err, codes.InvalidArgument, "csi_driver_config: Required value")
		_, err = tc.client.DeleteCsiDriverConfig(ctx, &ateapi.DeleteCsiDriverConfigRequest{CsiDriverConfig: &ateapi.ObjectRef{Atespace: testAtespace, Name: "csi-nfs"}})
		assertGrpcErrorRegex(t, err, codes.InvalidArgument, "atespace: Forbidden")
	})
}
