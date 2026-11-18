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

package apivalidation

import (
	"context"
	"strings"
	"testing"

	"go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

func validCsiDriverConfig(mutate ...func(*ateapi.CsiDriverConfig)) *ateapi.CsiDriverConfig {
	cfg := &ateapi.CsiDriverConfig{
		Metadata:           &ateapi.ResourceMetadata{Name: "csi-nfs"},
		DriverName:         "nfs.csi.k8s.io",
		ControllerEndpoint: "dns:///csi-nfs.ate-system.svc:9000",
	}
	for _, fn := range mutate {
		if fn != nil {
			fn(cfg)
		}
	}
	return cfg
}

func TestValidateCreateCsiDriverConfigRequest(t *testing.T) {
	cdc := field.NewPath("csi_driver_config")
	ep, sock, tlsPath := cdc.Child("controller_endpoint"), cdc.Child("node_socket_override"), cdc.Child("tls")
	forbiddenWithoutPreview := field.ErrorList{field.Forbidden(cdc, "")}
	tests := []struct {
		name            string
		req             *ateapi.CreateCsiDriverConfigRequest
		want            field.ErrorList
		wantWithPreview field.ErrorList
	}{
		{"missing csi_driver_config", &ateapi.CreateCsiDriverConfigRequest{}, field.ErrorList{field.Required(cdc, "")}, field.ErrorList{field.Required(cdc, "")}},
		{"valid default", &ateapi.CreateCsiDriverConfigRequest{CsiDriverConfig: validCsiDriverConfig()}, forbiddenWithoutPreview, nil},
		{"valid substrate.io driver + ipv6 tcp + socket + tls", &ateapi.CreateCsiDriverConfigRequest{CsiDriverConfig: validCsiDriverConfig(func(c *ateapi.CsiDriverConfig) {
			c.DriverName = "substrate.io/mock"
			c.ControllerEndpoint = "tcp://[::1]:9000"
			c.NodeSocketOverride = "unix:///var/lib/kubelet/plugins/mock/csi.sock"
			c.Tls = &ateapi.CsiDriverTlsConfig{Enabled: true, UsePodIdentity: true, ServerName: "csi.ate-system.svc"}
		})}, forbiddenWithoutPreview, nil},
		{"valid dns authority with port", &ateapi.CreateCsiDriverConfigRequest{CsiDriverConfig: validCsiDriverConfig(func(c *ateapi.CsiDriverConfig) { c.ControllerEndpoint = "dns://8.8.8.8:53/csi-nfs:9000" })}, forbiddenWithoutPreview, nil},
		{"valid dns ipv4 authority without port", &ateapi.CreateCsiDriverConfigRequest{CsiDriverConfig: validCsiDriverConfig(func(c *ateapi.CsiDriverConfig) { c.ControllerEndpoint = "dns://8.8.8.8/csi-nfs:9000" })}, forbiddenWithoutPreview, nil},
		{"valid dns ipv6 authority without port", &ateapi.CreateCsiDriverConfigRequest{CsiDriverConfig: validCsiDriverConfig(func(c *ateapi.CsiDriverConfig) { c.ControllerEndpoint = "dns://[::1]/csi-nfs:9000" })}, forbiddenWithoutPreview, nil},
		{"valid dns ipv6 authority and target with port", &ateapi.CreateCsiDriverConfigRequest{CsiDriverConfig: validCsiDriverConfig(func(c *ateapi.CsiDriverConfig) { c.ControllerEndpoint = "dns://[::1]:53/[::1]:9000" })}, forbiddenWithoutPreview, nil},
		{"valid explicit disabled tls", &ateapi.CreateCsiDriverConfigRequest{CsiDriverConfig: validCsiDriverConfig(func(c *ateapi.CsiDriverConfig) { c.Tls = &ateapi.CsiDriverTlsConfig{} })}, forbiddenWithoutPreview, nil},
		{"atespace forbidden", &ateapi.CreateCsiDriverConfigRequest{CsiDriverConfig: validCsiDriverConfig(func(c *ateapi.CsiDriverConfig) { c.Metadata.Atespace = "as" })}, forbiddenWithoutPreview, field.ErrorList{field.Forbidden(cdc.Child("metadata", "atespace"), "")}},
		{"missing driver_name", &ateapi.CreateCsiDriverConfigRequest{CsiDriverConfig: validCsiDriverConfig(func(c *ateapi.CsiDriverConfig) { c.DriverName = "" })}, forbiddenWithoutPreview, field.ErrorList{field.Required(cdc.Child("driver_name"), "")}},
		{"invalid driver_name", &ateapi.CreateCsiDriverConfigRequest{CsiDriverConfig: validCsiDriverConfig(func(c *ateapi.CsiDriverConfig) { c.DriverName = "Bad_Driver" })}, forbiddenWithoutPreview, field.ErrorList{field.Invalid(cdc.Child("driver_name"), nil, "")}},
		{"too-long driver_name", &ateapi.CreateCsiDriverConfigRequest{CsiDriverConfig: validCsiDriverConfig(func(c *ateapi.CsiDriverConfig) { c.DriverName = "nfs.csi." + strings.Repeat("a", 56) })}, forbiddenWithoutPreview, field.ErrorList{field.TooLong(cdc.Child("driver_name"), "", 63).WithOrigin("maxLength")}},
		{"missing controller_endpoint", &ateapi.CreateCsiDriverConfigRequest{CsiDriverConfig: validCsiDriverConfig(func(c *ateapi.CsiDriverConfig) { c.ControllerEndpoint = "" })}, forbiddenWithoutPreview, field.ErrorList{field.Required(ep, "")}},
		{"tcp missing port", &ateapi.CreateCsiDriverConfigRequest{CsiDriverConfig: validCsiDriverConfig(func(c *ateapi.CsiDriverConfig) { c.ControllerEndpoint = "tcp://127.0.0.1" })}, forbiddenWithoutPreview, field.ErrorList{field.Invalid(ep, nil, "")}},
		{"tcp missing host", &ateapi.CreateCsiDriverConfigRequest{CsiDriverConfig: validCsiDriverConfig(func(c *ateapi.CsiDriverConfig) { c.ControllerEndpoint = "tcp://:9000" })}, forbiddenWithoutPreview, field.ErrorList{field.Invalid(ep, nil, "")}},
		{"tcp malformed ipv6", &ateapi.CreateCsiDriverConfigRequest{CsiDriverConfig: validCsiDriverConfig(func(c *ateapi.CsiDriverConfig) { c.ControllerEndpoint = "tcp://[::1:9000" })}, forbiddenWithoutPreview, field.ErrorList{field.Invalid(ep, nil, "")}},
		{"tcp bracketed ipv4", &ateapi.CreateCsiDriverConfigRequest{CsiDriverConfig: validCsiDriverConfig(func(c *ateapi.CsiDriverConfig) { c.ControllerEndpoint = "tcp://[127.0.0.1]:9000" })}, forbiddenWithoutPreview, field.ErrorList{field.Invalid(ep, nil, "")}},
		{"dns bracketed hostname", &ateapi.CreateCsiDriverConfigRequest{CsiDriverConfig: validCsiDriverConfig(func(c *ateapi.CsiDriverConfig) { c.ControllerEndpoint = "dns:///[csi-nfs]:9000" })}, forbiddenWithoutPreview, field.ErrorList{field.Invalid(ep, nil, "")}},
		{"dns bracketed ipv4 authority", &ateapi.CreateCsiDriverConfigRequest{CsiDriverConfig: validCsiDriverConfig(func(c *ateapi.CsiDriverConfig) { c.ControllerEndpoint = "dns://[8.8.8.8]:53/csi-nfs:9000" })}, forbiddenWithoutPreview, field.ErrorList{field.Invalid(ep, nil, "")}},
		{"dns missing port on target", &ateapi.CreateCsiDriverConfigRequest{CsiDriverConfig: validCsiDriverConfig(func(c *ateapi.CsiDriverConfig) { c.ControllerEndpoint = "dns:///csi-nfs" })}, forbiddenWithoutPreview, field.ErrorList{field.Invalid(ep, nil, "")}},
		{"dns missing target path", &ateapi.CreateCsiDriverConfigRequest{CsiDriverConfig: validCsiDriverConfig(func(c *ateapi.CsiDriverConfig) { c.ControllerEndpoint = "dns://csi-nfs:9000" })}, forbiddenWithoutPreview, field.ErrorList{field.Invalid(ep, nil, "")}},
		{"dns invalid authority", &ateapi.CreateCsiDriverConfigRequest{CsiDriverConfig: validCsiDriverConfig(func(c *ateapi.CsiDriverConfig) { c.ControllerEndpoint = "dns://bad_auth/csi-nfs:9000" })}, forbiddenWithoutPreview, field.ErrorList{field.Invalid(ep, nil, "")}},
		{"non-canonical +port", &ateapi.CreateCsiDriverConfigRequest{CsiDriverConfig: validCsiDriverConfig(func(c *ateapi.CsiDriverConfig) { c.ControllerEndpoint = "dns:///csi-nfs:+80" })}, forbiddenWithoutPreview, field.ErrorList{field.Invalid(ep, nil, "")}},
		{"non-canonical 00port", &ateapi.CreateCsiDriverConfigRequest{CsiDriverConfig: validCsiDriverConfig(func(c *ateapi.CsiDriverConfig) { c.ControllerEndpoint = "tcp://127.0.0.1:0080" })}, forbiddenWithoutPreview, field.ErrorList{field.Invalid(ep, nil, "")}},
		{"percent-encoded endpoint", &ateapi.CreateCsiDriverConfigRequest{CsiDriverConfig: validCsiDriverConfig(func(c *ateapi.CsiDriverConfig) { c.ControllerEndpoint = "dns:///%63si-nfs:9000" })}, forbiddenWithoutPreview, field.ErrorList{field.Invalid(ep, nil, "")}},
		{"tcp with path", &ateapi.CreateCsiDriverConfigRequest{CsiDriverConfig: validCsiDriverConfig(func(c *ateapi.CsiDriverConfig) { c.ControllerEndpoint = "tcp://127.0.0.1:9000/p" })}, forbiddenWithoutPreview, field.ErrorList{field.Invalid(ep, nil, "")}},
		{"tcp with query", &ateapi.CreateCsiDriverConfigRequest{CsiDriverConfig: validCsiDriverConfig(func(c *ateapi.CsiDriverConfig) { c.ControllerEndpoint = "tcp://127.0.0.1:9000?a=1" })}, forbiddenWithoutPreview, field.ErrorList{field.Invalid(ep, nil, "")}},
		{"tcp with userinfo", &ateapi.CreateCsiDriverConfigRequest{CsiDriverConfig: validCsiDriverConfig(func(c *ateapi.CsiDriverConfig) { c.ControllerEndpoint = "tcp://u@127.0.0.1:9000" })}, forbiddenWithoutPreview, field.ErrorList{field.Invalid(ep, nil, "")}},
		{"unsupported endpoint scheme", &ateapi.CreateCsiDriverConfigRequest{CsiDriverConfig: validCsiDriverConfig(func(c *ateapi.CsiDriverConfig) { c.ControllerEndpoint = "http://127.0.0.1:9000" })}, forbiddenWithoutPreview, field.ErrorList{field.Invalid(ep, nil, "")}},
		{"socket wrong scheme", &ateapi.CreateCsiDriverConfigRequest{CsiDriverConfig: validCsiDriverConfig(func(c *ateapi.CsiDriverConfig) { c.NodeSocketOverride = "tcp://127.0.0.1:9000" })}, forbiddenWithoutPreview, field.ErrorList{field.Invalid(sock, nil, "")}},
		{"socket non-empty authority", &ateapi.CreateCsiDriverConfigRequest{CsiDriverConfig: validCsiDriverConfig(func(c *ateapi.CsiDriverConfig) { c.NodeSocketOverride = "unix://host/var/csi.sock" })}, forbiddenWithoutPreview, field.ErrorList{field.Invalid(sock, nil, "")}},
		{"socket root path", &ateapi.CreateCsiDriverConfigRequest{CsiDriverConfig: validCsiDriverConfig(func(c *ateapi.CsiDriverConfig) { c.NodeSocketOverride = "unix:///" })}, forbiddenWithoutPreview, field.ErrorList{field.Invalid(sock, nil, "")}},
		{"socket double slash", &ateapi.CreateCsiDriverConfigRequest{CsiDriverConfig: validCsiDriverConfig(func(c *ateapi.CsiDriverConfig) { c.NodeSocketOverride = "unix:///var//csi.sock" })}, forbiddenWithoutPreview, field.ErrorList{field.Invalid(sock, nil, "")}},
		{"socket dotdot", &ateapi.CreateCsiDriverConfigRequest{CsiDriverConfig: validCsiDriverConfig(func(c *ateapi.CsiDriverConfig) { c.NodeSocketOverride = "unix:///var/../csi.sock" })}, forbiddenWithoutPreview, field.ErrorList{field.Invalid(sock, nil, "")}},
		{"socket trailing slash", &ateapi.CreateCsiDriverConfigRequest{CsiDriverConfig: validCsiDriverConfig(func(c *ateapi.CsiDriverConfig) { c.NodeSocketOverride = "unix:///var/csi.sock/" })}, forbiddenWithoutPreview, field.ErrorList{field.Invalid(sock, nil, "")}},
		{"socket percent escape", &ateapi.CreateCsiDriverConfigRequest{CsiDriverConfig: validCsiDriverConfig(func(c *ateapi.CsiDriverConfig) { c.NodeSocketOverride = "unix:///var%2fcsi.sock" })}, forbiddenWithoutPreview, field.ErrorList{field.Invalid(sock, nil, "")}},
		{"socket space", &ateapi.CreateCsiDriverConfigRequest{CsiDriverConfig: validCsiDriverConfig(func(c *ateapi.CsiDriverConfig) { c.NodeSocketOverride = "unix:///var/run/csi sock" })}, forbiddenWithoutPreview, field.ErrorList{field.Invalid(sock, nil, "")}},
		{"tls enabled missing pod identity and server_name", &ateapi.CreateCsiDriverConfigRequest{CsiDriverConfig: validCsiDriverConfig(func(c *ateapi.CsiDriverConfig) {
			c.Tls = &ateapi.CsiDriverTlsConfig{Enabled: true}
		})}, forbiddenWithoutPreview, field.ErrorList{field.Invalid(tlsPath.Child("use_pod_identity"), nil, ""), field.Required(tlsPath.Child("server_name"), "")}},
		{"tls disabled rejects use_pod_identity and server_name", &ateapi.CreateCsiDriverConfigRequest{CsiDriverConfig: validCsiDriverConfig(func(c *ateapi.CsiDriverConfig) {
			c.Tls = &ateapi.CsiDriverTlsConfig{Enabled: false, UsePodIdentity: true, ServerName: "csi.ate-system.svc"}
		})}, forbiddenWithoutPreview, field.ErrorList{field.Forbidden(tlsPath.Child("use_pod_identity"), ""), field.Forbidden(tlsPath.Child("server_name"), "")}},
		{"tls invalid server_name format", &ateapi.CreateCsiDriverConfigRequest{CsiDriverConfig: validCsiDriverConfig(func(c *ateapi.CsiDriverConfig) {
			c.Tls = &ateapi.CsiDriverTlsConfig{Enabled: true, UsePodIdentity: true, ServerName: "Bad_Host"}
		})}, forbiddenWithoutPreview, field.ErrorList{field.Invalid(tlsPath.Child("server_name"), nil, "").WithOrigin("format=k8s-long-name")}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidate(t, func() field.ErrorList { return ValidateCreateCsiDriverConfigRequest(context.Background(), tt.req) },
				tt.want, map[string]field.ErrorList{"ExternalVolumes": tt.wantWithPreview})
		})
	}
}

func TestValidateGetCsiDriverConfigRequest(t *testing.T) {
	tests := []struct {
		name string
		ref  *ateapi.ObjectRef
		want field.ErrorList
	}{
		{"valid", &ateapi.ObjectRef{Name: "csi-nfs"}, nil},
		{"missing ref", nil, field.ErrorList{field.Required(field.NewPath("csi_driver_config"), "")}},
		{"atespace must be empty", &ateapi.ObjectRef{Atespace: "as", Name: "csi-nfs"}, field.ErrorList{field.Forbidden(field.NewPath("csi_driver_config", "atespace"), "")}},
		{"missing name", &ateapi.ObjectRef{}, field.ErrorList{field.Required(field.NewPath("csi_driver_config", "name"), "")}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, ValidateGetCsiDriverConfigRequest(context.Background(), &ateapi.GetCsiDriverConfigRequest{CsiDriverConfig: tt.ref}), tt.want)
		})
	}
}

func TestValidateDeleteCsiDriverConfigRequest(t *testing.T) {
	tests := []struct {
		name string
		req  *ateapi.DeleteCsiDriverConfigRequest
		want field.ErrorList
	}{
		{"valid", &ateapi.DeleteCsiDriverConfigRequest{CsiDriverConfig: &ateapi.ObjectRef{Name: "csi-nfs"}}, nil},
		{"missing ref", &ateapi.DeleteCsiDriverConfigRequest{}, field.ErrorList{field.Required(field.NewPath("csi_driver_config"), "")}},
		{"atespace must be empty", &ateapi.DeleteCsiDriverConfigRequest{CsiDriverConfig: &ateapi.ObjectRef{Atespace: "as", Name: "csi-nfs"}}, field.ErrorList{field.Forbidden(field.NewPath("csi_driver_config", "atespace"), "")}},
		{"missing name", &ateapi.DeleteCsiDriverConfigRequest{CsiDriverConfig: &ateapi.ObjectRef{}}, field.ErrorList{field.Required(field.NewPath("csi_driver_config", "name"), "")}},
		{"invalid delete options uid", &ateapi.DeleteCsiDriverConfigRequest{
			CsiDriverConfig: &ateapi.ObjectRef{Name: "csi-nfs"},
			Options:         &ateapi.DeleteOptions{Uid: "not-a-uuid"},
		}, field.ErrorList{field.Invalid(field.NewPath("options", "uid"), nil, "").WithOrigin("format=k8s-uuid")}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, ValidateDeleteCsiDriverConfigRequest(context.Background(), tt.req), tt.want)
		})
	}
}

func TestValidateListCsiDriverConfigsRequest(t *testing.T) {
	tests := []struct {
		name string
		req  *ateapi.ListCsiDriverConfigsRequest
		want field.ErrorList
	}{
		{"valid", &ateapi.ListCsiDriverConfigsRequest{}, nil},
		{"negative page_size", &ateapi.ListCsiDriverConfigsRequest{Options: &ateapi.ListOptions{PageSize: -1}}, field.ErrorList{field.Invalid(field.NewPath("options", "page_size"), int32(-1), "").WithOrigin("minimum")}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, ValidateListCsiDriverConfigsRequest(context.Background(), tt.req), tt.want)
		})
	}
}

func TestValidateUpdateCsiDriverConfigRequest(t *testing.T) {
	cdc := field.NewPath("csi_driver_config")
	forbiddenWithoutPreview := field.ErrorList{field.Forbidden(cdc, "")}
	validReq := func(mutate ...func(*ateapi.CsiDriverConfig)) *ateapi.UpdateCsiDriverConfigRequest {
		cfg := validCsiDriverConfig(func(c *ateapi.CsiDriverConfig) {
			c.Metadata.Uid = someActorUID
			c.Metadata.Version = 3
		})
		for _, fn := range mutate {
			fn(cfg)
		}
		return &ateapi.UpdateCsiDriverConfigRequest{CsiDriverConfig: cfg}
	}
	tests := []struct {
		name            string
		req             *ateapi.UpdateCsiDriverConfigRequest
		want            field.ErrorList
		wantWithPreview field.ErrorList
	}{
		{"missing csi_driver_config", &ateapi.UpdateCsiDriverConfigRequest{}, field.ErrorList{field.Required(cdc, "")}, field.ErrorList{field.Required(cdc, "")}},
		{"valid", validReq(), forbiddenWithoutPreview, nil},
		{"missing uid and version pass request validation", validReq(func(c *ateapi.CsiDriverConfig) { c.Metadata.Uid = ""; c.Metadata.Version = 0 }), forbiddenWithoutPreview, nil},
		{"missing metadata", validReq(func(c *ateapi.CsiDriverConfig) { c.Metadata = nil }), forbiddenWithoutPreview, field.ErrorList{field.Required(cdc.Child("metadata"), "")}},
		{"missing metadata.name", validReq(func(c *ateapi.CsiDriverConfig) { c.Metadata.Name = "" }), forbiddenWithoutPreview, field.ErrorList{field.Required(cdc.Child("metadata", "name"), "")}},
		{"invalid metadata.name", validReq(func(c *ateapi.CsiDriverConfig) { c.Metadata.Name = "Bad_Name" }), forbiddenWithoutPreview, field.ErrorList{field.Invalid(cdc.Child("metadata", "name"), nil, "").WithOrigin("format=k8s-short-name")}},
		{"invalid metadata.uid", validReq(func(c *ateapi.CsiDriverConfig) { c.Metadata.Uid = "not-a-uuid" }), forbiddenWithoutPreview, field.ErrorList{field.Invalid(cdc.Child("metadata", "uid"), nil, "").WithOrigin("format=k8s-uuid")}},
		{"metadata.atespace forbidden", validReq(func(c *ateapi.CsiDriverConfig) { c.Metadata.Atespace = "as" }), forbiddenWithoutPreview, field.ErrorList{field.Forbidden(cdc.Child("metadata", "atespace"), "")}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidate(t, func() field.ErrorList { return ValidateUpdateCsiDriverConfigRequest(context.Background(), tt.req) },
				tt.want, map[string]field.ErrorList{"ExternalVolumes": tt.wantWithPreview})
		})
	}
}

func TestValidateCsiDriverConfigUpdate(t *testing.T) {
	stored := func(mutate ...func(*ateapi.CsiDriverConfig)) *ateapi.CsiDriverConfig {
		return validCsiDriverConfig(append([]func(*ateapi.CsiDriverConfig){func(c *ateapi.CsiDriverConfig) {
			c.Metadata.Uid = someActorUID
			c.Metadata.Version = 2
			c.Metadata.CreateTime = &timestamppb.Timestamp{Seconds: 100}
			c.Metadata.UpdateTime = &timestamppb.Timestamp{Seconds: 200}
		}}, mutate...)...)
	}
	cdc := field.NewPath("csi_driver_config")
	tests := []struct {
		name   string
		mutate func(*ateapi.CsiDriverConfig)
		want   field.ErrorList
	}{
		{"valid mutable fields changed", func(c *ateapi.CsiDriverConfig) {
			c.ControllerEndpoint = "tcp://10.0.0.2:9000"
			c.NodeSocketOverride = "unix:///run/csi/nfs.sock"
			c.Tls = &ateapi.CsiDriverTlsConfig{Enabled: true, UsePodIdentity: true, ServerName: "csi.ate-system.svc"}
		}, nil},
		{"driver_name immutable", func(c *ateapi.CsiDriverConfig) { c.DriverName = "other.csi.k8s.io" }, field.ErrorList{field.Invalid(cdc.Child("driver_name"), nil, "").WithOrigin("immutable")}},
		{"metadata.name immutable", func(c *ateapi.CsiDriverConfig) { c.Metadata.Name = "other" }, field.ErrorList{field.Invalid(cdc.Child("metadata", "name"), nil, "").WithOrigin("immutable")}},
		{"metadata.uid immutable", func(c *ateapi.CsiDriverConfig) { c.Metadata.Uid = "11111111-2222-3333-4444-555555555555" }, field.ErrorList{field.Invalid(cdc.Child("metadata", "uid"), nil, "").WithOrigin("immutable")}},
		{"metadata.create_time immutable", func(c *ateapi.CsiDriverConfig) { c.Metadata.CreateTime = &timestamppb.Timestamp{Seconds: 101} }, field.ErrorList{field.Invalid(cdc.Child("metadata", "create_time"), nil, "").WithOrigin("immutable")}},
		{"metadata.atespace forbidden and immutable", func(c *ateapi.CsiDriverConfig) { c.Metadata.Atespace = "as" }, field.ErrorList{
			field.Forbidden(cdc.Child("metadata", "atespace"), ""),
			field.Invalid(cdc.Child("metadata", "atespace"), nil, "").WithOrigin("immutable"),
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, ValidateCsiDriverConfigUpdate(context.Background(), cdc, stored(tt.mutate), stored()), tt.want)
		})
	}
}
