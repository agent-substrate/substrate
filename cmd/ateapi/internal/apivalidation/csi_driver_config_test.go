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
	"testing"

	"go.ate.dev/substrate/api/ateapi/v1"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

func validCsiDriverConfig(mutate ...func(*ateapi.CsiDriverConfig)) *ateapi.CsiDriverConfig {
	cfg := &ateapi.CsiDriverConfig{
		Metadata:           &ateapi.ResourceMetadata{Name: "csi-nfs"},
		DriverName:         "nfs.csi.k8s.io",
		ControllerEndpoint: "dns:///csi-nfs.ate-system.svc:9000",
	}
	for _, fn := range mutate {
		fn(cfg)
	}
	return cfg
}

func TestValidateCsiDriverConfig(t *testing.T) {
	ep, sock, tlsPath := field.NewPath("controller_endpoint"), field.NewPath("node_socket_override"), field.NewPath("tls")
	tests := []struct {
		name   string
		mutate func(*ateapi.CsiDriverConfig)
		want   field.ErrorList
	}{
		{"valid default", nil, nil},
		{"valid substrate.io driver + ipv6 tcp + socket + tls", func(c *ateapi.CsiDriverConfig) {
			c.DriverName = "substrate.io/mock"
			c.ControllerEndpoint = "tcp://[::1]:9000"
			c.NodeSocketOverride = "unix:///var/lib/kubelet/plugins/mock/csi.sock"
			c.Tls = &ateapi.CsiDriverTlsConfig{Enabled: true, UsePodIdentity: true, ServerName: "csi.ate-system.svc"}
		}, nil},
		{"valid dns authority with port", func(c *ateapi.CsiDriverConfig) { c.ControllerEndpoint = "dns://8.8.8.8:53/csi-nfs:9000" }, nil},
		{"valid dns ipv4 authority without port", func(c *ateapi.CsiDriverConfig) { c.ControllerEndpoint = "dns://8.8.8.8/csi-nfs:9000" }, nil},
		{"valid dns ipv6 authority without port", func(c *ateapi.CsiDriverConfig) { c.ControllerEndpoint = "dns://[::1]/csi-nfs:9000" }, nil},
		{"valid dns ipv6 authority and target with port", func(c *ateapi.CsiDriverConfig) { c.ControllerEndpoint = "dns://[::1]:53/[::1]:9000" }, nil},
		{"valid explicit disabled tls", func(c *ateapi.CsiDriverConfig) { c.Tls = &ateapi.CsiDriverTlsConfig{} }, nil},
		{"atespace forbidden", func(c *ateapi.CsiDriverConfig) { c.Metadata.Atespace = "as" }, field.ErrorList{field.Forbidden(field.NewPath("metadata", "atespace"), "")}},
		{"invalid driver_name", func(c *ateapi.CsiDriverConfig) { c.DriverName = "Bad_Driver" }, field.ErrorList{field.Invalid(field.NewPath("driver_name"), nil, "")}},
		{"tcp missing port", func(c *ateapi.CsiDriverConfig) { c.ControllerEndpoint = "tcp://127.0.0.1" }, field.ErrorList{field.Invalid(ep, nil, "")}},
		{"tcp missing host", func(c *ateapi.CsiDriverConfig) { c.ControllerEndpoint = "tcp://:9000" }, field.ErrorList{field.Invalid(ep, nil, "")}},
		{"tcp malformed ipv6", func(c *ateapi.CsiDriverConfig) { c.ControllerEndpoint = "tcp://[::1:9000" }, field.ErrorList{field.Invalid(ep, nil, "")}},
		{"tcp bracketed ipv4", func(c *ateapi.CsiDriverConfig) { c.ControllerEndpoint = "tcp://[127.0.0.1]:9000" }, field.ErrorList{field.Invalid(ep, nil, "")}},
		{"dns bracketed hostname", func(c *ateapi.CsiDriverConfig) { c.ControllerEndpoint = "dns:///[csi-nfs]:9000" }, field.ErrorList{field.Invalid(ep, nil, "")}},
		{"dns bracketed ipv4 authority", func(c *ateapi.CsiDriverConfig) { c.ControllerEndpoint = "dns://[8.8.8.8]:53/csi-nfs:9000" }, field.ErrorList{field.Invalid(ep, nil, "")}},
		{"dns missing port on target", func(c *ateapi.CsiDriverConfig) { c.ControllerEndpoint = "dns:///csi-nfs" }, field.ErrorList{field.Invalid(ep, nil, "")}},
		{"dns missing target path", func(c *ateapi.CsiDriverConfig) { c.ControllerEndpoint = "dns://csi-nfs:9000" }, field.ErrorList{field.Invalid(ep, nil, "")}},
		{"dns invalid authority", func(c *ateapi.CsiDriverConfig) { c.ControllerEndpoint = "dns://bad_auth/csi-nfs:9000" }, field.ErrorList{field.Invalid(ep, nil, "")}},
		{"non-canonical +port", func(c *ateapi.CsiDriverConfig) { c.ControllerEndpoint = "dns:///csi-nfs:+80" }, field.ErrorList{field.Invalid(ep, nil, "")}},
		{"non-canonical 00port", func(c *ateapi.CsiDriverConfig) { c.ControllerEndpoint = "tcp://127.0.0.1:0080" }, field.ErrorList{field.Invalid(ep, nil, "")}},
		{"percent-encoded endpoint", func(c *ateapi.CsiDriverConfig) { c.ControllerEndpoint = "dns:///%63si-nfs:9000" }, field.ErrorList{field.Invalid(ep, nil, "")}},
		{"tcp with path", func(c *ateapi.CsiDriverConfig) { c.ControllerEndpoint = "tcp://127.0.0.1:9000/p" }, field.ErrorList{field.Invalid(ep, nil, "")}},
		{"tcp with query", func(c *ateapi.CsiDriverConfig) { c.ControllerEndpoint = "tcp://127.0.0.1:9000?a=1" }, field.ErrorList{field.Invalid(ep, nil, "")}},
		{"tcp with userinfo", func(c *ateapi.CsiDriverConfig) { c.ControllerEndpoint = "tcp://u@127.0.0.1:9000" }, field.ErrorList{field.Invalid(ep, nil, "")}},
		{"unsupported endpoint scheme", func(c *ateapi.CsiDriverConfig) { c.ControllerEndpoint = "http://127.0.0.1:9000" }, field.ErrorList{field.Invalid(ep, nil, "")}},
		{"socket wrong scheme", func(c *ateapi.CsiDriverConfig) { c.NodeSocketOverride = "tcp://127.0.0.1:9000" }, field.ErrorList{field.Invalid(sock, nil, "")}},
		{"socket non-empty authority", func(c *ateapi.CsiDriverConfig) { c.NodeSocketOverride = "unix://host/var/csi.sock" }, field.ErrorList{field.Invalid(sock, nil, "")}},
		{"socket root path", func(c *ateapi.CsiDriverConfig) { c.NodeSocketOverride = "unix:///" }, field.ErrorList{field.Invalid(sock, nil, "")}},
		{"socket double slash", func(c *ateapi.CsiDriverConfig) { c.NodeSocketOverride = "unix:///var//csi.sock" }, field.ErrorList{field.Invalid(sock, nil, "")}},
		{"socket dotdot", func(c *ateapi.CsiDriverConfig) { c.NodeSocketOverride = "unix:///var/../csi.sock" }, field.ErrorList{field.Invalid(sock, nil, "")}},
		{"socket trailing slash", func(c *ateapi.CsiDriverConfig) { c.NodeSocketOverride = "unix:///var/csi.sock/" }, field.ErrorList{field.Invalid(sock, nil, "")}},
		{"socket percent escape", func(c *ateapi.CsiDriverConfig) { c.NodeSocketOverride = "unix:///var%2fcsi.sock" }, field.ErrorList{field.Invalid(sock, nil, "")}},
		{"socket space", func(c *ateapi.CsiDriverConfig) { c.NodeSocketOverride = "unix:///var/run/csi sock" }, field.ErrorList{field.Invalid(sock, nil, "")}},
		{"tls enabled missing pod identity and server_name", func(c *ateapi.CsiDriverConfig) {
			c.Tls = &ateapi.CsiDriverTlsConfig{Enabled: true}
		}, field.ErrorList{field.Invalid(tlsPath.Child("use_pod_identity"), nil, ""), field.Required(tlsPath.Child("server_name"), "")}},
		{"tls disabled rejects use_pod_identity and server_name", func(c *ateapi.CsiDriverConfig) {
			c.Tls = &ateapi.CsiDriverTlsConfig{Enabled: false, UsePodIdentity: true, ServerName: "csi.ate-system.svc"}
		}, field.ErrorList{field.Forbidden(tlsPath.Child("use_pod_identity"), ""), field.Forbidden(tlsPath.Child("server_name"), "")}},
		{"tls invalid server_name format", func(c *ateapi.CsiDriverConfig) {
			c.Tls = &ateapi.CsiDriverTlsConfig{Enabled: true, UsePodIdentity: true, ServerName: "Bad_Host"}
		}, field.ErrorList{field.Invalid(tlsPath.Child("server_name"), nil, "").WithOrigin("format=k8s-long-name")}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			cfg := validCsiDriverConfig()
			if tt.mutate != nil {
				tt.mutate(cfg)
			}
			assertValidateErr(t, Validate_CsiDriverConfig(ctx, MakeCreateOp(ctx), nil, cfg, nil), tt.want)
		})
	}
	t.Run("driver_name immutable on update", func(t *testing.T) {
		ctx := context.Background()
		oldCfg := validCsiDriverConfig()
		newCfg := validCsiDriverConfig(func(c *ateapi.CsiDriverConfig) {
			c.DriverName = "other.csi.k8s.io"
			c.ControllerEndpoint = "tcp://10.0.0.2:9000"
		})
		assertValidateErr(t, Validate_CsiDriverConfig(ctx, MakeUpdateOp(ctx), nil, newCfg, oldCfg),
			field.ErrorList{field.Invalid(field.NewPath("driver_name"), nil, "").WithOrigin("immutable")})
	})
}
