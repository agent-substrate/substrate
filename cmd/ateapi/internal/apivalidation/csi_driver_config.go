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
	"net"
	"net/netip"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/internal/resources"
	"k8s.io/apimachinery/pkg/api/operation"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

func ValidateCustom_CsiDriverConfig_DriverName(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *string) field.ErrorList {
	return resources.ValidateVolumeType(fldPath, *value)
}

func ValidateCustom_CsiDriverConfig_ControllerEndpoint(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *string) field.ErrorList {
	raw := *value
	if strings.ContainsAny(raw, "%?# \t\r\n") {
		return field.ErrorList{field.Invalid(fldPath, raw, "must not contain percent-escapes, query parameters, fragments, or whitespace")}
	}
	u, err := url.Parse(raw)
	if err != nil || u.User != nil {
		return field.ErrorList{field.Invalid(fldPath, raw, "must be a valid tcp:// or dns:// endpoint URI")}
	}
	switch {
	case strings.HasPrefix(raw, "tcp://"):
		if u.Path != "" || !isValidEndpointHostPort(u.Host) {
			return field.ErrorList{field.Invalid(fldPath, raw, "tcp endpoint must be tcp://<host>:<port>")}
		}
		return nil
	case strings.HasPrefix(raw, "dns://"):
		target, ok := strings.CutPrefix(u.Path, "/")
		if (u.Host != "" && !isValidDNSAuthority(u.Host)) || !ok || !isValidEndpointHostPort(target) {
			return field.ErrorList{field.Invalid(fldPath, raw, "dns endpoint must be dns://[<authority>]/<host>:<port>")}
		}
		return nil
	default:
		return field.ErrorList{field.Invalid(fldPath, raw, "scheme must be tcp or dns")}
	}
}

func isValidEndpointHostPort(target string) bool {
	host, portStr, err := net.SplitHostPort(target)
	if err != nil {
		return false
	}
	port, err := strconv.Atoi(portStr)
	if err != nil || strconv.Itoa(port) != portStr || len(validation.IsValidPortNum(port)) > 0 {
		return false
	}
	if strings.HasPrefix(target, "[") {
		ip, err := netip.ParseAddr(host)
		return err == nil && ip.Is6() && ip.Zone() == ""
	}
	return isValidEndpointHost(host)
}

func isValidDNSAuthority(authority string) bool {
	if isValidEndpointHostPort(authority) {
		return true
	}
	if strings.HasPrefix(authority, "[") && strings.HasSuffix(authority, "]") {
		ip, err := netip.ParseAddr(authority[1 : len(authority)-1])
		return err == nil && ip.Is6() && ip.Zone() == ""
	}
	return !strings.Contains(authority, ":") && isValidEndpointHost(authority)
}

func isValidEndpointHost(host string) bool {
	if host == "" {
		return false
	}
	if ip, err := netip.ParseAddr(host); err == nil {
		return ip.Zone() == ""
	}
	return len(validation.IsDNS1123Subdomain(strings.TrimSuffix(host, "."))) == 0
}

func ValidateCustom_CsiDriverConfig_NodeSocketOverride(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *string) field.ErrorList {
	raw := *value
	p, ok := strings.CutPrefix(raw, "unix://")
	if !ok || strings.ContainsAny(raw, "?# \t\r\n\x00") {
		return field.ErrorList{field.Invalid(fldPath, raw, "must be unix://<clean-absolute-path>")}
	}
	u, err := url.Parse(raw)
	if err != nil || u.Host != "" || u.User != nil || u.Path != p {
		return field.ErrorList{field.Invalid(fldPath, raw, "must be unix://<clean-absolute-path>")}
	}
	if p == "/" || !filepath.IsAbs(p) || filepath.Clean(p) != p {
		return field.ErrorList{field.Invalid(fldPath, raw, "socket path must be a non-root clean absolute path")}
	}
	return nil
}

func ValidateCustom_CsiDriverTlsConfig(_ context.Context, _ operation.Operation, fldPath *field.Path, obj, _ *ateapi.CsiDriverTlsConfig) field.ErrorList {
	var errs field.ErrorList
	if !obj.GetEnabled() {
		if obj.GetUsePodIdentity() {
			errs = append(errs, field.Forbidden(fldPath.Child("use_pod_identity"), "may not be set when enabled is false"))
		}
		if obj.GetServerName() != "" {
			errs = append(errs, field.Forbidden(fldPath.Child("server_name"), "may not be set when enabled is false"))
		}
		return errs
	}
	if !obj.GetUsePodIdentity() {
		errs = append(errs, field.Invalid(fldPath.Child("use_pod_identity"), false, "must be true when enabled is true; manual certificates are not yet supported"))
	}
	if obj.GetServerName() == "" {
		errs = append(errs, field.Required(fldPath.Child("server_name"), "is required when enabled is true"))
	}
	return errs
}
