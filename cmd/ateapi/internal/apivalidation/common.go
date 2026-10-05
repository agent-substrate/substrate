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

package apivalidation

import (
	"context"
	"net/netip"

	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
	"k8s.io/apimachinery/pkg/api/operation"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// ateDeepEqual is the deep-equal function declarative validation's generated
// code calls by name; it delegates to resources.DeepEqual.
func ateDeepEqual[T any](a, b T) bool {
	return resources.DeepEqual(a, b)
}

// ValidateCustom_ResourceMetadata checks the server-stamped timestamps: each,
// when set, must be a valid google.protobuf.Timestamp, and update_time must
// not precede create_time. Both fields are scrubbed from input, so a
// violation here is a server stamping bug surfaced by the final-object
// validation pass, not a client error.
func ValidateCustom_ResourceMetadata(_ context.Context, _ operation.Operation, fldPath *field.Path, obj, _ *ateapipb.ResourceMetadata) field.ErrorList {
	var errs field.ErrorList
	createTimeValid := false
	if ct := obj.GetCreateTime(); ct != nil {
		if err := ct.CheckValid(); err != nil {
			errs = append(errs, field.Invalid(fldPath.Child("create_time"), ct.String(), err.Error()))
		} else {
			createTimeValid = true
		}
	}
	if ut := obj.GetUpdateTime(); ut != nil {
		if err := ut.CheckValid(); err != nil {
			errs = append(errs, field.Invalid(fldPath.Child("update_time"), ut.String(), err.Error()))
		} else if createTimeValid && ut.AsTime().Before(obj.GetCreateTime().AsTime()) {
			errs = append(errs, field.Invalid(fldPath.Child("update_time"), ut.String(), "must not precede create_time"))
		}
	}
	return errs
}

// ValidateCustom_Limits validates one limit with resources.ValidateLimit.
// Presence and uniqueness of names are enforced by tags.
func ValidateCustom_Limits(_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *ateapipb.Limits) field.ErrorList {
	return resources.ValidateLimit(fldPath, value.GetName(), value.GetQuantity())
}

// validateDualStackIPs checks that each entry is a valid, canonical IP and
// that the list holds at most one IPv4 and one IPv6 address, matching the
// Kubernetes rule for pod IPs. Presence and length are enforced by tags.
func validateDualStackIPs(fldPath *field.Path, ips []string) field.ErrorList {
	var errs field.ErrorList
	var seen4, seen6 bool
	for i, ip := range ips {
		idxPath := fldPath.Index(i)
		if ipErrs := validation.IsValidIP(idxPath, ip); len(ipErrs) > 0 {
			errs = append(errs, ipErrs...)
			continue
		}
		seen := &seen6
		if netip.MustParseAddr(ip).Is4() {
			seen = &seen4
		}
		if *seen {
			errs = append(errs, field.Invalid(idxPath, ip, "must not contain more than one address per IP family"))
		}
		*seen = true
	}
	return errs
}
