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

package resources

import (
	"context"
	"encoding/hex"
	"fmt"
	"net/netip"
	"net/url"
	"strings"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"k8s.io/apimachinery/pkg/api/operation"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/api/validate"
	"k8s.io/apimachinery/pkg/api/validate/content"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// ValidateResourceName checks that a string conforms to Agent Substrate's
// rules for a resource name, which is a subset of the rules for an RFC-1123
// DNS label.  This does not check for zero-length strings, which callers may
// want to handle differently (e.g., by returning a "required" error).
func ValidateResourceName(name string, fldPath *field.Path) field.ErrorList {
	var errs field.ErrorList
	for _, msg := range content.IsDNS1123Label(name) {
		errs = append(errs, field.Invalid(fldPath, name, msg))
	}
	return errs
}

// IsValidResourceName reports whether name is a valid Substrate resource name
// (a DNS-1123 label; see ValidateResourceName for the rules). Use this for
// internal, non-proto checks where a plain predicate is wanted; to validate a
// proto request field with structured field-path errors, use
// ValidateResourceName. Empty is not a valid name.
func IsValidResourceName(name string) bool {
	return len(content.IsDNS1123Label(name)) == 0
}

// ValidateGlobalObjectRef checks that a reference to a global-scoped resource is
// well-formed: its atespace must be empty (global resources do not belong to an
// atespace) and its name must be a valid resource name. It does not check that
// the referenced resource actually exists.
//
// A nil ref is an error rather than a no-op: every global ref in the API names
// the resource a request acts on, and a request that names nothing cannot be
// served.
// TODO: EOL this when DV is fully implemented
func ValidateGlobalObjectRef(ref *ateapipb.ObjectRef, fldPath *field.Path) field.ErrorList {
	if ref == nil {
		return field.ErrorList{field.Required(fldPath, "")}
	}

	var errs field.ErrorList

	if val, fldPath := ref.Atespace, fldPath.Child("atespace"); val != "" {
		errs = append(errs, field.Invalid(fldPath, val, "must be empty for a global-scoped resource"))
	}

	if val, fldPath := ref.Name, fldPath.Child("name"); val == "" {
		errs = append(errs, field.Required(fldPath, ""))
	} else {
		errs = append(errs, ValidateResourceName(val, fldPath)...)
	}

	return errs
}

// ValidateAteomUID rejects a target ateom pod UID that could escape the host
// path built from it: the ateom control socket (.../ateoms/<uid>/ateom.sock).
// Kubernetes pod UIDs are UUIDs, which are valid DNS-1123 labels, so a label
// check accepts every legitimate value while rejecting separators and "..".
func ValidateAteomUID(targetAteomUID string) error {
	if errs := content.IsDNS1123Label(targetAteomUID); len(errs) > 0 {
		return fmt.Errorf("invalid target ateom UID %q: %s", targetAteomUID, strings.Join(errs, "; "))
	}
	return nil
}

// ValidateContainerNames ensures every application container name is safe to
// use as an OCI bundle path component. Each must be a DNS-1123 label (no
// separator or ".."), must not be the reserved "pause" name (which would
// collide with the sandbox-infra bundle and race its concurrent writer), and
// must be unique (duplicates map to the same bundle path and corrupt each
// other).
func ValidateContainerNames(names []string) error {
	seen := make(map[string]struct{})
	for _, name := range names {
		if errs := content.IsDNS1123Label(name); len(errs) > 0 {
			return fmt.Errorf("invalid container name %q: %s", name, strings.Join(errs, "; "))
		}
		if name == "pause" {
			return fmt.Errorf("invalid container name %q: reserved for sandbox infrastructure", name)
		}
		if _, dup := seen[name]; dup {
			return fmt.Errorf("duplicate container name %q", name)
		}
		seen[name] = struct{}{}
	}
	return nil
}

// ValidateRunscHash ensures the runsc SHA-256 hash is exactly 64 hex
// characters before it is used to build the on-disk binary path
// (static-files/runsc-<hash>) and, on a cache hit, returned for ateom to
// execute. Without this, a hash containing path separators or ".." could
// point the cache-hit early return (and the download target) at an arbitrary
// binary outside the static-files dir.
func ValidateRunscHash(sha256Hash string) error {
	if len(sha256Hash) != 64 {
		return fmt.Errorf("invalid runsc sha256 hash: want 64 hex chars, got %d", len(sha256Hash))
	}
	// Same decoder atelet's digest comparison uses.
	if _, err := hex.DecodeString(sha256Hash); err != nil {
		return fmt.Errorf("invalid runsc sha256 hash %q: must be hex", sha256Hash)
	}
	return nil
}

// ValidateSnapshotLocation ensures an ActorTemplate's snapshotConfig.location
// is a well-formed URI with a bucket, so a bad location fails fast instead of
// deep inside an object-storage call. It deliberately does not restrict the
// scheme: the storage layer only uses the host (bucket) and path, and which
// schemes are acceptable is a storage-backend policy, not a per-RPC one. The
// local paths used for snapshot upload/download are derived from the
// separately validated actor ref, not from this URI, so this is a sanity check
// rather than a path-traversal guard.
//
// This validates the base that many snapshots share, not any one snapshot's
// URI; SnapshotURI is the type for the latter, and it applies this check when
// it is built.
func ValidateSnapshotLocation(location string) error {
	u, err := url.Parse(location)
	if err != nil {
		return fmt.Errorf("invalid snapshot location %q: %v", location, err)
	}
	if u.Host == "" {
		return fmt.Errorf("invalid snapshot location %q: missing bucket", location)
	}
	// Snapshot and object names are appended to the location by string
	// concatenation. A query, fragment, or userinfo component would swallow
	// the appended name when the result is re-parsed (the storage layer uses
	// only host and path), silently redirecting the upload/download to a
	// different object.
	if u.Opaque != "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return fmt.Errorf("invalid snapshot location %q: must contain only a scheme, bucket, and path", location)
	}
	return nil
}

// ValidateIP checks that the given string is a valid IP address, is not an
// IPv4-mapped IPv6 address, and is in canonical form.
func ValidateIP(ip string, fldPath *field.Path) field.ErrorList {
	var errs field.ErrorList

	addr, err := netip.ParseAddr(ip)
	if err != nil || !addr.IsValid() {
		errs = append(errs, field.Invalid(fldPath, ip, "must be a valid IP address"))
		return errs
	}
	if addr.Is4In6() {
		errs = append(errs, field.Invalid(fldPath, ip, "must not be an IPv4-mapped IPv6 address"))
	}
	if canon := addr.String(); ip != canon {
		errs = append(errs, field.Invalid(fldPath, ip, fmt.Sprintf("must be in canonical form (%q)", canon)))
	}
	//TODO(thockin): prevent localhost and link-local addresses which might confuse callers?

	return errs
}

// ValidateUUID verifies that the specified value is a valid UUID (RFC 4122).
//   - must be 36 characters long
//   - must be in the normalized form `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`
//   - must use only lowercase hexadecimal characters
func ValidateUUID(uuid string, fldPath *field.Path) field.ErrorList {
	const uuidErrorMessage = "must be a lowercase UUID in 8-4-4-4-12 format"

	if len(uuid) != 36 {
		return field.ErrorList{field.Invalid(fldPath, uuid, uuidErrorMessage)}
	}

	for idx := 0; idx < len(uuid); idx++ {
		character := uuid[idx]
		switch idx {
		case 8, 13, 18, 23:
			if character != '-' {
				return field.ErrorList{field.Invalid(fldPath, uuid, uuidErrorMessage)}
			}
		default:
			// should be lower case hexadecimal.
			if (character < '0' || character > '9') && (character < 'a' || character > 'f') {
				return field.ErrorList{field.Invalid(fldPath, uuid, uuidErrorMessage)}
			}
		}
	}
	return nil
}

// cpuLimitMax bounds cpu limits: they must be less than 1000 cores.
var cpuLimitMax = resource.MustParse("1k")

// ValidateLimits validates the contents of Resources.limits: only cpu and
// memory are supported, each quantity must be greater than zero, and the cpu
// limit must be less than 1000 cores. Presence, length, and uniqueness of
// names are enforced by the tags on the message.
//
// It backs controlapi's Resources.limits custom validation hook and
// ValidateWorkerResources, so both apply one rule.
func ValidateLimits(fldPath *field.Path, limits []*ateapipb.Limits) field.ErrorList {
	var errs field.ErrorList
	for i, limit := range limits {
		if limit == nil {
			continue
		}
		if limit.Name != ResourceCPU && limit.Name != ResourceMemory {
			errs = append(errs, field.NotSupported(fldPath.Index(i).Child("name"), limit.Name, []string{ResourceCPU, ResourceMemory}))
			continue
		}
		if limit.Quantity == "" {
			continue // required is enforced by tags
		}
		q, err := resource.ParseQuantity(limit.Quantity)
		if err != nil {
			errs = append(errs, field.Invalid(fldPath.Index(i).Child("quantity"), limit.Quantity, fmt.Sprintf("must be a Kubernetes resource quantity: %v", err)))
			continue
		}
		if q.Sign() <= 0 {
			errs = append(errs, field.Invalid(fldPath.Index(i).Child("quantity"), limit.Quantity, "must be greater than zero"))
		}
		if limit.Name == ResourceCPU && q.Cmp(cpuLimitMax) >= 0 {
			errs = append(errs, field.Invalid(fldPath.Index(i).Child("quantity"), limit.Quantity, "cpu limit must be less than 1000 cores"))
		}
	}
	return errs
}

// ValidateWorkerResources applies the declarative rules on
// ateapipb.WorkerResources, Resources, and Limits for callers outside
// controlapi. validation-gen cannot generate them there: it only follows a
// type into another package if that package holds the custom hooks, and
// ateapipb is generated proto code. TestValidateWorkerResourcesParity in
// controlapi holds this to the generated Validate_WorkerResources, so a tag
// change there fails it.
func ValidateWorkerResources(ctx context.Context, fldPath *field.Path, obj *ateapipb.WorkerResources) field.ErrorList {
	if obj == nil {
		return nil
	}
	op := operation.Operation{Type: operation.Create}
	var errs field.ErrorList

	// resources: +k8s:optional
	if obj.Resources != nil {
		errs = append(errs, validateResources(ctx, op, fldPath.Child("resources"), obj.Resources)...)
	}

	// actors: +k8s:optional, +k8s:minimum=1
	actorsPath := fldPath.Child("actors")
	if len(validate.OptionalValue(ctx, op, actorsPath, &obj.Actors, nil)) == 0 {
		errs = append(errs, validate.Minimum(ctx, op, actorsPath, &obj.Actors, nil, 1)...)
	}
	return errs
}

func validateResources(ctx context.Context, op operation.Operation, fldPath *field.Path, obj *ateapipb.Resources) field.ErrorList {
	fldPath = fldPath.Child("limits")
	limits := obj.Limits

	// limits: +k8s:optional, +k8s:maxItems=2, +k8s:listType=map,
	// +k8s:listMapKey=name, +k8s:customValidation.
	var errs field.ErrorList
	errs = append(errs, validate.PtrSliceNoNils[ateapipb.Limits](ctx, op, fldPath, limits, nil)...)
	errs = append(errs, validate.MaxItems(ctx, op, fldPath, limits, nil, 2)...)
	if len(errs) != 0 || len(validate.OptionalSlice(ctx, op, fldPath, limits, nil)) != 0 {
		return errs
	}
	errs = ValidateLimits(fldPath, limits)
	errs = append(errs, validate.PtrSliceUnique(ctx, op, fldPath, limits, nil,
		func(a, b *ateapipb.Limits) bool { return a.Name == b.Name })...)
	for i, limit := range limits {
		errs = append(errs, validateLimit(ctx, op, fldPath.Index(i), limit)...)
	}
	return errs
}

func validateLimit(ctx context.Context, op operation.Operation, fldPath *field.Path, obj *ateapipb.Limits) field.ErrorList {
	var errs field.ErrorList
	// name: +k8s:required, +k8s:maxLength=16
	namePath := fldPath.Child("name")
	if e := validate.RequiredValue(ctx, op, namePath, &obj.Name, nil); len(e) != 0 {
		errs = append(errs, e...)
	} else {
		errs = append(errs, validate.MaxLength(ctx, op, namePath, &obj.Name, nil, 16)...)
	}
	// quantity: +k8s:required, +k8s:maxLength=32
	quantityPath := fldPath.Child("quantity")
	if e := validate.RequiredValue(ctx, op, quantityPath, &obj.Quantity, nil); len(e) != 0 {
		errs = append(errs, e...)
	} else {
		errs = append(errs, validate.MaxLength(ctx, op, quantityPath, &obj.Quantity, nil, 32)...)
	}
	return errs
}
