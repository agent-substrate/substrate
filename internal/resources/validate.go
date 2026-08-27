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
	"encoding/hex"
	"fmt"
	"net/netip"
	"net/url"
	"strings"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
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

// ValidateObjectRef checks that the object reference is well-formed and that
// each of its components is a valid resource name.
// TODO: EOL this when DV is fully implemented
func ValidateObjectRef(ref *ateapipb.ObjectRef, fldPath *field.Path) field.ErrorList {
	if ref == nil {
		return nil
	}

	var errs field.ErrorList

	if val, fldPath := ref.Atespace, fldPath.Child("atespace"); val == "" {
		errs = append(errs, field.Required(fldPath, ""))
	} else {
		errs = append(errs, ValidateResourceName(val, fldPath)...)
	}

	if val, fldPath := ref.Name, fldPath.Child("name"); val == "" {
		errs = append(errs, field.Required(fldPath, ""))
	} else {
		errs = append(errs, ValidateResourceName(val, fldPath)...)
	}

	return errs
}

// ValidateUpdateMetadataRef checks the metadata an update request uses to name
// the resource it acts on. All four fields are required: atespace and name
// identify the resource, and uid and version guard the incarnation and revision
// the update was written against.  It does not check the server-managed timestamps,
// which clients may not set. Unlike ValidateObjectRef, nil metadata is an error
// rather than a no-op: a request that names no resource cannot be served.
func ValidateUpdateMetadataRef(meta *ateapipb.ResourceMetadata, fldPath *field.Path) field.ErrorList {
	var errs field.ErrorList

	if val, fldPath := meta.GetAtespace(), fldPath.Child("atespace"); val == "" {
		errs = append(errs, field.Required(fldPath, ""))
	} else {
		errs = append(errs, ValidateResourceName(val, fldPath)...)
	}

	if val, fldPath := meta.GetName(), fldPath.Child("name"); val == "" {
		errs = append(errs, field.Required(fldPath, ""))
	} else {
		errs = append(errs, ValidateResourceName(val, fldPath)...)
	}

	if val, fldPath := meta.GetUid(), fldPath.Child("uid"); val == "" {
		errs = append(errs, field.Required(fldPath, ""))
	} else {
		errs = append(errs, ValidateUUID(val, fldPath)...)
	}

	if val, fldPath := meta.GetVersion(), fldPath.Child("version"); val == 0 {
		errs = append(errs, field.Required(fldPath, ""))
	} else if val < 0 {
		errs = append(errs, field.Invalid(fldPath, val, "must not be negative"))
	}

	return errs
}

// ValidateGlobalUpdateMetadataRef is the global-scoped counterpart of
// ValidateUpdateMetadataRef, and enforces the same rules: name identifies the
// resource, uid and version guard the incarnation and revision the update was
// written against, and all three are required. It differs only in atespace,
// which must be empty because a global resource belongs to none. It does not
// check the server-managed timestamps, which clients may not set. Unlike
// ValidateObjectRef, nil metadata is an error rather than a no-op: a request
// that names no resource cannot be served.
func ValidateGlobalUpdateMetadataRef(meta *ateapipb.ResourceMetadata, fldPath *field.Path) field.ErrorList {
	var errs field.ErrorList

	if val, fldPath := meta.GetAtespace(), fldPath.Child("atespace"); val != "" {
		errs = append(errs, field.Invalid(fldPath, val, "must be empty for a global-scoped resource"))
	}

	if val, fldPath := meta.GetName(), fldPath.Child("name"); val == "" {
		errs = append(errs, field.Required(fldPath, ""))
	} else {
		errs = append(errs, ValidateResourceName(val, fldPath)...)
	}

	if val, fldPath := meta.GetUid(), fldPath.Child("uid"); val == "" {
		errs = append(errs, field.Required(fldPath, ""))
	} else {
		errs = append(errs, ValidateUUID(val, fldPath)...)
	}

	if val, fldPath := meta.GetVersion(), fldPath.Child("version"); val == 0 {
		errs = append(errs, field.Required(fldPath, ""))
	} else if val < 0 {
		errs = append(errs, field.Invalid(fldPath, val, "must not be negative"))
	}

	return errs
}

// ValidateGlobalObjectRef checks that a reference to a global-scoped resource is
// well-formed: its atespace must be empty (global resources do not belong to an
// atespace) and its name must be a valid resource name. It does not check that
// the referenced resource actually exists.
//
// Unlike ValidateObjectRef, and like ValidateUpdateMetadataRef, nil is an
// error rather than a no-op: every global ref in the API names the resource a
// request acts on, and a request that names nothing cannot be served.
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
// paths built from it: the netns path (/run/netns/ateom:<uid>) and the ateom
// control socket (.../ateoms/<uid>/ateom.sock). Kubernetes pod UIDs are UUIDs,
// which are valid DNS-1123 labels, so a label check accepts every legitimate
// value while rejecting separators and "..".
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

// ValidateSnapshotLocation ensures an ActorTemplate's snapshotsConfig.location
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
