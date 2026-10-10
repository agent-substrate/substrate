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
	"os"
	"path/filepath"
	"reflect"
	"strings"

	"github.com/agent-substrate/substrate/internal/apierror"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/api/validate/content"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// ToGRPCStatusError turns validation errors into the InvalidArgument error an
// RPC handler responds with. Callers check len(errs) > 0 first.
//
// TODO: Delete once atelet's AteomSupport server returns apierrors, and use
// ToAPIError instead.
func ToGRPCStatusError(errs field.ErrorList) error {
	return status.Error(codes.InvalidArgument, errs.ToAggregate().Error())
}

// ToAPIError turns validation errors into the InvalidArgument error an RPC
// handler responds with. Callers check len(errs) > 0 first.
func ToAPIError(errs field.ErrorList) error {
	return apierror.InvalidArgument("%v", errs.ToAggregate())
}

// DeepEqual compares two values of any type, using proto.Equal if both are
// proto messages, and reflect.DeepEqual otherwise. Declarative validation's
// generated code reaches it through each generating package's ateDeepEqual.
func DeepEqual[T any](a, b T) bool {
	asProto := func(x any) proto.Message {
		pm, ok := x.(proto.Message)
		if !ok {
			return nil
		}
		return pm
	}

	if pa, pb := asProto(a), asProto(b); pa != nil && pb != nil {
		return proto.Equal(pa, pb)
	}
	return reflect.DeepEqual(a, b)
}

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

// ValidateWorkerPodUID rejects a worker pod UID that could escape the host
// path built from it: the ateom control socket (.../ateoms/<uid>/ateom.sock).
// Kubernetes pod UIDs are UUIDs, which are valid DNS-1123 labels, so a label
// check accepts every legitimate value while rejecting separators and "..".
func ValidateWorkerPodUID(workerPodUID string) error {
	if errs := content.IsDNS1123Label(workerPodUID); len(errs) > 0 {
		return fmt.Errorf("invalid worker pod UID %q: %s", workerPodUID, strings.Join(errs, "; "))
	}
	return nil
}

// ValidateActorDirs checks that every directory atelet passes to ateom is
// set, absolute and clean.
func ValidateActorDirs(actorDirs *ateompb.ActorDirs, fldPath *field.Path) field.ErrorList {
	if actorDirs == nil {
		return field.ErrorList{field.Required(fldPath, "")}
	}
	var errs field.ErrorList
	for _, actorDir := range []struct{ name, path string }{
		{"root_dir", actorDirs.GetRootDir()},
		{"oci_bundle_dir", actorDirs.GetOciBundleDir()},
		{"checkpoint_dir", actorDirs.GetCheckpointDir()},
		{"restore_dir", actorDirs.GetRestoreDir()},
		{"durable_dir_volume_mounts_dir", actorDirs.GetDurableDirVolumeMountsDir()},
		{"system_info_volume_roots_dir", actorDirs.GetSystemInfoVolumeRootsDir()},
		{"volumes_dir", actorDirs.GetVolumesDir()},
	} {
		errs = append(errs, validateAbsDir(actorDir.path, fldPath.Child(actorDir.name))...)
	}
	return errs
}

// ValidateSnapshotFidelity rejects a checkpoint or restore request whose
// fidelity no runtime can serve. ROOTFS is defined in the enum but no sandbox
// runtime captures rootfs changes without memory yet, so it is refused here
// as well as at template admission.
func ValidateSnapshotFidelity(fidelity ateompb.SnapshotFidelity, fldPath *field.Path) field.ErrorList {
	switch fidelity {
	case ateompb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES, ateompb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY:
		return nil
	case ateompb.SnapshotFidelity_SNAPSHOT_FIDELITY_UNSPECIFIED:
		return field.ErrorList{field.Required(fldPath, "")}
	case ateompb.SnapshotFidelity_SNAPSHOT_FIDELITY_ROOTFS:
		return field.ErrorList{field.Invalid(fldPath, fidelity.String(), "ROOTFS fidelity is not supported yet")}
	default:
		return field.ErrorList{field.NotSupported(fldPath, fidelity,
			[]string{ateompb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES.String(), ateompb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY.String()})}
	}
}

func validateAbsDir(dir string, fldPath *field.Path) field.ErrorList {
	if dir == "" {
		return field.ErrorList{field.Required(fldPath, "")}
	}
	if !filepath.IsAbs(dir) || filepath.Clean(dir) != dir {
		return field.ErrorList{field.Invalid(fldPath, dir, "must be an absolute, clean path")}
	}
	return nil
}

// ValidateRuntimeAssetPath ensures p is a regular file under root, with no symlinks
// ateom runs these as root, so they must be assets atelet fetched into root
func ValidateRuntimeAssetPath(root, p string, fldPath *field.Path) field.ErrorList {
	if p == "" {
		return field.ErrorList{field.Required(fldPath, "")}
	}
	if !filepath.IsAbs(p) || filepath.Clean(p) != p {
		return field.ErrorList{field.Invalid(fldPath, p, "must be an absolute, clean path")}
	}
	if rel, err := filepath.Rel(root, p); err != nil || rel == "." || !filepath.IsLocal(rel) {
		return field.ErrorList{field.Invalid(fldPath, p, fmt.Sprintf("must be inside %s", root))}
	}
	resolved, err := filepath.EvalSymlinks(p)
	if err != nil {
		return field.ErrorList{field.Invalid(fldPath, p, err.Error())}
	}
	if resolved != p {
		return field.ErrorList{field.Invalid(fldPath, p, "must not traverse a symlink")}
	}
	if fi, err := os.Stat(p); err != nil || !fi.Mode().IsRegular() {
		return field.ErrorList{field.Invalid(fldPath, p, "must be a regular file")}
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
// schemes are acceptable is a storage-backend policy, not a per-RPC one.
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
	// url.JoinPath cleans dot segments, and some backends trim spaces from
	// segments (or windows, trailing dots) and treat '\' as a separator, so
	// these could move snapshots outside of the location, e.g. into another
	// atespace's prefix
	if strings.Contains(u.Path, `\`) {
		return fmt.Errorf(`invalid snapshot location %q: must not contain '\'`, location)
	}
	for segment := range strings.SplitSeq(u.Path, "/") {
		if segment != "" && strings.Trim(segment, ". ") == "" {
			return fmt.Errorf("invalid snapshot location %q: must not contain path segments of only '.' and ' '", location)
		}
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

// ValidateLimit validates one resource limit entry at fldPath: only cpu and
// memory are supported, the quantity must be greater than zero, and the cpu
// limit must be less than 1000 cores. An empty quantity is left to the
// required tag.
func ValidateLimit(fldPath *field.Path, name, quantity string) field.ErrorList {
	if name != ResourceCPU && name != ResourceMemory {
		return field.ErrorList{field.NotSupported(fldPath.Child("name"), name, []string{ResourceCPU, ResourceMemory})}
	}
	if quantity == "" {
		return nil
	}
	q, err := resource.ParseQuantity(quantity)
	if err != nil {
		return field.ErrorList{field.Invalid(fldPath.Child("quantity"), quantity, fmt.Sprintf("must be a Kubernetes resource quantity: %v", err))}
	}
	var errs field.ErrorList
	if q.Sign() <= 0 {
		errs = append(errs, field.Invalid(fldPath.Child("quantity"), quantity, "must be greater than zero"))
	}
	if name == ResourceCPU && q.Cmp(cpuLimitMax) >= 0 {
		errs = append(errs, field.Invalid(fldPath.Child("quantity"), quantity, "cpu limit must be less than 1000 cores"))
	}
	return errs
}
