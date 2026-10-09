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
	"net"
	"net/netip"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"strings"

	"github.com/agent-substrate/substrate/internal/apierror"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	"github.com/agent-substrate/substrate/internal/volumepath"
	"github.com/distribution/reference"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/api/validate/content"
	"k8s.io/apimachinery/pkg/util/validation"
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

// cpuLimitMax bounds cpu limits. The wire carries ceil(cores × 1000)
// millicores and caps it at 999999 (strictly below 1000 cores), so a limit
// above 999.999 cores rounds up out of range.
var cpuLimitMax = resource.MustParse("999.999")

// memoryLimitMax bounds memory limits at what the wire's int64 byte count
// holds; resource.Quantity.Value wraps silently past it.
var memoryLimitMax = resource.MustParse("9223372036854775807")

// ValidateLimit validates one resource limit entry at fldPath: only cpu and
// memory are supported, the quantity must be greater than zero, and it must
// fit the scalars the control plane converts it to (cpuLimitMax and
// memoryLimitMax). The bounds compare Quantities rather than the converted
// integers, which wrap for very large values. An empty quantity is left to
// the required tag.
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
	switch {
	case name == ResourceCPU && q.Cmp(cpuLimitMax) > 0:
		errs = append(errs, field.Invalid(fldPath.Child("quantity"), quantity, "cpu limit must be at most 999.999 cores"))
	case name == ResourceMemory && q.Cmp(memoryLimitMax) > 0:
		errs = append(errs, field.Invalid(fldPath.Child("quantity"), quantity, "memory limit must be at most 9223372036854775807 bytes"))
	}
	return errs
}

// ValidatePinnedImage requires a well-formed OCI image reference pinned by
// digest (e.g. "name@sha256:..."): changing the image content under a fixed
// reference invalidates snapshots. It parses with the same grammar the
// container runtimes use, so a malformed digest is rejected rather than
// treated as pinned.
func ValidatePinnedImage(fldPath *field.Path, value string) field.ErrorList {
	if value == "" {
		return nil
	}
	ref, err := reference.ParseNormalizedNamed(value)
	if err != nil {
		return field.ErrorList{field.Invalid(fldPath, value, fmt.Sprintf("must be a well-formed image reference: %v", err))}
	}
	if _, ok := ref.(reference.Digested); !ok {
		return field.ErrorList{field.Invalid(fldPath, value, "must be pinned by digest (changing the image invalidates snapshots)")}
	}
	return nil
}

// capabilityRE constrains Linux capability names: uppercase, without the
// "CAP_" prefix (which is added when the OCI spec is written; the prefixed
// spelling would silently grant nothing).
var capabilityRE = regexp.MustCompile(`^[A-Z][A-Z0-9_]*$`)

// ValidateCapabilities checks each capability name. "ALL" is accepted only
// when allowAll is set, which is the case for drop but not add.
func ValidateCapabilities(fldPath *field.Path, caps []string, allowAll bool) field.ErrorList {
	var errs field.ErrorList
	for i, c := range caps {
		p := fldPath.Index(i)
		switch {
		case c == "ALL" && !allowAll:
			errs = append(errs, field.Invalid(p, c, "add does not accept 'ALL'; name the individual capabilities the container needs"))
		case c == "ALL":
		case len(c) > 63:
			errs = append(errs, field.TooLong(p, nil, 63))
		case strings.HasPrefix(c, "CAP_"):
			errs = append(errs, field.Invalid(p, c, "must be named without the 'CAP_' prefix (e.g. 'NET_BIND_SERVICE')"))
		case !capabilityRE.MatchString(c):
			errs = append(errs, field.Invalid(p, c, "must be an uppercase capability name like 'NET_BIND_SERVICE'"))
		}
	}
	return errs
}

// mountPathBadSegmentRE matches '.' or '..' path segments.
var mountPathBadSegmentRE = regexp.MustCompile(`(^|/)[.][.]?(/|$)`)

// ValidateMountPath requires a clean absolute Unix path that starts with '/',
// is not '/', and contains no ':', '.' or '..' segments, '//', trailing '/',
// or control characters.
func ValidateMountPath(fldPath *field.Path, p string) field.ErrorList {
	if p == "" {
		return nil
	}
	bad := !strings.HasPrefix(p, "/") || len(p) == 1 ||
		strings.HasSuffix(p, "/") || strings.Contains(p, "//") ||
		strings.Contains(p, ":") || mountPathBadSegmentRE.MatchString(p)
	if !bad {
		for _, r := range p {
			if r < 0x20 || r == 0x7f {
				bad = true
				break
			}
		}
	}
	if bad {
		return field.ErrorList{field.Invalid(fldPath, p, "must be a clean absolute Unix path: must start with '/', not be '/', and contain no ':', '..', '.', '//', trailing '/', or control characters")}
	}
	return nil
}

// ValidateNestedMountPaths rejects mount paths that nest under or over one
// another: volumes cannot mount onto other volumes. paths[i] is reported at
// fldPath[i].mount_path. Identical paths are left to the list-key uniqueness
// check.
func ValidateNestedMountPaths(fldPath *field.Path, paths []string) field.ErrorList {
	var errs field.ErrorList
	for i, path := range paths {
		if path == "" {
			continue
		}
		for _, prior := range paths[:i] {
			if prior == "" || prior == path {
				continue
			}
			if strings.HasPrefix(path, prior+"/") || strings.HasPrefix(prior, path+"/") {
				errs = append(errs, field.Invalid(fldPath.Index(i).Child("mount_path"), path,
					fmt.Sprintf("must not nest under or over another mount (%q)", prior)))
			}
		}
	}
	return errs
}

// httpGetPathRE constrains wakeup probe paths to RFC 3986 path-segment
// characters only, with well-formed percent-escapes, and no query string
// or fragment.
var httpGetPathRE = regexp.MustCompile(`^/([A-Za-z0-9\-._~!$&'()*+,;=:@/]|%[0-9A-Fa-f]{2})*$`)

// ValidateHTTPGetPath checks a wakeup probe's HTTP path.
func ValidateHTTPGetPath(fldPath *field.Path, p string) field.ErrorList {
	if p == "" {
		return nil
	}
	if !httpGetPathRE.MatchString(p) {
		return field.ErrorList{field.Invalid(fldPath, p, "must be a URL path starting with '/', using only RFC 3986 path-segment characters, without query or fragment")}
	}
	return nil
}

// envVarNameRE constrains env var names to any printable ASCII character
// except '='.
var envVarNameRE = regexp.MustCompile(`^[ -<>-~]+$`)

// ValidateEnvVarName checks an environment variable name.
func ValidateEnvVarName(fldPath *field.Path, name string) field.ErrorList {
	if name == "" {
		return nil
	}
	if !envVarNameRE.MatchString(name) {
		return field.ErrorList{field.Invalid(fldPath, name, "may contain any printable ASCII character except '='")}
	}
	return nil
}

// ValidateProjectedPath applies volumepath.ValidateProjected to a file path
// projected into a system-info volume. atelet re-checks it before writing to
// the host.
func ValidateProjectedPath(fldPath *field.Path, p string) field.ErrorList {
	if p == "" {
		return nil
	}
	if err := volumepath.ValidateProjected(p); err != nil {
		return field.ErrorList{field.Invalid(fldPath, p, err.Error())}
	}
	return nil
}

// ValidateStorageVolumeID rejects control characters (U+0000-U+0008, U+000B,
// U+000C, U+000E-U+001F, U+007F-U+009F) in an external volume's storage ID.
func ValidateStorageVolumeID(fldPath *field.Path, id string) field.ErrorList {
	for _, r := range id {
		if (r >= 0x0000 && r <= 0x0008) ||
			r == 0x000B ||
			r == 0x000C ||
			(r >= 0x000E && r <= 0x001F) ||
			(r >= 0x007F && r <= 0x009F) {
			return field.ErrorList{field.Invalid(fldPath, id, "must not contain control characters (U+0000-U+0008, U+000B, U+000C, U+000E-U+001F, U+007F-U+009F)")}
		}
	}
	return nil
}

// ValidateVolumeType allows an optional "substrate.io/" prefix, followed by a
// valid DNS-1123 subdomain.
func ValidateVolumeType(fldPath *field.Path, volumeType string) field.ErrorList {
	if volumeType == "" {
		return nil
	}
	var errs field.ErrorList
	for _, msg := range validation.IsDNS1123Subdomain(strings.TrimPrefix(volumeType, "substrate.io/")) {
		errs = append(errs, field.Invalid(fldPath, volumeType, msg))
	}
	return errs
}

// csiMapMaxBytes bounds a map a CSI driver produced. The CSI spec caps
// map<string, string> fields at 4 KiB; this allows ten times that so a future
// revision of the limit does not turn compliant drivers away. It is a
// guardrail against unbounded input, not a contract drivers are held to.
const csiMapMaxBytes = 40 * 1024

// ValidateCSIMap bounds a map a CSI driver produced (a volume or publish
// context) at csiMapMaxBytes, counting keys and values. The entries are the
// driver's own and are not validated further.
func ValidateCSIMap(fldPath *field.Path, m map[string]string) field.ErrorList {
	size := 0
	for k, v := range m {
		size += len(k) + len(v)
	}
	if size > csiMapMaxBytes {
		return field.ErrorList{field.TooLong(fldPath, nil, csiMapMaxBytes)}
	}
	return nil
}

// ValidateHostPort requires "host:port", where host is an IP address or a DNS
// subdomain name (a trailing dot, marking it fully qualified, is allowed) and
// port is a number in 1..65535.
func ValidateHostPort(fldPath *field.Path, value string) field.ErrorList {
	if value == "" {
		return nil
	}
	host, port, err := net.SplitHostPort(value)
	if err != nil {
		return field.ErrorList{field.Invalid(fldPath, value, "must be host:port")}
	}
	var errs field.ErrorList
	if _, err := netip.ParseAddr(host); err != nil {
		for _, msg := range content.IsDNS1123Subdomain(strings.TrimSuffix(host, ".")) {
			errs = append(errs, field.Invalid(fldPath, value, "host: "+msg))
		}
	}
	if n, err := strconv.Atoi(port); err != nil || n < 1 || n > 65535 {
		errs = append(errs, field.Invalid(fldPath, value, "port must be a number between 1 and 65535"))
	}
	return errs
}
