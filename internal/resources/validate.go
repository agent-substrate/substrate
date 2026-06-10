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
	"net/url"
	"strings"

	"k8s.io/apimachinery/pkg/util/validation"
)

// The validators in this file guard inputs that atelet turns into host
// filesystem paths. atelet listens on an insecure hostPort, so any reachable
// caller could otherwise smuggle a path separator or ".." through these
// fields and make atelet read/RemoveAll/write outside the intended directory
// tree, or collide OCI bundles. They are exported so the API server and
// controller can apply the same rules at their own boundaries.

// ValidateActorRef ensures every component of the per-actor directory tree is
// a valid DNS-1123 name. namespace+template+actorID are concatenated by
// ateompath.ActorPath into a host path on which atelet runs os.RemoveAll and
// os.MkdirAll, so all three must be validated. Checking only one would still
// leave a traversal window via the others. Template names are DNS-1123
// subdomains (dots allowed); namespaces and actor IDs are labels.
//
// The actor ID rule here is DNS-1123 label, which matches ValidateActorID;
// unifying the two implementations is tracked separately.
func ValidateActorRef(namespace, template, actorID string) error {
	if errs := validation.IsDNS1123Label(namespace); len(errs) > 0 {
		return fmt.Errorf("invalid namespace %q: %s", namespace, strings.Join(errs, "; "))
	}
	if errs := validation.IsDNS1123Subdomain(template); len(errs) > 0 {
		return fmt.Errorf("invalid template %q: %s", template, strings.Join(errs, "; "))
	}
	if errs := validation.IsDNS1123Label(actorID); len(errs) > 0 {
		return fmt.Errorf("invalid actor ID %q: %s", actorID, strings.Join(errs, "; "))
	}
	// The three names are joined into a single path component
	// (<namespace>:<template>:<actorID>, see ateompath.ActorPath), which must
	// fit the 255-byte filename limit of common filesystems. Individually
	// valid DNS names can exceed it: 63 + 253 + 63 plus separators is 381.
	if n := len(namespace) + 1 + len(template) + 1 + len(actorID); n > 255 {
		return fmt.Errorf("actor ref %s:%s:%s is %d bytes; the combined path component must be at most 255", namespace, template, actorID, n)
	}
	return nil
}

// ValidateAteomUID rejects a target ateom pod UID that could escape the host
// paths built from it: the netns path (/run/netns/ateom:<uid>) and the ateom
// control socket (.../ateoms/<uid>/ateom.sock). Kubernetes pod UIDs are UUIDs,
// which are valid DNS-1123 labels, so a label check accepts every legitimate
// value while rejecting separators and "..".
func ValidateAteomUID(targetAteomUID string) error {
	if errs := validation.IsDNS1123Label(targetAteomUID); len(errs) > 0 {
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
		if errs := validation.IsDNS1123Label(name); len(errs) > 0 {
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

// ValidateSnapshotURIPrefix ensures a checkpoint/restore snapshot location is
// a well-formed URI with a bucket, so a bad prefix fails fast at the RPC
// boundary instead of deep inside an object-storage call. It deliberately
// does not restrict the scheme: the storage layer only uses the host (bucket)
// and path, and which schemes are acceptable is a storage-backend policy, not
// a per-RPC one. The local paths used for snapshot upload/download are
// derived from the separately validated actor ref, not from this URI, so this
// is a sanity check rather than a path-traversal guard.
func ValidateSnapshotURIPrefix(prefix string) error {
	u, err := url.Parse(prefix)
	if err != nil {
		return fmt.Errorf("invalid snapshot URI prefix %q: %v", prefix, err)
	}
	if u.Host == "" {
		return fmt.Errorf("invalid snapshot URI prefix %q: missing bucket", prefix)
	}
	// Object names are appended to the prefix by string concatenation. A
	// query, fragment, or userinfo component would swallow the appended name
	// when the result is re-parsed (the storage layer uses only host and
	// path), silently redirecting the upload/download to a different object.
	if u.Opaque != "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return fmt.Errorf("invalid snapshot URI prefix %q: must contain only a scheme, bucket, and path", prefix)
	}
	return nil
}
