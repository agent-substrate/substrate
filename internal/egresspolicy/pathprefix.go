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

package egresspolicy

import (
	"errors"
	"strings"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// A path prefix scopes a CredentialHeader to the requests under it. Prefixes
// and request paths are compared as written, byte for byte, so both must be
// in a form an origin cannot read differently: a path with a dot segment, an
// empty segment, a backslash, or a percent-encoded '.', '/' or '\' could
// resolve outside a prefix it starts with. Admission refuses such a prefix,
// and such a request path matches no prefix: it receives only the header's
// binding without one.

// ValidatePathPrefix reports why prefix cannot scope a CredentialHeader, or
// nil when it can.
func ValidatePathPrefix(prefix string) error {
	if !strings.HasPrefix(prefix, "/") {
		return errors.New(`must start with "/"`)
	}
	if !isCanonicalPath(prefix) {
		return errors.New(`must be printable ASCII without a query, a dot or empty segment, a backslash, or a percent-encoded ".", "/" or "\"`)
	}
	return nil
}

// PathPrefixMatches reports whether path lies under prefix: path equals
// prefix, or continues it at a segment boundary. "/api" matches "/api" and
// "/api/v4" but not "/apis"; "/api/" matches "/api/v4" but not "/api".
func PathPrefixMatches(prefix, path string) bool {
	if !strings.HasPrefix(path, prefix) {
		return false
	}
	return len(path) == len(prefix) || strings.HasSuffix(prefix, "/") || path[len(prefix)] == '/'
}

// ReplaceHeadersFor returns the CredentialHeaders effects apply to a request
// with the request target target (its :path), at most one per header name,
// in the order the names first appear in effects. Of a name's entries, the
// one with the longest path_prefix that matches the request path applies,
// the one without a path_prefix otherwise; a name with neither applies none.
// The query is not part of the path, and a target that is not a canonical
// absolute path matches no prefix.
func ReplaceHeadersFor(effects *ateapipb.HttpRuleEffects, target string) []*ateapipb.CredentialHeader {
	path, canonical := requestPath(target)
	var (
		order  []string
		chosen = map[string]*ateapipb.CredentialHeader{}
	)
	for _, inj := range effects.GetReplaceHeaders() {
		name := strings.ToLower(inj.GetHeader())
		prev, seen := chosen[name]
		if !seen {
			order = append(order, name)
			chosen[name] = nil
		}
		prefix := inj.GetPathPrefix()
		if prefix != "" && (!canonical || !PathPrefixMatches(prefix, path)) {
			continue
		}
		if prev == nil || len(prefix) > len(prev.GetPathPrefix()) {
			chosen[name] = inj
		}
	}
	out := make([]*ateapipb.CredentialHeader, 0, len(order))
	for _, name := range order {
		if inj := chosen[name]; inj != nil {
			out = append(out, inj)
		}
	}
	return out
}

// requestPath returns the path of target without its query, and whether it
// is a canonical absolute path.
func requestPath(target string) (string, bool) {
	path, _, _ := strings.Cut(target, "?")
	return path, strings.HasPrefix(path, "/") && isCanonicalPath(path)
}

// isCanonicalPath reports whether an absolute path reads the same to every
// origin, as described at the top of this file.
func isCanonicalPath(path string) bool {
	for _, c := range []byte(path) {
		if c <= ' ' || c >= 0x7f || c == '\\' || c == '?' || c == '#' {
			return false
		}
	}
	lower := strings.ToLower(path)
	for _, encoded := range []string{"%2e", "%2f", "%5c"} {
		if strings.Contains(lower, encoded) {
			return false
		}
	}
	segments := strings.Split(path[1:], "/")
	for i, segment := range segments {
		if segment == "" && i != len(segments)-1 {
			return false
		}
		// A ";" starts path parameters, which some origins strip before
		// resolving dot segments.
		name, _, _ := strings.Cut(segment, ";")
		if name == "." || name == ".." {
			return false
		}
	}
	return true
}
