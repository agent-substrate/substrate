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

package credentialprovider

import (
	"fmt"
	"net"
	"net/url"
	"path/filepath"
	"sort"
	"strings"
)

// matchesImage reports whether glob matches image, following the kubelet's
// urlsMatch (k8s.io/kubernetes/pkg/credentialprovider): domain split on ".",
// path on "/", each segment matched with filepath.Match so a glob never spans
// one ("*.io" does not match "k8s.gcr.io"), the glob's segments a prefix of the
// image's, and ports equal.
//
// Both arguments are scheme-less: "*.pkg.dev", "registry.io:8080/path".
func matchesImage(glob, image string) (bool, error) {
	globURL, err := parseSchemelessURL(glob)
	if err != nil {
		return false, fmt.Errorf("while parsing match pattern %q: %w", glob, err)
	}
	imageURL, err := parseSchemelessURL(image)
	if err != nil {
		return false, fmt.Errorf("while parsing image %q: %w", image, err)
	}

	globParts, globPort := splitURL(globURL)
	imageParts, imagePort := splitURL(imageURL)
	if globPort != imagePort {
		return false, nil
	}
	// The pattern may be less specific than the image (a bare registry matches
	// every repository under it), but never more.
	if len(globParts) > len(imageParts) {
		return false, nil
	}
	for i, globPart := range globParts {
		matched, err := filepath.Match(globPart, imageParts[i])
		if err != nil {
			return false, fmt.Errorf("while matching pattern %q against image %q: %w", glob, image, err)
		}
		if !matched {
			return false, nil
		}
	}
	return true, nil
}

// parseSchemelessURL parses a registry/repository string that carries no
// scheme by lending it one, so net/url splits host, port and path for us.
func parseSchemelessURL(schemeless string) (*url.URL, error) {
	parsed, err := url.Parse("https://" + schemeless)
	if err != nil {
		return nil, err
	}
	parsed.Scheme = ""
	return parsed, nil
}

// splitURL flattens a URL into the segment list matchesImage compares: the
// host split on "." followed by the path split on "/", with the port returned
// separately (globs are not allowed in ports).
func splitURL(u *url.URL) (parts []string, port string) {
	host, port, err := net.SplitHostPort(u.Host)
	if err != nil {
		host, port = u.Host, ""
	}
	return append(strings.Split(host, "."), strings.Split(u.Path, "/")...), port
}

// bestAuthKey picks the key of auth that best matches image, or "" when none
// does. Ties break by reverse lexical order, as the kubelet's docker keyring
// does: longer keys before shorter ones sharing their prefix, and concrete keys
// before wildcards ("*" sorts below any character a registry can start with).
func bestAuthKey[V any](auth map[string]V, image string) (string, error) {
	var matches []string
	for key := range auth {
		matched, err := matchesImage(key, image)
		if err != nil {
			return "", err
		}
		if matched {
			matches = append(matches, key)
		}
	}
	if len(matches) == 0 {
		return "", nil
	}
	sort.Sort(sort.Reverse(sort.StringSlice(matches)))
	return matches[0], nil
}

// registryOf returns the domain (with port, if any) of an image reference,
// which is the cache key for the "Registry" plugin cache-key type.
func registryOf(image string) string {
	parsed, err := parseSchemelessURL(image)
	if err != nil {
		return image
	}
	return parsed.Host
}
