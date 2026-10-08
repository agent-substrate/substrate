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
// URLsMatch (k8s.io/kubernetes/pkg/credentialprovider): the hosts must have
// the same number of "."-separated segments, each matched with filepath.Match
// so a glob never spans one ("*.io" does not match "k8s.gcr.io"), the ports
// must be equal, and the glob's path must be a string prefix of the image's.
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

	globHost, globPort := splitHost(globURL)
	imageHost, imagePort := splitHost(imageURL)
	if globPort != imagePort || len(globHost) != len(imageHost) {
		return false, nil
	}
	for i, globPart := range globHost {
		matched, err := filepath.Match(globPart, imageHost[i])
		if err != nil {
			return false, fmt.Errorf("while matching pattern %q against image %q: %w", glob, image, err)
		}
		if !matched {
			return false, nil
		}
	}
	return strings.HasPrefix(imageURL.Path, globURL.Path), nil
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

// splitHost returns u's host split on "." and its port, which is compared
// separately (globs are not allowed in ports).
func splitHost(u *url.URL) (parts []string, port string) {
	host, port, err := net.SplitHostPort(u.Host)
	if err != nil {
		host, port = u.Host, ""
	}
	return strings.Split(host, "."), port
}

// matchingAuthKeys returns the keys of auth that match image, best first.
func matchingAuthKeys[V any](auth map[string]V, image string) ([]string, error) {
	var matches []string
	for key := range auth {
		matched, err := matchesImage(key, image)
		if err != nil {
			return nil, err
		}
		if matched {
			matches = append(matches, key)
		}
	}
	sortAuthKeys(matches)
	return matches, nil
}

// sortAuthKeys sorts keys in reverse lexical order, like the kubelet's
// keyring: longer paths before their prefixes, wildcards last.
func sortAuthKeys(keys []string) {
	sort.Sort(sort.Reverse(sort.StringSlice(keys)))
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
