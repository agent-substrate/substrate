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
	"slices"
	"testing"
)

func TestMatchesImage(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		glob  string
		image string
		want  bool
	}{
		{name: "exact registry", glob: "gcr.io", image: "gcr.io/proj/img", want: true},
		{name: "registry only, no repo", glob: "gcr.io", image: "gcr.io", want: true},
		{name: "different registry", glob: "gcr.io", image: "quay.io/proj/img"},
		{name: "wildcard subdomain", glob: "*.gcr.io", image: "us.gcr.io/proj/img", want: true},
		{name: "wildcard does not match bare domain", glob: "*.gcr.io", image: "gcr.io/proj/img"},
		{name: "wildcard spans one segment only", glob: "*.gcr.io", image: "a.b.gcr.io/proj/img"},
		{name: "pkg.dev regional", glob: "*.pkg.dev", image: "us-central1-docker.pkg.dev/proj/repo/img", want: true},
		{name: "partial subdomain glob", glob: "app*.k8s.io", image: "appfoo.k8s.io/img", want: true},
		{name: "top level domain glob", glob: "k8s.*", image: "k8s.io/img", want: true},
		{name: "multiple globs", glob: "*.*.registry.io", image: "a.b.registry.io/img", want: true},
		{name: "path prefix matches", glob: "registry.io/path", image: "registry.io/path/deeper/img", want: true},
		{name: "path prefix must match exactly", glob: "registry.io/path", image: "registry.io/other/img"},
		{name: "trailing slash in pattern", glob: "registry.io/team/", image: "registry.io/team/img", want: true},
		{name: "path is a string prefix, as in the kubelet", glob: "registry.io/team", image: "registry.io/teammate/img", want: true},
		{name: "image shallower than pattern", glob: "registry.io/a/b/c", image: "registry.io/a"},
		{name: "matching port", glob: "registry.io:8080/path", image: "registry.io:8080/path/img", want: true},
		{name: "mismatched port", glob: "registry.io:8080/path", image: "registry.io:9090/path/img"},
		{name: "pattern port, image none", glob: "registry.io:8080", image: "registry.io/img"},
		{name: "image port, pattern none", glob: "registry.io", image: "registry.io:8080/img"},
		{name: "localhost registry", glob: "localhost:5000", image: "localhost:5000/img", want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := matchesImage(tc.glob, tc.image)
			if err != nil {
				t.Fatalf("matchesImage(%q, %q) returned unexpected error: %v", tc.glob, tc.image, err)
			}
			if got != tc.want {
				t.Errorf("matchesImage(%q, %q) = %v, want %v", tc.glob, tc.image, got, tc.want)
			}
		})
	}
}

func TestMatchingAuthKeys(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		auth  map[string]string
		image string
		want  []string
	}{
		{
			name:  "no keys",
			auth:  map[string]string{},
			image: "gcr.io/proj/img",
		},
		{
			name:  "no matching key",
			auth:  map[string]string{"quay.io": "a"},
			image: "gcr.io/proj/img",
		},
		{
			name:  "single match",
			auth:  map[string]string{"*.pkg.dev": "a"},
			image: "us-docker.pkg.dev/proj/repo/img",
			want:  []string{"*.pkg.dev"},
		},
		{
			name:  "concrete key before wildcard",
			auth:  map[string]string{"*.pkg.dev": "a", "us-docker.pkg.dev": "b"},
			image: "us-docker.pkg.dev/proj/repo/img",
			want:  []string{"us-docker.pkg.dev", "*.pkg.dev"},
		},
		{
			name:  "longer path before shorter",
			auth:  map[string]string{"gcr.io": "a", "gcr.io/proj": "b", "quay.io": "c"},
			image: "gcr.io/proj/img",
			want:  []string{"gcr.io/proj", "gcr.io"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := matchingAuthKeys(tc.auth, tc.image)
			if err != nil {
				t.Fatalf("matchingAuthKeys returned unexpected error: %v", err)
			}
			if !slices.Equal(got, tc.want) {
				t.Errorf("matchingAuthKeys(%v, %q) = %q, want %q", tc.auth, tc.image, got, tc.want)
			}
		})
	}
}

func TestRegistryOf(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		image string
		want  string
	}{
		{image: "gcr.io/proj/img", want: "gcr.io"},
		{image: "us-docker.pkg.dev/proj/repo/img", want: "us-docker.pkg.dev"},
		{image: "localhost:5000/img", want: "localhost:5000"},
		{image: "gcr.io", want: "gcr.io"},
	} {
		t.Run(tc.image, func(t *testing.T) {
			t.Parallel()
			if got := registryOf(tc.image); got != tc.want {
				t.Errorf("registryOf(%q) = %q, want %q", tc.image, got, tc.want)
			}
		})
	}
}
