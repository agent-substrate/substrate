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

package memorypullcache

import (
	"testing"
)

func TestIsLocalRegistry(t *testing.T) {
	tests := []struct {
		ref  string
		want bool
	}{
		{ref: "localhost/foo", want: true},
		{ref: "localhost:5001/foo", want: true},
		{ref: "127.0.0.1/foo", want: true},
		{ref: "127.0.0.1:5001/foo", want: true},
		{ref: "127.0.0.2/foo", want: true},
		{ref: "127.0.0.2:8080/foo", want: true},
		{ref: "kind-registry/foo", want: false},
		{ref: "kind-registry:5000/foo", want: false},
		{ref: "my-registry.local/foo", want: false},
		{ref: "my-registry.local:8080/foo", want: false},
		{ref: "gcr.io/foo", want: false},
		{ref: "example.com/foo", want: false},
		{ref: "foo", want: false},
		{ref: "", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.ref, func(t *testing.T) {
			got := isLocalRegistry(tt.ref)
			if got != tt.want {
				t.Errorf("isLocalRegistry(%q) = %v; want %v", tt.ref, got, tt.want)
			}
		})
	}
}

func TestRewriteLocalRegistry(t *testing.T) {
	c := &MemoryPullCache{
		localhostRegistryReplacement: "kind-registry:5000",
	}

	tests := []struct {
		ref  string
		want string
	}{
		{ref: "localhost/foo", want: "kind-registry:5000/foo"},
		{ref: "localhost:5001/foo", want: "kind-registry:5000/foo"},
		{ref: "localhost:8080/foo", want: "kind-registry:5000/foo"},
		{ref: "127.0.0.1/foo", want: "kind-registry:5000/foo"},
		{ref: "127.0.0.1:3000/foo", want: "kind-registry:5000/foo"},
		{ref: "127.0.0.2/foo", want: "kind-registry:5000/foo"},
		{ref: "127.0.0.2:8080/foo", want: "kind-registry:5000/foo"},
		{ref: "kind-registry/foo", want: "kind-registry/foo"},
		{ref: "kind-registry:5000/foo", want: "kind-registry:5000/foo"},
		{ref: "my-registry.local/foo", want: "my-registry.local/foo"},
		{ref: "gcr.io/foo", want: "gcr.io/foo"},
		{ref: "foo", want: "foo"},
		{ref: "", want: ""},
	}

	for _, tt := range tests {
		t.Run(tt.ref, func(t *testing.T) {
			got := c.rewriteLocalRegistry(tt.ref)
			if got != tt.want {
				t.Errorf("rewriteLocalRegistry(%q) = %q; want %q", tt.ref, got, tt.want)
			}
		})
	}
}
