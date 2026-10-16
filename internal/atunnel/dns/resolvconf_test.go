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

package dns

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestResolvConfNameservers(t *testing.T) {
	for _, tc := range []struct {
		name    string
		content string
		want    []string
		wantErr bool
	}{
		{
			name:    "cluster resolv.conf",
			content: "search ate-system.svc.cluster.local svc.cluster.local\nnameserver 10.96.0.10\noptions ndots:5\n",
			want:    []string{"10.96.0.10:53"},
		},
		{
			name:    "several, in order",
			content: "nameserver 10.96.0.10\nnameserver 8.8.8.8\n",
			want:    []string{"10.96.0.10:53", "8.8.8.8:53"},
		},
		{
			name:    "comments and blanks",
			content: "# generated\n\n  nameserver 10.96.0.10  # cluster\n;nameserver 1.1.1.1\n",
			want:    []string{"10.96.0.10:53"},
		},
		{name: "no nameserver", content: "search cluster.local\n", wantErr: true},
		{name: "unparsable address", content: "nameserver not-an-ip\n", wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "resolv.conf")
			if err := os.WriteFile(path, []byte(tc.content), 0o644); err != nil {
				t.Fatal(err)
			}
			got, err := resolvConfNameservers(path)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("resolvConfNameservers() = %v, want an error", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("resolvConfNameservers: %v", err)
			}
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("nameservers mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
