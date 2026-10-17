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

import "testing"

func TestValidateSnapshotFileNames(t *testing.T) {
	for _, tc := range []struct {
		name    string
		files   []string
		wantErr bool
	}{
		{name: "empty list", files: nil},
		{name: "plain names", files: []string{"manifest.json", "memory.img", "durable-dir.tar"}},
		{name: "fs subdir names", files: []string{"checkpoint.img", "fs/fscheckpoint.pb", "fs/multitar.img", "fs/pages.img", "fs/pages_meta.img"}},
		{name: "empty name", files: []string{""}, wantErr: true},
		{name: "dot", files: []string{"."}, wantErr: true},
		{name: "dot dot", files: []string{".."}, wantErr: true},
		{name: "absolute", files: []string{"/etc/passwd"}, wantErr: true},
		{name: "nested arbitrary subdir", files: []string{"a/b"}, wantErr: true},
		{name: "deeply nested under fs", files: []string{"fs/a/b"}, wantErr: true},
		{name: "escaping", files: []string{"../x"}, wantErr: true},
		{name: "duplicate", files: []string{"a", "b", "a"}, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateSnapshotFileNames(tc.files)
			if gotErr := err != nil; gotErr != tc.wantErr {
				t.Errorf("ValidateSnapshotFileNames(%q) = %v, want error: %v", tc.files, err, tc.wantErr)
			}
		})
	}
}
