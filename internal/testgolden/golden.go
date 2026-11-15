// Copyright 2026 The Agent Substrate Authors
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

package testgolden

import (
	"bytes"
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

// Check compares got with a golden file under the caller's testdata directory.
func Check(t testing.TB, name string, got []byte) []byte {
	t.Helper()
	path := filepath.Join("testdata", name)
	refresh, err := strconv.ParseBool(os.Getenv("REFRESH_GOLDENS"))
	if err != nil && os.Getenv("REFRESH_GOLDENS") != "" {
		t.Fatalf("REFRESH_GOLDENS: %v", err)
	}
	if refresh {
		if err := os.WriteFile(path, got, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("output differs from %s (run hack/verify-goldens.sh to regenerate):\n%s", path, got)
	}
	return want
}
