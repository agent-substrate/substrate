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

package filecache

import (
	"regexp"
	"strings"
	"testing"
)

const testDigest = "af2a7458c2c05df1a01d0b2f335f4849a2de84e83160fefdc31a6266015642d4"

var entryDirNameRE = regexp.MustCompile(`^[0-9a-f]{64}$`)

func TestSHA256Key(t *testing.T) {
	k, err := SHA256Key(testDigest)
	if err != nil {
		t.Fatalf("SHA256Key(%q): %v", testDigest, err)
	}
	if want := "sha256:" + testDigest; k.String() != want {
		t.Errorf("String() = %q, want %q", k.String(), want)
	}
	if !entryDirNameRE.MatchString(k.dir) {
		t.Errorf("dir = %q, want 64 lowercase hex chars", k.dir)
	}

	upper, err := SHA256Key(strings.ToUpper(testDigest))
	if err != nil {
		t.Fatalf("SHA256Key(upper): %v", err)
	}
	if upper != k {
		t.Errorf("uppercase digest yielded a different key: %q vs %q", upper.dir, k.dir)
	}
}

func TestSHA256KeyRejectsBadDigests(t *testing.T) {
	for _, digest := range []string{
		"",
		"abc123",              // too short
		testDigest + "00",     // too long
		testDigest[:63] + "g", // not hex
	} {
		if _, err := SHA256Key(digest); err == nil {
			t.Errorf("SHA256Key(%q) succeeded, want error", digest)
		}
	}
}

func TestURIKey(t *testing.T) {
	base := URIKey("gs://bucket/golden-v3", "mem.img")
	if base != URIKey("gs://bucket/golden-v3", "mem.img") {
		t.Error("equal parts yielded different keys")
	}
	if !entryDirNameRE.MatchString(base.dir) {
		t.Errorf("dir = %q, want 64 lowercase hex chars", base.dir)
	}

	// Part boundaries must be unambiguous: shifting a separator across a
	// part boundary, or adding an empty part, is a different identity.
	distinct := []Key{
		base,
		URIKey("gs://bucket/golden-v3/mem.img"),
		URIKey("gs://bucket/golden-v3", "mem.img", ""),
		URIKey("gs://bucket", "golden-v3/mem.img"),
	}
	for i, a := range distinct {
		for j, b := range distinct {
			if i != j && a == b {
				t.Errorf("keys %d and %d collide: %q", i, j, a.dir)
			}
		}
	}
}

func TestKeySpacesAreDisjoint(t *testing.T) {
	// A URI that happens to spell a digest must not collide with the
	// content-addressed key for that digest.
	sha, err := SHA256Key(testDigest)
	if err != nil {
		t.Fatal(err)
	}
	if uri := URIKey(testDigest); uri == sha {
		t.Errorf("URIKey and SHA256Key collide for %q", testDigest)
	}
}
