// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package versionlabel

import (
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/util/validation"
)

func TestValue(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{"dev", "dev"},
		{"v1.2.3", "v1.2.3"},
		{"v0.1.0-3-gabc1234-dirty", "v0.1.0-3-gabc1234-dirty"},
		{"1.2.3+build.4", "1.2.3-build.4"},
		{"-leading-dash", "leading-dash"},
		{"trailing-dot.", "trailing-dot"},
		{"", "unknown"},
		{"+++", "unknown"},
		{strings.Repeat("a", 100), strings.Repeat("a", 63)},
	}
	for _, tt := range tests {
		got := Value(tt.in)
		if got != tt.want {
			t.Errorf("Value(%q) = %q, want %q", tt.in, got, tt.want)
		}
		if errs := validation.IsValidLabelValue(got); len(errs) != 0 {
			t.Errorf("Value(%q) = %q is not a valid label value: %v", tt.in, got, errs)
		}
	}
}

func TestNameSuffix(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{"dev", "dev"},
		{"v1.2.3", "v1-2-3"},
		{"v0.1.0-3-gabc1234-dirty", "v0-1-0-3-gabc1234-dirty"},
		{"1.2.3+build.4", "1-2-3-build-4"},
		{"UPPER.Case", "upper-case"},
		{"-v1-", "v1"},
		{"", "unknown"},
	}
	for _, tt := range tests {
		got := NameSuffix(tt.in)
		if got != tt.want {
			t.Errorf("NameSuffix(%q) = %q, want %q", tt.in, got, tt.want)
		}
		if errs := validation.IsDNS1123Label(got); len(errs) != 0 {
			t.Errorf("NameSuffix(%q) = %q is not DNS-1123-label-safe: %v", tt.in, got, errs)
		}
	}
}

// NameSuffix is lossy: distinct versions can share a suffix, so writers must
// compare the version label under Key before mutating an object found at a
// derived name.
func TestNameSuffixCollides(t *testing.T) {
	if a, b := NameSuffix("1.2.3"), NameSuffix("1-2-3"); a != b {
		t.Errorf("expected %q and %q to collide, got %q vs %q", "1.2.3", "1-2-3", a, b)
	}
}

// TestNameSuffixFallsBackToHash covers versions that sanitize to nothing or
// exceed the length cap: both must yield a short, deterministic, DNS-safe
// suffix that still differs across versions.
func TestNameSuffixFallsBackToHash(t *testing.T) {
	long := "v" + strings.Repeat("1.0.", 20)
	for _, in := range []string{"+++", long, long + "x"} {
		got := NameSuffix(in)
		if len(got) != 11 || !strings.HasPrefix(got, "v") {
			t.Errorf("NameSuffix(%q) = %q, want an 11-char hash suffix starting with 'v'", in, got)
		}
		if again := NameSuffix(in); again != got {
			t.Errorf("NameSuffix(%q) not deterministic: %q vs %q", in, got, again)
		}
		if errs := validation.IsDNS1123Label(got); len(errs) != 0 {
			t.Errorf("NameSuffix(%q) = %q is not DNS-1123-label-safe: %v", in, got, errs)
		}
	}
	if NameSuffix(long) == NameSuffix(long+"x") {
		t.Errorf("hash suffixes for distinct versions collided: %q", NameSuffix(long))
	}
}
