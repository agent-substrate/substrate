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

package apivalidation

import (
	"fmt"
	"maps"
	"slices"
	"strings"
	"testing"

	"go.ate.dev/substrate/internal/preview"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

const (
	testAtespace = "test-atespace"
)

const (
	// someActorUID stands in for the UID the store assigns an Actor, for tests
	// that need a well-formed snapshot URI but never exercise who owns it. Those
	// seed their Actor in a single call, before a real UID exists.
	someActorUID = "6b1f9d0c-4a2e-4d38-9c77-5e0a1b2c3d4e"
)

// assertValidate checks validate against want with no preview gates enabled.
// Then, for each entry in wantWithPreview, it enables the gates the key names
// and checks validate against that entry's errors. Only cases that set fields
// behind a preview gate need wantWithPreview.
func assertValidate(t *testing.T, validate func() field.ErrorList, want field.ErrorList, wantWithPreview map[string]field.ErrorList) {
	t.Helper()
	setPreviewForTest(t, "")
	assertValidateErr(t, validate(), want)
	for _, gates := range slices.Sorted(maps.Keys(wantWithPreview)) {
		t.Run("preview="+gates, func(t *testing.T) {
			t.Helper()
			setPreviewForTest(t, gates)
			assertValidateErr(t, validate(), wantWithPreview[gates])
		})
	}
}

// setPreviewForTest enables the preview gates named by gates, a --preview
// value such as "ALL", for the duration of t. An empty value enables none.
func setPreviewForTest(t *testing.T, gates string) {
	t.Helper()
	var values []string
	if gates != "" {
		values = strings.Split(gates, ",")
	}
	preview.InitForTest(t, values...)
}

func selectorLabelsOfSize(n int) map[string]string {
	labels := make(map[string]string, n)
	for i := 0; i < n; i++ {
		labels[fmt.Sprintf("k%d", i)] = "v"
	}
	return labels
}

func annotationsOfBytes(extra int) map[string]string {
	out := make(map[string]string, 64)
	for i := range 64 {
		valLen := 253
		if i == 0 {
			valLen += extra
		}
		out[fmt.Sprintf("k%02d", i)] = strings.Repeat("v", valLen)
	}
	return out
}

func TestValidateAnnotations(t *testing.T) {
	path := field.NewPath("annotations")
	tests := []struct {
		name        string
		annotations map[string]string
		want        field.ErrorList
	}{{
		name: "nil",
	}, {
		name:        "arbitrary values",
		annotations: map[string]string{"owner/team": "platform", "runbook": "https://runbooks.example.com/a?b=1&c=2"},
	}, {
		name:        "reserved exact prefix ate.dev/",
		annotations: map[string]string{"ate.dev/internal": "1"},
		want:        field.ErrorList{field.Invalid(path, "ate.dev/internal", "")},
	}, {
		name:        "reserved subdomain prefix foo.ate.dev/",
		annotations: map[string]string{"foo.ate.dev/internal": "1"},
		want:        field.ErrorList{field.Invalid(path, "foo.ate.dev/internal", "")},
	}, {
		name:        "non-reserved lookalike domain myate.dev/",
		annotations: map[string]string{"myate.dev/ok": "1"},
	}, {
		name:        "exactly max total bytes",
		annotations: annotationsOfBytes(0),
	}, {
		name:        "exceeds max total bytes by 1",
		annotations: annotationsOfBytes(1),
		want:        field.ErrorList{field.TooLong(path, "", maxAnnotationsSizeBytes)},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertValidateErr(t, validateAnnotations(path, tt.annotations), tt.want)
		})
	}
}

func assertValidateErr(t *testing.T, got field.ErrorList, want field.ErrorList) {
	t.Helper()
	field.ErrorMatcher{}.ByType().ByField().ByOrigin().Test(t, want, got)
}
