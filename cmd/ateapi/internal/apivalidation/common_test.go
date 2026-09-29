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

package apivalidation

import (
	"fmt"
	"maps"
	"slices"
	"strings"
	"testing"

	"github.com/agent-substrate/substrate/internal/preview"
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
// value such as "*", for the duration of t. An empty value enables none.
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

func assertValidateErr(t *testing.T, got field.ErrorList, want field.ErrorList) {
	t.Helper()
	field.ErrorMatcher{}.ByType().ByField().ByOrigin().Test(t, want, got)
}
