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
	"testing"

	"github.com/agent-substrate/substrate/internal/preview"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

const (
	testAtespace = "test-atespace"
)

const (
	// testStorageLocation is the snapshot_config.storage_location the tests
	// build snapshot URIs under.
	testStorageLocation = "gs://bucket/root"

	// someActorUID stands in for the UID the store assigns an Actor, for tests
	// that need a well-formed snapshot URI but never exercise who owns it. Those
	// seed their Actor in a single call, before a real UID exists.
	someActorUID = "6b1f9d0c-4a2e-4d38-9c77-5e0a1b2c3d4e"
)

// setPreviewForTest sets all preview gates on or off for the duration of t.
func setPreviewForTest(t *testing.T, enabled bool) {
	t.Helper()
	if enabled {
		preview.SetForTest(t, "*")
	} else {
		preview.SetForTest(t)
	}
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
