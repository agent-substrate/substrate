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

package controlapi

import (
	"fmt"
	"os"
	"testing"

	"go.ate.dev/substrate/cmd/ateapi/internal/store/storetest"
	"go.ate.dev/substrate/internal/preview"
)

func TestMain(m *testing.M) {
	// Most tests store snapshots under their template's storageLocation, which
	// is honored only with this gate. Tests that need it off call
	// preview.InitForTest.
	if err := preview.Init(string(preview.GatePerTemplateSnapshotLocation)); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	storetest.RunTests(m)
}
