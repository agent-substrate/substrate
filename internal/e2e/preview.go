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

package e2e

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/agent-substrate/substrate/internal/preview"
)

// PreviewEnv lists the preview gates the install under test enabled, in the
// same syntax as ate-setup's ATE_PREVIEW: comma- or space-separated gate
// names, or "*" for all of them. Unset or empty means none.
const PreviewEnv = "E2E_PREVIEW"

// previewRequired reports whether PreviewEnv must be set. A lane that forgets
// it would otherwise skip every preview test and still pass, so CI must
// declare its preview mode, even if only as empty.
func previewRequired() bool {
	return os.Getenv("CI") == "true"
}

// previewGates returns the gates PreviewEnv declares.
func previewGates() ([]string, error) {
	value, ok := os.LookupEnv(PreviewEnv)
	if !ok && previewRequired() {
		return nil, fmt.Errorf("%s must be set in CI (empty for no preview gates)", PreviewEnv)
	}
	return strings.FieldsFunc(value, func(r rune) bool {
		return r == ',' || r == ' ' || r == '\t' || r == '\n'
	}), nil
}

// initPreview sets the process-wide preview gates from PreviewEnv.
func initPreview() error {
	gates, err := previewGates()
	if err != nil {
		return err
	}
	if err := preview.Init(gates...); err != nil {
		return fmt.Errorf("%s: %w", PreviewEnv, err)
	}
	return nil
}

// RequirePreview skips t unless the install under test enabled gate. Tests of
// a preview feature call it first, so a run without the gate skips them.
func RequirePreview(t testing.TB, gate preview.Gate) {
	t.Helper()
	if !preview.IsEnabled(gate) {
		t.Skipf("preview gate %q is not enabled (set %s)", gate, PreviewEnv)
	}
}
