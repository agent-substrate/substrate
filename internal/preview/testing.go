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

package preview

// The methods we need from testing.TB, so we do not have to import testing
// into non-test code.
type testingTB interface {
	Helper()
	Setenv(key, value string)
	Cleanup(func())
	Fatalf(format string, args ...any)
}

// SetForTest sets the process-wide preview gates for the duration of tb, as
// Init would with values, and restores the previous state when tb ends. It may
// be called whether or not Init has been. Tests that use it must not run in
// parallel.
func SetForTest(tb testingTB, values ...string) {
	tb.Helper()
	// Setenv fails the test if it, or an ancestor, is parallel.
	tb.Setenv("ATE_PREVIEW_SET_FOR_TEST", "1")
	m, err := newGates(values...)
	if err != nil {
		tb.Fatalf("preview.SetForTest: %v", err)
	}
	prev := stateMap
	stateMap = m
	tb.Cleanup(func() { stateMap = prev })
}
