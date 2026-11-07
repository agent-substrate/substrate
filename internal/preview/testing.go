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

package preview

// The methods we need from testing.TB, so we do not have to import testing
// into non-test code.
type testingTB interface {
	Helper()
	Fatalf(format string, args ...any)
	Log(args ...any)
}

// NewForTest returns the gates New would with values. It fails tb on an
// unknown gate.
func NewForTest(tb testingTB, values ...string) *Gates {
	tb.Helper()
	return NewForTestFake(tb, realKnownGates, values...)
}

// NewForTestFake is NewForTest with fakeKnownGates in place of the real ones.
func NewForTestFake(tb testingTB, fakeKnownGates []Gate, values ...string) *Gates {
	tb.Helper()
	g, err := newGates(fakeKnownGates, values...)
	if err != nil {
		tb.Fatalf("preview.NewForTestFake: %v", err)
	}
	tb.Log("preview gates for test:", g)
	return g
}
