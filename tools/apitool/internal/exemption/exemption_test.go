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

package exemption_test

import (
	"path/filepath"
	"reflect"
	"testing"

	"github.com/agent-substrate/substrate/tools/apitool/internal/exemption"
)

func TestLoadMissingFileIsEmpty(t *testing.T) {
	got, err := exemption.Load(filepath.Join(t.TempDir(), "does-not-exist.json"))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("Load() = %v, want empty", got)
	}
}

func TestSaveThenLoadRoundTrips(t *testing.T) {
	path := filepath.Join(t.TempDir(), "exemptions.json")
	in := []exemption.Exemption{
		{Rule: "documented", Subject: "ateapi.Foo", Message: "message has no doc comment"},
		{Rule: "delete-request-shape", Subject: "ateapi.Control.DeleteActor", Message: `field "any_state" is not a message`},
	}

	if err := exemption.Save(path, in); err != nil {
		t.Fatalf("Save: %v", err)
	}
	got, err := exemption.Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	want := []exemption.Exemption{in[1], in[0]}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("round trip = %+v, want %+v", got, want)
	}
}

func TestSaveSortsDeterministically(t *testing.T) {
	path := filepath.Join(t.TempDir(), "exemptions.json")
	if err := exemption.Save(path, []exemption.Exemption{
		{Rule: "documented", Subject: "ateapi.B", Message: "m"},
		{Rule: "documented", Subject: "ateapi.A", Message: "m"},
	}); err != nil {
		t.Fatalf("Save: %v", err)
	}

	got, err := exemption.Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	want := []exemption.Exemption{
		{Rule: "documented", Subject: "ateapi.A", Message: "m"},
		{Rule: "documented", Subject: "ateapi.B", Message: "m"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Save() order = %+v, want %+v", got, want)
	}
}

func TestSetConsume(t *testing.T) {
	set := exemption.NewSet([]exemption.Exemption{
		{Rule: "documented", Subject: "ateapi.Foo", Message: "message has no doc comment"},
	})

	if !set.Consume("documented", "ateapi.Foo", "message has no doc comment") {
		t.Error("Consume() = false for an exempted finding, want true")
	}
	if set.Consume("documented", "ateapi.Foo", "message has no doc comment") {
		t.Error("Consume() = true for an already-claimed exemption, want false")
	}
	if set.Consume("documented", "ateapi.Bar", "message has no doc comment") {
		t.Error("Consume() = true for a finding with no matching exemption, want false")
	}
}

func TestDiff(t *testing.T) {
	current := []exemption.Exemption{
		{Rule: "documented", Subject: "ateapi.Foo", Message: "message has no doc comment"},
		{Rule: "documented", Subject: "ateapi.Bar", Message: "message has no doc comment"},
	}
	exemptions := []exemption.Exemption{
		{Rule: "documented", Subject: "ateapi.Foo", Message: "message has no doc comment"},
		{Rule: "documented", Subject: "ateapi.Baz", Message: "message has no doc comment"},
	}

	missing, stale := exemption.Diff(current, exemptions)

	wantMissing := []exemption.Exemption{
		{Rule: "documented", Subject: "ateapi.Bar", Message: "message has no doc comment"},
	}
	if !reflect.DeepEqual(missing, wantMissing) {
		t.Errorf("Diff() missing = %+v, want %+v", missing, wantMissing)
	}
	wantStale := []exemption.Exemption{
		{Rule: "documented", Subject: "ateapi.Baz", Message: "message has no doc comment"},
	}
	if !reflect.DeepEqual(stale, wantStale) {
		t.Errorf("Diff() stale = %+v, want %+v", stale, wantStale)
	}
}

func TestSetUnusedReportsUnclaimedExemptions(t *testing.T) {
	set := exemption.NewSet([]exemption.Exemption{
		{Rule: "documented", Subject: "ateapi.Foo", Message: "message has no doc comment"},
		{Rule: "documented", Subject: "ateapi.Bar", Message: "message has no doc comment"},
	})
	set.Consume("documented", "ateapi.Foo", "message has no doc comment")

	got := set.Unused()
	want := []exemption.Exemption{
		{Rule: "documented", Subject: "ateapi.Bar", Message: "message has no doc comment"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Unused() = %+v, want %+v", got, want)
	}
}
