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

package lint_test

import (
	"testing"

	"github.com/google/go-cmp/cmp"

	"go.ate.dev/substrate/tools/apitool/internal/lint"
	"go.ate.dev/substrate/tools/apitool/internal/model"
)

func TestDocFormat(t *testing.T) {
	tests := []struct {
		name    string
		comment string
		want    []lint.Finding
	}{
		{
			name:    "summary, details, internal note, and directives",
			comment: "Summary.\n\nDetails.\n\n(-- Internal\nnote. --)\n\n+k8s:optional\n+k8s:maxItems=2 # a guardrail",
		},
		{
			name:    "ate and k8s directives",
			comment: "Summary.\n\n+ate:preview=ExternalVolumes\n+k8s:optional",
		},
		{
			name:    "two internal notes",
			comment: "A (-- one --) and B (-- two --).",
		},
		{
			name:    "unclosed internal note",
			comment: "Summary. (-- note",
			want:    []lint.Finding{{Subject: "test.Widget.name", Message: `doc comment has an internal note that isn't opened with "(--" and closed with "--)"`}},
		},
		{
			name:    "close without open",
			comment: "Summary. note --)",
			want:    []lint.Finding{{Subject: "test.Widget.name", Message: `doc comment has an internal note that isn't opened with "(--" and closed with "--)"`}},
		},
		{
			name:    "nested internal notes",
			comment: "(-- a (-- b --) --)",
			want:    []lint.Finding{{Subject: "test.Widget.name", Message: `doc comment has an internal note that isn't opened with "(--" and closed with "--)"`}},
		},
		{
			name:    "directive note continued on the next line",
			comment: "Summary.\n\n+k8s:update=NoUnset # set-once; immutable would reject\nthe create",
			want:    []lint.Finding{{Subject: "test.Widget.name", Message: `doc comment has text after its +k8s: directives: "the create"`}},
		},
		{
			name:    "placeholder in a code span",
			comment: "Formatted as `<Reason>: <detail>`.",
		},
		{
			name:    "placeholder outside a code span",
			comment: "Formatted as <Reason>: <detail>.",
			want:    []lint.Finding{{Subject: "test.Widget.name", Message: `doc comment has raw HTML, which isn't rendered; put a placeholder in a code span: "<Reason>"`}},
		},
		{
			name:    "html block",
			comment: "Summary.\n\n<div>\nnot rendered\n</div>",
			want:    []lint.Finding{{Subject: "test.Widget.name", Message: `doc comment has raw HTML, which isn't rendered; put a placeholder in a code span: "<div>\nnot rendered\n</div>"`}},
		},
		{
			name:    "prose after directives",
			comment: "Summary.\n\n+k8s:optional\n\nMore prose.",
			want:    []lint.Finding{{Subject: "test.Widget.name", Message: `doc comment has text after its +k8s: directives: "More prose."`}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			api := &model.API{Messages: []model.Message{
				{FullName: "test.Widget", Name: "Widget", Comment: "A widget.", Fields: []model.Field{{Name: "name", Comment: tt.comment}}},
			}}
			findings, err := lint.DocFormat.Check(api)
			if err != nil {
				t.Fatalf("Check() error = %v", err)
			}
			if diff := cmp.Diff(tt.want, findings); diff != "" {
				t.Errorf("Check() findings mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestDocFormat_EveryElement(t *testing.T) {
	const bad = "Summary. (-- unclosed"
	api := &model.API{
		Services: []model.Service{{Name: "Control", Methods: []model.Method{
			{Name: "DoThing", ServiceFullName: "test.Control", ServiceName: "Control", Comment: bad},
		}}},
		Messages: []model.Message{
			{FullName: "test.Widget", Name: "Widget", Comment: bad, Fields: []model.Field{{Name: "name", Comment: bad}}},
		},
		Enums: []model.Enum{
			{FullName: "test.State", Name: "State", Comment: bad, Values: []model.EnumValue{{Name: "STATE_UNSPECIFIED", Comment: bad}}},
		},
	}
	findings, err := lint.DocFormat.Check(api)
	if err != nil {
		t.Fatalf("Check() error = %v", err)
	}
	var got []string
	for _, f := range findings {
		got = append(got, f.Subject)
	}
	want := []string{"test.Widget", "test.Widget.name", "test.State", "test.State.STATE_UNSPECIFIED", "test.Control.DoThing"}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("finding subjects mismatch (-want +got):\n%s", diff)
	}
}
