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

	"github.com/agent-substrate/substrate/tools/apitool/internal/lint"
	"github.com/agent-substrate/substrate/tools/apitool/internal/model"
)

func TestDocumented(t *testing.T) {
	tests := []struct {
		name           string
		messageComment string
		enumComment    string
		fieldComment   string
		methodComment  string
		want           []lint.Finding
	}{
		{
			name:           "all documented",
			messageComment: "a request",
			enumComment:    "a state",
			fieldComment:   "does a thing",
			methodComment:  "does another thing",
		},
		{
			name:           "message undocumented",
			messageComment: "",
			enumComment:    "a state",
			fieldComment:   "does a thing",
			methodComment:  "does another thing",
			want: []lint.Finding{
				{Subject: "test.DoThingRequest", Message: "message has no doc comment"},
			},
		},
		{
			name:           "enum undocumented",
			messageComment: "a request",
			enumComment:    "",
			fieldComment:   "does a thing",
			methodComment:  "does another thing",
			want: []lint.Finding{
				{Subject: "test.Widget.State", Message: "enum has no doc comment"},
			},
		},
		{
			name:           "field undocumented",
			messageComment: "a request",
			enumComment:    "a state",
			fieldComment:   "",
			methodComment:  "does another thing",
			want: []lint.Finding{
				{Subject: "test.DoThingRequest.widget", Message: "field has no doc comment"},
			},
		},
		{
			name:           "method undocumented",
			messageComment: "a request",
			enumComment:    "a state",
			fieldComment:   "does a thing",
			methodComment:  "",
			want: []lint.Finding{
				{Subject: "test.Control.DoThing", Message: "method has no doc comment"},
			},
		},
		{
			name:           "nothing documented",
			messageComment: "",
			enumComment:    "",
			fieldComment:   "",
			methodComment:  "",
			want: []lint.Finding{
				{Subject: "test.DoThingRequest", Message: "message has no doc comment"},
				{Subject: "test.DoThingRequest.widget", Message: "field has no doc comment"},
				{Subject: "test.Widget.State", Message: "enum has no doc comment"},
				{Subject: "test.Control.DoThing", Message: "method has no doc comment"},
			},
		},
		{
			name:           "whitespace-only comment counts as undocumented",
			messageComment: "\t",
			enumComment:    "\t",
			fieldComment:   "   ",
			methodComment:  "\n",
			want: []lint.Finding{
				{Subject: "test.DoThingRequest", Message: "message has no doc comment"},
				{Subject: "test.DoThingRequest.widget", Message: "field has no doc comment"},
				{Subject: "test.Widget.State", Message: "enum has no doc comment"},
				{Subject: "test.Control.DoThing", Message: "method has no doc comment"},
			},
		},
		{
			name:           "empty comment lines",
			messageComment: "a request",
			enumComment:    "a state",
			fieldComment:   "\n\n",
			methodComment:  "does another thing",
			want: []lint.Finding{
				{Subject: "test.DoThingRequest.widget", Message: "field has no doc comment"},
			},
		},
		{
			name:           "tags only",
			messageComment: "+k8s:required",
			enumComment:    "+k8s:optional",
			fieldComment:   "+k8s:required\n+k8s:format=k8s-short-name",
			methodComment:  "does another thing",
			want: []lint.Finding{
				{Subject: "test.DoThingRequest", Message: "message has no doc comment"},
				{Subject: "test.DoThingRequest.widget", Message: "field has no doc comment"},
				{Subject: "test.Widget.State", Message: "enum has no doc comment"},
			},
		},
		{
			name:           "prose and tags",
			messageComment: "a request\n\n+k8s:required",
			enumComment:    "a state",
			fieldComment:   "does a thing\n\n+k8s:immutable",
			methodComment:  "does another thing",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			api := &model.API{
				Services: []model.Service{{
					Name: "Control",
					Methods: []model.Method{
						{Name: "DoThing", ServiceFullName: "test.Control", ServiceName: "Control", Comment: tt.methodComment, InputName: "test.DoThingRequest", OutputName: "test.DoThingResponse"},
					},
				}},
				Messages: []model.Message{
					{FullName: "test.DoThingRequest", Name: "DoThingRequest", Comment: tt.messageComment, Fields: []model.Field{
						{Name: "widget", Comment: tt.fieldComment},
					}},
				},
				Enums: []model.Enum{
					{FullName: "test.Widget.State", Name: "Widget.State", ParentFullName: "test.Widget", Comment: tt.enumComment},
				},
			}
			findings, err := lint.Documented.Check(api)
			if err != nil {
				t.Fatalf("Check() error = %v", err)
			}
			if diff := cmp.Diff(tt.want, findings); diff != "" {
				t.Errorf("Check() findings mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
