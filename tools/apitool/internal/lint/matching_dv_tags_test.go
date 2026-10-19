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

func TestMatchingDVTags(t *testing.T) {
	// upstream is the field the copies below match.
	upstream := msg("Template",
		model.Field{Name: "volumes", Comment: "+k8s:optional\n+k8s:maxItems=32\n+k8s:eachVal=+k8s:maxLength=253\n+k8s:update=NoModify\n+k8s:update=NoUnset\n+k8s:ifEnabled(Foo)=+k8s:minItems=1"},
		model.Field{Name: "name", Comment: "+k8s:maxLength=253"},
		model.Field{Name: "labels", Comment: "+k8s:eachKey=+k8s:maxLength=64\n+k8s:eachVal=+k8s:maxLength=4096"},
	)

	tests := []struct {
		name    string
		api     *model.API
		want    []lint.Finding
		wantErr bool
	}{
		{
			name: "tag without a matches note",
			api:  messagesAPI(upstream, msg("Spec", model.Field{Name: "volumes", Comment: "+k8s:maxItems=16 # guardrail"})),
		},
		{
			name: "same value",
			api:  messagesAPI(upstream, msg("Spec", model.Field{Name: "volumes", Comment: "+k8s:maxItems=32 # matches test.Template.volumes"})),
		},
		{
			name: "same value, with free text after the reference",
			api:  messagesAPI(upstream, msg("Spec", model.Field{Name: "volumes", Comment: "+k8s:maxItems=32 # matches test.Template.volumes, the request this is forwarded to"})),
		},
		{
			name: "same value on a tag applied to each item",
			api:  messagesAPI(upstream, msg("Spec", model.Field{Name: "volumes", Comment: "+k8s:eachVal=+k8s:maxLength=253 # matches test.Template.volumes"})),
		},
		{
			name: "same value as one of a repeated tag",
			api:  messagesAPI(upstream, msg("Spec", model.Field{Name: "volumes", Comment: "+k8s:update=NoUnset # matches test.Template.volumes"})),
		},
		{
			name: "same value in another int base",
			api:  messagesAPI(upstream, msg("Spec", model.Field{Name: "volumes", Comment: "+k8s:maxItems=0x20 # matches test.Template.volumes"})),
		},
		{
			name: "different value",
			api:  messagesAPI(upstream, msg("Spec", model.Field{Name: "volumes", Comment: "+k8s:maxItems=16 # matches test.Template.volumes"})),
			want: []lint.Finding{
				{Subject: "test.Spec.volumes", Message: "+k8s:maxItems=16 matches test.Template.volumes, which has +k8s:maxItems=32"},
			},
		},
		{
			name: "same value, with a period ending the reference",
			api:  messagesAPI(upstream, msg("Spec", model.Field{Name: "volumes", Comment: "+k8s:maxItems=32 # matches test.Template.volumes."})),
		},
		{
			name: "different value, with free text after a period",
			api:  messagesAPI(upstream, msg("Spec", model.Field{Name: "volumes", Comment: "+k8s:maxItems=16 # matches test.Template.volumes. The request this is forwarded to"})),
			want: []lint.Finding{
				{Subject: "test.Spec.volumes", Message: "+k8s:maxItems=16 matches test.Template.volumes, which has +k8s:maxItems=32"},
			},
		},
		{
			name: "different value on a tag applied to each item",
			api:  messagesAPI(upstream, msg("Spec", model.Field{Name: "volumes", Comment: "+k8s:eachVal=+k8s:maxLength=63 # matches test.Template.volumes"})),
			want: []lint.Finding{
				{Subject: "test.Spec.volumes", Message: "+k8s:eachVal=+k8s:maxLength=63 matches test.Template.volumes, which has +k8s:eachVal=+k8s:maxLength=253"},
			},
		},
		{
			name: "referenced field lacks the tag",
			api:  messagesAPI(upstream, msg("Spec", model.Field{Name: "volumes", Comment: "+k8s:minItems=1 # matches test.Template.volumes"})),
			want: []lint.Finding{
				{Subject: "test.Spec.volumes", Message: "+k8s:minItems matches test.Template.volumes, which has no +k8s:minItems tag"},
			},
		},
		{
			name: "unknown field",
			api:  messagesAPI(upstream, msg("Spec", model.Field{Name: "volumes", Comment: "+k8s:maxItems=32 # matches test.Template.mounts"})),
			want: []lint.Finding{
				{Subject: "test.Spec.volumes", Message: `+k8s:maxItems matches "test.Template.mounts", which is not a field`},
			},
		},
		{
			name: "unknown message",
			api:  messagesAPI(upstream, msg("Spec", model.Field{Name: "volumes", Comment: "+k8s:maxItems=32 # matches test.Pod.volumes"})),
			want: []lint.Finding{
				{Subject: "test.Spec.volumes", Message: `+k8s:maxItems matches "test.Pod.volumes", which is not a field`},
			},
		},
		{
			name: "prose instead of a reference",
			api:  messagesAPI(upstream, msg("Spec", model.Field{Name: "volumes", Comment: "+k8s:maxItems=32 # matches the template's volumes bound"})),
			want: []lint.Finding{
				{Subject: "test.Spec.volumes", Message: `+k8s:maxItems matches "the", which is not a field`},
			},
		},
		{
			name: "field matching itself",
			api:  messagesAPI(upstream, msg("Spec", model.Field{Name: "volumes", Comment: "+k8s:maxItems=32 # matches test.Spec.volumes"})),
			want: []lint.Finding{
				{Subject: "test.Spec.volumes", Message: "+k8s:maxItems matches its own field"},
			},
		},
		{
			name: "bare matches note",
			api:  messagesAPI(upstream, msg("Spec", model.Field{Name: "volumes", Comment: "+k8s:maxItems=32 # matches"})),
			want: []lint.Finding{
				{Subject: "test.Spec.volumes", Message: `+k8s:maxItems matches "", which is not a field`},
			},
		},
		{
			name: "note that only starts with the word",
			api:  messagesAPI(upstream, msg("Spec", model.Field{Name: "volumes", Comment: "+k8s:maxItems=16 # matchesish test.Template.volumes"})),
		},
		{
			name: "capitalized matches note",
			api:  messagesAPI(upstream, msg("Spec", model.Field{Name: "volumes", Comment: "+k8s:maxItems=16 # Matches test.Template.volumes"})),
			want: []lint.Finding{
				{Subject: "test.Spec.volumes", Message: `+k8s:maxItems note "Matches test.Template.volumes" must start with "matches "`},
			},
		},
		{
			name: "matches note with a colon",
			api:  messagesAPI(upstream, msg("Spec", model.Field{Name: "volumes", Comment: "+k8s:maxItems=16 # matches: test.Template.volumes"})),
			want: []lint.Finding{
				{Subject: "test.Spec.volumes", Message: `+k8s:maxItems note "matches: test.Template.volumes" must start with "matches "`},
			},
		},
		{
			name: "same value, with free text after a comma and no space",
			api:  messagesAPI(upstream, msg("Spec", model.Field{Name: "volumes", Comment: "+k8s:maxItems=32 # matches test.Template.volumes,the request this is forwarded to"})),
		},
		{
			name: "different value, with free text after a comma and no space",
			api:  messagesAPI(upstream, msg("Spec", model.Field{Name: "volumes", Comment: "+k8s:maxItems=16 # matches test.Template.volumes,the request this is forwarded to"})),
			want: []lint.Finding{
				{Subject: "test.Spec.volumes", Message: "+k8s:maxItems=16 matches test.Template.volumes, which has +k8s:maxItems=32"},
			},
		},
		{
			name: "same tag spelled differently",
			api:  messagesAPI(upstream, msg("Spec", model.Field{Name: "volumes", Comment: `+k8s:ifEnabled("Foo")=+k8s:minItems=0x1 # matches test.Template.volumes`})),
		},
		{
			name: "different value from every repeated tag",
			api:  messagesAPI(upstream, msg("Spec", model.Field{Name: "volumes", Comment: "+k8s:update=NoSet # matches test.Template.volumes"})),
			want: []lint.Finding{
				{Subject: "test.Spec.volumes", Message: "+k8s:update=NoSet matches test.Template.volumes, which has +k8s:update=NoModify,NoUnset"},
			},
		},
		{
			name: "scalar bound against a per-item bound",
			api:  messagesAPI(upstream, msg("Spec", model.Field{Name: "volume", Comment: "+k8s:maxLength=253 # matches test.Template.volumes"})),
			want: []lint.Finding{
				{Subject: "test.Spec.volume", Message: "+k8s:maxLength matches test.Template.volumes, which has no +k8s:maxLength tag"},
			},
		},
		{
			name: "per-item bound against a scalar bound",
			api:  messagesAPI(upstream, msg("Spec", model.Field{Name: "names", Comment: "+k8s:eachVal=+k8s:maxLength=253 # matches test.Template.name"})),
			want: []lint.Finding{
				{Subject: "test.Spec.names", Message: "+k8s:eachVal=+k8s:maxLength matches test.Template.name, which has no +k8s:eachVal=+k8s:maxLength tag"},
			},
		},
		{
			name: "map key bound against a map key bound",
			api:  messagesAPI(upstream, msg("Spec", model.Field{Name: "labels", Comment: "+k8s:eachKey=+k8s:maxLength=64 # matches test.Template.labels"})),
		},
		{
			name: "map key bound with the value bound's value",
			api:  messagesAPI(upstream, msg("Spec", model.Field{Name: "labels", Comment: "+k8s:eachKey=+k8s:maxLength=4096 # matches test.Template.labels"})),
			want: []lint.Finding{
				{Subject: "test.Spec.labels", Message: "+k8s:eachKey=+k8s:maxLength=4096 matches test.Template.labels, which has +k8s:eachKey=+k8s:maxLength=64"},
			},
		},
		{
			name: "map value bound with the key bound's value",
			api:  messagesAPI(upstream, msg("Spec", model.Field{Name: "labels", Comment: "+k8s:eachVal=+k8s:maxLength=64 # matches test.Template.labels"})),
			want: []lint.Finding{
				{Subject: "test.Spec.labels", Message: "+k8s:eachVal=+k8s:maxLength=64 matches test.Template.labels, which has +k8s:eachVal=+k8s:maxLength=4096"},
			},
		},
		{
			name:    "malformed tag on the copy",
			api:     messagesAPI(upstream, msg("Spec", model.Field{Name: "volumes", Comment: `+k8s:maxLength="unterminated # matches test.Template.volumes`})),
			wantErr: true,
		},
		{
			name: "malformed tag on the referenced field",
			api: messagesAPI(
				msg("Template", model.Field{Name: "volumes", Comment: `+k8s:validateError="unterminated`}),
				msg("Spec", model.Field{Name: "volumes", Comment: "+k8s:maxItems=32 # matches test.Template.volumes"}),
			),
			wantErr: true,
		},
		{
			name: "chain with the same value at every hop",
			api: messagesAPI(
				upstream,
				msg("Spec", model.Field{Name: "volumes", Comment: "+k8s:maxItems=32 # matches test.Template.volumes"}),
				msg("Request", model.Field{Name: "volumes", Comment: "+k8s:maxItems=32 # matches test.Spec.volumes"}),
			),
		},
		{
			name: "chain reports only the hop that differs",
			api: messagesAPI(
				upstream,
				msg("Spec", model.Field{Name: "volumes", Comment: "+k8s:maxItems=16 # matches test.Template.volumes"}),
				msg("Request", model.Field{Name: "volumes", Comment: "+k8s:maxItems=16 # matches test.Spec.volumes"}),
			),
			want: []lint.Finding{
				{Subject: "test.Spec.volumes", Message: "+k8s:maxItems=16 matches test.Template.volumes, which has +k8s:maxItems=32"},
			},
		},
		{
			name: "cycle with the same value",
			api: messagesAPI(
				msg("Spec", model.Field{Name: "volumes", Comment: "+k8s:maxItems=32 # matches test.Request.volumes"}),
				msg("Request", model.Field{Name: "volumes", Comment: "+k8s:maxItems=32 # matches test.Spec.volumes"}),
			),
		},
		{
			name: "cycle with different values reports both sides",
			api: messagesAPI(
				msg("Spec", model.Field{Name: "volumes", Comment: "+k8s:maxItems=32 # matches test.Request.volumes"}),
				msg("Request", model.Field{Name: "volumes", Comment: "+k8s:maxItems=16 # matches test.Spec.volumes"}),
			),
			want: []lint.Finding{
				{Subject: "test.Spec.volumes", Message: "+k8s:maxItems=32 matches test.Request.volumes, which has +k8s:maxItems=16"},
				{Subject: "test.Request.volumes", Message: "+k8s:maxItems=16 matches test.Spec.volumes, which has +k8s:maxItems=32"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			findings, err := lint.MatchingDVTags.Check(tt.api)
			if (err != nil) != tt.wantErr {
				t.Fatalf("Check() error = %v, wantErr %v", err, tt.wantErr)
			}
			if diff := cmp.Diff(tt.want, findings); diff != "" {
				t.Errorf("Check() findings mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
