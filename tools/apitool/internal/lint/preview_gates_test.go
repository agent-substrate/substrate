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

package lint_test

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"k8s.io/gengo/v2/codetags"

	"go.ate.dev/substrate/internal/preview"
	"go.ate.dev/substrate/tools/apitool/internal/lint"
	"go.ate.dev/substrate/tools/apitool/internal/model"
)

func TestPreviewGates(t *testing.T) {
	preview.InitForTestFake(t, []preview.Gate{"Sizes"})

	ifDisabledForbidden := func(gate string) codetags.Tag {
		return codetags.Tag{
			Name:      "k8s:ifDisabled",
			Args:      []codetags.Arg{{Value: gate, Type: codetags.ArgTypeString}},
			ValueType: codetags.ValueTypeTag,
			ValueTag:  &codetags.Tag{Name: "k8s:forbidden"},
		}
	}
	methodWithGate := func(name, gate string) model.Method {
		m := rpc(name, "Widget", "Widget")
		m.PreviewGate = gate
		return m
	}

	tests := []struct {
		name string
		api  *model.API
		want []lint.Finding
	}{
		{
			name: "ungated method",
			api:  methodAPI(methodWithGate("ResizeWidget", "")),
		},
		{
			name: "gated method",
			api:  methodAPI(methodWithGate("ResizeWidget", "Sizes")),
		},
		{
			name: "ungated field",
			api:  messagesAPI(msg("Widget", model.Field{Name: "size"})),
		},
		{
			name: "gated field with ifDisabled",
			api:  messagesAPI(msg("Widget", model.Field{Name: "size", PreviewGate: "Sizes", Tags: []codetags.Tag{ifDisabledForbidden("Sizes")}})),
		},
		{
			name: "method with unknown gate",
			api:  methodAPI(methodWithGate("ResizeWidget", "Colors")),
			want: []lint.Finding{
				{Subject: "test.Control.ResizeWidget", Message: `preview_gate "Colors" is not a gate in internal/preview`},
			},
		},
		{
			name: "field gated by ALL",
			api:  messagesAPI(msg("Widget", model.Field{Name: "size", PreviewGate: "ALL", Tags: []codetags.Tag{ifDisabledForbidden("ALL")}})),
			want: []lint.Finding{
				{Subject: "test.Widget.size", Message: `preview_gate "ALL" is not a gate in internal/preview`},
			},
		},
		{
			name: "preview_gate without ifDisabled",
			api:  messagesAPI(msg("Widget", model.Field{Name: "size", PreviewGate: "Sizes"})),
			want: []lint.Finding{
				{Subject: "test.Widget.size", Message: `has [(preview_gate) = "Sizes"], so it needs +k8s:ifDisabled(Sizes)=+k8s:forbidden`},
			},
		},
		{
			name: "ifDisabled without preview_gate",
			api:  messagesAPI(msg("Widget", model.Field{Name: "size", Tags: []codetags.Tag{ifDisabledForbidden("Sizes")}})),
			want: []lint.Finding{
				{Subject: "test.Widget.size", Message: `has +k8s:ifDisabled(Sizes)=+k8s:forbidden, so it needs [(preview_gate) = "Sizes"]`},
			},
		},
		{
			name: "ifDisabled with another preview_gate",
			api:  messagesAPI(msg("Widget", model.Field{Name: "size", PreviewGate: "Sizes", Tags: []codetags.Tag{ifDisabledForbidden("Colors")}})),
			want: []lint.Finding{
				{Subject: "test.Widget.size", Message: `has [(preview_gate) = "Sizes"], so it needs +k8s:ifDisabled(Sizes)=+k8s:forbidden`},
				{Subject: "test.Widget.size", Message: `has +k8s:ifDisabled(Colors)=+k8s:forbidden, so it needs [(preview_gate) = "Colors"]`},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			findings, err := lint.PreviewGates.Check(tt.api)
			if err != nil {
				t.Fatalf("Check() error = %v", err)
			}
			if diff := cmp.Diff(tt.want, findings); diff != "" {
				t.Errorf("Check() findings mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
