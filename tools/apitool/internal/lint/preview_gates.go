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

package lint

import (
	"slices"

	"google.golang.org/protobuf/reflect/protoreflect"
	"k8s.io/gengo/v2/codetags"

	"go.ate.dev/substrate/internal/preview"
	"go.ate.dev/substrate/tools/apitool/internal/model"
)

var PreviewGates = Rule{
	Name:        "preview-gates",
	Description: "Every preview_gate names a gate in internal/preview, and a field has preview_gate set to <Gate> exactly when it has +k8s:ifDisabled(<Gate>)=+k8s:forbidden.",
	Check: allOf(
		forEachMethod(checkMethodPreviewGate),
		forEachField(checkFieldPreviewGate),
	),
}

func checkMethodPreviewGate(rpc model.Method) []Finding {
	if rpc.PreviewGate == "" {
		return nil
	}
	return checkGateName(subjectOf(rpc), rpc.PreviewGate)
}

func checkFieldPreviewGate(m model.Message, f model.Field) []Finding {
	subject := m.FullName.Append(f.Name)
	tagGates := gatesFromTags(f.Tags)

	var findings []Finding
	if f.PreviewGate != "" {
		findings = append(findings, checkGateName(subject, f.PreviewGate)...)
		if !slices.Contains(tagGates, f.PreviewGate) {
			findings = append(findings, findingf(subject, "has [(preview_gate) = %q], so it needs +k8s:ifDisabled(%s)=+k8s:forbidden", f.PreviewGate, f.PreviewGate))
		}
	}
	for _, gate := range tagGates {
		if gate != f.PreviewGate {
			findings = append(findings, findingf(subject, "has +k8s:ifDisabled(%s)=+k8s:forbidden, so it needs [(preview_gate) = %q]", gate, gate))
		}
	}
	return findings
}

// gatesFromTags returns the gate name from each
// +k8s:ifDisabled(<Gate>)=+k8s:forbidden tag in tags.
func gatesFromTags(tags []codetags.Tag) []string {
	var gates []string
	for _, tag := range tags {
		if tag.Name != "k8s:ifDisabled" || tag.ValueTag == nil || tag.ValueTag.Name != "k8s:forbidden" {
			continue
		}
		if gate, ok := tag.PositionalArg(); ok {
			gates = append(gates, gate.Value)
		}
	}
	return gates
}

func checkGateName(subject protoreflect.FullName, gate string) []Finding {
	if gate == preview.All || preview.Verify(gate) != nil {
		return []Finding{findingf(subject, "preview_gate %q is not a gate in internal/preview", gate)}
	}
	return nil
}
