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

package lint

import (
	"google.golang.org/protobuf/reflect/protoreflect"

	"go.ate.dev/substrate/tools/apitool/internal/model"
)

const resourceMetadataTypeFullName protoreflect.FullName = "substrate.ateapi.v1.ResourceMetadata"

var ResourceMetadata = Rule{
	Name:        "resource-metadata",
	Description: `Every resource message has a "ResourceMetadata metadata = 1" field.`,
	Check:       forEachResource(checkResourceMetadata),
}

func checkResourceMetadata(r model.Resource) []Finding {
	field := findFieldByName(r.Message, "metadata")
	switch {
	case field == nil:
		return []Finding{findingf(r.Message.FullName, `has no "metadata" field`)}
	case !isMessageOf(*field, resourceMetadataTypeFullName):
		return []Finding{findingf(r.Message.FullName, `"metadata" field is %s, want %s`, typeOf(*field), resourceMetadataTypeFullName)}
	case field.Number != 1:
		return []Finding{findingf(r.Message.FullName, `"metadata" field is number %d, want 1`, field.Number)}
	}
	return nil
}

var ResourceStatusFieldShape = Rule{
	Name:        "resource-status-field-shape",
	Description: `A resource's "status" field, if present, is typed "{ResourceName}Status".`,
	Check:       forEachResource(checkResourceStatusField),
}

func checkResourceStatusField(r model.Resource) []Finding {
	field := findFieldByName(r.Message, "status")
	if field == nil {
		return nil
	}
	if want := r.Message.FullName + "Status"; !isMessageOf(*field, want) {
		return []Finding{findingf(r.Message.FullName, `"status" field is %s, want %s`, typeOf(*field), want)}
	}
	return nil
}

var ResourceFieldOrder = Rule{
	Name:        "resource-field-order",
	Description: `A resource message declares "metadata" first and "status", if present, last.`,
	Check:       forEachResource(checkResourceFieldOrder),
}

// checkResourceFieldOrder checks declaration order, not field numbers.
// resource-metadata already pins "metadata" to number 1, but a field's number
// and the position it is written at are independent: a resource can hold
// "metadata = 1" halfway down the message and still satisfy that rule. Reading
// a resource is easier when its identity is at the top and its observed state
// at the bottom, in every message.
//
// A missing "metadata" is resource-metadata's finding to report, not this
// one's, so its absence is passed over here rather than reported twice.
func checkResourceFieldOrder(r model.Resource) []Finding {
	fields := r.Message.Fields
	if len(fields) == 0 {
		return nil
	}
	var findings []Finding
	if i := indexOfFieldByName(fields, "metadata"); i > 0 {
		findings = append(findings, findingf(r.Message.FullName,
			`"metadata" is declared after %q, want it declared first`, fields[0].Name))
	}
	// A "status" that is also the first field is reported once, as being out of
	// place at the end, because a one-field message cannot be wrong both ways.
	if i := indexOfFieldByName(fields, "status"); i >= 0 && i != len(fields)-1 {
		findings = append(findings, findingf(r.Message.FullName,
			`"status" is declared before %q, want it declared last`, fields[len(fields)-1].Name))
	}
	return findings
}

func indexOfFieldByName(fields []model.Field, name protoreflect.Name) int {
	for i := range fields {
		if fields[i].Name == name {
			return i
		}
	}
	return -1
}

func findFieldByName(m model.Message, name protoreflect.Name) *model.Field {
	for i := range m.Fields {
		if m.Fields[i].Name == name {
			return &m.Fields[i]
		}
	}
	return nil
}
