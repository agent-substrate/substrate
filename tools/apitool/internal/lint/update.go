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
	"fmt"

	"github.com/agent-substrate/substrate/tools/apitool/internal/model"
)

const fieldMaskTypeFullName = "google.protobuf.FieldMask"

// UpdateRequestShape requires an Update method's request to embed the
// resource with exactly one field, named after the resource's snake_case
// type name - nothing else, aside from a google.protobuf.FieldMask field,
// which this rule does not police since field masks are being phased out.
var UpdateRequestShape = Rule{
	Name:        "update-request-shape",
	Description: "An Update method's request has exactly one field, of the resource's own type and named after its snake_case type name - nothing else (field masks, where still present, are not policed by this rule).",
	Check:       checkUpdateRequestShape,
}

func checkUpdateRequestShape(api *model.API) ([]Finding, error) {
	resources, err := model.Resources(api)
	if err != nil {
		return nil, err
	}
	messagesByName := api.MessagesByFullName()

	var findings []Finding
	for _, rg := range resources {
		for _, m := range rg.Methods {
			verb, ok := standardVerbFor(m.Name, rg.Message.Name)
			if !ok || verb != "Update" {
				continue
			}
			subject := m.ServiceFullName + "." + m.Name

			req, ok := messagesByName[m.InputName]
			if !ok {
				findings = append(findings, Finding{Subject: subject, Message: "request type " + m.InputName + " not found"})
				continue
			}

			var resourceFields int
			for _, f := range req.Fields {
				switch {
				case f.TypeKind == "message" && f.TypeFullName == rg.Message.FullName:
					resourceFields++
					if want := fieldNameForResource(rg.Message.Name); f.Name != want {
						findings = append(findings, Finding{
							Subject: subject,
							Message: fmt.Sprintf("resource field is named %q, want %q", f.Name, want),
						})
					}
				case f.TypeKind == "message" && f.TypeFullName == fieldMaskTypeFullName:
					// Field masks are being phased out; not policed by this rule.
				default:
					findings = append(findings, Finding{
						Subject: subject,
						Message: fmt.Sprintf("field %q is %s, want %s - non-resource control fields belong elsewhere", f.Name, fieldTypeDescription(f), rg.Message.FullName),
					})
				}
			}
			if resourceFields != 1 {
				findings = append(findings, Finding{
					Subject: subject,
					Message: fmt.Sprintf("request has %d field(s) of type %s, want exactly 1 embedding the resource", resourceFields, rg.Message.FullName),
				})
			}
		}
	}
	return findings, nil
}
