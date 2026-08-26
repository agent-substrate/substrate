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

const deleteOptionsTypeFullName = "ateapi.DeleteOptions"

// DeleteRequestShape requires a Delete method's request to identify the
// resource with exactly one ObjectRef field, named after the resource's
// snake_case type name, plus an optional DeleteOptions field for
// preconditions.
var DeleteRequestShape = Rule{
	Name:        "delete-request-shape",
	Description: "A Delete method's request has exactly one ObjectRef field, named after the resource's snake_case type name, plus an optional DeleteOptions field - nothing else.",
	Check:       checkDeleteRequestShape,
}

func checkDeleteRequestShape(api *model.API) ([]Finding, error) {
	resources, err := model.Resources(api)
	if err != nil {
		return nil, err
	}
	messagesByName := api.MessagesByFullName()

	var findings []Finding
	for _, rg := range resources {
		for _, m := range rg.Methods {
			verb, ok := standardVerbFor(m.Name, rg.Message.Name)
			if !ok || verb != "Delete" {
				continue
			}
			subject := m.ServiceFullName + "." + m.Name

			req, ok := messagesByName[m.InputName]
			if !ok {
				findings = append(findings, Finding{Subject: subject, Message: "request type " + m.InputName + " not found"})
				continue
			}

			var objectRefFields, optionsFields int
			for _, f := range req.Fields {
				switch {
				case f.TypeKind == "message" && f.TypeFullName == objectRefTypeFullName:
					objectRefFields++
					if want := fieldNameForResource(rg.Message.Name); f.Name != want {
						findings = append(findings, Finding{
							Subject: subject,
							Message: fmt.Sprintf("resource field is named %q, want %q", f.Name, want),
						})
					}
				case f.TypeKind == "message" && f.TypeFullName == deleteOptionsTypeFullName:
					optionsFields++
				default:
					findings = append(findings, Finding{
						Subject: subject,
						Message: fmt.Sprintf("field %q is %s, want %s or %s - non-resource control fields belong in %s", f.Name, fieldTypeDescription(f), objectRefTypeFullName, deleteOptionsTypeFullName, deleteOptionsTypeFullName),
					})
				}
			}
			if objectRefFields != 1 {
				findings = append(findings, Finding{
					Subject: subject,
					Message: fmt.Sprintf("request has %d field(s) of type %s, want exactly 1 identifying the resource", objectRefFields, objectRefTypeFullName),
				})
			}
			if optionsFields > 1 {
				findings = append(findings, Finding{
					Subject: subject,
					Message: fmt.Sprintf("request has %d field(s) of type %s, want at most 1", optionsFields, deleteOptionsTypeFullName),
				})
			}
		}
	}
	return findings, nil
}

// DeleteOptionsShape requires the shared ateapi.DeleteOptions message to
// have only its two documented precondition fields: version (int64) and
// uid (string) - nothing else. Presence of either field is not required;
// this rule only bounds what's allowed.
var DeleteOptionsShape = Rule{
	Name:        "delete-options-shape",
	Description: "The shared ateapi.DeleteOptions message has only version (int64) and uid (string) fields - nothing else.",
	Check:       checkDeleteOptionsShape,
}

func checkDeleteOptionsShape(api *model.API) ([]Finding, error) {
	opts, ok := api.MessagesByFullName()[deleteOptionsTypeFullName]
	if !ok {
		return nil, nil // DeleteOptions isn't declared in this API - nothing to check.
	}

	var findings []Finding
	for _, f := range opts.Fields {
		switch f.Name {
		case "version":
			if f.TypeDisplay != "int64" {
				findings = append(findings, Finding{
					Subject: opts.FullName,
					Message: fmt.Sprintf("%q field is %s, want int64", f.Name, f.TypeDisplay),
				})
			}
		case "uid":
			if f.TypeDisplay != "string" {
				findings = append(findings, Finding{
					Subject: opts.FullName,
					Message: fmt.Sprintf("%q field is %s, want string", f.Name, f.TypeDisplay),
				})
			}
		default:
			findings = append(findings, Finding{
				Subject: opts.FullName,
				Message: fmt.Sprintf("field %q is not part of DeleteOptions - only version (int64) and uid (string) are allowed", f.Name),
			})
		}
	}
	return findings, nil
}
