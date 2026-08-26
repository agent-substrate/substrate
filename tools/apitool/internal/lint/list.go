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
	"strings"

	"github.com/agent-substrate/substrate/tools/apitool/internal/model"
)

// ListMethodShape requires a "List*" method's request to have exactly
// page_size/page_token fields plus an optional atespace field, and its
// response to have exactly a repeated message field plus a next_page_token
// field - nothing else.
var ListMethodShape = Rule{
	Name: "list-method-shape",
	Description: `A "List*" method's request has page_size/page_token fields (plus an optional atespace field), and its ` +
		"response has a repeated message field, named after the method's own plural, plus a next_page_token field - nothing else.",
	Check: checkListMethodShape,
}

// ListResponseNameMatchesMethod requires a "List*" method's response to be
// named "{MethodName}Response".
var ListResponseNameMatchesMethod = Rule{
	Name:        "list-response-name-matches-method",
	Description: `Every "List*" method's response message is named "{MethodName}Response".`,
	Check:       checkListResponseNameMatchesMethod,
}

func checkListMethodShape(api *model.API) ([]Finding, error) {
	messagesByName := api.MessagesByFullName()

	var findings []Finding
	for _, svc := range api.Services {
		for _, method := range svc.Methods {
			if !strings.HasPrefix(method.Name, "List") {
				continue
			}
			subject := method.ServiceFullName + "." + method.Name
			findings = append(findings, checkListRequest(messagesByName, subject, method.InputName)...)
			findings = append(findings, checkListResponse(messagesByName, subject, method.Name, method.OutputName)...)
		}
	}
	return findings, nil
}

// listRequestAllowedFields are the only fields a List request may have:
// page_size/page_token (required) plus an optional atespace scoping field.
// Sorting and filtering per AIP-132 are not supported, so nothing else
// belongs here.
var listRequestAllowedFields = map[string]bool{
	"atespace":   true,
	"page_size":  true,
	"page_token": true,
}

func checkListRequest(messagesByName map[string]model.Message, subject, inputName string) []Finding {
	req, ok := messagesByName[inputName]
	if !ok {
		return []Finding{{Subject: subject, Message: "request type " + inputName + " not found"}}
	}

	var findings []Finding
	if findFieldByName(req, "page_size") == nil {
		findings = append(findings, Finding{Subject: subject, Message: "request has no page_size field"})
	}
	if findFieldByName(req, "page_token") == nil {
		findings = append(findings, Finding{Subject: subject, Message: "request has no page_token field"})
	}
	for _, f := range req.Fields {
		if !listRequestAllowedFields[f.Name] {
			findings = append(findings, Finding{
				Subject: subject,
				Message: fmt.Sprintf("field %q is not part of the standard List request shape - only atespace, page_size, and page_token are allowed (sorting/filtering per AIP-132 is not supported)", f.Name),
			})
		}
	}
	return findings
}

func checkListResponse(messagesByName map[string]model.Message, subject, methodName, outputName string) []Finding {
	resp, ok := messagesByName[outputName]
	if !ok {
		return []Finding{{Subject: subject, Message: "response type " + outputName + " not found"}}
	}

	var findings []Finding
	repeatedIdx := -1
	for i := range resp.Fields {
		if resp.Fields[i].Repeated && resp.Fields[i].TypeKind == "message" {
			repeatedIdx = i
			break
		}
	}
	if repeatedIdx == -1 {
		findings = append(findings, Finding{Subject: subject, Message: "response has no repeated message field for the page of results"})
	} else if want := repeatedFieldNameForList(methodName); resp.Fields[repeatedIdx].Name != want {
		findings = append(findings, Finding{
			Subject: subject,
			Message: fmt.Sprintf("repeated field is named %q, want %q (the plural resource name, matching the method name)", resp.Fields[repeatedIdx].Name, want),
		})
	}
	if findFieldByName(resp, "next_page_token") == nil {
		findings = append(findings, Finding{Subject: subject, Message: "response has no next_page_token field"})
	}
	for i, f := range resp.Fields {
		if i == repeatedIdx || f.Name == "next_page_token" {
			continue
		}
		findings = append(findings, Finding{
			Subject: subject,
			Message: fmt.Sprintf("field %q is not part of the standard List response shape - only the repeated resource field and next_page_token are allowed", f.Name),
		})
	}
	return findings
}

func checkListResponseNameMatchesMethod(api *model.API) ([]Finding, error) {
	messagesByName := api.MessagesByFullName()

	var findings []Finding
	for _, svc := range api.Services {
		for _, method := range svc.Methods {
			if !strings.HasPrefix(method.Name, "List") {
				continue
			}
			subject := method.ServiceFullName + "." + method.Name
			want := method.Name + "Response"

			resp, ok := messagesByName[method.OutputName]
			if !ok {
				findings = append(findings, Finding{Subject: subject, Message: "response type " + method.OutputName + " not found"})
				continue
			}
			if resp.Name != want {
				findings = append(findings, Finding{
					Subject: subject,
					Message: fmt.Sprintf("response message is named %q, want %q", resp.Name, want),
				})
			}
		}
	}
	return findings, nil
}
