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
	"strings"

	"github.com/agent-substrate/substrate/tools/apitool/internal/model"
)

// Documented requires every message, enum, message field, and RPC method
// to have a doc comment.
var Documented = Rule{
	Name:        "documented",
	Description: "Every message, enum, message field, and RPC method has a doc comment.",
	Check:       checkDocumented,
}

func checkDocumented(api *model.API) ([]Finding, error) {
	var findings []Finding
	for _, m := range api.Messages {
		if strings.TrimSpace(m.Comment) == "" {
			findings = append(findings, Finding{Subject: m.FullName, Message: "message has no doc comment"})
		}
		for _, f := range m.Fields {
			if strings.TrimSpace(f.Comment) == "" {
				findings = append(findings, Finding{Subject: m.FullName + "." + f.Name, Message: "field has no doc comment"})
			}
		}
	}
	for _, e := range api.Enums {
		if strings.TrimSpace(e.Comment) == "" {
			findings = append(findings, Finding{Subject: e.FullName, Message: "enum has no doc comment"})
		}
	}
	for _, svc := range api.Services {
		for _, method := range svc.Methods {
			if strings.TrimSpace(method.Comment) == "" {
				findings = append(findings, Finding{Subject: method.ServiceFullName + "." + method.Name, Message: "method has no doc comment"})
			}
		}
	}
	return findings, nil
}
