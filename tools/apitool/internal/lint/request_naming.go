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

// RequestNameMatchesMethod requires every method's request to be named
// "{MethodName}Request".
var RequestNameMatchesMethod = Rule{
	Name:        "request-name-matches-method",
	Description: `Every method's request message is named "{MethodName}Request".`,
	Check:       checkRequestNameMatchesMethod,
}

func checkRequestNameMatchesMethod(api *model.API) ([]Finding, error) {
	messagesByName := api.MessagesByFullName()

	var findings []Finding
	for _, svc := range api.Services {
		for _, method := range svc.Methods {
			subject := method.ServiceFullName + "." + method.Name
			want := method.Name + "Request"

			req, ok := messagesByName[method.InputName]
			if !ok {
				findings = append(findings, Finding{Subject: subject, Message: "request type " + method.InputName + " not found"})
				continue
			}
			if req.Name != want {
				findings = append(findings, Finding{
					Subject: subject,
					Message: fmt.Sprintf("request message is named %q, want %q", req.Name, want),
				})
			}
		}
	}
	return findings, nil
}
