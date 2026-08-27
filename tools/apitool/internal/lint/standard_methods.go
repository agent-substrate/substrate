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

const objectRefTypeFullName = "ateapi.ObjectRef"

// StandardMethodReturnsResource requires a Get/Create/Update/Delete
// method's response to be the resource itself, never a wrapper message.
var StandardMethodReturnsResource = Rule{
	Name:        "standard-method-returns-resource",
	Description: "A Get/Create/Update/Delete method's response is the resource itself, not a wrapper message.",
	Check:       checkStandardMethodReturnsResource,
}

func checkStandardMethodReturnsResource(api *model.API) ([]Finding, error) {
	resources, err := model.Resources(api)
	if err != nil {
		return nil, err
	}

	var findings []Finding
	for _, rg := range resources {
		for _, m := range rg.Methods {
			verb, ok := standardVerbFor(m.Name, rg.Message.Name)
			if !ok {
				continue // a custom or List method - not this rule's concern.
			}
			if m.OutputName != rg.Message.FullName {
				findings = append(findings, Finding{
					Subject: m.ServiceFullName + "." + m.Name,
					Message: fmt.Sprintf("%s method returns %s, want the resource itself (%s)", verb, m.OutputName, rg.Message.FullName),
				})
			}
		}
	}
	return findings, nil
}

func standardVerbFor(methodName, resourceName string) (verb string, ok bool) {
	for _, v := range []string{"Get", "Create", "Update", "Delete"} {
		if methodName == v+resourceName {
			return v, true
		}
	}
	return "", false
}
