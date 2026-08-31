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

// ResourceStatusFieldShape requires a resource's "status" field, if
// present, to be typed "{ResourceName}Status".
var ResourceStatusFieldShape = Rule{
	Name:        "resource-status-field-shape",
	Description: `A resource's "status" field, if present, is typed "{ResourceName}Status".`,
	Check:       checkResourceStatusFieldShape,
}

func checkResourceStatusFieldShape(api *model.API) ([]Finding, error) {
	resources, err := model.Resources(api)
	if err != nil {
		return nil, err
	}

	var findings []Finding
	for _, rg := range resources {
		field := findFieldByName(rg.Message, "status")
		if field == nil {
			continue
		}
		want := expectedStatusTypeFullName(rg.Message.FullName, rg.Message.Name)
		if field.TypeKind != "message" || field.TypeFullName != want {
			findings = append(findings, Finding{
				Subject: rg.Message.FullName,
				Message: fmt.Sprintf(`"status" field is %s, want %s`, fieldTypeDescription(*field), want),
			})
		}
	}
	return findings, nil
}

// expectedStatusTypeFullName derives "{pkg}.{ResourceName}Status" from the
// resource's own full and short names.
func expectedStatusTypeFullName(resourceFullName, resourceName string) string {
	pkgPrefix := strings.TrimSuffix(resourceFullName, resourceName)
	return pkgPrefix + resourceName + "Status"
}
