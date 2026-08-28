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

const resourceMetadataTypeFullName = "ateapi.ResourceMetadata"

// ResourceMetadata requires every resource to declare a ResourceMetadata
// field named "metadata" as field 1.
var ResourceMetadata = Rule{
	Name:        "resource-metadata",
	Description: `Every resource message has a "ResourceMetadata metadata = 1" field.`,
	Check:       checkResourceMetadata,
}

func checkResourceMetadata(api *model.API) ([]Finding, error) {
	resources, err := model.Resources(api)
	if err != nil {
		return nil, err
	}

	var findings []Finding
	for _, rg := range resources {
		field := findFieldByName(rg.Message, "metadata")
		switch {
		case field == nil:
			findings = append(findings, Finding{
				Subject: rg.Message.FullName,
				Message: `has no "metadata" field`,
			})
		case field.TypeKind != "message" || field.TypeFullName != resourceMetadataTypeFullName:
			findings = append(findings, Finding{
				Subject: rg.Message.FullName,
				Message: fmt.Sprintf(`"metadata" field is %s, want %s`, fieldTypeDescription(*field), resourceMetadataTypeFullName),
			})
		case field.Number != 1:
			findings = append(findings, Finding{
				Subject: rg.Message.FullName,
				Message: fmt.Sprintf(`"metadata" field is number %d, want 1`, field.Number),
			})
		}
	}
	return findings, nil
}

func findFieldByName(m model.Message, name string) *model.Field {
	for i := range m.Fields {
		if m.Fields[i].Name == name {
			return &m.Fields[i]
		}
	}
	return nil
}

func fieldTypeDescription(f model.Field) string {
	if f.TypeKind == "" {
		return "not a message"
	}
	return f.TypeFullName
}
