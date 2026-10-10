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

// ResourceMetadata requires every resource to declare a ResourceMetadata
// field named "metadata" as field 1.
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
		return []Finding{findingf(r.Message.FullName, `"metadata" field is %s, want %s`, fieldTypeDescription(*field), resourceMetadataTypeFullName)}
	case field.Number != 1:
		return []Finding{findingf(r.Message.FullName, `"metadata" field is number %d, want 1`, field.Number)}
	}
	return nil
}

// ResourceStatusFieldShape requires a resource's "status" field, if
// present, to be typed "{ResourceName}Status".
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
	if want := expectedStatusTypeFullName(r.Message.FullName, r.Message.Name); !isMessageOf(*field, want) {
		return []Finding{findingf(r.Message.FullName, `"status" field is %s, want %s`, fieldTypeDescription(*field), want)}
	}
	return nil
}

// expectedStatusTypeFullName derives "{pkg}.{ResourceName}Status" from the
// resource's own full and short names.
func expectedStatusTypeFullName(resourceFullName, resourceName string) string {
	pkgPrefix := strings.TrimSuffix(resourceFullName, resourceName)
	return pkgPrefix + resourceName + "Status"
}
