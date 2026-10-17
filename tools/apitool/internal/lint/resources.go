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

const resourceMetadataTypeFullName protoreflect.FullName = "ateapi.ResourceMetadata"

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

func findFieldByName(m model.Message, name protoreflect.Name) *model.Field {
	for i := range m.Fields {
		if m.Fields[i].Name == name {
			return &m.Fields[i]
		}
	}
	return nil
}
