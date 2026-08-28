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

// Fixture builders shared by more than one verb's test file - e.g. a
// Create fixture used both by create_test.go and by another verb's test
// confirming it ignores Create methods.
package lint_test

import (
	"github.com/agent-substrate/substrate/tools/apitool/internal/model"
)

// standardMethodAPI builds a minimal API with one Control.GetAtespace
// method (see resourceAPI's comment) whose response is outputName,
// targeting an "Atespace" resource with the given fields.
func standardMethodAPI(outputName string, atespaceFields []model.Field) *model.API {
	return &model.API{
		Services: []model.Service{{
			Name: "Control",
			Methods: []model.Method{
				{Name: "GetAtespace", ServiceName: "Control", InputName: "test.GetAtespaceRequest", OutputName: outputName},
			},
		}},
		Messages: []model.Message{
			{FullName: "test.Atespace", Name: "Atespace", Fields: atespaceFields},
			{FullName: "test.GetAtespaceRequest", Name: "GetAtespaceRequest", Fields: []model.Field{
				{Name: "atespace", TypeKind: "message", TypeFullName: "ateapi.ObjectRef"},
			}},
			{FullName: "test.GetAtespaceResponse", Name: "GetAtespaceResponse"},
		},
	}
}

// deleteMethodAPI builds a minimal API with one Control.DeleteAtespace
// method targeting an "Atespace" resource, with the given request fields.
func deleteMethodAPI(reqFields []model.Field) *model.API {
	return &model.API{
		Services: []model.Service{{
			Name: "Control",
			Methods: []model.Method{
				{Name: "DeleteAtespace", ServiceName: "Control", InputName: "test.DeleteAtespaceRequest", OutputName: "test.Atespace"},
			},
		}},
		Messages: []model.Message{
			{FullName: "test.Atespace", Name: "Atespace"},
			{FullName: "test.DeleteAtespaceRequest", Name: "DeleteAtespaceRequest", Fields: reqFields},
		},
	}
}

// createAtespaceAPI builds a minimal API with one Control.CreateAtespace
// method whose request embeds the resource (not an ObjectRef) - used to
// confirm Get/Delete-only rules ignore Create/Update.
func createAtespaceAPI() *model.API {
	return createMethodAPI([]model.Field{
		{Name: "atespace", TypeKind: "message", TypeFullName: "test.Atespace"},
	})
}

// createMethodAPI builds a minimal API with one Control.CreateAtespace
// method targeting an "Atespace" resource, with the given request fields.
func createMethodAPI(reqFields []model.Field) *model.API {
	return &model.API{
		Services: []model.Service{{
			Name: "Control",
			Methods: []model.Method{
				{Name: "CreateAtespace", ServiceName: "Control", InputName: "test.CreateAtespaceRequest", OutputName: "test.Atespace"},
			},
		}},
		Messages: []model.Message{
			{FullName: "test.Atespace", Name: "Atespace"},
			{FullName: "test.CreateAtespaceRequest", Name: "CreateAtespaceRequest", Fields: reqFields},
		},
	}
}

// updateMethodAPI builds a minimal API with one Control.UpdateAtespace
// method targeting an "Atespace" resource, with the given request fields.
func updateMethodAPI(reqFields []model.Field) *model.API {
	return &model.API{
		Services: []model.Service{{
			Name: "Control",
			Methods: []model.Method{
				{Name: "UpdateAtespace", ServiceName: "Control", InputName: "test.UpdateAtespaceRequest", OutputName: "test.Atespace"},
			},
		}},
		Messages: []model.Message{
			{FullName: "test.Atespace", Name: "Atespace"},
			{FullName: "test.UpdateAtespaceRequest", Name: "UpdateAtespaceRequest", Fields: reqFields},
		},
	}
}
