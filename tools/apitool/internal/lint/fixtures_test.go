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

// Builders for the APIs rule tests check. They declare every message in
// package "test", and every method on service "test.Control".
package lint_test

import (
	"github.com/agent-substrate/substrate/tools/apitool/internal/model"
)

// methodAPI builds an API whose test.Control service has the one method
// rpc, plus messages.
func methodAPI(rpc model.Method, messages ...model.Message) *model.API {
	rpc.ServiceFullName = "test.Control"
	rpc.ServiceName = "Control"
	return &model.API{
		Services: []model.Service{{Name: "Control", Methods: []model.Method{rpc}}},
		Messages: messages,
	}
}

// messagesAPI builds an API with just messages.
func messagesAPI(messages ...model.Message) *model.API {
	return &model.API{Messages: messages}
}

// rpc returns a method named name, annotated with resource, taking
// test.{name}Request and returning test.{output}.
func rpc(name, resource, output string) model.Method {
	return model.Method{Name: name, Resource: resource, InputName: "test." + name + "Request", OutputName: "test." + output}
}

// resource returns a resource message test.{name}, with the given parents
// and fields.
func resource(name string, parents []string, fields ...model.Field) model.Message {
	return model.Message{FullName: "test." + name, Name: name, Fields: fields, Resource: &model.ResourceAnnotation{Parents: parents}}
}

// msg returns a message test.{name} with the given fields.
func msg(name string, fields ...model.Field) model.Message {
	return model.Message{FullName: "test." + name, Name: name, Fields: fields}
}

// singletonResource returns a singleton resource message test.{name}, with
// the given parents.
func singletonResource(name string, parents []string) model.Message {
	m := resource(name, parents)
	m.Resource.Singleton = true
	return m
}
