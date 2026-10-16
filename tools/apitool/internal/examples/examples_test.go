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

package examples_test

import (
	"maps"
	"slices"
	"testing"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/reflect/protoregistry"

	"go.ate.dev/substrate/pkg/proto/ateapipb"
	"go.ate.dev/substrate/tools/apitool/internal/examples"
	"go.ate.dev/substrate/tools/apitool/internal/model"
)

// resources returns the API's resource messages by name.
func resources(t *testing.T) map[string]model.Message {
	t.Helper()
	api, err := model.Build(t.Context(), ateapipb.Source)
	if err != nil {
		t.Fatalf("model.Build() error = %v", err)
	}
	rs, err := model.Resources(model.ScopeToService(api, "Control"))
	if err != nil {
		t.Fatalf("model.Resources() error = %v", err)
	}
	byName := map[string]model.Message{}
	for _, r := range rs {
		byName[r.Message.Name] = r.Message
	}
	return byName
}

func TestByResource_OnePerResource(t *testing.T) {
	got, err := examples.ByResource()
	if err != nil {
		t.Fatalf("ByResource() error = %v", err)
	}
	rs := resources(t)
	for name := range rs {
		if _, ok := got[name]; !ok {
			t.Errorf("resource %s has no example: add %s.json", name, name)
		}
	}
	for name := range got {
		if _, ok := rs[name]; !ok {
			t.Errorf("%s.json names no resource", name)
		}
	}
}

func TestByResource_ParsesAsResource(t *testing.T) {
	got, err := examples.ByResource()
	if err != nil {
		t.Fatalf("ByResource() error = %v", err)
	}
	rs := resources(t)
	for _, name := range slices.Sorted(maps.Keys(got)) {
		msg, ok := rs[name]
		if !ok {
			continue // Reported by TestByResource_OnePerResource.
		}
		t.Run(name, func(t *testing.T) {
			mt, err := protoregistry.GlobalTypes.FindMessageByName(msg.FullName)
			if err != nil {
				t.Fatalf("FindMessageByName(%s) error = %v", msg.FullName, err)
			}
			if err := protojson.Unmarshal([]byte(got[name]), mt.New().Interface()); err != nil {
				t.Errorf("%s.json doesn't parse as %s: %v", name, msg.FullName, err)
			}
		})
	}
}
