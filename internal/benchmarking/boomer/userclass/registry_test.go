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

package userclass

import (
	"context"
	"testing"
)

func TestAddRejectsDuplicate(t *testing.T) {
	name := "test-dup"
	Add(Entry{
		Name:      name,
		UserClass: "TestDupUser",
		Init: func(*Config) (func(), func(context.Context)) {
			return nil, nil
		},
	})
	defer func() {
		mu.Lock()
		delete(registry, name)
		mu.Unlock()
	}()

	defer func() {
		r := recover()
		if r == nil {
			t.Errorf("expected Add with duplicate name %q to panic, but it did not", name)
		}
	}()

	Add(Entry{
		Name:      name,
		UserClass: "TestDupUser2",
		Init: func(*Config) (func(), func(context.Context)) {
			return nil, nil
		},
	})
}

func TestLookupUnknown(t *testing.T) {
	_, ok := Lookup("nonexistent-class-name")
	if ok {
		t.Errorf("Lookup(nonexistent) got ok=true, want false")
	}
}
