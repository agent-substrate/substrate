// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package primitiveregistry provides a registry for primitive constructors.
package primitiveregistry

import (
	"fmt"
	"reflect"

	"github.com/tink-crypto/tink-go/v2/internal/syncmap"
	"github.com/tink-crypto/tink-go/v2/key"
)

var (
	primitiveConstructors = syncmap.New[reflect.Type, primitiveConstructor]()
)

type primitiveConstructor func(key key.Key) (any, error)

// RegisterPrimitiveConstructor registers a function that constructs primitives
// from a given [key.Key] to the global registry.
func RegisterPrimitiveConstructor[K key.Key](constructor primitiveConstructor) error {
	keyType := reflect.TypeFor[K]()
	existing, loaded := primitiveConstructors.LoadOrStore(keyType, constructor)
	if loaded && reflect.ValueOf(existing).Pointer() != reflect.ValueOf(constructor).Pointer() {
		return fmt.Errorf("a different constructor already registered for %v", keyType)
	}
	return nil
}

// UnregisterPrimitiveConstructor removes the primitive constructor for the
// given key type.
//
// This function is intended to be used in tests only.
func UnregisterPrimitiveConstructor[K key.Key]() {
	primitiveConstructors.Delete(reflect.TypeFor[K]())
}

// Primitive constructs a primitive from a given [key.Key].
func Primitive(key key.Key) (any, error) {
	if key == nil {
		return nil, fmt.Errorf("key is nil")
	}
	constructor, found := primitiveConstructors.Load(reflect.TypeOf(key))
	if !found {
		return nil, fmt.Errorf("no constructor found for key %T", key)
	}
	return constructor(key)
}
