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

package validation

import (
	"reflect"

	"google.golang.org/protobuf/proto"
)

// ateDeepEqual compares two values of any type, using proto.Equal if both are
// proto messages, and reflect.DeepEqual otherwise.  This is called by
// declarative validation's generated code.
func ateDeepEqual[T any](a, b T) bool {
	asProto := func(x any) proto.Message {
		pm, ok := x.(proto.Message)
		if !ok {
			return nil
		}
		return pm
	}

	if pa, pb := asProto(a), asProto(b); pa != nil && pb != nil {
		return proto.Equal(pa, pb)
	}
	return reflect.DeepEqual(a, b)
}
