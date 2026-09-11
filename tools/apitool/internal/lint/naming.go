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
	"regexp"
	"strings"

	"github.com/agent-substrate/substrate/tools/apitool/internal/model"
)

var pascalBoundary = regexp.MustCompile(`([a-z0-9])([A-Z])`)

func snakeCase(s string) string {
	return strings.ToLower(pascalBoundary.ReplaceAllString(s, "${1}_${2}"))
}

func fieldNameForResource(resourceName string) string {
	return snakeCase(resourceName)
}

func repeatedFieldNameForList(methodName string) string {
	return snakeCase(strings.TrimPrefix(methodName, "List"))
}

// locatorResourceName returns the resource a Get/Delete request should
// identify by ObjectRef: resourceName itself for a top-level resource, or
// its parent for a sub-resource - a sub-resource's Get/Delete request
// identifies the parent, since the sub-resource itself has a fixed, implied
// name (for example, GetActorEgressPolicyRequest carries an "actor" field,
// not an "egress_policy" one).
func locatorResourceName(resourceName string) string {
	if parent, ok := model.ParentResourceName(resourceName); ok {
		return parent
	}
	return resourceName
}
