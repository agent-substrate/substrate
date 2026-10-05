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

package serverboot

import (
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/util/validation"
)

// ValidateWatchNamespace accepts a Kubernetes namespace or the empty all-namespaces scope.
func ValidateWatchNamespace(namespace string) error {
	if namespace != "" {
		if problems := validation.IsDNS1123Label(namespace); len(problems) > 0 {
			return fmt.Errorf("--watch-namespace=%q: %s", namespace, strings.Join(problems, "; "))
		}
	}
	return nil
}
