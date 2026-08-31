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
	"fmt"

	"github.com/agent-substrate/substrate/tools/apitool/internal/model"
)

// NoOneofs requires that no field belong to an explicit `oneof` group.
var NoOneofs = Rule{
	Name:        "no-oneofs",
	Description: "No field belongs to an explicit oneof group.",
	Check:       checkNoOneofs,
}

func checkNoOneofs(api *model.API) ([]Finding, error) {
	var findings []Finding
	for _, m := range api.Messages {
		for _, f := range m.Fields {
			if f.OneofName != "" {
				findings = append(findings, Finding{
					Subject: m.FullName + "." + f.Name,
					Message: fmt.Sprintf("belongs to oneof %q - oneofs are not used in this API", f.OneofName),
				})
			}
		}
	}
	return findings, nil
}
