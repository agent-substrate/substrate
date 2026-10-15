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
	"github.com/agent-substrate/substrate/tools/apitool/internal/model"
)

var NoOneofs = Rule{
	Name:        "no-oneofs",
	Description: "No field belongs to an explicit oneof group.",
	Check:       forEachField(checkNoOneofs),
}

func checkNoOneofs(m model.Message, f model.Field) []Finding {
	if f.OneofName == "" {
		return nil
	}
	return []Finding{findingf(m.FullName.Append(f.Name), "belongs to oneof %q - oneofs are not used in this API", f.OneofName)}
}
