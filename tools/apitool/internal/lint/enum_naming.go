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
	"strings"

	"github.com/agent-substrate/substrate/tools/apitool/internal/model"
)

// EnumZeroValueUnspecified requires every enum's zero value to be named
// "{EnumName}_UNSPECIFIED".
var EnumZeroValueUnspecified = Rule{
	Name:        "enum-zero-value-unspecified",
	Description: `Every enum's zero value is named "{EnumName}_UNSPECIFIED".`,
	Check:       checkEnumZeroValueUnspecified,
}

// EnumValuesPrefixed requires every enum's values - top-level or nested -
// to be prefixed with the enum's own name.
var EnumValuesPrefixed = Rule{
	Name:        "enum-values-prefixed",
	Description: `Every enum's values are prefixed with "{ENUM_NAME}_".`,
	Check:       checkEnumValuesPrefixed,
}

func checkEnumZeroValueUnspecified(api *model.API) ([]Finding, error) {
	var findings []Finding
	for _, e := range api.Enums {
		want := strings.ToUpper(snakeCase(lastNameSegment(e.Name))) + "_UNSPECIFIED"
		zero := e.ValueByNumber(0)
		switch {
		case zero == nil:
			findings = append(findings, Finding{Subject: e.FullName, Message: "has no value numbered 0"})
		case zero.Name != want:
			findings = append(findings, Finding{
				Subject: e.FullName,
				Message: fmt.Sprintf("zero value is named %q, want %q", zero.Name, want),
			})
		}
	}
	return findings, nil
}

func checkEnumValuesPrefixed(api *model.API) ([]Finding, error) {
	var findings []Finding
	for _, e := range api.Enums {
		prefix := strings.ToUpper(snakeCase(lastNameSegment(e.Name))) + "_"
		for _, v := range e.Values {
			if !strings.HasPrefix(v.Name, prefix) {
				findings = append(findings, Finding{
					Subject: e.FullName + "." + v.Name,
					Message: fmt.Sprintf("not prefixed with %q", prefix),
				})
			}
		}
	}
	return findings, nil
}

func lastNameSegment(name string) string {
	if i := strings.LastIndex(name, "."); i >= 0 {
		return name[i+1:]
	}
	return name
}
