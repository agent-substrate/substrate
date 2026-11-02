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
	"go.ate.dev/substrate/tools/apitool/internal/model"
)

var Documented = Rule{
	Name:        "documented",
	Description: "Every message, enum, message field, and RPC method has a doc comment.",
	Check: allOf(
		forEachMessage(checkMessageDocumented),
		forEachField(checkFieldDocumented),
		forEachEnum(checkEnumDocumented),
		forEachMethod(checkMethodDocumented),
	),
}

func checkMessageDocumented(m model.Message) []Finding {
	if model.PublicDoc(m.Comment) == "" {
		return []Finding{findingf(m.FullName, "message has no doc comment")}
	}
	return nil
}

func checkFieldDocumented(m model.Message, f model.Field) []Finding {
	if model.PublicDoc(f.Comment) == "" {
		return []Finding{findingf(m.FullName.Append(f.Name), "field has no doc comment")}
	}
	return nil
}

func checkEnumDocumented(e model.Enum) []Finding {
	if model.PublicDoc(e.Comment) == "" {
		return []Finding{findingf(e.FullName, "enum has no doc comment")}
	}
	return nil
}

func checkMethodDocumented(rpc model.Method) []Finding {
	if model.PublicDoc(rpc.Comment) == "" {
		return []Finding{findingf(subjectOf(rpc), "method has no doc comment")}
	}
	return nil
}
