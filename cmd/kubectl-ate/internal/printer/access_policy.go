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

package printer

import (
	"fmt"
	"io"
	"strings"
	"text/tabwriter"

	"go.ate.dev/substrate/api/ateapi/v1"
)

// PrintAccessPolicyTo prints an access policy, one table row per binding.
// json and yaml emit the bare AccessPolicy. The policy's metadata carries no
// atespace, so the caller passes it; empty means the global policy.
func PrintAccessPolicyTo(out io.Writer, atespace string, policy *ateapi.AccessPolicy, format string) error {
	switch format {
	case "json", "yaml":
		return printProto(out, policy, format)
	case "table":
		scope := atespace
		if scope == "" {
			scope = "<global>"
		}
		w := tabwriter.NewWriter(out, 0, 0, 3, ' ', 0)
		fmt.Fprintln(w, "SCOPE\tROLE\tMEMBERS")
		if len(policy.GetBindings()) == 0 {
			fmt.Fprintf(w, "%s\t<none>\t<none>\n", scope)
		}
		for _, b := range policy.GetBindings() {
			fmt.Fprintf(w, "%s\t%s\t%s\n", scope, b.GetRole(), strings.Join(b.GetMembers(), ","))
		}
		return w.Flush()
	default:
		return fmt.Errorf("unsupported format %q", format)
	}
}
