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
	"text/tabwriter"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// PrintEgressPolicyTo prints one actor's egress policy. json and yaml emit the
// bare EgressPolicy. The policy carries no actor name, so the caller passes it.
func PrintEgressPolicyTo(out io.Writer, actor string, policy *ateapipb.EgressPolicy, format string) error {
	switch format {
	case "json", "yaml":
		return printProto(out, policy, format)
	case "table":
		w := tabwriter.NewWriter(out, 0, 0, 3, ' ', 0)
		fmt.Fprintln(w, "ATESPACE\tACTOR\tRULES\tVERSION\tAGE")
		fmt.Fprintf(w, "%s\t%s\t%d\t%d\t%s\n",
			policy.GetMetadata().GetAtespace(), actor, len(policy.GetRules()),
			policy.GetMetadata().GetVersion(), formatAge(policy.GetMetadata().GetCreateTime()))
		return w.Flush()
	default:
		return fmt.Errorf("unsupported format %q", format)
	}
}
