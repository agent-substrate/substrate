// Copyright 2026 The Agent Substrate Authors
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
	"cmp"
	"fmt"
	"io"
	"slices"
	"text/tabwriter"

	"go.ate.dev/substrate/api/ateapi/v1"
)

func formatNodeSocketOverride(socket string) string {
	if socket == "" {
		return "-"
	}
	return socket
}

func formatCsiDriverTLS(tls *ateapi.CsiDriverTlsConfig) string {
	if !tls.GetEnabled() {
		return "disabled"
	}
	if tls.GetUsePodIdentity() {
		return "mtls (pod-identity)"
	}
	return "enabled"
}

// PrintCsiDriverConfigsTo prints a slice of CSI driver configs to the provided writer.
func PrintCsiDriverConfigsTo(out io.Writer, configs []*ateapi.CsiDriverConfig, format string) error {
	slices.SortFunc(configs, func(a, b *ateapi.CsiDriverConfig) int {
		return cmp.Compare(a.GetMetadata().GetName(), b.GetMetadata().GetName())
	})
	switch format {
	case "json", "yaml":
		return printProto(out, &ateapi.ListCsiDriverConfigsResponse{CsiDriverConfigs: configs}, format)
	case "table":
		w := tabwriter.NewWriter(out, 0, 0, 3, ' ', 0)
		fmt.Fprintln(w, "NAME\tDRIVER\tCONTROLLER-ENDPOINT\tNODE-SOCKET-OVERRIDE\tTLS\tAGE")
		for _, c := range configs {
			fmt.Fprintf(
				w,
				"%s\t%s\t%s\t%s\t%s\t%s\n",
				c.GetMetadata().GetName(),
				c.GetDriverName(),
				c.GetControllerEndpoint(),
				formatNodeSocketOverride(c.GetNodeSocketOverride()),
				formatCsiDriverTLS(c.GetTls()),
				formatAge(c.GetMetadata().GetCreateTime()),
			)
		}
		return w.Flush()
	default:
		return fmt.Errorf("unsupported format %q", format)
	}
}

// PrintCsiDriverConfigTo prints a single CSI driver config to the provided writer.
func PrintCsiDriverConfigTo(out io.Writer, config *ateapi.CsiDriverConfig, format string) error {
	if format == "json" || format == "yaml" {
		return printProto(out, config, format)
	}
	return PrintCsiDriverConfigsTo(out, []*ateapi.CsiDriverConfig{config}, format)
}
