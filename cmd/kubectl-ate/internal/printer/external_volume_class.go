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
	"maps"
	"slices"
	"strings"
	"text/tabwriter"

	"go.ate.dev/substrate/api/ateapi/v1"
)

func formatParameters(params map[string]string) string {
	if len(params) == 0 {
		return "-"
	}
	keys := slices.Sorted(maps.Keys(params))
	for i, k := range keys {
		keys[i] = k + "=" + params[k]
	}
	return strings.Join(keys, ",")
}

// PrintExternalVolumeClassesTo prints a slice of external volume classes to the provided writer.
func PrintExternalVolumeClassesTo(out io.Writer, classes []*ateapi.ExternalVolumeClass, format string) error {
	slices.SortFunc(classes, func(a, b *ateapi.ExternalVolumeClass) int {
		return cmp.Compare(a.GetMetadata().GetName(), b.GetMetadata().GetName())
	})
	switch format {
	case "json", "yaml":
		return printProto(out, &ateapi.ListExternalVolumeClassesResponse{ExternalVolumeClasses: classes}, format)
	case "table":
		w := tabwriter.NewWriter(out, 0, 0, 3, ' ', 0)
		fmt.Fprintln(w, "NAME\tDRIVER\tPARAMETERS\tAGE")
		for _, c := range classes {
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\n", c.GetMetadata().GetName(), c.GetDriverName(), formatParameters(c.GetParameters()), formatAge(c.GetMetadata().GetCreateTime()))
		}
		return w.Flush()
	default:
		return fmt.Errorf("unsupported format %q", format)
	}
}

// PrintExternalVolumeClassTo prints a single external volume class to the provided writer.
func PrintExternalVolumeClassTo(out io.Writer, class *ateapi.ExternalVolumeClass, format string) error {
	if format == "json" || format == "yaml" {
		return printProto(out, class, format)
	}
	return PrintExternalVolumeClassesTo(out, []*ateapi.ExternalVolumeClass{class}, format)
}
