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

package principal

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"strings"
)

// ParseList parses a comma-separated list of principals. It reads CSV, as a
// pflag StringSlice does, so a principal that contains a comma or a double
// quote goes in double quotes, with each embedded quote doubled:
//
//	alice, "Doe, Jane", "say ""hi"""
//
// Whitespace around a principal is ignored and empty entries are dropped, so
// an empty list or one of only separators is no principals.
func ParseList(s string) ([]string, error) {
	r := csv.NewReader(strings.NewReader(s))
	r.TrimLeadingSpace = true
	r.FieldsPerRecord = -1
	records, err := r.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("invalid principal list, quote a principal that contains a comma or a double quote: %w", err)
	}
	var principals []string
	for _, record := range records {
		for _, p := range record {
			if p = strings.TrimSpace(p); p != "" {
				principals = append(principals, p)
			}
		}
	}
	return principals, nil
}

// FormatList renders principals as a list ParseList and a pflag StringSlice
// read back as the same principals.
func FormatList(principals []string) string {
	var b bytes.Buffer
	w := csv.NewWriter(&b)
	// A bytes.Buffer does not fail to write.
	_ = w.Write(principals)
	w.Flush()
	return strings.TrimSuffix(b.String(), "\n")
}
