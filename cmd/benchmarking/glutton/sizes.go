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

package main

import (
	"fmt"
	"strconv"
	"strings"
)

// parseBytes parses a human-friendly size: plain bytes, or a Ki/Mi/Gi
// (binary) or K/M/G (decimal) suffix, e.g. "512Mi", "2Gi", "1000000".
func parseBytes(s string) (int64, error) {
	multipliers := []struct {
		suffix string
		mult   int64
	}{
		{"Ki", 1 << 10}, {"Mi", 1 << 20}, {"Gi", 1 << 30},
		{"K", 1000}, {"M", 1000 * 1000}, {"G", 1000 * 1000 * 1000},
	}
	for _, m := range multipliers {
		if v, ok := strings.CutSuffix(s, m.suffix); ok {
			n, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
			if err != nil {
				return 0, fmt.Errorf("parsing size %q: %w", s, err)
			}
			return n * m.mult, nil
		}
	}
	n, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parsing size %q: %w", s, err)
	}
	return n, nil
}
