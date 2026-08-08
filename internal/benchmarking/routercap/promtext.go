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

// A small scanner for the Prometheus text format. cAdvisor, Envoy and the sidecar
// all speak it, and none of them needs the full client library to be read.

package routercap

import (
	"bufio"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
)

// promSample is one line of Prometheus text exposition.
type promSample struct {
	Name   string
	Labels map[string]string
	Value  float64
	// TimestampMs is the sample's own timestamp, or 0 when the exposition
	// omits one. The kubelet's cAdvisor endpoint sets it, and the harness
	// depends on it to know when a measurement was actually taken.
	TimestampMs int64
}

// scanPromText streams r and invokes fn for each sample whose metric name is in
// want.
func scanPromText(r io.Reader, want map[string]bool, fn func(promSample)) error {
	return scanPromTextMatch(r, func(name string) bool { return want[name] }, fn)
}

// scanPromTextMatch streams r and invokes fn for each sample whose metric name
// satisfies match. It decides on the name before parsing anything else because
// a busy node's cAdvisor payload runs to megabytes every few seconds.
func scanPromTextMatch(r io.Reader, match func(name string) bool, fn func(promSample)) error {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	line := 0
	for sc.Scan() {
		line++
		text := sc.Text()
		if text == "" || text[0] == '#' {
			continue
		}
		// Cheap name prefix check before any allocation.
		end := strings.IndexAny(text, "{ ")
		if end < 0 {
			continue
		}
		name := text[:end]
		if !match(name) {
			continue
		}
		s, err := parsePromLine(name, text[end:])
		if err != nil {
			return fmt.Errorf("line %d (%s): %w", line, name, err)
		}
		fn(s)
	}
	return sc.Err()
}

// parsePromLine parses the remainder of an exposition line after the metric
// name: an optional {label set}, a value, and an optional timestamp.
func parsePromLine(name, rest string) (promSample, error) {
	s := promSample{Name: name, Labels: map[string]string{}}
	if strings.HasPrefix(rest, "{") {
		close := findLabelSetEnd(rest)
		if close < 0 {
			return s, fmt.Errorf("unterminated label set")
		}
		if err := parseLabels(rest[1:close], s.Labels); err != nil {
			return s, err
		}
		rest = rest[close+1:]
	}
	fields := strings.Fields(rest)
	if len(fields) == 0 {
		return s, fmt.Errorf("no value")
	}
	v, err := parsePromValue(fields[0])
	if err != nil {
		return s, fmt.Errorf("value %q: %w", fields[0], err)
	}
	s.Value = v
	if len(fields) > 1 {
		ts, err := strconv.ParseInt(fields[1], 10, 64)
		if err != nil {
			return s, fmt.Errorf("timestamp %q: %w", fields[1], err)
		}
		s.TimestampMs = ts
	}
	return s, nil
}

// findLabelSetEnd returns the index of the '}' closing the label set, skipping
// braces that appear inside quoted label values.
func findLabelSetEnd(s string) int {
	inQuote, escaped := false, false
	for i := 1; i < len(s); i++ {
		switch {
		case escaped:
			escaped = false
		case s[i] == '\\' && inQuote:
			escaped = true
		case s[i] == '"':
			inQuote = !inQuote
		case s[i] == '}' && !inQuote:
			return i
		}
	}
	return -1
}

// parseLabels splits a comma-separated label list into out.
func parseLabels(s string, out map[string]string) error {
	for len(s) > 0 {
		s = strings.TrimLeft(s, " ,")
		if s == "" {
			return nil
		}
		eq := strings.IndexByte(s, '=')
		if eq < 0 {
			return fmt.Errorf("label %q has no '='", s)
		}
		key := strings.TrimSpace(s[:eq])
		s = s[eq+1:]
		if len(s) == 0 || s[0] != '"' {
			return fmt.Errorf("label %q value is not quoted", key)
		}
		val, n, err := unquoteLabelValue(s)
		if err != nil {
			return fmt.Errorf("label %q: %w", key, err)
		}
		out[key] = val
		s = s[n:]
	}
	return nil
}

// unquoteLabelValue decodes a quoted label value starting at s[0] == '"',
// returning the value and how many bytes it consumed including both quotes.
func unquoteLabelValue(s string) (string, int, error) {
	var b strings.Builder
	for i := 1; i < len(s); i++ {
		switch s[i] {
		case '\\':
			if i+1 >= len(s) {
				return "", 0, fmt.Errorf("trailing escape")
			}
			i++
			switch s[i] {
			case 'n':
				b.WriteByte('\n')
			case '\\', '"':
				b.WriteByte(s[i])
			default:
				b.WriteByte('\\')
				b.WriteByte(s[i])
			}
		case '"':
			return b.String(), i + 1, nil
		default:
			b.WriteByte(s[i])
		}
	}
	return "", 0, fmt.Errorf("unterminated value")
}

// parsePromValue handles the three spellings Prometheus allows for
// non-finite values alongside ordinary floats.
func parsePromValue(s string) (float64, error) {
	switch s {
	case "+Inf":
		return math.Inf(1), nil
	case "-Inf":
		return math.Inf(-1), nil
	case "NaN":
		return math.NaN(), nil
	}
	return strconv.ParseFloat(s, 64)
}
