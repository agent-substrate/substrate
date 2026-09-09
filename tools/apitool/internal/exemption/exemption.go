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

// Package exemption records lint findings that are exempted and should not
// fail `apitool validate`.
//
// An exemption's identity is the (Rule, Subject, Message) triple - the same
// three values that identify a lint.Finding, plus the name of the rule that
// raised it. That triple is exact on purpose: it lets validate tell "this
// finding is exempted" apart from "the API changed and this is a new
// finding", even when both share a Subject.
package exemption

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
)

// Exemption is one specific lint finding that's allowed to fail `apitool
// validate`, identified by the rule that raised it, the finding's Subject,
// and its Message.
type Exemption struct {
	Rule    string `json:"rule"`
	Subject string `json:"subject"`
	Message string `json:"message"`
}

// Load reads exemptions from path. A missing file is treated as zero
// exemptions rather than an error.
func Load(path string) ([]Exemption, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("while reading %s: %w", path, err)
	}
	var exemptions []Exemption
	if err := json.Unmarshal(data, &exemptions); err != nil {
		return nil, fmt.Errorf("while parsing %s: %w", path, err)
	}
	return exemptions, nil
}

// Save writes exemptions to path as sorted, indented JSON.
func Save(path string, exemptions []Exemption) error {
	sorted := append([]Exemption(nil), exemptions...)
	sortExemptions(sorted)
	if sorted == nil {
		sorted = []Exemption{}
	}
	data, err := json.MarshalIndent(sorted, "", "  ")
	if err != nil {
		return fmt.Errorf("while encoding exemptions: %w", err)
	}
	data = append(data, '\n')
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return fmt.Errorf("while writing %s: %w", path, err)
	}
	return nil
}

func sortExemptions(exemptions []Exemption) {
	sort.Slice(exemptions, func(i, j int) bool {
		a, b := exemptions[i], exemptions[j]
		if a.Rule != b.Rule {
			return a.Rule < b.Rule
		}
		if a.Subject != b.Subject {
			return a.Subject < b.Subject
		}
		return a.Message < b.Message
	})
}

// Set tracks a list of exemptions as counts of each distinct Exemption value.
type Set struct {
	remaining map[Exemption]int
}

// NewSet builds a Set from exemptions.
func NewSet(exemptions []Exemption) *Set {
	s := &Set{remaining: make(map[Exemption]int, len(exemptions))}
	for _, e := range exemptions {
		s.remaining[e]++
	}
	return s
}

// Consume reports whether (rule, subject, message) matches an exemption
// that hasn't already been claimed by an earlier finding, claiming it if so.
func (s *Set) Consume(rule, subject, message string) bool {
	e := Exemption{Rule: rule, Subject: subject, Message: message}
	if s.remaining[e] <= 0 {
		return false
	}
	s.remaining[e]--
	return true
}

// Unused returns every exemption that was never matched by a finding.
func (s *Set) Unused() []Exemption {
	var out []Exemption
	for e, n := range s.remaining {
		for i := 0; i < n; i++ {
			out = append(out, e)
		}
	}
	sortExemptions(out)
	return out
}

// Diff compares two lists of exemptions.
func Diff(current, exemptions []Exemption) (missing, stale []Exemption) {
	set := NewSet(exemptions)
	for _, c := range current {
		if !set.Consume(c.Rule, c.Subject, c.Message) {
			missing = append(missing, c)
		}
	}
	sortExemptions(missing)
	return missing, set.Unused()
}
