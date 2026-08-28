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

// Package validate runs apitool's lint rules against the Substrate API and
// compares the result to a checked-in exemptions file. This is the logic
// behind both the `apitool validate` command and its presubmit gate, the
// TestExemptions golden-file test.
package validate

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/agent-substrate/substrate/tools/apitool/internal/exemption"
	"github.com/agent-substrate/substrate/tools/apitool/internal/lint"
	"github.com/agent-substrate/substrate/tools/apitool/internal/model"
)

// serviceName is the RPC service apitool validates: ateapi.proto's public,
// documented API surface.
const serviceName = "Control"

// API builds ateapi.proto's model.API and returns it scoped to Control -
// the one entry point both `apitool validate` and TestExemptions build
// from, so which service and proto file that is stays this package's own
// concern.
func API(ctx context.Context) (*model.API, error) {
	api, err := model.Build(ctx, ateapipb.Source)
	if err != nil {
		return nil, fmt.Errorf("while building API model: %w", err)
	}
	return model.ScopeToService(api, serviceName), nil
}

// RuleFindings pairs one rule with the findings it raised against an API,
// preserving lint.All's order.
type RuleFindings struct {
	Rule     lint.Rule
	Findings []lint.Finding
}

// All runs every rule in lint.All against api.
func All(api *model.API) ([]RuleFindings, error) {
	var out []RuleFindings
	for _, rule := range lint.All {
		findings, err := rule.Check(api)
		if err != nil {
			return nil, fmt.Errorf("rule %s: %w", rule.Name, err)
		}
		out = append(out, RuleFindings{Rule: rule, Findings: findings})
	}
	return out, nil
}

// AsExemptions flattens rf into the exemption identity of each finding: the
// rule that raised it, plus the finding's subject and message.
func AsExemptions(rf []RuleFindings) []exemption.Exemption {
	var out []exemption.Exemption
	for _, r := range rf {
		for _, f := range r.Findings {
			out = append(out, exemption.Exemption{Rule: r.Rule.Name, Subject: f.Subject, Message: f.Message})
		}
	}
	return out
}

// DefaultExemptionsPath is exemptions.json's default location: next to
// go.mod at the repo root, resolved from the repo root so it's found
// regardless of the caller's working directory.
func DefaultExemptionsPath() (string, error) {
	root, err := repoRoot()
	if err != nil {
		return "", err
	}
	return root + "/tools/apitool/exemptions.json", nil
}

const rootModule = "module github.com/agent-substrate/substrate"

// repoRoot walks up from the working directory to find the repo root, i.e.
// the directory containing the go.mod declaring rootModule.
func repoRoot() (string, error) {
	start, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("while getting working directory: %w", err)
	}
	for dir := start; ; {
		goMod, err := os.ReadFile(filepath.Join(dir, "go.mod"))
		if err == nil {
			first, _, _ := strings.Cut(string(goMod), "\n")
			if strings.TrimSpace(first) == rootModule {
				return dir, nil
			}
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("could not find a go.mod declaring %q above %s", rootModule, start)
		}
		dir = parent
	}
}
