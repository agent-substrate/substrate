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

	"go.ate.dev/substrate/api/ateapi/v1"
	"go.ate.dev/substrate/internal/proto/ateletpb"
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
	"go.ate.dev/substrate/tools/apitool/internal/exemption"
	"go.ate.dev/substrate/tools/apitool/internal/lint"
	"go.ate.dev/substrate/tools/apitool/internal/model"
)

// serviceName is the RPC service apitool validates: ateapi.proto's public,
// documented API surface.
const serviceName = "Control"

// API builds ateapi.proto's model.API and returns it scoped to Control -
// the one entry point both `apitool validate` and TestExemptions build
// from, so which service and proto file that is stays this package's own
// concern.
func API(ctx context.Context) (*model.API, error) {
	api, err := model.Build(ctx, ateapi.Source)
	if err != nil {
		return nil, fmt.Errorf("while building API model: %w", err)
	}
	return model.ScopeToService(api, serviceName), nil
}

// protoSources are the protos the lint.CrossProto rules run over: the
// public API and the two internal hops a request takes from it, to atelet
// and on to the worker.
var protoSources = []struct{ name, source string }{
	{name: "ateapi.proto", source: ateapi.Source},
	{name: "atelet.proto", source: ateletpb.Source},
	{name: "ateworker.proto", source: ateworkerpb.Source},
}

// Protos builds one model.API holding every message of protoSources,
// unscoped. None of them imports another, so each builds on its own.
func Protos(ctx context.Context) (*model.API, error) {
	all := &model.API{}
	for _, p := range protoSources {
		api, err := model.Build(ctx, p.source)
		if err != nil {
			return nil, fmt.Errorf("while building %s model: %w", p.name, err)
		}
		all.Services = append(all.Services, api.Services...)
		all.Messages = append(all.Messages, api.Messages...)
		all.Enums = append(all.Enums, api.Enums...)
	}
	return all, nil
}

// RuleFindings pairs one rule with the findings it raised against an API,
// preserving the order of the rule list it came from (e.g., lint.All or lint.CrossProto)
type RuleFindings struct {
	Rule     lint.Rule
	Findings []lint.Finding
}

// All runs every rule in lint.All against api.
func All(api *model.API) ([]RuleFindings, error) {
	return run(lint.All, api)
}

// CrossProto runs every rule in lint.CrossProto against protos. Its findings
// cannot be exempted.
func CrossProto(protos *model.API) ([]RuleFindings, error) {
	return run(lint.CrossProto, protos)
}

func run(rules []lint.Rule, api *model.API) ([]RuleFindings, error) {
	var out []RuleFindings
	for _, rule := range rules {
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

const rootModule = "module go.ate.dev/substrate"

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
