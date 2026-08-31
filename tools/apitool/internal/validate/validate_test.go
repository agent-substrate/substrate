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

package validate_test

import (
	"testing"

	"github.com/agent-substrate/substrate/tools/apitool/internal/exemption"
	"github.com/agent-substrate/substrate/tools/apitool/internal/validate"
)

func TestExemptions(t *testing.T) {
	api, err := validate.API(t.Context())
	if err != nil {
		t.Fatalf("validate.API() error = %v", err)
	}

	results, err := validate.All(api)
	if err != nil {
		t.Fatalf("validate.All() error = %v", err)
	}
	current := validate.AsExemptions(results)

	path, err := validate.DefaultExemptionsPath()
	if err != nil {
		t.Fatalf("DefaultExemptionsPath() error = %v", err)
	}
	want, err := exemption.Load(path)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	missing, stale := exemption.Diff(current, want)
	for _, f := range missing {
		t.Errorf("finding has no exemption in %s: [%s] %s: %s", path, f.Rule, f.Subject, f.Message)
	}
	for _, e := range stale {
		t.Errorf("exemption no longer matches any finding, remove it from %s: [%s] %s: %s", path, e.Rule, e.Subject, e.Message)
	}
	if len(missing) > 0 || len(stale) > 0 {
		t.Logf("run `apitool validate --update` (from tools/apitool) to regenerate %s", path)
	}
}
