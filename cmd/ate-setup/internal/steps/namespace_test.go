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

package steps

import (
	"strings"
	"testing"

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/config"
)

func TestEnvNamespace(t *testing.T) {
	tests := []struct {
		name string
		cfg  *config.Config
		want string
	}{
		{"falls back to the canonical namespace when unset", &config.Config{}, NamespaceAteSystem},
		{"uses the configured namespace", &config.Config{Namespace: "substrate-dev"}, "substrate-dev"},
		{"tolerates a nil config", nil, NamespaceAteSystem},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &Env{Cfg: tt.cfg}
			if got := e.Namespace(); got != tt.want {
				t.Errorf("Namespace() = %q, want %q", got, tt.want)
			}
		})
	}
}

// The checked-in manifests under manifests/ate-install/ name ate-system
// literally, so the steps that apply them must refuse any other namespace
// rather than scatter the install across two.
func TestRequireCanonicalNamespace(t *testing.T) {
	t.Run("permits the canonical namespace", func(t *testing.T) {
		e := &Env{Cfg: &config.Config{Namespace: NamespaceAteSystem}}
		if err := e.RequireCanonicalNamespace("deploy ate-system"); err != nil {
			t.Errorf("RequireCanonicalNamespace() = %v, want nil", err)
		}
	})

	t.Run("refuses a relocated namespace and names both the step and the value", func(t *testing.T) {
		e := &Env{Cfg: &config.Config{Namespace: "substrate-dev"}}
		err := e.RequireCanonicalNamespace("deploy ate-system")
		if err == nil {
			t.Fatal("RequireCanonicalNamespace() = nil, want an error")
		}
		for _, want := range []string{"deploy ate-system", "substrate-dev", NamespaceAteSystem} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("error %q does not mention %q", err, want)
			}
		}
	})
}
