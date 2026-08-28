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

// Package demos holds the contract every bundled demo implements, the registry
// the command tree is built from, and the shared behavior of a demo that is one
// manifest template.
//
// Each demo lives in its own package under this one and registers itself from
// init, so adding a demo means adding a directory and one import rather than
// editing a shared switch, and no demo can reach into another's placeholders,
// namespaces, or template.
package demos

import (
	"context"
	"sort"

	"github.com/spf13/pflag"

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/config"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/steps"
)

// Demo is one installable example workload.
//
// The shell scripts expressed this with naming conventions: a name appended to
// ATE_DEMOS plus `${name}_deploy`, `${name}_delete`, `${name}_usage`, and
// `${name}_cmdline` functions discovered with `declare -F`. An interface makes
// the same contract explicit, and a demo that forgets a method no longer fails
// silently at runtime.
type Demo interface {
	// Name is the demo's registry name, e.g. "demo-counter".
	Name() string
	// Description is the one-line summary shown in help.
	Description() string
	// Flags registers demo-specific flags, if any.
	Flags(fs *pflag.FlagSet)
	// Deploy installs the demo.
	Deploy(ctx context.Context, e *steps.Env) error
	// Delete removes the demo. It must succeed on a cluster where the demo was
	// never installed, because DeleteAll runs every demo's Delete.
	Delete(ctx context.Context, e *steps.Env) error
}

// kindOnlyDemo is implemented by demos that cannot run on GKE.
type kindOnlyDemo interface {
	KindOnly() bool
}

// registry holds every demo, keyed by name.
var registry = map[string]Demo{}

// Register adds a demo to the registry. It panics on a duplicate name, which
// can only be a programming error.
func Register(d Demo) {
	if _, dup := registry[d.Name()]; dup {
		panic("duplicate demo registration: " + d.Name())
	}
	registry[d.Name()] = d
}

// All returns every registered demo, sorted by name.
func All() []Demo {
	list := make([]Demo, 0, len(registry))
	for _, d := range registry {
		list = append(list, d)
	}
	sort.Slice(list, func(i, j int) bool { return list[i].Name() < list[j].Name() })
	return list
}

// For returns the demos that apply to cfg, sorted by name.
//
// Kind-only demos are filtered out for non-Kind installs, mirroring how the
// autoscaled-workerpool demo only registered itself with the shell installer
// when ATE_INSTALL_KIND was true. DeleteAll iterates this, so a GKE teardown
// does not try to delete resources that could never have been installed.
func For(cfg *config.Config) []Demo {
	var list []Demo
	for _, d := range All() {
		if ko, ok := d.(kindOnlyDemo); ok && ko.KindOnly() && !cfg.Kind {
			continue
		}
		list = append(list, d)
	}
	return list
}

// Deleters returns the demos that apply to cfg as the narrower interface
// [steps.Env.DeleteAll] takes.
//
// The conversion lives here because the dependency only runs one way: the demos
// import steps for Env, so steps cannot name this package's Demo.
func Deleters(cfg *config.Config) []steps.Deleter {
	list := For(cfg)
	deleters := make([]steps.Deleter, 0, len(list))
	for _, d := range list {
		deleters = append(deleters, d)
	}
	return deleters
}
